package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mvcc"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// MarkerKey names the register that marks a completed import. Import
// writes it after every object and every index entry.
var MarkerKey = []byte("fleet/.import-complete")

// Marker is the value of the marker register.
type Marker struct {
	Format   string `json:"format"`
	Group    string `json:"group"`
	Revision uint64 `json:"revision"`
	Objects  int    `json:"objects"`
}

// ErrConflict means that cask already holds an object with the same name
// as an object in the file, and the two differ. Import writes nothing
// when it finds one before its first write.
var ErrConflict = errors.New("migrate: cask holds a different object with the same name")

// Result counts what Import did.
type Result struct {
	// Written is the number of objects that Import wrote.
	Written int
	// Unchanged is the number of objects that cask already held, equal
	// to the file.
	Unchanged int
}

// item is one object of the file with its storage form.
type item struct {
	resource string
	name     string
	uid      string
	// raw is the stored form: the object with no resourceVersion.
	raw []byte
	// obj is raw decoded, for comparison with a stored object.
	obj map[string]any
}

// Import writes the objects of an export file into cask through kv. h and
// objects are what Read returned.
//
// Each object goes into its own object register, exactly as the file has
// it, with no metadata.resourceVersion. Then its index entry is written.
// The index sequence of that entry is the new resourceVersion. After all
// objects, Import writes the marker register.
//
// Import checks the whole file and every existing register before its
// first write. It writes nothing when an object is invalid, or when cask
// holds a different object with the same name.
//
// Import is idempotent. An object that cask already holds, equal to the
// file, is not written again. Its index entry is written only when a
// crash left it out. A second run with the same file changes nothing.
func Import(ctx context.Context, kv *mvcc.KV, h Header, objects []*unstructured.Unstructured) (Result, error) {
	items, err := prepare(h, objects)
	if err != nil {
		return Result{}, err
	}
	marker, err := json.Marshal(Marker{Format: h.Format, Group: h.Group, Revision: h.Revision, Objects: len(items)})
	if err != nil {
		return Result{}, err
	}
	cur, found, err := kv.Get(ctx, MarkerKey)
	if err != nil {
		return Result{}, err
	}
	if found && !bytes.Equal(cur, marker) {
		return Result{}, fmt.Errorf("%w: cask already holds the import %s", ErrConflict, cur)
	}

	// Check every object before the first write. A conflict found here
	// leaves cask unchanged.
	var errs []error
	for _, it := range items {
		if _, _, err := check(ctx, kv, it); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return Result{}, errors.Join(errs...)
	}

	var res Result
	for _, it := range items {
		wrote, err := put(ctx, kv, it)
		if err != nil {
			return res, err
		}
		if wrote {
			res.Written++
		} else {
			res.Unchanged++
		}
	}
	// The marker goes last, so it is present only when every object and
	// every index entry is.
	if !found {
		if _, err := kv.CreateAt(ctx, MarkerKey, 0, marker); err != nil {
			return res, fmt.Errorf("migrate: write the import marker: %w", err)
		}
	}
	return res, nil
}

// prepare checks every object of the file and builds its stored form. It
// maps each object to its resource by the header counts, which Read
// checked.
func prepare(h Header, objects []*unstructured.Unstructured) ([]item, error) {
	if h.Format != Format {
		return nil, fmt.Errorf("migrate: format %q is not %q", h.Format, Format)
	}
	total := 0
	for _, rh := range h.Resources {
		total += rh.Count
	}
	if total != len(objects) {
		return nil, fmt.Errorf("migrate: the header counts %d objects, got %d", total, len(objects))
	}
	var (
		items []item
		errs  []error
		i     int
	)
	seen := map[string]bool{}
	for _, rh := range h.Resources {
		for range rh.Count {
			o := objects[i]
			i++
			it, err := prepareOne(rh.Resource, o)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if key := string(storage.ObjectKey(it.resource, it.name)); seen[key] {
				errs = append(errs, fmt.Errorf("migrate: %s %q appears twice", it.resource, it.name))
			} else {
				seen[key] = true
			}
			items = append(items, it)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return items, nil
}

func prepareOne(resource string, o *unstructured.Unstructured) (item, error) {
	name := o.GetName()
	if name == "" {
		return item{}, fmt.Errorf("migrate: a %s object has no name", resource)
	}
	if o.GetNamespace() != "" {
		return item{}, fmt.Errorf("migrate: %s %q has a namespace; every fleet resource is cluster-scoped", resource, name)
	}
	// The stored object keeps the uid and the creationTimestamp of the
	// source. An object without them cannot keep them, so the import
	// stops before it writes.
	//= docs/spec/fleet.md#7-migration
	//# The migration MUST preserve each object's uid.
	uid := string(o.GetUID())
	if uid == "" {
		return item{}, fmt.Errorf("migrate: %s %q has no uid", resource, name)
	}
	//= docs/spec/fleet.md#7-migration
	//# The migration MUST preserve each object's creationTimestamp.
	if ts := o.GetCreationTimestamp(); ts.IsZero() {
		return item{}, fmt.Errorf("migrate: %s %q has no creationTimestamp", resource, name)
	}
	// The source resourceVersion is an etcd revision. Cask serves the
	// index sequence as the resourceVersion, so the stored bytes carry
	// none. Every other field stays as the file has it.
	c := o.DeepCopy()
	unstructured.RemoveNestedField(c.Object, "metadata", "resourceVersion")
	raw, err := json.Marshal(c.Object)
	if err != nil {
		return item{}, fmt.Errorf("migrate: encode %s %q: %w", resource, name, err)
	}
	obj, err := decodeObject(raw)
	if err != nil {
		return item{}, fmt.Errorf("migrate: decode %s %q: %w", resource, name, err)
	}
	return item{resource: resource, name: name, uid: uid, raw: raw, obj: obj}, nil
}

func decodeObject(raw []byte) (map[string]any, error) {
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	unstructured.RemoveNestedField(u.Object, "metadata", "resourceVersion")
	return u.Object, nil
}

// check reads the object register of it. It returns the head sequence
// that a create must pass to CreateAt, or live true when the register
// holds a live object equal to it. It returns ErrConflict when the
// register holds a live object that differs.
func check(ctx context.Context, kv *mvcc.KV, it item) (head uint64, live bool, err error) {
	chain, err := kv.History(ctx, storage.ObjectKey(it.resource, it.name))
	if err != nil {
		return 0, false, err
	}
	n := len(chain.Versions)
	if n == 0 {
		return 0, false, nil
	}
	v := chain.Versions[n-1]
	if !v.Live() {
		return v.Seq, false, nil
	}
	got, err := decodeObject(v.Value)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %s %q: decode the stored object: %v", ErrConflict, it.resource, it.name, err)
	}
	gotUID, _, _ := unstructured.NestedString(got, "metadata", "uid")
	if gotUID != it.uid {
		return 0, false, fmt.Errorf("%w: %s %q has uid %q in cask and %q in the file",
			ErrConflict, it.resource, it.name, gotUID, it.uid)
	}
	if !reflect.DeepEqual(got, it.obj) {
		return 0, false, fmt.Errorf("%w: %s %q has the same uid but other content in cask",
			ErrConflict, it.resource, it.name)
	}
	return v.Seq, true, nil
}

// put writes one object and its index entry. It reports whether it wrote
// the object register.
func put(ctx context.Context, kv *mvcc.KV, it item) (bool, error) {
	reg := storage.ObjectKey(it.resource, it.name)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		head, live, err := check(ctx, kv, it)
		if err != nil {
			return false, err
		}
		wrote := false
		if !live {
			// A create over a tombstone first makes the index record the
			// removal, as a create through the API server does.
			seq, exists, err := storage.PrepareCreate(ctx, kv, it.resource, it.name)
			if err != nil {
				return false, err
			}
			if exists || seq != head {
				// Another writer changed the register. Check it again.
				continue
			}
			_, err = kv.CreateAt(ctx, reg, seq, it.raw)
			if errors.Is(err, caspaxos.ErrConflict) {
				continue
			}
			if err != nil {
				return false, fmt.Errorf("migrate: write %s %q: %w", it.resource, it.name, err)
			}
			wrote = true
		}
		// The object register comes first. The index entry then records
		// it, and its index sequence is the new resourceVersion. For an
		// object already in cask, this repairs an index write that a
		// crash lost, and does nothing otherwise.
		//= docs/spec/fleet.md#3-storage-model
		//# A mutation MUST write the object register before the index register.
		if _, _, err := storage.WriteIndex(ctx, kv, it.resource, it.name); err != nil {
			return wrote, fmt.Errorf("migrate: index %s %q: %w", it.resource, it.name, err)
		}
		return wrote, nil
	}
}
