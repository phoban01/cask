package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mvcc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// storageCheckTimeout bounds the consensus read of one readiness check. It
// is below the one-second default timeout of a kubelet probe.
const storageCheckTimeout = 800 * time.Millisecond

// storageCheck is a readyz check. It passes only when a linearizable read
// of the Device index register completes, so the server reports not ready
// while it cannot reach a majority of the acceptors. The read proposes no
// change. It checks each request again, so readiness follows the storage
// both ways.
type storageCheck struct {
	kv      *mvcc.KV
	timeout time.Duration
}

func newStorageCheck(kv *mvcc.KV) storageCheck {
	return storageCheck{kv: kv, timeout: storageCheckTimeout}
}

// Name is the check name under /readyz.
func (storageCheck) Name() string { return "cask-storage" }

// Check reads the Device index register through consensus. ReadIndex
// retries a read that lost its round to another proposer.
func (c storageCheck) Check(r *http.Request) error {
	//= docs/spec/fleet.md#2-resources
	//# The extension server MUST report not ready until its storage is reachable.
	ctx, cancel := context.WithTimeout(r.Context(), c.timeout)
	defer cancel()
	if _, err := storage.ReadIndex(ctx, c.kv, "devices"); err != nil {
		return fmt.Errorf("cask storage is not reachable: %w", err)
	}
	return nil
}

// errImportPending means that the import marker is not in cask yet.
var errImportPending = errors.New("the import has not completed: no import marker")

// markerReadRetries bounds the retries of one marker read that loses its
// round.
const markerReadRetries = 8

// importCheck is the readyz check of --expect-import. It fails until the
// import marker is in cask, names the export format, the fleet group, and
// the expected source revision, and every owner that an object names in
// its ownerReferences exists. After the first pass it stays passed: a
// later delete of an owner is an ordinary delete and does not stop the
// server.
//
// Check does no reads. run verifies in the background, so a slow scan of
// a large import cannot make a probe time out.
type importCheck struct {
	kv     *mvcc.KV
	stores map[string]*storage.Store
	// revision is the source etcd revision that the marker must name.
	// Zero accepts any revision.
	revision uint64

	mu   sync.Mutex
	done bool
	last error
}

func newImportCheck(kv *mvcc.KV, stores map[string]*storage.Store, revision uint64) *importCheck {
	return &importCheck{kv: kv, stores: stores, revision: revision, last: errImportPending}
}

// Name is the check name under /readyz.
func (*importCheck) Name() string { return "cask-import" }

// Check returns the result of the last verification.
func (c *importCheck) Check(*http.Request) error {
	//= docs/spec/fleet.md#2-resources
	//# The extension server MUST report not ready until any pending migration import is complete.
	//= docs/spec/fleet.md#7-migration
	//# The APIService MUST NOT become available before the import has completed.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return nil
	}
	return c.last
}

// run verifies at each interval until a verification passes or ctx ends.
func (c *importCheck) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		err := c.verify(ctx)
		c.mu.Lock()
		c.done, c.last = err == nil, err
		c.mu.Unlock()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// markerRead is one read of the marker register.
type markerRead struct {
	value []byte
	found bool
}

// verify reads the marker, then checks the owners.
func (c *importCheck) verify(ctx context.Context) error {
	r, err := caspaxos.RetryLost(ctx, markerReadRetries, backoff.FullJitter(time.Millisecond, 50*time.Millisecond),
		func() (markerRead, error) {
			v, found, err := c.kv.Get(ctx, migrate.MarkerKey)
			return markerRead{value: v, found: found}, err
		})
	if err != nil {
		return fmt.Errorf("read the import marker: %w", err)
	}
	if !r.found {
		return errImportPending
	}
	var m migrate.Marker
	if err := json.Unmarshal(r.value, &m); err != nil {
		return fmt.Errorf("decode the import marker: %w", err)
	}
	switch {
	case m.Format != migrate.Format:
		return fmt.Errorf("the import marker has format %q, want %q", m.Format, migrate.Format)
	case m.Group != v1alpha1.GroupName:
		return fmt.Errorf("the import marker has group %q, want %q", m.Group, v1alpha1.GroupName)
	case c.revision != 0 && m.Revision != c.revision:
		return fmt.Errorf("the import marker has revision %d, want %d", m.Revision, c.revision)
	}
	return c.checkOwners(ctx)
}

// checkOwners lists every Device and DeviceClaim. It fails when an object
// names an owner in the fleet group that cask does not hold with the uid
// of the reference.
//
// The import writes the marker after its last object. So when the marker
// is present, cask holds every object of the export. Writers stay frozen
// until the APIService is available, so on the first pass the objects in
// cask are the imported set. An owner in another group is not an
// imported object, so the check skips it.
func (c *importCheck) checkOwners(ctx context.Context) error {
	//= docs/spec/fleet.md#7-migration
	//# The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.
	opts := apistorage.ListOptions{Recursive: true, Predicate: apistorage.Everything}
	var devices v1alpha1.DeviceList
	if err := c.stores["devices"].GetList(ctx, "/devices", opts, &devices); err != nil {
		return fmt.Errorf("list devices: %w", err)
	}
	var claims v1alpha1.DeviceClaimList
	if err := c.stores["deviceclaims"].GetList(ctx, "/deviceclaims", opts, &claims); err != nil {
		return fmt.Errorf("list deviceclaims: %w", err)
	}
	have := map[string]types.UID{}
	var objs []metav1.Object
	for i := range devices.Items {
		d := &devices.Items[i]
		have["Device/"+d.Name] = d.UID
		objs = append(objs, d)
	}
	for i := range claims.Items {
		cl := &claims.Items[i]
		have["DeviceClaim/"+cl.Name] = cl.UID
		objs = append(objs, cl)
	}
	var missing []string
	for _, o := range objs {
		for _, ref := range o.GetOwnerReferences() {
			gv, err := schema.ParseGroupVersion(ref.APIVersion)
			if err != nil || gv.Group != v1alpha1.GroupName {
				continue
			}
			if uid, ok := have[ref.Kind+"/"+ref.Name]; !ok || uid != ref.UID {
				missing = append(missing, fmt.Sprintf("%s %q (owner of %q)", ref.Kind, ref.Name, o.GetName()))
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("imported owners are missing: %s", strings.Join(missing, ", "))
	}
	return nil
}
