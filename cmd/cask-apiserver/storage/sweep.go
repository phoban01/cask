package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/phoban01/cask/internal/mvcc"
)

// KeyLister lists register keys. The lister that Sweep uses must return
// the union of the keys held on a majority of the acceptors. A committed
// register is on a majority, and two majorities share an acceptor, so
// that union names every committed register. The keys of one acceptor
// are not enough: a lagging acceptor can miss a committed register.
type KeyLister func(ctx context.Context) ([][]byte, error)

// Sweep repairs the index register of resource. A crash between the
// object write and the index write of a mutation leaves the index behind
// the object. Sweep finds those objects and writes their index entries.
//
// Sweep reads the index, then lists the object keys with list. For each
// name that the index or the listing names, it reads the object head. If
// the index entry does not match the head, Sweep calls WriteIndex for
// that name. It returns the number of names it wrote.
//
// Sweep is safe while other writers and other sweeps run. It changes the
// index only through WriteIndex, which never moves an entry backwards and
// never records a sequence above the object head. An object created after
// the listing is not missed: its own index write records it.
func Sweep(ctx context.Context, kv *mvcc.KV, resource string, list KeyLister) (int, error) {
	idx, err := ReadIndex(ctx, kv, resource)
	if err != nil {
		return 0, err
	}
	keys, err := list(ctx)
	if err != nil {
		return 0, fmt.Errorf("cask storage: list %s keys: %w", resource, err)
	}
	names := make([]string, 0, len(idx.Entries)+len(keys))
	for name := range idx.Entries {
		names = append(names, name)
	}
	for _, k := range keys {
		if name, ok := objectName(resource, k); ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)

	var (
		wrote int
		errs  []error
	)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return wrote, err
		}
		seq, live, err := objectHead(ctx, kv, resource, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s %q: %w", resource, name, err))
			continue
		}
		entry, indexed := idx.Entries[name]
		if (live && indexed && entry == seq) || (!live && !indexed) {
			continue
		}
		if err := WriteIndex(ctx, kv, resource, name); err != nil {
			errs = append(errs, fmt.Errorf("%s %q: %w", resource, name, err))
			continue
		}
		wrote++
	}
	if len(errs) > 0 {
		return wrote, fmt.Errorf("cask storage: sweep %s: %w", resource, errors.Join(errs...))
	}
	return wrote, nil
}

// objectName returns the object name in an object register key of
// resource. It returns false for any other key, such as the index key or
// a key of another resource.
func objectName(resource string, key []byte) (string, bool) {
	name, ok := strings.CutPrefix(string(key), "fleet/"+resource+"/")
	// A Kubernetes object name has no slash.
	return name, ok && name != "" && !strings.Contains(name, "/")
}
