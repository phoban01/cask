package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
)

// fleetStore keeps API objects in cask registers. Every object is one MVCC
// register (per-object linearizability, resourceVersion = per-key Seq). Each
// resource type also keeps one index register that maps every name to the
// object's latest sequence. Create, update, and delete write the object
// first and the index second. LIST reads the index and then each object at
// the sequence the index recorded, with no scan primitive.
type fleetStore struct {
	kv       *mvcc.KV
	sessions *lease.Sessions
	locks    *lease.Locks

	// afterRead, if set, runs after update or delete reads the object and
	// before it writes. Tests use it to land a concurrent write in that gap.
	afterRead func()
}

// errConflict maps to HTTP 409 (stale resourceVersion or failed create-only).
var errConflict = errors.New("apiserver: conflict")

// errNotFound maps to HTTP 404.
var errNotFound = errors.New("apiserver: not found")

// objectKey names an object's register. Duvet citations live inside
// function bodies: gofmt rewrites "//=" in doc comments to "// =".
func objectKey(resource, name string) []byte {
	//= docs/spec/fleet.md#3-storage-model
	//# Each object MUST be stored in one cask register keyed by resource type and name.
	return storage.ObjectKey(resource, name)
}

// deviceLockName is the cask lock whose holder IS the device's global lease.
func deviceLockName(device string) string { return "device/" + device }

// claimSessionID is the lease session backing one claim's holdership.
func claimSessionID(cluster, claim string) string {
	return fmt.Sprintf("claim/%s/%s", cluster, claim)
}

// get returns the raw object bytes and their resourceVersion (per-key Seq).
func (s *fleetStore) get(ctx context.Context, resource, name string) ([]byte, uint64, error) {
	chain, err := s.kv.History(ctx, objectKey(resource, name))
	if err != nil {
		return nil, 0, err
	}
	n := len(chain.Versions)
	if n == 0 || chain.Versions[n-1].Tombstone {
		return nil, 0, errNotFound
	}
	head := chain.Versions[n-1]
	return head.Value, head.Seq, nil
}

// create commits raw as a new object (fails if one exists) and records the
// new sequence in the type index. Returns the new resourceVersion.
func (s *fleetStore) create(ctx context.Context, resource, name string, raw []byte) (uint64, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A create MUST use a compare-and-set that requires the object register to be absent.
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	v, err := s.kv.CAS(ctx, objectKey(resource, name), nil, raw)
	if errors.Is(err, caspaxos.ErrConflict) {
		return 0, fmt.Errorf("%w: %s %q already exists", errConflict, resource, name)
	}
	if err != nil {
		return 0, err
	}
	if err := storage.WriteIndex(ctx, s.kv, resource, name); err != nil {
		return 0, err
	}
	return v.Seq, nil
}

// update commits raw over the version the client read (optimistic
// concurrency: k8s resourceVersion semantics ARE per-key CAS), then records
// the new sequence in the type index.
func (s *fleetStore) update(ctx context.Context, resource, name string, raw []byte, expectRV uint64) (uint64, error) {
	_, rv, err := s.get(ctx, resource, name)
	if err != nil {
		return 0, err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# An update whose compare-and-set fails MUST return a conflict.
	if rv != expectRV {
		return 0, fmt.Errorf("%w: resourceVersion %d is stale (current %d)", errConflict, expectRV, rv)
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	// Compare the sequence, not the value. A value can change and change
	// back, but a sequence never repeats.
	//= docs/spec/fleet.md#3-storage-model
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	v, err := s.kv.CASSeq(ctx, objectKey(resource, name), expectRV, raw)
	// An accept that reached only a minority gives an unknown outcome.
	// mvcc retries it and finds its own OpID if the write landed, but the
	// retry budget can run out. The caller then sees an error for a write
	// that may have landed.
	//= docs/spec/fleet.md#3-storage-model
	//# A write that returned a conflict MAY have been committed.
	if errors.Is(err, caspaxos.ErrConflict) {
		return 0, fmt.Errorf("%w: concurrent update of %s %q", errConflict, resource, name)
	}
	if err != nil {
		return 0, err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	if err := storage.WriteIndex(ctx, s.kv, resource, name); err != nil {
		return 0, err
	}
	return v.Seq, nil
}

// delete tombstones the version it read, then removes the name from the
// type index. A write that lands between the read and the tombstone makes
// the delete return errConflict. The caller re-reads and decides again.
func (s *fleetStore) delete(ctx context.Context, resource, name string) error {
	_, rv, err := s.get(ctx, resource, name)
	if err != nil {
		return err
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A delete MUST tombstone the object register before it removes the name from the index register.
	_, err = s.kv.DeleteSeq(ctx, objectKey(resource, name), rv)
	//= docs/spec/fleet.md#3-storage-model
	//# A write that returned a conflict MAY have been committed.
	if errors.Is(err, caspaxos.ErrConflict) {
		return fmt.Errorf("%w: concurrent write to %s %q", errConflict, resource, name)
	}
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	return storage.WriteIndex(ctx, s.kv, resource, name)
}

// list returns every object that the index names, with its raw bytes and
// the sequence the index recorded, in name order. It reads each object at
// that recorded sequence, not at its head.
func (s *fleetStore) list(ctx context.Context, resource string) (names []string, raws [][]byte, rvs []uint64, err error) {
	idx, err := storage.ReadIndex(ctx, s.kv, resource)
	if err != nil {
		return nil, nil, nil, err
	}
	all := make([]string, 0, len(idx.Entries))
	for name := range idx.Entries {
		all = append(all, name)
	}
	sort.Strings(all)
	for _, name := range all {
		seq := idx.Entries[name]
		v, found, gerr := s.kv.GetAt(ctx, objectKey(resource, name), seq)
		if gerr != nil {
			return nil, nil, nil, gerr
		}
		if !found || v.Tombstone {
			return nil, nil, nil, fmt.Errorf("apiserver: %s %q has no live version at index sequence %d", resource, name, seq)
		}
		names = append(names, name)
		raws = append(raws, v.Value)
		rvs = append(rvs, seq)
	}
	return names, raws, rvs, nil
}

// formatRV / parseRV translate between wire resourceVersions and Seq.
func formatRV(seq uint64) string { return strconv.FormatUint(seq, 10) }

func parseRV(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: resourceVersion required for update", errConflict)
	}
	return strconv.ParseUint(s, 10, 64)
}
