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
// register. Each resource type also keeps one index register. Its entry
// for a name records the object's sequence and the index sequence at
// which the index recorded it. That index sequence is the object's
// resourceVersion. Create, update, and delete write the object first and
// the index second. Get and LIST read the index and then each object at
// the sequence the index recorded, with no scan primitive.
type fleetStore struct {
	kv       *mvcc.KV
	sessions *lease.Sessions
	locks    *lease.Locks

	// afterRead, if set, runs after update or delete reads the index entry
	// and before it writes. Tests use it to land a concurrent write in
	// that gap.
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

// entry reads the index entry of name. It returns errNotFound when the
// index does not name it.
func (s *fleetStore) entry(ctx context.Context, resource, name string) (storage.Entry, error) {
	idx, err := storage.ReadIndex(ctx, s.kv, resource)
	if err != nil {
		return storage.Entry{}, err
	}
	e, ok := idx.Entries[name]
	if !ok {
		return storage.Entry{}, errNotFound
	}
	return e, nil
}

// at reads the object version that the entry e records.
func (s *fleetStore) at(ctx context.Context, resource, name string, e storage.Entry) ([]byte, error) {
	v, found, err := s.kv.GetAt(ctx, objectKey(resource, name), e.Obj)
	if err != nil {
		return nil, err
	}
	if !found || v.Tombstone {
		return nil, fmt.Errorf("apiserver: %s %q has no live version at object sequence %d", resource, name, e.Obj)
	}
	return v.Value, nil
}

// get returns the raw object bytes and their resourceVersion, the index
// sequence of the object's index entry. An object that the index does
// not record yet is not found.
func (s *fleetStore) get(ctx context.Context, resource, name string) ([]byte, uint64, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A get MUST serve an object only once the index register records it.
	e, err := s.entry(ctx, resource, name)
	if err != nil {
		return nil, 0, err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A get MUST read the object at the object register sequence that the index entry records.
	raw, err := s.at(ctx, resource, name, e)
	if err != nil {
		return nil, 0, err
	}
	return raw, e.Idx, nil
}

// settle writes the index after an object write of raw at object sequence
// seq. It returns the object that the index records and its
// resourceVersion. When a newer write landed first and the index records
// that one, it returns the newer object, so the bytes and the
// resourceVersion always agree.
func (s *fleetStore) settle(ctx context.Context, resource, name string, seq uint64, raw []byte) ([]byte, uint64, error) {
	e, live, err := storage.WriteIndex(ctx, s.kv, resource, name)
	if err != nil {
		return nil, 0, err
	}
	if live && e.Obj != seq {
		if raw, err = s.at(ctx, resource, name, e); err != nil {
			return nil, 0, err
		}
	}
	return raw, e.Idx, nil
}

// create commits raw as a new object (fails if one exists) and records the
// new sequence in the type index. It returns the stored object and its
// resourceVersion.
func (s *fleetStore) create(ctx context.Context, resource, name string, raw []byte) ([]byte, uint64, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A create MUST use a compare-and-set that requires the object register to be absent.
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	v, err := s.kv.CAS(ctx, objectKey(resource, name), nil, raw)
	if errors.Is(err, caspaxos.ErrConflict) {
		// The live object may not be in the index yet. Record it, so a
		// get agrees with this answer.
		if _, _, err := storage.WriteIndex(ctx, s.kv, resource, name); err != nil {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: %s %q already exists", errConflict, resource, name)
	}
	if err != nil {
		return nil, 0, err
	}
	return s.settle(ctx, resource, name, v.Seq, raw)
}

// update commits raw over the version the client read, then records the
// new sequence in the type index. expectRV is an index sequence. It must
// match the index entry, and the object write compares and sets on the
// object sequence that the entry records. It returns the stored object
// and its resourceVersion.
func (s *fleetStore) update(ctx context.Context, resource, name string, raw []byte, expectRV uint64) ([]byte, uint64, error) {
	e, err := s.entry(ctx, resource, name)
	if err != nil {
		return nil, 0, err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A resourceVersion precondition MUST be checked against the index sequence of the object's index entry.
	//= docs/spec/fleet.md#3-storage-model
	//# An update whose compare-and-set fails MUST return a conflict.
	if e.Idx != expectRV {
		return nil, 0, fmt.Errorf("%w: resourceVersion %d is stale (current %d)", errConflict, expectRV, e.Idx)
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	// Compare the object sequence, not the value. A value can change and
	// change back, but a sequence never repeats.
	//= docs/spec/fleet.md#3-storage-model
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	v, err := s.kv.CASSeq(ctx, objectKey(resource, name), e.Obj, raw)
	// An accept that reached only a minority gives an unknown outcome.
	// mvcc retries it and finds its own OpID if the write landed, but the
	// retry budget can run out. The caller then sees an error for a write
	// that may have landed.
	//= docs/spec/fleet.md#3-storage-model
	//# A write that returned a conflict MAY have been committed.
	if errors.Is(err, caspaxos.ErrConflict) {
		// The register holds a write that the entry does not record.
		// Catch the index up, so the caller's re-read sees it.
		if _, _, err := storage.WriteIndex(ctx, s.kv, resource, name); err != nil {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: concurrent update of %s %q", errConflict, resource, name)
	}
	if err != nil {
		return nil, 0, err
	}
	return s.settle(ctx, resource, name, v.Seq, raw)
}

// delete tombstones the version that the index entry records, then
// removes the name from the type index. A write that lands between the
// read and the tombstone makes the delete return errConflict. The caller
// re-reads and decides again.
func (s *fleetStore) delete(ctx context.Context, resource, name string) error {
	e, err := s.entry(ctx, resource, name)
	if err != nil {
		return err
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A delete MUST tombstone the object register before it removes the name from the index register.
	_, err = s.kv.DeleteSeq(ctx, objectKey(resource, name), e.Obj)
	//= docs/spec/fleet.md#3-storage-model
	//# A write that returned a conflict MAY have been committed.
	if errors.Is(err, caspaxos.ErrConflict) {
		if _, _, err := storage.WriteIndex(ctx, s.kv, resource, name); err != nil {
			return err
		}
		return fmt.Errorf("%w: concurrent write to %s %q", errConflict, resource, name)
	}
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	_, _, err = storage.WriteIndex(ctx, s.kv, resource, name)
	return err
}

// list returns every object that the index names, with its raw bytes and
// its resourceVersion, the index sequence of its entry, in name order. It
// reads each object at the sequence the index recorded, not at its head.
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
		e := idx.Entries[name]
		raw, err := s.at(ctx, resource, name, e)
		if err != nil {
			return nil, nil, nil, err
		}
		names = append(names, name)
		raws = append(raws, raw)
		rvs = append(rvs, e.Idx)
	}
	return names, raws, rvs, nil
}

// formatRV / parseRV translate between wire resourceVersions and index
// sequences.
func formatRV(seq uint64) string { return strconv.FormatUint(seq, 10) }

func parseRV(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: resourceVersion required for update", errConflict)
	}
	return strconv.ParseUint(s, 10, 64)
}
