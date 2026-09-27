package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
)

// fleetStore keeps API objects in cask registers. Every object is one MVCC
// register (per-object linearizability, resourceVersion = per-key Seq); each
// resource type additionally keeps ONE index register holding the sorted
// name set, CAS-maintained on create/delete, so LIST is a linearizable read
// of index + members without any scan primitive.
type fleetStore struct {
	kv       *mvcc.KV
	sessions *lease.Sessions
	locks    *lease.Locks
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
	return fmt.Appendf(nil, "fleet/%s/%s", resource, name)
}

func indexKey(resource string) []byte {
	return fmt.Appendf(nil, "fleet/%s.index", resource)
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

// create commits raw as a new object (fails if one exists) and registers the
// name in the type index. Returns the new resourceVersion.
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
	if err := s.indexAdd(ctx, resource, name); err != nil {
		return 0, err
	}
	return v.Seq, nil
}

// update commits raw over the version the client read (optimistic
// concurrency: k8s resourceVersion semantics ARE per-key CAS).
func (s *fleetStore) update(ctx context.Context, resource, name string, raw []byte, expectRV uint64) (uint64, error) {
	cur, rv, err := s.get(ctx, resource, name)
	if err != nil {
		return 0, err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# An update whose compare-and-set fails MUST return a conflict.
	if rv != expectRV {
		return 0, fmt.Errorf("%w: resourceVersion %d is stale (current %d)", errConflict, expectRV, rv)
	}
	//= docs/spec/fleet.md#3-storage-model
	//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
	v, err := s.kv.CAS(ctx, objectKey(resource, name), cur, raw)
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
	return v.Seq, nil
}

func (s *fleetStore) delete(ctx context.Context, resource, name string) error {
	if _, _, err := s.get(ctx, resource, name); err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A delete MUST tombstone the object register before it removes the name from the index register.
	if _, err := s.kv.Delete(ctx, objectKey(resource, name)); err != nil {
		return err
	}
	return s.indexRemove(ctx, resource, name)
}

// list returns every live object's name, raw bytes, and RV — index-aligned —
// in name order.
func (s *fleetStore) list(ctx context.Context, resource string) (names []string, raws [][]byte, rvs []uint64, err error) {
	all, err := s.indexNames(ctx, resource)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, name := range all {
		raw, rv, gerr := s.get(ctx, resource, name)
		if errors.Is(gerr, errNotFound) {
			continue // deleted between index read and member read
		}
		if gerr != nil {
			return nil, nil, nil, gerr
		}
		names = append(names, name)
		raws = append(raws, raw)
		rvs = append(rvs, rv)
	}
	return names, raws, rvs, nil
}

// --- the type index register -----------------------------------------------

func (s *fleetStore) indexNames(ctx context.Context, resource string) ([]string, error) {
	raw, found, err := s.kv.Get(ctx, indexKey(resource))
	if err != nil || !found || len(raw) == 0 {
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return nil, fmt.Errorf("apiserver: decode %s index: %w", resource, err)
	}
	return names, nil
}

func (s *fleetStore) indexMutate(ctx context.Context, resource string, mutate func([]string) []string) error {
	for range 8 { // CAS retry against concurrent index writers
		cur, found, err := s.kv.Get(ctx, indexKey(resource))
		if err != nil {
			return err
		}
		var names []string
		if found && len(cur) > 0 {
			if err := json.Unmarshal(cur, &names); err != nil {
				return err
			}
		}
		next := mutate(names)
		sort.Strings(next)
		raw, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if bytes.Equal(cur, raw) {
			return nil
		}
		var expected []byte
		if found {
			expected = cur
		}
		if _, err := s.kv.CAS(ctx, indexKey(resource), expected, raw); err == nil {
			return nil
		} else if !errors.Is(err, caspaxos.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("apiserver: %s index contended out", resource)
}

func (s *fleetStore) indexAdd(ctx context.Context, resource, name string) error {
	return s.indexMutate(ctx, resource, func(names []string) []string {
		for _, n := range names {
			if n == name {
				return names
			}
		}
		return append(names, name)
	})
}

func (s *fleetStore) indexRemove(ctx context.Context, resource, name string) error {
	return s.indexMutate(ctx, resource, func(names []string) []string {
		out := names[:0:0]
		for _, n := range names {
			if n != name {
				out = append(out, n)
			}
		}
		return out
	})
}

// formatRV / parseRV translate between wire resourceVersions and Seq.
func formatRV(seq uint64) string { return strconv.FormatUint(seq, 10) }

func parseRV(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: resourceVersion required for update", errConflict)
	}
	return strconv.ParseUint(s, 10, 64)
}
