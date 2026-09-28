package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
)

func newTestFleetStore(t *testing.T) *fleetStore {
	t.Helper()
	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var now atomic.Int64
	return &fleetStore{kv: mvcc.New(caspaxos.NewProposer(1, acceptors), hlc.New(func() int64 { return now.Add(1) }), 1)}
}

func TestListReturnsRecordedSequences(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	ctx := context.Background()
	s := newTestFleetStore(t)

	_, rv, err := s.create(ctx, "devices", "gpu-0", []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	_, rv, err = s.update(ctx, "devices", "gpu-0", []byte(`{"v":2}`), rv)
	if err != nil {
		t.Fatal(err)
	}
	names, raws, rvs, err := s.list(ctx, "devices")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || rvs[0] != rv || string(raws[0]) != `{"v":2}` {
		t.Fatalf("list = %v %q %v, want gpu-0 at %d", names, raws, rvs, rv)
	}

	// An object write whose index write was lost: the list still serves
	// the object at the sequence the index recorded.
	if _, err := s.kv.Put(ctx, objectKey("devices", "gpu-0"), []byte(`{"v":3}`)); err != nil {
		t.Fatal(err)
	}
	_, raws, rvs, err = s.list(ctx, "devices")
	if err != nil {
		t.Fatal(err)
	}
	if rvs[0] != rv || string(raws[0]) != `{"v":2}` {
		t.Fatalf("list = %q at %d, want the recorded version %d", raws[0], rvs[0], rv)
	}

	// The delete compares on the recorded version, so the unindexed write
	// makes it conflict. The conflict catches the index up, and the next
	// delete removes the name from the index.
	if err := s.delete(ctx, "devices", "gpu-0"); !errors.Is(err, errConflict) {
		t.Fatalf("delete over an unindexed write: err = %v, want conflict", err)
	}
	if err := s.delete(ctx, "devices", "gpu-0"); err != nil {
		t.Fatal(err)
	}
	idx, err := storage.ReadIndex(ctx, s.kv, "devices")
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Entries) != 0 {
		t.Fatalf("index after delete = %v, want empty", idx.Entries)
	}
}

func TestResourceVersionIsIndexSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# An object's resourceVersion MUST be the index sequence at which the index register recorded that object version.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A resourceVersion precondition MUST be checked against the index sequence of the object's index entry.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A get MUST serve an object only once the index register records it.
	ctx := context.Background()
	s := newTestFleetStore(t)

	// gpu-0 is at index sequence 1 and gpu-1 at 2. Both objects are at
	// object sequence 1.
	if _, rv, err := s.create(ctx, "devices", "gpu-0", []byte(`{"v":1}`)); err != nil || rv != 1 {
		t.Fatalf("create gpu-0: rv=%d err=%v, want 1", rv, err)
	}
	if _, rv, err := s.create(ctx, "devices", "gpu-1", []byte(`{"v":1}`)); err != nil || rv != 2 {
		t.Fatalf("create gpu-1: rv=%d err=%v, want 2", rv, err)
	}
	if _, rv, err := s.get(ctx, "devices", "gpu-1"); err != nil || rv != 2 {
		t.Fatalf("get gpu-1: rv=%d err=%v, want 2", rv, err)
	}

	// The object sequence is not a valid precondition.
	if _, _, err := s.update(ctx, "devices", "gpu-1", []byte(`{"v":2}`), 1); !errors.Is(err, errConflict) {
		t.Fatalf("update at object sequence: err=%v, want conflict", err)
	}
	_, rv, err := s.update(ctx, "devices", "gpu-1", []byte(`{"v":2}`), 2)
	if err != nil || rv != 3 {
		t.Fatalf("update at index sequence: rv=%d err=%v, want 3", rv, err)
	}

	// An object write that the index does not record is not served. An
	// update on the recorded version conflicts and catches the index up.
	if _, err := s.kv.Put(ctx, objectKey("devices", "gpu-1"), []byte(`{"v":3}`)); err != nil {
		t.Fatal(err)
	}
	if raw, got, err := s.get(ctx, "devices", "gpu-1"); err != nil || got != 3 || string(raw) != `{"v":2}` {
		t.Fatalf("get = %q at %d, err=%v, want the recorded version at 3", raw, got, err)
	}
	if _, _, err := s.update(ctx, "devices", "gpu-1", []byte(`{"v":4}`), 3); !errors.Is(err, errConflict) {
		t.Fatalf("update over an unindexed write: err=%v, want conflict", err)
	}
	if raw, got, err := s.get(ctx, "devices", "gpu-1"); err != nil || got != 4 || string(raw) != `{"v":3}` {
		t.Fatalf("get after repair = %q at %d, err=%v, want v3 at 4", raw, got, err)
	}

	// An object that the index does not name is not found.
	if _, err := s.kv.Put(ctx, objectKey("devices", "gpu-2"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.get(ctx, "devices", "gpu-2"); !errors.Is(err, errNotFound) {
		t.Fatalf("get unindexed: err=%v, want not found", err)
	}
}
