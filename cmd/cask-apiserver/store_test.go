package main

import (
	"context"
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

	rv, err := s.create(ctx, "devices", "gpu-0", []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	rv, err = s.update(ctx, "devices", "gpu-0", []byte(`{"v":2}`), rv)
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

	// Delete removes the name from the index.
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
