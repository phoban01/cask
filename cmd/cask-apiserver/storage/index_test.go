package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
)

// newCask returns an in-process cask: three in-memory acceptors. Each call
// of the returned function adds one proposer with its own node id, as one
// more cluster would.
func newCask(t *testing.T) func(node uint64) *mvcc.KV {
	t.Helper()
	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var now atomic.Int64
	clock := hlc.New(func() int64 { return now.Add(1) })
	return func(node uint64) *mvcc.KV {
		// The backoff matches production: without it, concurrent rounds
		// preempt each other until the round budget runs out.
		prop := caspaxos.NewProposer(node, acceptors,
			caspaxos.WithBackoff(backoff.FullJitter(time.Millisecond, 20*time.Millisecond)))
		return mvcc.New(prop, clock, node)
	}
}

// newKV returns one proposer over a new in-process cask.
func newKV(t *testing.T) *mvcc.KV {
	t.Helper()
	return newCask(t)(1)
}

func mustIndex(t *testing.T, kv *mvcc.KV, resource string) Index {
	t.Helper()
	idx, err := ReadIndex(context.Background(), kv, resource)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestWriteIndexRecordsObjectSequence(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	ctx := context.Background()
	kv := newKV(t)

	for i := range 3 {
		v, err := kv.Put(ctx, ObjectKey("devices", "gpu-0"), fmt.Appendf(nil, "v%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := WriteIndex(ctx, kv, "devices", "gpu-0"); err != nil {
			t.Fatal(err)
		}
		if got := mustIndex(t, kv, "devices").Entries["gpu-0"].Obj; got != v.Seq {
			t.Fatalf("write %d: index records %d, object sequence is %d", i, got, v.Seq)
		}
	}
	if _, err := kv.Put(ctx, ObjectKey("devices", "gpu-1"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := WriteIndex(ctx, kv, "devices", "gpu-1"); err != nil {
		t.Fatal(err)
	}
	idx := mustIndex(t, kv, "devices")
	if len(idx.Entries) != 2 || idx.Entries["gpu-0"] != (Entry{Obj: 3, Idx: 3}) || idx.Entries["gpu-1"] != (Entry{Obj: 1, Idx: 4}) {
		t.Fatalf("entries = %v, want gpu-0:{3 3} gpu-1:{1 4}", idx.Entries)
	}
	if idx.Seq != 4 {
		t.Fatalf("index sequence = %d, want 4 (one per index change)", idx.Seq)
	}

	// Tombstone first, then the index write removes the name.
	if _, err := kv.Delete(ctx, ObjectKey("devices", "gpu-0")); err != nil {
		t.Fatal(err)
	}
	e, live, err := WriteIndex(ctx, kv, "devices", "gpu-0")
	if err != nil {
		t.Fatal(err)
	}
	if live || e.Idx != 5 {
		t.Fatalf("removal returned %+v live=%v, want the removal at index sequence 5", e, live)
	}
	if _, ok := mustIndex(t, kv, "devices").Entries["gpu-0"]; ok {
		t.Fatal("index still names a tombstoned object")
	}

	// A second index write for the same state changes nothing and
	// reports the index sequence it read.
	if e, live, err := WriteIndex(ctx, kv, "devices", "gpu-0"); err != nil || live || e.Idx != 5 {
		t.Fatalf("repeat = %+v live=%v err=%v, want no change at 5", e, live, err)
	}
	if e, live, err := WriteIndex(ctx, kv, "devices", "gpu-1"); err != nil || !live || e != (Entry{Obj: 1, Idx: 4}) {
		t.Fatalf("repeat = %+v live=%v err=%v, want the entry {1 4}", e, live, err)
	}
	if seq := mustIndex(t, kv, "devices").Seq; seq != 5 {
		t.Fatalf("index sequence = %d after no-op writes, want 5", seq)
	}
}

func TestWriteIndexCatchesUpAfterLostWrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	ctx := context.Background()
	kv := newKV(t)
	key := ObjectKey("devices", "gpu-0")

	if _, err := kv.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := WriteIndex(ctx, kv, "devices", "gpu-0"); err != nil {
		t.Fatal(err)
	}
	// The second mutation writes the object and then crashes before its
	// index write.
	if _, err := kv.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if got := mustIndex(t, kv, "devices").Entries["gpu-0"].Obj; got != 1 {
		t.Fatalf("index records %d before catch-up, want 1", got)
	}
	// The third mutation's index write records the current sequence.
	v3, err := kv.Put(ctx, key, []byte("v3"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := WriteIndex(ctx, kv, "devices", "gpu-0"); err != nil {
		t.Fatal(err)
	}
	if got := mustIndex(t, kv, "devices").Entries["gpu-0"].Obj; got != v3.Seq {
		t.Fatalf("index records %d, want the current sequence %d", got, v3.Seq)
	}
}

func TestWriteIndexNeverAheadUnderConcurrentWriters(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The index register MUST NOT record a sequence higher than the object register holds.
	ctx := context.Background()
	kv := newKV(t)
	const writers, rounds = 4, 5

	// Writers race on two objects. Each mutation writes the object and
	// then the index, in that order.
	var wg sync.WaitGroup
	errs := make(chan error, writers*rounds)
	for w := range writers {
		wg.Go(func() {
			name := fmt.Sprintf("gpu-%d", w%2)
			for r := range rounds {
				if _, err := kv.Put(ctx, ObjectKey("devices", name), fmt.Appendf(nil, "w%d-r%d", w, r)); err != nil {
					errs <- err
					return
				}
				if _, _, err := WriteIndex(ctx, kv, "devices", name); err != nil {
					errs <- err
					return
				}
				// Check the rule while the race runs: read the index
				// first, then the object, so the object can only be newer.
				idx, err := ReadIndex(ctx, kv, "devices")
				if err != nil {
					errs <- err
					return
				}
				seq, _, err := objectHead(ctx, kv, "devices", name)
				if err != nil {
					errs <- err
					return
				}
				if idx.Entries[name].Obj > seq {
					errs <- fmt.Errorf("index records %d for %s, object holds %d", idx.Entries[name].Obj, name, seq)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// With every writer finished, each entry equals the object's head.
	idx := mustIndex(t, kv, "devices")
	for _, name := range []string{"gpu-0", "gpu-1"} {
		seq, _, err := objectHead(ctx, kv, "devices", name)
		if err != nil {
			t.Fatal(err)
		}
		if idx.Entries[name].Obj != seq {
			t.Fatalf("%s: index records %d, object holds %d", name, idx.Entries[name].Obj, seq)
		}
	}
}
