package mvcc_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"pgregory.net/rapid"
)

// cluster builds n acceptors and a monotonic test clock.
func cluster(n int) ([]caspaxos.AcceptorClient, *hlc.Clock) {
	acc := make([]caspaxos.AcceptorClient, n)
	for i := range acc {
		acc[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var tick int64
	clock := hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })
	return acc, clock
}

func TestPutGetDelete(t *testing.T) {
	ctx := context.Background()
	acc, clock := cluster(3)
	kv := mvcc.New(caspaxos.NewProposer(1, acc), clock, 1)
	key := []byte("k")

	if v, found, _ := mustGet(t, kv, key); found {
		t.Fatalf("expected absent, got %q", v)
	}
	if _, err := kv.Put(ctx, key, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if v, found, _ := mustGet(t, kv, key); !found || string(v) != "a" {
		t.Fatalf("get = %q,%v want a,true", v, found)
	}
	if _, err := kv.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := mustGet(t, kv, key); found {
		t.Fatal("expected absent after delete")
	}
}

func TestCASSemantics(t *testing.T) {
	ctx := context.Background()
	acc, clock := cluster(3)
	kv := mvcc.New(caspaxos.NewProposer(1, acc), clock, 1)
	key := []byte("k")

	// CAS from absent (expected nil) succeeds.
	if _, err := kv.CAS(ctx, key, nil, []byte("v1")); err != nil {
		t.Fatalf("cas absent->v1: %v", err)
	}
	// Wrong expectation fails with ErrConflict, leaves value intact.
	if _, err := kv.CAS(ctx, key, []byte("wrong"), []byte("v2")); !errors.Is(err, caspaxos.ErrConflict) {
		t.Fatalf("cas wrong = %v, want ErrConflict", err)
	}
	if v, _, _ := mustGet(t, kv, key); string(v) != "v1" {
		t.Fatalf("value mutated by failed CAS: %q", v)
	}
	// Correct expectation succeeds.
	if _, err := kv.CAS(ctx, key, []byte("v1"), []byte("v2")); err != nil {
		t.Fatalf("cas v1->v2: %v", err)
	}
	if v, _, _ := mustGet(t, kv, key); string(v) != "v2" {
		t.Fatalf("value = %q, want v2", v)
	}
}

func TestTimeTravelAndSnapshot(t *testing.T) {
	ctx := context.Background()
	acc, clock := cluster(3)
	kv := mvcc.New(caspaxos.NewProposer(1, acc), clock, 1)
	key := []byte("k")

	v1, _ := kv.Put(ctx, key, []byte("a"))
	v2, _ := kv.Put(ctx, key, []byte("b"))

	// GetAt by sequence.
	if got, ok, _ := kv.GetAt(ctx, key, v1.Seq); !ok || string(got.Value) != "a" {
		t.Fatalf("GetAt(seq1) = %q,%v want a,true", got.Value, ok)
	}
	// SnapshotAt v1.HLC sees "a"; at v2.HLC sees "b".
	if got, ok, _ := kv.SnapshotAt(ctx, key, v1.HLC); !ok || string(got.Value) != "a" {
		t.Fatalf("snapshot@v1 = %q,%v want a,true", got.Value, ok)
	}
	if got, ok, _ := kv.SnapshotAt(ctx, key, v2.HLC); !ok || string(got.Value) != "b" {
		t.Fatalf("snapshot@v2 = %q,%v want b,true", got.Value, ok)
	}
	// A snapshot strictly before the first version sees nothing.
	if _, ok, _ := kv.SnapshotAt(ctx, key, hlc.Timestamp{}); ok {
		t.Fatal("snapshot before first version should be empty")
	}
}

// Model-based property test: a random sequence of operations from multiple
// proposers against one key must behave as a single linearizable register, with
// strictly increasing sequence numbers and HLC timestamps. This is the
// executable echo of the M0 TLA+ invariants (agreement + monotonicity).
func TestRegisterModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		rf := rapid.SampledFrom([]int{3, 5}).Draw(t, "rf")
		acc, clock := cluster(rf)

		nProposers := rapid.IntRange(1, 3).Draw(t, "proposers")
		kvs := make([]*mvcc.KV, nProposers)
		for i := range kvs {
			kvs[i] = mvcc.New(caspaxos.NewProposer(uint64(i+1), acc), clock, uint64(i+1))
		}
		key := []byte("k")

		// Reference model.
		var (
			model   []byte
			present bool
			lastSeq uint64
			lastHLC hlc.Timestamp
			haveHLC bool
			values  = rapid.SampledFrom([]string{"x", "y", "z", "w"})
		)

		ops := rapid.IntRange(1, 60).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			kv := kvs[rapid.IntRange(0, nProposers-1).Draw(t, "who")]
			switch rapid.IntRange(0, 3).Draw(t, "kind") {
			case 0: // Put
				val := []byte(values.Draw(t, "put"))
				v, err := kv.Put(ctx, key, val)
				if err != nil {
					t.Fatalf("put: %v", err)
				}
				model, present = val, true
				checkVersion(t, v, &lastSeq, &lastHLC, &haveHLC)
			case 1: // Delete
				v, err := kv.Delete(ctx, key)
				if err != nil {
					t.Fatalf("delete: %v", err)
				}
				model, present = nil, false
				checkVersion(t, v, &lastSeq, &lastHLC, &haveHLC)
			case 2: // CAS with the model's current value (should succeed)
				var expect []byte
				if present {
					expect = model
				}
				val := []byte(values.Draw(t, "cas"))
				v, err := kv.CAS(ctx, key, expect, val)
				if err != nil {
					t.Fatalf("cas(correct expect) failed: %v", err)
				}
				model, present = val, true
				checkVersion(t, v, &lastSeq, &lastHLC, &haveHLC)
			case 3: // Get must equal the model
				got, found, err := kv.Get(ctx, key)
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				if found != present || (found && string(got) != string(model)) {
					t.Fatalf("get = %q,%v want %q,%v", got, found, model, present)
				}
			}
		}
	})
}

func checkVersion(t *rapid.T, v mvcc.Version, lastSeq *uint64, lastHLC *hlc.Timestamp, haveHLC *bool) {
	if v.Seq != *lastSeq+1 {
		t.Fatalf("seq jumped: got %d after %d", v.Seq, *lastSeq)
	}
	*lastSeq = v.Seq
	if *haveHLC && !lastHLC.Less(v.HLC) {
		t.Fatalf("hlc not strictly increasing: %s then %s", *lastHLC, v.HLC)
	}
	*lastHLC, *haveHLC = v.HLC, true
}

func mustGet(t *testing.T, kv *mvcc.KV, key []byte) ([]byte, bool, error) {
	t.Helper()
	v, found, err := kv.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return v, found, err
}
