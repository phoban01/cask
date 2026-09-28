package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
)

func newMemberKV(m *membership) *mvcc.KV {
	clock := hlc.New(func() int64 { return time.Now().UnixNano() })
	return mvcc.New(m, clock, m.self.NodeID)
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// indexEntry reads the index entry of name.
func indexEntry(t *testing.T, kv *mvcc.KV, resource, name string) (uint64, bool) {
	t.Helper()
	idx, err := storage.ReadIndex(context.Background(), kv, resource)
	if err != nil {
		t.Fatal(err)
	}
	seq, ok := idx.Entries[name]
	return seq, ok
}

// A crash lost the index write of an object. The startup sweep of the
// founding member writes the entry before the server serves.
func TestStartupSweepRepairsLostIndexWrite(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The extension server MUST reconcile the index register against the object registers at startup.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	founder := newTestMember(t, 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatal(err)
	}
	kv := newMemberKV(founder)

	// The object write lands. The index write never runs.
	d, err := kv.Put(ctx, storage.ObjectKey("devices", "gpu-0"), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	c, err := kv.Put(ctx, storage.ObjectKey("deviceclaims", "claim-0"), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := indexEntry(t, kv, "devices", "gpu-0"); ok {
		t.Fatal("test setup: the index already names gpu-0")
	}

	sw := &indexSweeper{kv: kv, list: founder.listDataKeys, log: quietLog()}
	if err := sw.sweepAtStartup(ctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if seq, ok := indexEntry(t, kv, "devices", "gpu-0"); !ok || seq != d.Seq {
		t.Fatalf("devices index entry for gpu-0 = %d (present %v), want %d", seq, ok, d.Seq)
	}
	if seq, ok := indexEntry(t, kv, "deviceclaims", "claim-0"); !ok || seq != c.Seq {
		t.Fatalf("deviceclaims index entry for claim-0 = %d (present %v), want %d", seq, ok, c.Seq)
	}
}

// A running server repairs a lost index write on the next interval, with
// no restart. The loop stops when its context ends.
func TestIntervalSweepRepairsWithoutRestart(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The extension server MUST reconcile the index register against the object registers at a fixed interval.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	founder := newTestMember(t, 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatal(err)
	}
	kv := newMemberKV(founder)
	sw := &indexSweeper{kv: kv, list: founder.listDataKeys, log: quietLog()}
	if err := sw.sweepAtStartup(ctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	sctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sw.sweepEvery(sctx, 20*time.Millisecond)
	}()

	// After startup, an object write lands and its index write is lost.
	d, err := kv.Put(ctx, storage.ObjectKey("devices", "gpu-0"), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the interval sweep to record gpu-0", func() bool {
		seq, ok := indexEntry(t, kv, "devices", "gpu-0")
		return ok && seq == d.Seq
	})

	stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep loop did not stop when its context ended")
	}
}

func TestWithJitter(t *testing.T) {
	for range 100 {
		if got := withJitter(time.Second); got < time.Second || got >= time.Second+100*time.Millisecond {
			t.Fatalf("withJitter(1s) = %v, want in [1s, 1.1s)", got)
		}
	}
	if got := withJitter(5); got != 5 {
		t.Fatalf("withJitter(5ns) = %v, want 5ns", got)
	}
}

// On a core of three with one voter down, the sweep lists the keys on the
// two voters that answer and still finds the object.
func TestSweepListsMajorityOfCore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, stopFounder := startFleetOfThree(t, ctx)
	founder, second, third := ms[0], ms[1], ms[2]
	if _, err := founder.changeCore(ctx, []uint64{1, 2, 3}); err != nil {
		t.Fatalf("change core: %v", err)
	}
	for _, m := range ms[1:] {
		waitFor(t, 5*time.Second, "the voters to see core [1 2 3]", func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}
	stopFounder()

	d, err := newMemberKV(second).Put(ctx, storage.ObjectKey("devices", "gpu-0"), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	kv := newMemberKV(third)
	sw := &indexSweeper{kv: kv, list: third.listDataKeys, log: quietLog()}
	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	defer scancel()
	if err := sw.sweepAtStartup(sctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if seq, ok := indexEntry(t, kv, "devices", "gpu-0"); !ok || seq != d.Seq {
		t.Fatalf("index entry for gpu-0 = %d (present %v), want %d", seq, ok, d.Seq)
	}
}
