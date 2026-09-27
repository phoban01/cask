package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

// The §3.0 contract: an acknowledged Store survives closing and reopening the
// database — a reply implies durability. Missing keys load the zero Register.
func TestPebbleDurabilityRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := store.NewPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	want := caspaxos.Register{
		Promise:  caspaxos.Ballot{Counter: 7, NodeID: 2},
		Accepted: caspaxos.Ballot{Counter: 7, NodeID: 2},
		Value:    []byte("hello-durable"),
	}
	if err := p.Store(ctx, []byte("k"), want); err != nil {
		t.Fatal(err)
	}
	empty := caspaxos.Register{Promise: caspaxos.Ballot{Counter: 3, NodeID: 1}} // promise, no value
	if err := p.Store(ctx, []byte("promise-only"), empty); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := store.NewPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	got, err := p2.Load(ctx, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Promise != want.Promise || got.Accepted != want.Accepted || string(got.Value) != string(want.Value) {
		t.Fatalf("reloaded register = %+v, want %+v", got, want)
	}
	got, err = p2.Load(ctx, []byte("promise-only"))
	if err != nil || got.Promise != empty.Promise || got.Value != nil {
		t.Fatalf("promise-only register = %+v err=%v, want %+v", got, err, empty)
	}
	zero, err := p2.Load(ctx, []byte("never-stored"))
	if err != nil || !zero.Promise.IsZero() || !zero.Accepted.IsZero() || zero.Value != nil {
		t.Fatalf("missing key = %+v err=%v, want zero register", zero, err)
	}
}

// Group commit coalesces concurrent Stores into far fewer synced commits, and
// every acknowledged write is still durable across reopen.
func TestPebbleGroupCommitCoalesces(t *testing.T) {
	dir := t.TempDir()
	p, err := store.NewPebble(dir, store.WithGroupCommit())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	const writers = 64
	// Hold the first commit until every other writer has joined the next
	// group. Without this the test depends on disk speed: on a fast disk
	// each sync ends before the next write arrives, and nothing coalesces.
	var first sync.Once
	p.SetBeforeCommit(func(writes int) {
		first.Do(func() {
			deadline := time.Now().Add(10 * time.Second)
			for writes+p.PendingWrites() < writers {
				if time.Now().After(deadline) {
					t.Errorf("only %d of %d writers enqueued", writes+p.PendingWrites(), writers)
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Appendf(nil, "k%d", i)
			r := caspaxos.Register{
				Promise:  caspaxos.Ballot{Counter: uint64(i + 1), NodeID: 1},
				Accepted: caspaxos.Ballot{Counter: uint64(i + 1), NodeID: 1},
				Value:    fmt.Appendf(nil, "v%d", i),
			}
			if err := p.Store(ctx, key, r); err != nil {
				t.Errorf("store %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// The first group commits; everything else joined one second group.
	if f := p.Flushes(); f < 1 || f > 2 {
		t.Fatalf("flushes = %d for %d concurrent writers; want 1 or 2", f, writers)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := store.NewPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	for i := range writers {
		got, err := p2.Load(ctx, fmt.Appendf(nil, "k%d", i))
		if err != nil || string(got.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d after reopen = %+v err=%v", i, got, err)
		}
	}
}

// A full CASPaxos acceptor stack over Pebble: propose, restart the store,
// propose again — the crash-recovery path the in-memory store cannot test.
func TestAcceptorOverPebbleSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	open := func() *store.Pebble {
		p, err := store.NewPebble(dir, store.WithGroupCommit())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	p := open()
	prop := caspaxos.NewProposer(1, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(p)})
	if _, err := prop.Propose(ctx, []byte("k"), caspaxos.Write([]byte("before-crash"))); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p = open()
	defer p.Close()
	prop = caspaxos.NewProposer(2, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(p)})
	got, err := prop.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "before-crash" {
		t.Fatalf("read after restart = %q, want before-crash", got)
	}
}

// Throughput with and without group commit (the roadmap's demonstration that
// the window is the win). Run with: go test -bench BenchmarkPebbleStore ./internal/store/
func BenchmarkPebbleStore(b *testing.B) {
	for _, mode := range []struct {
		name  string
		group bool
	}{{"sync-every-write", false}, {"group-commit", true}} {
		b.Run(mode.name, func(b *testing.B) {
			p, err := store.NewPebble(b.TempDir(), pebbleOpts(mode.group)...)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			ctx := context.Background()
			reg := caspaxos.Register{
				Promise:  caspaxos.Ballot{Counter: 1, NodeID: 1},
				Accepted: caspaxos.Ballot{Counter: 1, NodeID: 1},
				Value:    []byte("benchmark-value"),
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Appendf(nil, "k%d", i)
					i++
					if err := p.Store(ctx, key, reg); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func pebbleOpts(group bool) []store.PebbleOption {
	if group {
		return []store.PebbleOption{store.WithGroupCommit()}
	}
	return nil
}
