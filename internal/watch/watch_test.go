package watch_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/watch"
	"github.com/phoban01/cask/testutil/sim"
)

func newKV() *mvcc.KV {
	stores := make([]caspaxos.Storage, 3)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	var tick int64
	clk := hlc.New(func() int64 { return atomic.AddInt64(&tick, 1) })
	return mvcc.New(caspaxos.NewProposer(1, nw.Clients()), clk, 1)
}

func vals(evs []watch.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = string(e.Version.Value)
	}
	return out
}

func TestKeyWatcherStreamsAndResumes(t *testing.T) {
	ctx := context.Background()
	kv := newKV()
	key := []byte("k")
	for _, v := range []string{"a", "b", "c"} {
		if _, err := kv.Put(ctx, key, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	// From the beginning: all three, in order.
	w := watch.NewKeyWatcher(kv, key, 0)
	evs, err := w.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := vals(evs); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("stream = %v, want [a b c]", got)
	}
	// Polling again yields nothing until there is a new version.
	if evs, _ := w.Poll(ctx); len(evs) != 0 {
		t.Fatalf("expected no new events, got %v", vals(evs))
	}
	if _, err := kv.Put(ctx, key, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if evs, _ := w.Poll(ctx); len(evs) != 1 || string(evs[0].Version.Value) != "d" {
		t.Fatalf("expected [d], got %v", vals(evs))
	}

	// Resume from seq 1: only b, c, d.
	r := watch.NewKeyWatcher(kv, key, 1)
	evs, _ = r.Poll(ctx)
	if got := vals(evs); len(got) != 3 || got[0] != "b" {
		t.Fatalf("resume from seq1 = %v, want [b c d]", got)
	}
}

func TestKeyWatcherCompacted(t *testing.T) {
	ctx := context.Background()
	kv := newKV()
	key := []byte("k")
	for _, v := range []string{"a", "b", "c", "d"} {
		kv.Put(ctx, key, []byte(v))
	}
	// Compact away seqs 1,2 (retain from seq 3).
	if err := kv.Compact(ctx, key, 3); err != nil {
		t.Fatal(err)
	}

	// A watcher resuming from before the watermark is told to re-list.
	stale := watch.NewKeyWatcher(kv, key, 1)
	if _, err := stale.Poll(ctx); !errors.Is(err, watch.ErrCompacted) {
		t.Fatalf("resume below watermark = %v, want ErrCompacted", err)
	}
	// A watcher resuming at the watermark still works.
	ok := watch.NewKeyWatcher(kv, key, 2)
	evs, err := ok.Poll(ctx)
	if err != nil {
		t.Fatalf("resume at watermark: %v", err)
	}
	if got := vals(evs); len(got) != 2 || got[0] != "c" {
		t.Fatalf("resume at watermark = %v, want [c d]", got)
	}
}

func TestPrefixWatcherMergesByHLC(t *testing.T) {
	ctx := context.Background()
	kv := newKV()
	// Interleaved writes across keys sharing a prefix; the shared clock orders
	// them, so the prefix stream must reflect commit order.
	seq := []struct{ k, v string }{
		{"p/a", "a1"}, {"p/b", "b1"}, {"p/a", "a2"}, {"p/c", "c1"}, {"p/b", "b2"},
	}
	for _, s := range seq {
		if _, err := kv.Put(ctx, []byte(s.k), []byte(s.v)); err != nil {
			t.Fatal(err)
		}
	}
	keys := [][]byte{[]byte("p/a"), []byte("p/b"), []byte("p/c")}
	lister := func(context.Context, []byte) ([][]byte, error) { return keys, nil }

	w := watch.NewPrefixWatcher(kv, lister, []byte("p/"), hlc.Timestamp{})
	evs, err := w.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a1", "b1", "a2", "c1", "b2"}
	got := vals(evs)
	if len(got) != len(want) {
		t.Fatalf("prefix stream len %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefix stream = %v, want %v", got, want)
		}
	}
	// Resuming polls return nothing new.
	if evs, _ := w.Poll(ctx); len(evs) != 0 {
		t.Fatalf("expected no new prefix events, got %v", vals(evs))
	}
}

func TestSafeCompactPoint(t *testing.T) {
	// No watchers: retention alone applies.
	if got := watch.SafeCompactPoint(5, nil); got != 5 {
		t.Fatalf("SafeCompactPoint(5,nil) = %d, want 5", got)
	}
	// A watcher at cursor 2 forces keeping from <= 3.
	if got := watch.SafeCompactPoint(5, []uint64{2}); got != 3 {
		t.Fatalf("SafeCompactPoint(5,[2]) = %d, want 3", got)
	}
	// The oldest cursor wins.
	if got := watch.SafeCompactPoint(5, []uint64{4, 1, 3}); got != 2 {
		t.Fatalf("SafeCompactPoint(5,[4,1,3]) = %d, want 2", got)
	}
}

// GC driven by SafeCompactPoint must never strand an active watcher.
func TestWatcherAwareGCKeepsWatcherAlive(t *testing.T) {
	ctx := context.Background()
	kv := newKV()
	key := []byte("k")
	for _, v := range []string{"a", "b", "c", "d", "e", "f"} {
		kv.Put(ctx, key, []byte(v))
	}
	// A slow watcher sits at cursor 2; aggressive retention would keep only from 5.
	w := watch.NewKeyWatcher(kv, key, 2)
	keepFrom := watch.SafeCompactPoint(5, []uint64{w.Cursor()})
	if err := kv.Compact(ctx, key, keepFrom); err != nil {
		t.Fatal(err)
	}
	// The watcher is not compacted out: it still streams from seq 3.
	evs, err := w.Poll(ctx)
	if err != nil {
		t.Fatalf("watcher stranded by GC: %v", err)
	}
	if got := vals(evs); len(got) != 4 || got[0] != "c" {
		t.Fatalf("post-GC stream = %v, want [c d e f]", got)
	}
}

// One upstream subscription serves many client watchers: storage is read once
// per pump regardless of subscriber count.
func TestFanOutServesManyFromOneUpstream(t *testing.T) {
	ctx := context.Background()
	kv := newKV()
	key := []byte("k")

	var reads int64
	counting := countingReader{inner: kv, reads: &reads}
	fo := watch.NewFanOut(watch.NewKeyWatcher(counting, key, 0))

	const subscribers = 5
	chans := make([]<-chan watch.Event, subscribers)
	for i := 0; i < subscribers; i++ {
		ch, cancel := fo.Subscribe(16)
		defer cancel()
		chans[i] = ch
	}

	for _, v := range []string{"a", "b"} {
		kv.Put(ctx, key, []byte(v))
	}
	n, err := fo.Pump(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pumped %d events, want 2", n)
	}
	// One storage read served all subscribers.
	if got := atomic.LoadInt64(&reads); got != 1 {
		t.Fatalf("upstream read %d times for one pump, want 1", got)
	}
	// Every subscriber received both events.
	for i, ch := range chans {
		for _, want := range []string{"a", "b"} {
			select {
			case ev := <-ch:
				if string(ev.Version.Value) != want {
					t.Fatalf("sub %d got %q, want %q", i, ev.Version.Value, want)
				}
			default:
				t.Fatalf("sub %d missing event %q", i, want)
			}
		}
	}
}

type countingReader struct {
	inner watch.HistoryReader
	reads *int64
}

func (c countingReader) History(ctx context.Context, key []byte) (mvcc.Chain, error) {
	atomic.AddInt64(c.reads, 1)
	return c.inner.History(ctx, key)
}
