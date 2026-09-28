package storage

import (
	"context"
	"errors"
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

// sweepCask is an in-process cask of three in-memory acceptors whose
// stores a test can list.
type sweepCask struct {
	stores    []*store.Mem
	acceptors []caspaxos.AcceptorClient
	clock     *hlc.Clock
}

func newSweepCask(t *testing.T) *sweepCask {
	t.Helper()
	c := &sweepCask{}
	for range 3 {
		st := store.NewMem()
		c.stores = append(c.stores, st)
		c.acceptors = append(c.acceptors, caspaxos.NewAcceptor(st))
	}
	var now atomic.Int64
	c.clock = hlc.New(func() int64 { return now.Add(1) })
	return c
}

// kv returns a proposer over the acceptors at the given positions.
func (c *sweepCask) kv(node uint64, at ...int) *mvcc.KV {
	acc := make([]caspaxos.AcceptorClient, 0, len(at))
	for _, i := range at {
		acc = append(acc, c.acceptors[i])
	}
	prop := caspaxos.NewProposer(node, acc,
		caspaxos.WithBackoff(backoff.FullJitter(time.Millisecond, 20*time.Millisecond)))
	return mvcc.New(prop, c.clock, node)
}

// lister lists the union of the keys on the stores at the given positions.
func (c *sweepCask) lister(at ...int) KeyLister {
	return func(ctx context.Context) ([][]byte, error) {
		var out [][]byte
		for _, i := range at {
			keys, err := c.stores[i].Keys(ctx)
			if err != nil {
				return nil, err
			}
			out = append(out, keys...)
		}
		return out, nil
	}
}

func mustSweep(t *testing.T, kv *mvcc.KV, list KeyLister) int {
	t.Helper()
	n, err := Sweep(context.Background(), kv, "devices", list)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A crash after the object write leaves no index entry. The sweep writes
// it at the object's sequence.
func TestSweepRecordsObjectWithLostIndexWrite(t *testing.T) {
	ctx := context.Background()
	c := newSweepCask(t)
	kv := c.kv(1, 0, 1, 2)

	// gpu-0 has an index entry. gpu-1 and a second version of gpu-0 lost
	// their index writes. A key of another resource must stay out.
	if _, err := kv.Put(ctx, ObjectKey("devices", "gpu-0"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := WriteIndex(ctx, kv, "devices", "gpu-0"); err != nil {
		t.Fatal(err)
	}
	v2, err := kv.Put(ctx, ObjectKey("devices", "gpu-0"), []byte("v2"))
	if err != nil {
		t.Fatal(err)
	}
	v1, err := kv.Put(ctx, ObjectKey("devices", "gpu-1"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, ObjectKey("deviceclaims", "claim-0"), []byte("v1")); err != nil {
		t.Fatal(err)
	}

	if n := mustSweep(t, kv, c.lister(0, 1)); n != 2 {
		t.Fatalf("sweep wrote %d names, want 2", n)
	}
	idx := mustIndex(t, kv, "devices")
	want := map[string]uint64{"gpu-0": v2.Seq, "gpu-1": v1.Seq}
	if len(idx.Entries) != len(want) || idx.Entries["gpu-0"].Obj != want["gpu-0"] || idx.Entries["gpu-1"].Obj != want["gpu-1"] {
		t.Fatalf("entries = %v, want %v", idx.Entries, want)
	}
	// A second sweep finds nothing to repair and leaves the index alone.
	seq := idx.Seq
	if n := mustSweep(t, kv, c.lister(0, 1)); n != 0 {
		t.Fatalf("second sweep wrote %d names, want 0", n)
	}
	if got := mustIndex(t, kv, "devices").Seq; got != seq {
		t.Fatalf("second sweep moved the index sequence from %d to %d", seq, got)
	}
}

// A crash after the tombstone leaves the name in the index. The sweep
// removes it.
func TestSweepRemovesTombstonedEntry(t *testing.T) {
	ctx := context.Background()
	c := newSweepCask(t)
	kv := c.kv(1, 0, 1, 2)

	for _, name := range []string{"gpu-0", "gpu-1"} {
		if _, err := kv.Put(ctx, ObjectKey("devices", name), []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := WriteIndex(ctx, kv, "devices", name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kv.Delete(ctx, ObjectKey("devices", "gpu-0")); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustIndex(t, kv, "devices").Entries["gpu-0"]; !ok {
		t.Fatal("test setup: the lost index write already removed gpu-0")
	}

	if n := mustSweep(t, kv, c.lister(0, 1)); n != 1 {
		t.Fatalf("sweep wrote %d names, want 1", n)
	}
	idx := mustIndex(t, kv, "devices")
	if _, ok := idx.Entries["gpu-0"]; ok {
		t.Fatal("index still names the tombstoned gpu-0")
	}
	if idx.Entries["gpu-1"].Obj != 1 {
		t.Fatalf("entries = %v, want gpu-1:1 only", idx.Entries)
	}
}

// A committed write can miss one acceptor. A listing of that acceptor
// alone misses the object; a listing of a majority finds it.
func TestSweepNeedsMajorityListing(t *testing.T) {
	ctx := context.Background()
	c := newSweepCask(t)
	// The write reaches acceptors 0 and 1, a majority of three.
	if _, err := c.kv(2, 0, 1).Put(ctx, ObjectKey("devices", "gpu-0"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	kv := c.kv(1, 0, 1, 2)

	if n := mustSweep(t, kv, c.lister(2)); n != 0 {
		t.Fatalf("sweep over the lagging acceptor wrote %d names, want 0", n)
	}
	// Acceptors 1 and 2 are a majority. Acceptor 1 holds the object.
	if n := mustSweep(t, kv, c.lister(1, 2)); n != 1 {
		t.Fatalf("sweep over a majority wrote %d names, want 1", n)
	}
	if got := mustIndex(t, kv, "devices").Entries["gpu-0"].Obj; got != 1 {
		t.Fatalf("index records %d for gpu-0, want 1", got)
	}
}

// Writers, some of which crash before the index write, race with two
// sweepers on other nodes. No reader ever sees an index entry above the
// object head. After the writers stop, one sweep makes every entry equal
// the head.
func TestSweepNeverAheadUnderConcurrentWritersAndSweeps(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# The index register MUST NOT record a sequence higher than the object register holds.
	ctx := context.Background()
	c := newSweepCask(t)
	names := []string{"gpu-0", "gpu-1", "gpu-2"}
	const writers, rounds = 3, 6

	errs := make(chan error, 64)
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}
	var writersWG, sweepersWG sync.WaitGroup
	for w := range writers {
		kv := c.kv(uint64(10+w), 0, 1, 2)
		writersWG.Go(func() {
			for r := range rounds {
				name := names[(w+r)%len(names)]
				key := ObjectKey("devices", name)
				err := untilNotPreempted(func() error {
					if (w+r)%4 == 3 {
						_, err := kv.Delete(ctx, key)
						return err
					}
					_, err := kv.Put(ctx, key, fmt.Appendf(nil, "w%d-r%d", w, r))
					return err
				})
				if err != nil {
					report(err)
					return
				}
				if r%2 == 1 {
					continue // a crash before the index write
				}
				if err := untilNotPreempted(func() error { _, _, err := WriteIndex(ctx, kv, "devices", name); return err }); err != nil {
					report(err)
					return
				}
			}
		})
	}
	stop := make(chan struct{})
	for s := range 2 {
		kv := c.kv(uint64(20+s), 0, 1, 2)
		list := c.lister(s, s+1)
		sweepersWG.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Contention can preempt a round. That costs progress,
				// not safety, so the next pass tries again.
				if _, err := Sweep(ctx, kv, "devices", list); err != nil && !errors.Is(err, caspaxos.ErrPreempted) {
					report(err)
					return
				}
				// Read the index first, then each object, so the object
				// can only be newer than the entry.
				idx, err := ReadIndex(ctx, kv, "devices")
				if errors.Is(err, caspaxos.ErrPreempted) {
					continue
				}
				if err != nil {
					report(err)
					return
				}
				for name, entry := range idx.Entries {
					seq, _, err := objectHead(ctx, kv, "devices", name)
					if errors.Is(err, caspaxos.ErrPreempted) {
						continue
					}
					if err != nil {
						report(err)
						return
					}
					if entry.Obj > seq {
						report(fmt.Errorf("index records %d for %s, object holds %d", entry.Obj, name, seq))
						return
					}
				}
			}
		})
	}
	writersWG.Wait()
	close(stop)
	sweepersWG.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	kv := c.kv(1, 0, 1, 2)
	mustSweep(t, kv, c.lister(0, 1))
	idx := mustIndex(t, kv, "devices")
	var got []string
	for _, name := range names {
		seq, live, err := objectHead(ctx, kv, "devices", name)
		if err != nil {
			t.Fatal(err)
		}
		entry, ok := idx.Entries[name]
		if live != ok || (live && entry.Obj != seq) {
			t.Fatalf("%s: index entry %+v (present %v), object head %d (live %v)", name, entry, ok, seq, live)
		}
		if ok {
			got = append(got, name)
		}
	}
	if len(idx.Entries) != len(got) {
		t.Fatalf("index names %v, want only the live objects %v", idx.Entries, got)
	}
}

func TestObjectName(t *testing.T) {
	for _, tc := range []struct {
		key  string
		name string
		ok   bool
	}{
		{"fleet/devices/gpu-0", "gpu-0", true},
		{"fleet/devices.index", "", false},
		{"fleet/deviceclaims/c-0", "", false},
		{"fleet/devices/", "", false},
		{"fleet/devices/a/b", "", false},
		{"cask/roster", "", false},
	} {
		name, ok := objectName("devices", []byte(tc.key))
		if ok != tc.ok || (ok && name != tc.name) {
			t.Errorf("objectName(%q) = %q, %v; want %q, %v", tc.key, name, ok, tc.name, tc.ok)
		}
	}
}

// untilNotPreempted runs f again while contention preempts it. Running a
// Put, a Delete, or an index write twice is harmless in these tests.
func untilNotPreempted(f func() error) error {
	for {
		err := f()
		if !errors.Is(err, caspaxos.ErrPreempted) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}
