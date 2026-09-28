package mvcc_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
)

// TestSnapshotReadGuard is the Go form of the stepNoReadGuard control in
// quint/cross_range.qnt. A writer with a slow clock must not commit a
// version at or below a snapshot time that a reader already read. If it
// could, a second read at the same time would see a value the first read
// did not.
func TestSnapshotReadGuard(t *testing.T) {
	ctx := context.Background()
	acc, _ := cluster(3)
	var slow int64
	writer := mvcc.New(caspaxos.NewProposer(1, acc), hlc.New(func() int64 { return atomic.AddInt64(&slow, 1) }), 1)
	fast := int64(1_000)
	reader := mvcc.New(caspaxos.NewProposer(2, acc), hlc.New(func() int64 { return atomic.AddInt64(&fast, 1) }), 2)
	key := []byte("k")

	if _, err := writer.Put(ctx, key, []byte("old")); err != nil {
		t.Fatal(err)
	}
	at := hlc.Timestamp{Physical: 500}
	first, ok, err := reader.SnapshotAt(ctx, key, at)
	if err != nil || !ok || string(first.Value) != "old" {
		t.Fatalf("first snapshot = %q,%v,%v want old,true,nil", first.Value, ok, err)
	}

	// The writer's clock is near 1, far below the snapshot time 500.
	late, err := writer.Put(ctx, key, []byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	if late.HLC.LessEqual(at) {
		t.Errorf("write after the snapshot stamped %s, at or below snapshot time %s", late.HLC, at)
	}
	second, ok, err := reader.SnapshotAt(ctx, key, at)
	if err != nil || !ok || string(second.Value) != string(first.Value) || second.Seq != first.Seq {
		t.Fatalf("second snapshot = %q seq %d,%v,%v want %q seq %d: a write landed below a snapshot already read",
			second.Value, second.Seq, ok, err, first.Value, first.Seq)
	}
}

// TestSnapshotAheadRefused checks that a snapshot time ahead of the
// reader's clock is refused, not served, and that the refusal is typed.
func TestSnapshotAheadRefused(t *testing.T) {
	ctx := context.Background()
	acc, clock := cluster(3)
	kv := mvcc.New(caspaxos.NewProposer(1, acc), clock, 1)
	key := []byte("k")
	if _, err := kv.Put(ctx, key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	future := hlc.Timestamp{Physical: 1 << 40}
	if _, _, err := kv.SnapshotAt(ctx, key, future); !errors.Is(err, mvcc.ErrSnapshotAhead) {
		t.Fatalf("SnapshotAt(future) err = %v, want ErrSnapshotAhead", err)
	}
	if _, err := kv.SnapshotRead(ctx, [][]byte{key}, future); !errors.Is(err, mvcc.ErrSnapshotAhead) {
		t.Fatalf("SnapshotRead(future) err = %v, want ErrSnapshotAhead", err)
	}
}
