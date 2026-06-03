package hlc

import (
	"sync"
	"testing"

	"pgregory.net/rapid"
)

// fakePhysical returns a controllable physical clock and a knob to set it.
func fakePhysical() (PhysicalFunc, *int64, *sync.Mutex) {
	var mu sync.Mutex
	var now int64
	return func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return now
	}, &now, &mu
}

func TestNowStrictlyIncreasesUnderFrozenPhysical(t *testing.T) {
	phys, _, _ := fakePhysical()
	c := New(phys) // physical frozen at 0
	prev := c.Now()
	for i := 0; i < 1000; i++ {
		ts := c.Now()
		if !prev.Less(ts) {
			t.Fatalf("Now not strictly increasing: %s then %s", prev, ts)
		}
		prev = ts
	}
}

func TestUpdateHappensAfterRemote(t *testing.T) {
	phys, _, _ := fakePhysical()
	c := New(phys)
	remote := Timestamp{Physical: 5_000, Logical: 7}
	got := c.Update(remote)
	if !remote.Less(got) {
		t.Fatalf("Update(%s) = %s, want strictly after remote", remote, got)
	}
}

// Property: across any interleaving of Now and Update calls, every timestamp a
// clock emits is strictly greater than the previous one it emitted.
func TestClockMonotonicProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		phys, now, mu := fakePhysical()
		c := New(phys)
		var prev Timestamp
		n := rapid.IntRange(1, 200).Draw(t, "ops")
		for i := 0; i < n; i++ {
			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0: // advance physical
				mu.Lock()
				*now += rapid.Int64Range(0, 1000).Draw(t, "tick")
				mu.Unlock()
			case 1:
				ts := c.Now()
				if !prev.IsZero() && !prev.Less(ts) {
					t.Fatalf("Now regressed: %s then %s", prev, ts)
				}
				prev = ts
			case 2:
				remote := Timestamp{
					Physical: rapid.Int64Range(0, 10_000).Draw(t, "rp"),
					Logical:  uint32(rapid.IntRange(0, 50).Draw(t, "rl")),
				}
				ts := c.Update(remote)
				if !prev.IsZero() && !prev.Less(ts) {
					t.Fatalf("Update regressed: %s then %s", prev, ts)
				}
				if ts.LessEqual(remote) {
					t.Fatalf("Update(%s)=%s did not advance past remote", remote, ts)
				}
				prev = ts
			}
		}
	})
}
