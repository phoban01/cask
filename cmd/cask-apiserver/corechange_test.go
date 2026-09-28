package main

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/roster"
)

// waitFor polls cond until it holds or the timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func hasCore(m *membership, core []uint64) bool {
	v, ok := m.current()
	return ok && slices.Equal(v.Core, core) && v.Joint == nil
}

func hasMembers(m *membership, ids []uint64) bool {
	v, ok := m.current()
	return ok && slices.Equal(memberIDs(v), ids)
}

// startFleetOfThree founds a fleet on member 1 and joins members 2 and 3 as
// participants. It returns the founder's server and a cancel func that stops
// the founder's loop.
func startFleetOfThree(t *testing.T, ctx context.Context, opts ...func(*membershipConfig)) (ms []*membership, stopFounder func()) {
	t.Helper()
	fctx, fcancel := context.WithCancel(ctx)
	founder, fsrv := newTestMemberServer(t, 1, true, nil, 10*time.Second, opts...)
	if err := founder.start(fctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	ms = []*membership{founder}
	for _, id := range []uint64{2, 3} {
		j := newTestMember(t, id, false, []string{founder.cfg.Advertise}, 10*time.Second, opts...)
		if err := j.start(ctx); err != nil {
			t.Fatalf("joiner %d: %v", id, err)
		}
		ms = append(ms, j)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to see three members", func() bool { return hasMembers(m, []uint64{1, 2, 3}) })
	}
	return ms, func() {
		fcancel()
		_ = fsrv.Close()
	}
}

// Keys written on a one-voter core survive growth to three voters and the
// loss of the founder. Without the carry-forward the keys live only on the
// founder, and the two new voters read nothing.
func TestCoreGrowthCarriesDataRegisters(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST carry every data register forward to the new core before it releases the old core.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, stopFounder := startFleetOfThree(t, ctx)
	founder, second := ms[0], ms[1]

	// Write on the one-voter core, from the founder and from a participant.
	want := writeKeys(t, ctx, 20, founder, second)

	// Grow the core from {1} to {1, 2, 3} through the roster reconfiguration.
	v, err := founder.changeCore(ctx, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("change core: %v", err)
	}
	if !slices.Equal(v.Core, []uint64{1, 2, 3}) || v.Joint != nil {
		t.Fatalf("core after change = %v (joint %v), want [1 2 3]", v.Core, v.Joint)
	}
	for _, m := range ms[1:] {
		waitFor(t, 5*time.Second, "the new voters to see core [1 2 3]", func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}

	// Stop the founder. Voters 2 and 3 are a quorum of the new core.
	stopFounder()
	checkKeys(t, ctx, second, want, "after the founder stopped")
}

// writeKeys writes n data keys, taking the writers in turn, and returns
// the values it wrote.
func writeKeys(t *testing.T, ctx context.Context, n int, writers ...*membership) map[string]string {
	t.Helper()
	want := map[string]string{}
	for i := range n {
		w := writers[i%len(writers)]
		key, val := fmt.Sprintf("fleet/devices/d%02d", i), fmt.Sprintf("v%d", i)
		if _, err := w.Propose(ctx, []byte(key), func([]byte) ([]byte, error) { return []byte(val), nil }); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		want[key] = val
	}
	return want
}

// checkKeys reads every key in want through reader and fails if a value
// is missing or wrong.
func checkKeys(t *testing.T, ctx context.Context, reader *membership, want map[string]string, when string) {
	t.Helper()
	for key, val := range want {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := reader.Propose(rctx, []byte(key), caspaxos.Identity)
		rcancel()
		if err != nil {
			t.Fatalf("read %s %s: %v", key, when, err)
		}
		if string(got) != val {
			t.Fatalf("read %s = %q %s, want %q: the core change lost a committed write", key, got, when, val)
		}
	}
}

// A resumed carry skips only the keys carried under the same joint
// configuration. A Remove that narrows the new core, or any later core
// change, gives a new configuration, and every key must go again.
func TestCarryRecordIsPerJointConfiguration(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A resumed core change MUST carry every data register that it did not carry under the same joint configuration.
	joint := func(gen uint64, old, nw []uint64) roster.Value {
		return roster.Value{ConfigGen: gen, Core: old, Joint: &roster.Joint{Old: old, New: nw}}
	}
	a := joint(1, []uint64{1}, []uint64{1, 2, 3})
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	names := func(ks [][]byte) []string {
		var out []string
		for _, k := range ks {
			out = append(out, string(k))
		}
		return out
	}

	var r carryRecord
	if got := names(r.pending(a, keys)); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("first attempt pending = %v, want every key", got)
	}
	r.mark(a, []byte("a"))
	r.mark(joint(3, []uint64{1}, []uint64{1, 2, 3}), []byte("b")) // another configuration
	if got := names(r.pending(a, keys)); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("resume pending = %v, want [b c]", got)
	}
	for name, v := range map[string]roster.Value{
		"newer gen": joint(2, []uint64{1}, []uint64{1, 2, 3}),
		"narrowed":  joint(1, []uint64{1}, []uint64{1, 2}),
		"other old": joint(1, []uint64{1, 2, 3}, []uint64{1, 2, 3}),
	} {
		var r carryRecord
		r.pending(a, keys)
		r.mark(a, []byte("a"))
		if got := names(r.pending(v, keys)); !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Errorf("%s: pending = %v, want every key", name, got)
		}
	}
}

// In the joint phase a data write needs a quorum of the new core too. With
// the two new voters down, the founder alone is a quorum of the old core but
// not of the joint one, so the write must fail.
func TestJointCoreWriteNeedsBothQuorums(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# During a core change, a data write MUST reach a quorum of both the old and the new core.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	founder, _ := newTestMemberServer(t, 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	cur, _ := founder.current()
	// Members 2 and 3 have addresses, but nothing listens there.
	cur.Members = append(cur.Members, roster.Member{NodeID: 2, Addr: "127.0.0.1:1"}, roster.Member{NodeID: 3, Addr: "127.0.0.1:2"})
	cur.Joint = &roster.Joint{Old: []uint64{1}, New: []uint64{1, 2, 3}}
	cur.ConfigGen++
	founder.learn(cur.Members)
	founder.store(cur)

	wctx, wcancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer wcancel()
	_, err := founder.Propose(wctx, []byte("fleet/devices/d"), func([]byte) ([]byte, error) { return []byte("v"), nil })
	if err == nil {
		t.Fatal("a joint-phase write committed on the old core alone")
	}
}
