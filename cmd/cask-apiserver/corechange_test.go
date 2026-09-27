package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
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
func startFleetOfThree(t *testing.T, ctx context.Context) (ms []*membership, stopFounder func()) {
	t.Helper()
	fctx, fcancel := context.WithCancel(ctx)
	founder, fsrv := newTestMemberServer(t, 1, true, nil, 10*time.Second)
	if err := founder.start(fctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	ms = []*membership{founder}
	for _, id := range []uint64{2, 3} {
		j := newTestMember(t, id, false, []string{founder.cfg.Advertise}, 10*time.Second)
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
	want := map[string]string{}
	for i := range 20 {
		w := founder
		if i%2 == 1 {
			w = second
		}
		key, val := fmt.Sprintf("fleet/devices/d%02d", i), fmt.Sprintf("v%d", i)
		if _, err := w.Propose(ctx, []byte(key), func([]byte) ([]byte, error) { return []byte(val), nil }); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		want[key] = val
	}

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

	for key, val := range want {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := second.Propose(rctx, []byte(key), caspaxos.Identity)
		rcancel()
		if err != nil {
			t.Fatalf("read %s after the founder stopped: %v", key, err)
		}
		if string(got) != val {
			t.Fatalf("read %s = %q after the founder stopped, want %q: the core change lost a committed write", key, got, val)
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

// A voter rejects a data write that names an older core configuration, on
// the local path and on the wire. The roster key is not fenced here.
func TestVoterRejectsWriteFromOlderCore(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A voter MUST reject a data write that names an older core configuration than the one it knows.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	founder := newTestMember(t, 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	cur, _ := founder.current()
	gen := cur.ConfigGen
	cur.ConfigGen++
	founder.store(cur)

	peer := newTestMember(t, 2, false, nil, time.Second)
	peer.learn([]roster.Member{{NodeID: 1, Addr: founder.cfg.Advertise}})
	peer.mu.Lock()
	remote, ok := peer.clientLocked(1)
	peer.mu.Unlock()
	if !ok {
		t.Fatal("no client for the founder")
	}
	founder.mu.Lock()
	local, _ := founder.clientLocked(1)
	founder.mu.Unlock()
	key := []byte("fleet/devices/d")
	b := caspaxos.Ballot{Counter: 1, NodeID: 2}

	for name, c := range map[string]caspaxos.AcceptorClient{"local": local, "remote": remote} {
		stale := ranges.WithClaimedEpoch(ctx, gen)
		if _, err := c.Prepare(stale, key, b); !errors.Is(err, caspaxos.ErrRangeChanged) {
			t.Errorf("%s: prepare from an older core: err = %v, want ErrRangeChanged", name, err)
		}
		if _, err := c.Accept(stale, key, b, []byte("v")); !errors.Is(err, errStaleAccept) {
			t.Errorf("%s: accept from an older core: err = %v, want a missing vote", name, err)
		}
		if _, err := c.Prepare(ranges.WithClaimedEpoch(ctx, gen+1), key, b); err != nil {
			t.Errorf("%s: prepare from the current core: %v", name, err)
		}
		if _, err := c.Prepare(stale, []byte("\x00roster-test"), b); err != nil {
			t.Errorf("%s: prepare on the roster key: %v", name, err)
		}
	}
}
