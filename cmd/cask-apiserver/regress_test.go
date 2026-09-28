package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
)

// hangs maps a test member to its hang switch. A hung member accepts
// connections but never answers, like a peer stuck in a long pause or
// behind a black-holed route.
var hangs sync.Map // *membership -> *atomic.Bool

// delays maps a test member to the time it adds to every request, like a
// peer in another region.
var delays sync.Map // *membership -> *atomic.Int64

func hangable(t *testing.T, m *membership, h http.Handler) http.Handler {
	hung := &atomic.Bool{}
	hangs.Store(m, hung)
	delay := &atomic.Int64{}
	delays.Store(m, delay)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d := time.Duration(delay.Load()); d > 0 {
			time.Sleep(d)
		}
		if hung.Load() {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		h.ServeHTTP(w, r)
	})
}

func hang(m *membership) {
	v, _ := hangs.Load(m)
	v.(*atomic.Bool).Store(true)
}

// slow adds d to every request that m serves. Zero removes the delay.
func slow(m *membership, d time.Duration) {
	v, _ := delays.Load(m)
	v.(*atomic.Int64).Store(int64(d))
}

// fleetMember is a test member with its own run-loop context.
type fleetMember struct {
	*membership
	srv    *http.Server
	cancel context.CancelFunc
}

// startFleet founds a fleet on member 1, joins members 2..n as
// participants, and grows the core to core. The driver of the new core is
// its highest id.
func startFleet(t *testing.T, n int, core []uint64) []*fleetMember {
	t.Helper()
	var ms []*fleetMember
	for id := uint64(1); id <= uint64(n); id++ {
		var seeds []string
		if id > 1 {
			seeds = []string{ms[0].cfg.Advertise}
		}
		m, srv := newTestMemberServer(t, id, id == 1, seeds, 10*time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := m.start(ctx); err != nil {
			t.Fatalf("member %d: %v", id, err)
		}
		ms = append(ms, &fleetMember{membership: m, srv: srv, cancel: cancel})
	}
	var all []uint64
	for id := uint64(1); id <= uint64(n); id++ {
		all = append(all, id)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to join", func() bool { return hasMembers(m.membership, all) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := ms[0].changeCore(ctx, core); err != nil {
		t.Fatalf("grow core to %v: %v", core, err)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, fmt.Sprintf("member %d to see core %v", m.self.NodeID, core), func() bool { return hasCore(m.membership, core) })
	}
	return ms
}

func put(ctx context.Context, m *membership, key, val string) error {
	_, err := m.Propose(ctx, []byte(key), func([]byte) ([]byte, error) { return []byte(val), nil })
	return err
}

// Old voters whose run loops stall keep a stale view of the core. One of
// them keeps writing while the driver grows the core from three to five.
// Every write it was told had committed must survive the release and the
// loss of both stale voters.
func TestStaleOldVotersDuringCarry(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A voter MUST reject a data write that names an older core configuration than the roster value it has accepted.
	ms := startFleet(t, 5, []uint64{1, 2, 3})
	driver, reader := ms[2], ms[3]

	// Voters 1 and 2 stop learning the roster.
	ms[0].cancel()
	ms[1].cancel()

	ctx := context.Background()
	stop := make(chan struct{})
	acked := map[string]string{}
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key, val := fmt.Sprintf("fleet/devices/s%04d", i), fmt.Sprintf("v%d", i)
			wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := put(wctx, ms[0].membership, key, val)
			cancel()
			if err == nil {
				mu.Lock()
				acked[key] = val
				mu.Unlock()
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := driver.changeCore(cctx, []uint64{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("grow core to five: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the stale writer run after the release
	close(stop)
	<-done
	waitFor(t, 5*time.Second, "the reader to see core [1 2 3 4 5]", func() bool { return hasCore(reader.membership, []uint64{1, 2, 3, 4, 5}) })

	// Stop both stale voters. 3, 4 and 5 are a quorum of the new core.
	_ = ms[0].srv.Close()
	_ = ms[1].srv.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) == 0 {
		t.Fatal("the stale writer committed nothing; the test has no teeth")
	}
	t.Logf("checking %d acknowledged writes", len(acked))
	var lost []string
	for key, val := range acked {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := reader.Propose(rctx, []byte(key), caspaxos.Identity)
		rcancel()
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if string(got) != val {
			lost = append(lost, key)
		}
	}
	if len(lost) > 0 {
		t.Fatalf("lost %d of %d acknowledged writes across the core change: %v", len(lost), len(acked), lost)
	}
}

// One old voter hangs. The core change must still finish, because a
// majority of the old core answers.
func TestHungOldVoterDoesNotWedgeCoreChange(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	ms := startFleet(t, 5, []uint64{1, 2, 3})
	driver, reader := ms[2], ms[3]
	ctx := context.Background()
	for i := range 5 {
		if err := put(ctx, driver.membership, fmt.Sprintf("fleet/devices/h%d", i), "v"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	hang(ms[1].membership)

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := driver.changeCore(cctx, []uint64{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("grow core to five with voter 2 hung: %v", err)
	}
	waitFor(t, 5*time.Second, "the reader to see core [1 2 3 4 5]", func() bool { return hasCore(reader.membership, []uint64{1, 2, 3, 4, 5}) })
	for i := range 5 {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := reader.Propose(rctx, []byte(fmt.Sprintf("fleet/devices/h%d", i)), caspaxos.Identity)
		rcancel()
		if err != nil || string(got) != "v" {
			t.Fatalf("read h%d = %q, %v; want v", i, got, err)
		}
	}
}

// The second review of PR #117 grew a core that held 300 keys, with 5 ms
// added to every consensus request and a two-second CarryTimeout. The carry
// ran one key at a time under one deadline for the whole change, and each
// resume started again from the first key. So the fleet stayed joint.
func TestCarryTooSlowStillFinishes(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	for _, tc := range []struct {
		name string
		cfg  func(*membershipConfig)
	}{
		{"pool", func(c *membershipConfig) { c.CarryTimeout = 2 * time.Second }},
		// One key at a time takes longer than CarryTimeout in total. The
		// change still finishes, because each key counts as progress.
		{"one worker", func(c *membershipConfig) {
			c.CarryTimeout = time.Second
			c.CarryWorkers = 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { carryTooSlow(t, tc.cfg) })
	}
}

// carryTooSlow writes 300 keys on a one-voter core, adds 5 ms to every
// request, and grows the core to three. The change must finish, and the two
// new voters must hold every key after the founder stops.
func carryTooSlow(t *testing.T, cfg func(*membershipConfig)) {
	const keys = 300
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, stopFounder := startFleetOfThree(t, ctx, cfg)
	founder, second := ms[0], ms[1]
	for i := range keys {
		if err := put(ctx, founder, fmt.Sprintf("fleet/devices/d%03d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	for _, m := range ms {
		slow(m, 5*time.Millisecond)
	}
	cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
	defer ccancel()
	if _, err := founder.changeCore(cctx, []uint64{1, 2, 3}); err != nil {
		t.Logf("first attempt: %v; the run loop resumes the change", err)
	}
	for _, m := range ms {
		waitFor(t, 15*time.Second, fmt.Sprintf("member %d to see core [1 2 3]", m.self.NodeID), func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}
	for _, m := range ms {
		slow(m, 0)
	}

	// Stop the founder. Voters 2 and 3 are a quorum of the new core.
	stopFounder()
	for i := range keys {
		key, val := fmt.Sprintf("fleet/devices/d%03d", i), fmt.Sprintf("v%d", i)
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := second.Propose(rctx, []byte(key), caspaxos.Identity)
		rcancel()
		if err != nil || string(got) != val {
			t.Fatalf("read %s = %q, %v after the founder stopped; want %q", key, got, err, val)
		}
	}
}
