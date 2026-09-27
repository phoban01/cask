// Package jepsen holds in-process, Jepsen-style correctness gates: many clients
// hammer a primitive under a fault nemesis, and the recorded history is checked
// against the property that matters. This file is the lease/lock RELEASE GATE —
// the fencing-token monotonicity check Jepsen's lock workload uses (Kleppmann's
// fencing): if one acquire completes before another begins, the earlier must
// have a strictly smaller token. A real out-of-process Jepsen harness (Clojure,
// against a running cask binary) lives under jepsen/; this gate runs the same
// invariant deterministically in-process so CI can enforce it cheaply.
package jepsen

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

type acquireRec struct {
	client int
	token  uint64
	call   int64
	ret    int64
}

func TestLockFencingUnderPartition(t *testing.T) {
	ctx := context.Background()

	const (
		rf      = 5
		clients = 4
		opsEach = 60
		maxDown = 2 // a minority outage: a quorum always remains
	)

	stores := make([]caspaxos.Storage, rf)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)

	// A constant lease clock with a huge TTL: sessions never expire, so the lock
	// changes hands only by explicit release under contention — this isolates the
	// gate to fencing under partitions (session expiry is covered in M7).
	const ttl = 1 << 60
	leaseNow := func() int64 { return 1 }

	var evClock int64
	now := func() int64 { return atomic.AddInt64(&evClock, 1) }

	// Nemesis: roll a random minority partition, then heal.
	stop := make(chan struct{})
	var nwg sync.WaitGroup
	nwg.Add(1)
	go func() {
		defer nwg.Done()
		rng := rand.New(rand.NewSource(0xF00D))
		for {
			select {
			case <-stop:
				nw.Heal()
				return
			default:
			}
			down := rng.Intn(maxDown + 1)
			perm := rng.Perm(rf)
			for i := 0; i < down; i++ {
				nw.SetReachable(perm[i], false)
			}
			nw.Heal()
		}
	}()

	recs := make([][]acquireRec, clients)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			prop := caspaxos.NewProposer(uint64(cid+1), nw.Clients())
			ss := lease.NewSessions(prop, leaseNow)
			lk := lease.NewLocks(prop, ss)
			sess := sessionID(cid)
			ss.Grant(ctx, sess, sess, ttl)

			var local []acquireRec
			for i := 0; i < opsEach; i++ {
				call := now()
				tok, err := lk.Acquire(ctx, "global-lock", sess)
				if err != nil {
					continue // held by another, or partition transient
				}
				ret := now()
				local = append(local, acquireRec{client: cid, token: tok, call: call, ret: ret})
				// A release can fail under partition and leave the lock
				// held. The next acquire is then re-entrant and returns
				// the same token (issue #120). Retry until the release
				// lands, so every acquire above starts a new tenure.
				if err := releaseUntilDone(ctx, lk, ss, sess); err != nil {
					t.Errorf("c%d: release: %v", cid, err)
					return
				}
			}
			recs[cid] = local
		}(c)
	}
	wg.Wait()
	close(stop)
	nwg.Wait()

	var all []acquireRec
	for _, r := range recs {
		all = append(all, r...)
	}
	if len(all) < 30 {
		t.Fatalf("history too thin (%d acquires); gate would be vacuous", len(all))
	}

	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# An acquire by the session that already holds the lock MUST return the current fence and MUST NOT mint a new one.

	// Invariant 1: a token belongs to one holder tenure. Only a re-entrant
	// acquire by the same client may repeat it. Invariant 2 then shows that
	// no other holder came in between.
	seen := map[uint64]int{}
	for _, r := range all {
		if prev, dup := seen[r.token]; dup && prev != r.client {
			t.Fatalf("token %d issued to clients %d and %d", r.token, prev, r.client)
		}
		seen[r.token] = r.client
	}

	// Invariant 2 (fencing monotonicity): if acquire i completes before acquire j
	// begins (non-overlapping in real time), token(i) < token(j). The one
	// exception is a re-entrant acquire in the same tenure: same client, same
	// token. Any other holder between them has a higher token, so this check
	// still fails when the client acquires again after that holder.
	for i := range all {
		for j := range all {
			sameTenure := all[i].client == all[j].client && all[i].token == all[j].token
			if all[i].ret < all[j].call && !(all[i].token < all[j].token) && !sameTenure {
				t.Fatalf("fencing violated: acquire by c%d (token %d, ret %d) precedes "+
					"c%d (token %d, call %d) but token did not increase",
					all[i].client, all[i].token, all[i].ret,
					all[j].client, all[j].token, all[j].call)
			}
		}
	}
	t.Logf("verified %d acquires across %d clients under partition nemesis", len(all), clients)
}

func sessionID(c int) string { return "sess-" + string(rune('A'+c)) }

// releaseUntilDone retries Release until it succeeds or the session is no
// longer live. A dead session cannot hold the lock, so there is then nothing
// left to release.
func releaseUntilDone(ctx context.Context, lk *lease.Locks, ss *lease.Sessions, sess string) error {
	const maxAttempts = 1000
	var err error
	for range maxAttempts {
		if err = lk.Release(ctx, "global-lock", sess); err == nil {
			return nil
		}
		if live, lerr := ss.Live(ctx, sess); lerr == nil && !live {
			return nil
		}
	}
	return err
}
