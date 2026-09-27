package main

// A core change moves the data registers, not only the roster key.
//
// Every data register in the extension server uses the roster core as its
// acceptor set. The roster changes its core in three steps: publish a joint
// core {Old, New}, carry forward, release to New (internal/roster). The
// roster carries its own key. This file carries the data registers, the way
// internal/ranges carries a range:
//
//  1. Publish the joint view on this member. From here this member proposes
//     to a quorum of both cores, and its acceptor rejects a write that names
//     an older core configuration (the write fence).
//  2. Wait for every member to see the joint view (Settle). After that no
//     write can commit on the old core alone, because every old-core voter
//     rejects it.
//  3. List the keys on a majority of the old core. Every committed key is on
//     every majority, so the union has every committed key.
//  4. Run a joint Identity round on each key (reconfig.CarryForwardKeys). It
//     writes the committed value to a quorum of the new core.
//
// The roster then releases the old core. quint/core_change.qnt models the
// rules, with a negative control for each.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/reconfig"
	"github.com/phoban01/cask/internal/roster"
)

// dataKeysPath lists the keys the member's acceptor holds.
const dataKeysPath = "/roster/keys"

// errStaleAccept stands in for a fenced accept. The proposer counts it as
// a missing vote, so a fenced accept never makes it run change again.
var errStaleAccept = errors.New("membership: accept fenced by a newer core")

// coreChange is a request to the run loop to move the core to target.
type coreChange struct {
	target []uint64
	done   chan coreResult
}

type coreResult struct {
	v   roster.Value
	err error
}

// changeCore moves the roster core to target through joint consensus and
// carries every data register with it. Only the driver runs it, inside its
// run loop. The promote endpoint (issue #45) will call it.
func (m *membership) changeCore(ctx context.Context, target []uint64) (roster.Value, error) {
	req := coreChange{target: target, done: make(chan coreResult, 1)}
	select {
	case m.coreReqs <- req:
	case <-ctx.Done():
		return roster.Value{}, ctx.Err()
	}
	select {
	case r := <-req.done:
		return r.v, r.err
	case <-ctx.Done():
		return roster.Value{}, ctx.Err()
	}
}

// runCoreChange is changeCore on the run loop.
func (m *membership) runCoreChange(ctx context.Context, target []uint64) coreResult {
	prev, _ := m.snap.Load()
	if !cluster.IsDriver(m.self.NodeID, prev.Core) {
		return coreResult{err: fmt.Errorf("membership: node %d is not the driver of core %v", m.self.NodeID, prev.Core)}
	}
	v, err := m.rost.Reconfigure(ctx, target)
	if err != nil {
		return coreResult{err: err}
	}
	m.apply(prev, v)
	return coreResult{v: v}
}

// carry is the roster's CarryFunc. v is the joint roster value.
func (m *membership) carry(ctx context.Context, v roster.Value) error {
	//= docs/spec/fleet.md#6-membership
	//# A core change MUST carry every data register forward to the new core before it releases the old core.
	if v.Joint == nil {
		return nil
	}
	m.learn(v.Members)
	m.store(v)
	t := time.NewTimer(m.cfg.Settle)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C:
	}

	old, err := m.allClients(v.Joint.Old)
	if err != nil {
		return err
	}
	nw, err := m.allClients(v.Joint.New)
	if err != nil {
		return err
	}
	keys, err := m.majorityKeys(ctx, v.Joint.Old)
	if err != nil {
		return err
	}
	cctx := ranges.WithClaimedEpoch(ctx, v.ConfigGen)
	if err := reconfig.CarryForwardKeys(cctx, m.self.NodeID, keys, old, nw); err != nil {
		return fmt.Errorf("membership: carry data registers to core %v: %w", v.Joint.New, err)
	}
	m.log.Info("carried data registers", "keys", len(keys), "old", v.Joint.Old, "new", v.Joint.New)
	return nil
}

// majorityKeys returns the union of the data keys held by at least a
// majority of the old core. The roster key is left out: the roster carries
// it itself.
func (m *membership) majorityKeys(ctx context.Context, old []uint64) ([][]byte, error) {
	need := len(old)/2 + 1
	got := 0
	union := map[string]struct{}{}
	var lastErr error
	for _, id := range old {
		keys, err := m.nodeKeys(ctx, id)
		if err != nil {
			lastErr = err
			continue
		}
		got++
		for _, k := range keys {
			if bytes.HasPrefix(k, roster.Key) {
				continue
			}
			union[string(k)] = struct{}{}
		}
	}
	if got < need {
		return nil, fmt.Errorf("membership: listed keys on %d of %d old voters, need %d: %w", got, len(old), need, lastErr)
	}
	out := make([][]byte, 0, len(union))
	for k := range union {
		out = append(out, []byte(k))
	}
	return out, nil
}

// nodeKeys lists the keys on one voter: the local store directly, a peer
// through its keys endpoint.
func (m *membership) nodeKeys(ctx context.Context, id uint64) ([][]byte, error) {
	if id == m.self.NodeID {
		if m.cfg.Keys == nil {
			return nil, errors.New("membership: local store cannot list keys")
		}
		return m.cfg.Keys(ctx)
	}
	m.mu.Lock()
	addr, ok := m.addrs[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("membership: no address for node %d", id)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+dataKeysPath, nil)
	if err != nil {
		return nil, err
	}
	resp, err := m.cfg.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("membership: keys from node %d: %s", id, resp.Status)
	}
	var keys [][]byte
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// serveDataKeys answers GET /roster/keys with the local acceptor's keys.
func (m *membership) serveDataKeys(w http.ResponseWriter, r *http.Request) {
	if m.cfg.Keys == nil {
		http.Error(w, "store cannot list keys", http.StatusNotImplemented)
		return
	}
	keys, err := m.cfg.Keys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(keys)
}

// coreGen is the write fence: the core configuration (ConfigGen) this
// member knows, for every data key. The transport rejects a data write that
// names an older one. The roster key has its own ConfigGen guard, so it is
// not fenced here.
func (m *membership) coreGen(key []byte) (uint64, bool) {
	//= docs/spec/fleet.md#6-membership
	//# A voter MUST reject a data write that names an older core configuration than the one it knows.
	if bytes.HasPrefix(key, roster.Key) {
		return 0, false
	}
	v, ok := m.snap.Load()
	if !ok {
		return 0, false
	}
	return v.ConfigGen, true
}

// fencedClient wraps an acceptor client. With gen set it applies the write
// fence itself, for the local acceptor, which no transport handler guards.
// It also turns a fenced accept into a missing vote. A fenced prepare stays
// caspaxos.ErrRangeChanged: it ends the round before change runs, so the
// caller may refresh its core view and run again. A fenced accept may come
// after other acceptors took the value, and running change again then would
// apply it twice. As a missing vote it ends in ErrUnknownOutcome instead,
// which callers already handle by re-reading.
type fencedClient struct {
	caspaxos.AcceptorClient
	gen func(key []byte) (uint64, bool)
}

func (c *fencedClient) fenced(ctx context.Context, key []byte) bool {
	if c.gen == nil {
		return false
	}
	claimed, ok := ranges.ClaimedEpoch(ctx)
	if !ok {
		return false
	}
	cur, ok := c.gen(key)
	return ok && claimed < cur
}

func (c *fencedClient) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	if c.fenced(ctx, key) {
		return caspaxos.PrepareReply{}, caspaxos.ErrRangeChanged
	}
	return c.AcceptorClient.Prepare(ctx, key, b)
}

func (c *fencedClient) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	if c.fenced(ctx, key) {
		return caspaxos.AcceptReply{}, errStaleAccept
	}
	r, err := c.AcceptorClient.Accept(ctx, key, b, val)
	if errors.Is(err, caspaxos.ErrRangeChanged) {
		return caspaxos.AcceptReply{}, errStaleAccept
	}
	return r, err
}
