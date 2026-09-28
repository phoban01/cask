package main

// A core change moves the data registers, not only the roster key.
//
// Every data register in the extension server uses the roster core as its
// acceptor set. The roster changes its core in three steps: publish a joint
// core {Old, New}, carry forward, release to New (internal/roster). The
// roster carries its own key, which leaves the joint value on a majority of
// the old core. Then this file carries the data registers:
//
//  1. Publish the joint view on this member, so its own writes use a quorum
//     of both cores. Wait Settle so other members can switch too. The wait
//     is for liveness only: safety comes from the fence (fence.go).
//  2. List the data keys on old voters in parallel. Count only voters whose
//     accepted roster value is the joint one or newer, and stop at a
//     majority of the old core. Each listing is atomic with the voter's
//     fence. So any write that commits on an old quorum without the joint
//     quorum is on a listed voter, or a listed voter rejects it.
//  3. Run a joint Identity round on each listed key
//     (reconfig.CarryForwardPool). It writes the committed value to a quorum
//     of the new core. A write that names the joint configuration already
//     reached a quorum of the new core, so a key created after the listing
//     needs no carry.
//
// The roster then releases the old core. quint/core_change.qnt models the
// rules, with a negative control for each.
//
// A large carry must finish too. Step 3 runs CarryWorkers rounds at once.
// The change fails only when it makes no progress for CarryTimeout, not when
// the whole change takes longer. A resumed change skips the keys that the
// driver already carried under the same joint configuration (carryRecord).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/reconfig"
	"github.com/phoban01/cask/internal/roster"
)

// dataKeysPath lists the keys the member's acceptor holds.
const dataKeysPath = "/roster/keys"

// coreChange is a request to the run loop to move the core to target.
type coreChange struct {
	ctx    context.Context
	target []uint64
	done   chan coreResult
}

type coreResult struct {
	v   roster.Value
	err error
}

// changeCore moves the roster core to target through joint consensus and
// carries every data register with it. Only the driver runs it, inside its
// run loop. Cancelling ctx stops the change; the roster keeps the joint
// value, and the driver's run loop resumes it. The promote endpoint
// (issue #45) will call it.
func (m *membership) changeCore(ctx context.Context, target []uint64) (roster.Value, error) {
	req := coreChange{ctx: ctx, target: target, done: make(chan coreResult, 1)}
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

// runCoreChange is changeCore on the run loop. It stops when the caller's
// context or the run loop's context ends, or when the change makes no
// progress for CarryTimeout.
func (m *membership) runCoreChange(runCtx, reqCtx context.Context, target []uint64) coreResult {
	//= docs/spec/fleet.md#6-membership
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	ctx, cancel := context.WithCancelCause(reqCtx)
	defer cancel(nil)
	stop := context.AfterFunc(runCtx, func() { cancel(context.Cause(runCtx)) })
	defer stop()
	prog := newProgress(m.cfg.CarryTimeout, func() {
		cancel(fmt.Errorf("membership: core change made no progress for %v", m.cfg.CarryTimeout))
	})
	defer prog.stop()
	ctx = context.WithValue(ctx, progressKey{}, prog)

	prev, _ := m.snap.Load()
	if !cluster.IsDriver(m.self.NodeID, prev.Core) {
		return coreResult{err: fmt.Errorf("membership: node %d is not the driver of core %v", m.self.NodeID, prev.Core)}
	}
	v, err := m.rost.Reconfigure(ctx, target)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil && !errors.Is(err, cause) {
			err = fmt.Errorf("%w (%w)", err, cause)
		}
		return coreResult{err: err}
	}
	m.apply(prev, v)
	return coreResult{v: v}
}

// progress cancels a core change that makes no progress for one window. The
// carry calls touch after the key listing and after each key it carries. So
// a large carry that moves keeps going, and a carry that stalls, for example
// because a majority of a core does not answer, ends after one window.
type progress struct {
	mu     sync.Mutex
	t      *time.Timer
	window time.Duration
}

type progressKey struct{}

func newProgress(window time.Duration, stalled func()) *progress {
	return &progress{t: time.AfterFunc(window, stalled), window: window}
}

// progressOf returns the progress of the core change that ctx belongs to.
// It returns nil outside a core change; the methods accept nil.
func progressOf(ctx context.Context) *progress {
	p, _ := ctx.Value(progressKey{}).(*progress)
	return p
}

// touch starts a new window.
func (p *progress) touch() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.t.Reset(p.window)
}

func (p *progress) stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.t.Stop()
}

// carryRecord is the driver's record of the data keys it carried under one
// joint configuration: the roster ConfigGen and the old and new cores. A key
// goes in only after its joint Identity round committed on a quorum of both
// cores. That value then stays on a quorum of the new core: every later
// write names the joint configuration or a newer one, and so reaches a quorum
// of the new core too.
//
// The record is in memory. A restarted driver starts with an empty record
// and carries every listed key again. That is safe, because the Identity
// round writes back the committed value and changes nothing. With the pool
// and the progress rule, the full carry also finishes.
type carryRecord struct {
	mu       sync.Mutex
	gen      uint64
	old, new []uint64
	keys     map[string]struct{}
}

// sameJoint reports whether the record belongs to the joint configuration of
// v. The caller holds mu.
func (r *carryRecord) sameJoint(v roster.Value) bool {
	return r.keys != nil && r.gen == v.ConfigGen &&
		slices.Equal(r.old, v.Joint.Old) && slices.Equal(r.new, v.Joint.New)
}

// pending returns the keys that the driver has not carried under the joint
// configuration of v. It drops a record kept for any other configuration.
func (r *carryRecord) pending(v roster.Value, keys [][]byte) [][]byte {
	//= docs/spec/fleet.md#6-membership
	//# A resumed core change MUST carry every data register that it did not carry under the same joint configuration.
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.sameJoint(v) {
		r.gen, r.old, r.new = v.ConfigGen, slices.Clone(v.Joint.Old), slices.Clone(v.Joint.New)
		r.keys = map[string]struct{}{}
	}
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		if _, ok := r.keys[string(k)]; !ok {
			out = append(out, k)
		}
	}
	return out
}

// mark records that the joint round for key committed under the joint
// configuration of v. It ignores a key from any other configuration.
func (r *carryRecord) mark(v roster.Value, key []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sameJoint(v) {
		r.keys[string(key)] = struct{}{}
	}
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
	keys, err := m.majorityKeys(ctx, v)
	if err != nil {
		return err
	}
	prog := progressOf(ctx)
	prog.touch()

	todo := m.carried.pending(v, keys)
	cctx := ranges.WithClaimedEpoch(ctx, v.ConfigGen)
	//= docs/spec/fleet.md#6-membership
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	err = reconfig.CarryForwardPool(cctx, m.self.NodeID, todo, old, nw, m.cfg.CarryWorkers, func(key []byte) {
		// CarryForwardPool calls this only after the key's joint round
		// committed on a quorum of both cores.
		m.carried.mark(v, key)
		prog.touch()
	})
	if err != nil {
		return fmt.Errorf("membership: carry data registers to core %v: %w", v.Joint.New, err)
	}
	m.log.Info("carried data registers", "keys", len(keys), "skipped", len(keys)-len(todo), "old", v.Joint.Old, "new", v.Joint.New)
	return nil
}

// majorityKeys lists the data keys on a majority of the old core whose
// voters hold the joint roster value. If too few hold it, it reads the
// roster through the joint quorum, which writes the value to the voters
// that answer, and lists again.
func (m *membership) majorityKeys(ctx context.Context, v roster.Value) ([][]byte, error) {
	for {
		keys, err := m.listOnce(ctx, v)
		if err == nil {
			return keys, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w (last: %v)", ctx.Err(), err)
		}
		m.log.Warn("key listing", "err", err)
		_, _ = m.rost.Get(ctx)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w (last: %v)", ctx.Err(), err)
		case <-time.After(m.cfg.Interval):
		}
	}
}

// listOnce asks every old voter at once and returns as soon as a majority
// of them, each holding the joint roster value, has answered.
func (m *membership) listOnce(ctx context.Context, v roster.Value) ([][]byte, error) {
	//= docs/spec/fleet.md#6-membership
	//# A core change MUST finish while a majority of the old core and a majority of the new core answer.
	old := v.Joint.Old
	need := len(old)/2 + 1
	lctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the stragglers once a majority answered

	type answer struct {
		l   keyListing
		err error
	}
	answers := make(chan answer, len(old))
	for _, id := range old {
		go func(id uint64) {
			l, err := m.nodeKeys(lctx, id)
			answers <- answer{l, err}
		}(id)
	}
	union := map[string]struct{}{}
	fresh, stale := 0, 0
	var lastErr error
	for range old {
		var a answer
		select {
		case a = <-answers:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if a.err != nil {
			lastErr = a.err
			continue
		}
		//= docs/spec/fleet.md#6-membership
		//# A core change MUST list data keys only on old voters that have accepted the joint roster value.
		if a.l.Gen < v.ConfigGen {
			stale++
			continue
		}
		fresh++
		for _, k := range a.l.Keys {
			union[string(k)] = struct{}{}
		}
		if fresh >= need {
			out := make([][]byte, 0, len(union))
			for k := range union {
				out = append(out, []byte(k))
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("membership: %d of %d old voters hold the joint roster value, need %d (%d behind; last error: %v)", fresh, len(old), need, stale, lastErr)
}

// nodeKeys lists the keys on one voter: the local acceptor directly, a
// peer through its keys endpoint.
func (m *membership) nodeKeys(ctx context.Context, id uint64) (keyListing, error) {
	if id == m.self.NodeID {
		return m.fence.listKeys(ctx)
	}
	m.mu.Lock()
	addr, ok := m.addrs[id]
	m.mu.Unlock()
	if !ok {
		return keyListing{}, fmt.Errorf("membership: no address for node %d", id)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+dataKeysPath, nil)
	if err != nil {
		return keyListing{}, err
	}
	resp, err := m.cfg.HTTP.Do(req)
	if err != nil {
		return keyListing{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return keyListing{}, fmt.Errorf("membership: keys from node %d: %s", id, resp.Status)
	}
	var l keyListing
	if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
		return keyListing{}, err
	}
	return l, nil
}

// serveDataKeys answers GET /roster/keys with the local acceptor's roster
// ConfigGen and data keys.
func (m *membership) serveDataKeys(w http.ResponseWriter, r *http.Request) {
	l, err := m.fence.listKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(l)
}
