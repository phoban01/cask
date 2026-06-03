package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/failure"
	"github.com/phoban01/cask/internal/roster"
)

// probeTimeout bounds a single /health probe. It must be well under
// reconcileInterval: the overlay HTTP client has no timeout of its own, so
// without this a probe to a dead peer would hang forever and freeze the whole
// reconcile loop (the failure detector would then never fire).
const probeTimeout = 2 * time.Second

// phiThreshold is the phi-accrual suspicion level at which this node locally
// considers a peer unreachable. ~8 corresponds to a ~1e-8 chance the silence is
// explained by normal jitter, so a peer must be several heartbeat intervals
// overdue before we suspect it.
const phiThreshold = 8.0

// healthMonitor turns overlay reachability into roster removals. Each reconcile
// tick it probes every member's /health endpoint: a reply is a heartbeat (fed to
// a phi-accrual detector) and also carries that peer's own suspicion vector. It
// then runs a majority cut detector over all observers, so a node is condemned
// only when a strict majority of the roster independently suspects it — never on
// one node's flaky link. This is the evidence the roster Controller's `down` set
// requires.
type healthMonitor struct {
	self uint64
	det  *failure.Detector
	hc   *http.Client

	mu       sync.Mutex
	suspects []uint64           // self's current suspicions, published at /health
	phi      map[string]float64 // last computed phi per peer (diagnostics)
}

func newHealthMonitor(self uint64, hc *http.Client) *healthMonitor {
	// window of 8 intervals; floor std at a quarter interval so steady gaps
	// don't make us overconfident.
	return &healthMonitor{
		self: self,
		det:  failure.New(8, float64(reconcileInterval)/4),
		hc:   hc,
	}
}

type healthReport struct {
	Node    uint64             `json:"node"`
	Suspect []uint64           `json:"suspect"`
	Phi     map[string]float64 `json:"phi,omitempty"` // diagnostics: phi per peer
}

// serveHealth publishes this node's suspicion vector so peers can aggregate it
// as an independent observer.
func (h *healthMonitor) serveHealth(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	s := append([]uint64(nil), h.suspects...)
	phi := make(map[string]float64, len(h.phi))
	for k, v := range h.phi {
		phi[k] = v
	}
	h.mu.Unlock()
	_ = json.NewEncoder(w).Encode(healthReport{Node: h.self, Suspect: s, Phi: phi})
}

// forget drops all detector state for the given peers (called when they leave
// the roster) so a later rejoin with the same id starts clean rather than
// inheriting stale high-phi state. Called from the reconcile goroutine, the same
// one that runs scan, so it does not race on the detector.
func (h *healthMonitor) forget(ids []uint64) {
	for _, id := range ids {
		h.det.Forget(strconv.FormatUint(id, 10))
	}
	h.mu.Lock()
	for _, id := range ids {
		delete(h.phi, strconv.FormatUint(id, 10))
	}
	h.mu.Unlock()
}

// scan probes peers, updates suspicion, and returns the node ids a majority of
// observers agree are down (excluding self). `now` is a nanosecond clock.
func (h *healthMonitor) scan(ctx context.Context, members []roster.Member, now int64) []uint64 {
	type res struct {
		id  uint64
		ok  bool
		rep healthReport
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []res
	)
	for _, m := range members {
		if m.NodeID == h.self {
			continue
		}
		m := m
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, ok := h.probe(ctx, m.Addr)
			mu.Lock()
			results = append(results, res{id: m.NodeID, ok: ok, rep: rep})
			mu.Unlock()
		}()
	}
	wg.Wait()

	for _, r := range results {
		if r.ok {
			h.det.Heartbeat(strconv.FormatUint(r.id, 10), now)
		}
	}

	// This node's own opinion, derived from phi over observed reachability.
	var local []uint64
	for _, m := range members {
		if m.NodeID == h.self {
			continue
		}
		if h.det.Phi(strconv.FormatUint(m.NodeID, 10), now) >= phiThreshold {
			local = append(local, m.NodeID)
		}
	}
	h.mu.Lock()
	h.suspects = local
	h.phi = h.det.Snapshot(now)
	h.mu.Unlock()

	// Aggregate observers: self plus every peer we could reach. A strict
	// majority of the roster must agree before a node is condemned.
	cut := failure.NewCutDetector(len(members)/2 + 1)
	self := strconv.FormatUint(h.self, 10)
	for _, s := range local {
		cut.Report(self, strconv.FormatUint(s, 10), true)
	}
	for _, r := range results {
		if !r.ok {
			continue
		}
		obs := strconv.FormatUint(r.id, 10)
		for _, s := range r.rep.Suspect {
			cut.Report(obs, strconv.FormatUint(s, 10), true)
		}
	}

	var down []uint64
	for _, n := range cut.Down() {
		if id, err := strconv.ParseUint(n, 10, 64); err == nil && id != h.self {
			down = append(down, id)
		}
	}
	return down
}

func (h *healthMonitor) probe(ctx context.Context, addr string) (healthReport, bool) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		return healthReport{}, false
	}
	resp, err := h.hc.Do(req)
	if err != nil {
		return healthReport{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return healthReport{}, false
	}
	var rep healthReport
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		return healthReport{}, false
	}
	return rep, true
}
