// Package failure provides cask's failure-detection inputs to the consensus
// roster: a phi-accrual detector that turns heartbeat timing into a continuous
// suspicion level, and a Rapid-style multi-observer cut detector that only
// declares a node up/down once several independent observers agree. Keeping this
// separate from the gossip view is deliberate — gossip liveness may flap, but the
// roster (which drives HRW placement and quorum membership) must change only on
// stable, multiply-witnessed evidence.
//
// Time is passed in as an integer count (nanoseconds in production); nothing here
// reads a wall clock, so detectors are deterministic and unit-testable.
package failure

import "math"

// Node identifies a monitored peer.
type Node = string

// Detector is a phi-accrual failure detector (Hayashibara et al.) over many
// peers. Phi rises smoothly as a heartbeat becomes overdue relative to the
// observed inter-arrival distribution, so a single threshold trades detection
// speed against false positives without per-network tuning.
type Detector struct {
	window int     // number of recent intervals to keep per peer
	minStd float64 // floor on standard deviation (avoids divide-by-zero / overconfidence)
	peers  map[Node]*peerState
}

type peerState struct {
	intervals []float64
	last      int64
	have      bool
}

// New returns a Detector keeping the last window inter-arrival samples per peer
// and clamping the interval standard deviation to at least minStd.
func New(window int, minStd float64) *Detector {
	if window < 2 {
		window = 2
	}
	if minStd <= 0 {
		minStd = 1
	}
	return &Detector{window: window, minStd: minStd, peers: map[Node]*peerState{}}
}

// Heartbeat records that a heartbeat from peer arrived at time now.
func (d *Detector) Heartbeat(peer Node, now int64) {
	s := d.peers[peer]
	if s == nil {
		s = &peerState{}
		d.peers[peer] = s
	}
	if s.have {
		s.intervals = append(s.intervals, float64(now-s.last))
		if len(s.intervals) > d.window {
			s.intervals = s.intervals[len(s.intervals)-d.window:]
		}
	}
	s.last = now
	s.have = true
}

// Phi returns the current suspicion level for peer at time now. It is 0 for a
// peer with no samples yet (we cannot suspect what we have never heard from).
func (d *Detector) Phi(peer Node, now int64) float64 {
	s := d.peers[peer]
	if s == nil || !s.have || len(s.intervals) == 0 {
		return 0
	}
	elapsed := float64(now - s.last)
	mean, std := meanStd(s.intervals, d.minStd)
	return phi(elapsed, mean, std)
}

// Suspected returns every peer whose phi is at or above threshold at time now.
func (d *Detector) Suspected(now int64, threshold float64) []Node {
	var out []Node
	for peer := range d.peers {
		if d.Phi(peer, now) >= threshold {
			out = append(out, peer)
		}
	}
	return out
}

// Forget drops all state for peer (e.g. once it is removed from the roster).
func (d *Detector) Forget(peer Node) { delete(d.peers, peer) }

// Snapshot returns the current phi for every tracked peer at time now.
func (d *Detector) Snapshot(now int64) map[Node]float64 {
	out := make(map[Node]float64, len(d.peers))
	for p := range d.peers {
		out[p] = d.Phi(p, now)
	}
	return out
}

func meanStd(xs []float64, minStd float64) (mean, std float64) {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean = sum / float64(len(xs))
	var varSum float64
	for _, x := range xs {
		varSum += (x - mean) * (x - mean)
	}
	std = math.Sqrt(varSum / float64(len(xs)))
	if std < minStd {
		std = minStd
	}
	return mean, std
}

// phi = -log10(P(later than elapsed)) using the logistic approximation to the
// normal CDF (as in Akka's implementation), which is cheap and accurate enough.
func phi(elapsed, mean, std float64) float64 {
	y := (elapsed - mean) / std
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	var p float64
	if elapsed > mean {
		p = e / (1 + e)
	} else {
		p = 1 - 1/(1+e)
	}
	if p < 1e-10 {
		p = 1e-10
	}
	return -math.Log10(p)
}
