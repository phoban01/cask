// Package membership implements cask's liveness/dissemination plane: an in-house
// HyParView partial-view overlay (this file) and, layered on it, Plumtree
// broadcast (later). Partial views give each node O(log N) state regardless of
// cluster size — the property that lets the agent tier reach 10k+ nodes where a
// full-membership protocol (every node knows every node) would not.
//
// The protocol is a pure, deterministic state machine: every handler takes an
// input and returns the messages to send, with randomness drawn from an injected
// source. There is no I/O or time here, so an entire overlay can be driven by a
// seeded simulator and checked for the properties that matter — a connected
// overlay with bounded views, and self-healing after node failure. This
// discipline is deliberate: HyParView's correctness under failure and asymmetry
// is subtle, and a deterministic harness is the only credible way to trust it.
//
// Active links are kept symmetric by a NEIGHBOR handshake: to take a peer into
// its active view a node requests it, and the peer adds the requester back when
// it accepts. This avoids the asymmetric-link hazard the HyParView paper warns
// about.
package membership

import "math/rand"

// ID identifies a node (in practice its address).
type ID string

// Config bounds the two partial views and the random-walk lengths. Defaults
// follow the HyParView paper's sizing for ~10k nodes.
type Config struct {
	ActiveSize  int // active view cap (~log N, e.g. 5)
	PassiveSize int // passive view cap (~k·log N, e.g. 30)
	ARWL        int // active random walk length for ForwardJoin
	PRWL        int // passive random walk length (when a ForwardJoin adds to passive)
	ShuffleK    int // number of ids contributed in a shuffle
}

// DefaultConfig returns the paper's sizing for a large cluster.
func DefaultConfig() Config {
	return Config{ActiveSize: 5, PassiveSize: 30, ARWL: 6, PRWL: 3, ShuffleK: 4}
}

// Kind enumerates the overlay message types.
type Kind int

const (
	Join          Kind = iota // a node asks a contact to admit it
	ForwardJoin               // disseminate a join along a random walk
	Neighbor                  // request to establish a symmetric active link
	NeighborReply             // accept/deny a Neighbor request
	Disconnect                // tear down an active link
	Shuffle                   // offer a sample of views for passive-view mixing
	ShuffleReply              // answer a Shuffle with a counter-sample
)

// Message is one overlay message. Not every field is meaningful for every Kind.
type Message struct {
	Kind   Kind
	From   ID
	To     ID
	Node   ID   // subject node (Join/ForwardJoin)
	TTL    int  // remaining walk length (ForwardJoin)
	High   bool // Neighbor: high priority; NeighborReply: accepted
	Sample []ID // Shuffle/ShuffleReply: offered ids
}

// HyParView is one node's partial-view state.
type HyParView struct {
	self    ID
	cfg     Config
	active  []ID
	passive []ID
	rng     *rand.Rand
}

// New returns a node's membership state seeded for deterministic randomness.
func New(self ID, cfg Config, seed int64) *HyParView {
	return &HyParView{self: self, cfg: cfg, rng: rand.New(rand.NewSource(seed))}
}

// ActiveView returns a copy of the active view.
func (h *HyParView) ActiveView() []ID { return append([]ID(nil), h.active...) }

// PassiveView returns a copy of the passive view.
func (h *HyParView) PassiveView() []ID { return append([]ID(nil), h.passive...) }

// Self returns this node's id.
func (h *HyParView) Self() ID { return h.self }

// Join begins joining the overlay through contact.
func (h *HyParView) Join(contact ID) []Message {
	return []Message{{Kind: Join, From: h.self, To: contact, Node: h.self}}
}

// Receive handles an inbound message and returns messages to send.
func (h *HyParView) Receive(m Message) []Message {
	switch m.Kind {
	case Join:
		return h.onJoin(m)
	case ForwardJoin:
		return h.onForwardJoin(m)
	case Neighbor:
		return h.onNeighbor(m)
	case NeighborReply:
		return h.onNeighborReply(m)
	case Disconnect:
		return h.onDisconnect(m)
	case Shuffle:
		return h.onShuffle(m)
	case ShuffleReply:
		return h.onShuffleReply(m)
	}
	return nil
}

// Down reports that an active neighbor has failed. The node drops it and tries
// to refill the active view from the passive view.
func (h *HyParView) Down(peer ID) []Message {
	hadActive := contains(h.active, peer)
	h.active = remove(h.active, peer)
	h.passive = remove(h.passive, peer)
	if !hadActive {
		return nil
	}
	return h.refill()
}

// --- handlers ---------------------------------------------------------------

func (h *HyParView) onJoin(m Message) []Message {
	out := h.tryActive(m.Node, true) // symmetric link with the joiner
	// Disseminate the join along a random walk to the rest of the overlay.
	for _, n := range h.active {
		if n == m.Node {
			continue
		}
		out = append(out, Message{Kind: ForwardJoin, From: h.self, To: n, Node: m.Node, TTL: h.cfg.ARWL})
	}
	return out
}

func (h *HyParView) onForwardJoin(m Message) []Message {
	// Walk ends: either the TTL is exhausted or we have no one else to forward
	// to. Admit the new node into the active view.
	if m.TTL == 0 || len(h.active) <= 1 {
		return h.tryActive(m.Node, true)
	}
	var out []Message
	if m.TTL == h.cfg.PRWL {
		h.addPassive(m.Node)
	}
	next := h.randomExcept(h.active, m.From)
	if next == "" {
		return h.tryActive(m.Node, true)
	}
	out = append(out, Message{Kind: ForwardJoin, From: h.self, To: next, Node: m.Node, TTL: m.TTL - 1})
	return out
}

func (h *HyParView) onNeighbor(m Message) []Message {
	if !m.High && len(h.active) >= h.cfg.ActiveSize {
		return []Message{{Kind: NeighborReply, From: h.self, To: m.From, High: false}}
	}
	out := h.addActive(m.From)
	return append(out, Message{Kind: NeighborReply, From: h.self, To: m.From, High: true})
}

func (h *HyParView) onNeighborReply(m Message) []Message {
	if !m.High {
		return nil // denied; leave the candidate in the passive view
	}
	return h.addActive(m.From)
}

func (h *HyParView) onDisconnect(m Message) []Message {
	if !contains(h.active, m.From) {
		return nil
	}
	h.active = remove(h.active, m.From)
	h.addPassive(m.From)
	return nil
}

func (h *HyParView) onShuffle(m Message) []Message {
	// Integrate the offered sample into the passive view, and reply with our own
	// sample so the exchange is mutual.
	reply := h.sample(h.cfg.ShuffleK, m.From, m.Node)
	for _, id := range m.Sample {
		h.addPassive(id)
	}
	return []Message{{Kind: ShuffleReply, From: h.self, To: m.From, Sample: reply}}
}

func (h *HyParView) onShuffleReply(m Message) []Message {
	for _, id := range m.Sample {
		h.addPassive(id)
	}
	return nil
}

// Heal tops the active view back up to capacity from the passive view. It is
// safe to call periodically; it is a no-op when the active view is already full.
func (h *HyParView) Heal() []Message { return h.refill() }

// Shuffle starts a passive-view exchange with a random active neighbor.
func (h *HyParView) Shuffle() []Message {
	target := h.random(h.active)
	if target == "" {
		return nil
	}
	return []Message{{Kind: Shuffle, From: h.self, To: target, Node: h.self, Sample: h.sample(h.cfg.ShuffleK, target)}}
}

// --- view maintenance -------------------------------------------------------

// tryActive initiates a symmetric active link with node via the NEIGHBOR
// handshake (no-op if it is self or already active).
func (h *HyParView) tryActive(node ID, high bool) []Message {
	if node == h.self || contains(h.active, node) {
		return nil
	}
	return []Message{{Kind: Neighbor, From: h.self, To: node, High: high}}
}

// addActive adds node to the active view, evicting a random neighbor if full.
func (h *HyParView) addActive(node ID) []Message {
	if node == h.self || contains(h.active, node) {
		return nil
	}
	var out []Message
	if len(h.active) >= h.cfg.ActiveSize {
		drop := h.random(h.active)
		h.active = remove(h.active, drop)
		h.addPassive(drop)
		out = append(out, Message{Kind: Disconnect, From: h.self, To: drop})
	}
	h.active = append(h.active, node)
	h.passive = remove(h.passive, node) // promoted out of passive
	return out
}

// addPassive adds node to the passive view, evicting a random entry if full.
func (h *HyParView) addPassive(node ID) {
	if node == h.self || contains(h.active, node) || contains(h.passive, node) {
		return
	}
	if len(h.passive) >= h.cfg.PassiveSize {
		h.passive = remove(h.passive, h.random(h.passive))
	}
	h.passive = append(h.passive, node)
}

// refill tops the active view back up to capacity from the passive view,
// preferring high priority when the active view is empty (so an isolated node
// can force its way back in).
func (h *HyParView) refill() []Message {
	var out []Message
	tried := map[ID]bool{}
	for len(h.active)+len(tried) < h.cfg.ActiveSize {
		cand := h.randomPassiveNotIn(tried)
		if cand == "" {
			break
		}
		tried[cand] = true
		high := len(h.active) == 0 && len(tried) == 1
		out = append(out, h.tryActive(cand, high)...)
	}
	return out
}

// sample returns up to ShuffleK ids drawn from this node plus its views,
// excluding the given ids (typically the shuffle peer and originator).
func (h *HyParView) sample(k int, exclude ...ID) []ID {
	pool := append([]ID{h.self}, h.active...)
	pool = append(pool, h.passive...)
	ex := map[ID]bool{}
	for _, e := range exclude {
		ex[e] = true
	}
	var cand []ID
	for _, id := range pool {
		if !ex[id] {
			cand = append(cand, id)
		}
	}
	h.rng.Shuffle(len(cand), func(i, j int) { cand[i], cand[j] = cand[j], cand[i] })
	if k < len(cand) {
		cand = cand[:k]
	}
	return cand
}

// --- small helpers ----------------------------------------------------------

func (h *HyParView) random(xs []ID) ID {
	if len(xs) == 0 {
		return ""
	}
	return xs[h.rng.Intn(len(xs))]
}

func (h *HyParView) randomExcept(xs []ID, ex ID) ID {
	var c []ID
	for _, x := range xs {
		if x != ex {
			c = append(c, x)
		}
	}
	return h.random(c)
}

func (h *HyParView) randomPassiveNotIn(seen map[ID]bool) ID {
	var c []ID
	for _, x := range h.passive {
		if !seen[x] && !contains(h.active, x) {
			c = append(c, x)
		}
	}
	return h.random(c)
}

func contains(xs []ID, x ID) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func remove(xs []ID, x ID) []ID {
	for i, v := range xs {
		if v == x {
			return append(xs[:i], xs[i+1:]...)
		}
	}
	return xs
}
