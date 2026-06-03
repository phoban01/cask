package membership

// Plumtree (Epidemic Broadcast Trees, Leitao et al.) disseminates messages over
// the HyParView overlay. Each node splits its neighbours into an EAGER set —
// the spanning-tree edges, over which full payloads are pushed — and a LAZY set,
// over which only message ids (IHAVE) are advertised. A node that receives a
// payload it already has PRUNEs the redundant edge (moving that neighbour to
// lazy); a node that hears an IHAVE for a message it is missing GRAFTs the edge
// back (moving the neighbour to eager and pulling the payload). The eager edges
// thus converge to a spanning tree, while the lazy edges repair it when tree
// branches fail — combining a tree's efficiency with gossip's resilience.
//
// Like the overlay, this is a pure, deterministic state machine: handlers return
// the messages to send, and missing-message timeouts are driven explicitly by
// FireMissing so a simulator controls all timing.

// MsgID identifies a broadcast message.
type MsgID string

// TreeKind enumerates Plumtree message types.
type TreeKind int

const (
	Gossip TreeKind = iota // full payload push along an eager edge
	IHave                  // lazy advertisement of a message id
	Graft                  // request a missing payload, promoting the edge to eager
	Prune                  // demote a redundant eager edge to lazy
)

// TreeMessage is one Plumtree message.
type TreeMessage struct {
	Kind    TreeKind
	From    ID
	To      ID
	ID      MsgID
	Payload []byte
}

// Tree is one node's Plumtree state over its overlay neighbours.
type Tree struct {
	self    ID
	eager   map[ID]bool
	lazy    map[ID]bool
	store   map[MsgID][]byte // payloads we have (to serve grafts)
	missing map[MsgID][]ID   // id -> peers that advertised it, pending graft
	order   []MsgID          // delivery order, for test inspection
}

// NewTree returns a Tree whose neighbours all start as eager (the first
// broadcast floods, then prunes down to a spanning tree).
func NewTree(self ID, peers []ID) *Tree {
	t := &Tree{
		self:    self,
		eager:   map[ID]bool{},
		lazy:    map[ID]bool{},
		store:   map[MsgID][]byte{},
		missing: map[MsgID][]ID{},
	}
	for _, p := range peers {
		if p != self {
			t.eager[p] = true
		}
	}
	return t
}

// Has reports whether this node has delivered message id.
func (t *Tree) Has(id MsgID) bool { _, ok := t.store[id]; return ok }

// Delivered returns the ids delivered to the application, in order.
func (t *Tree) Delivered() []MsgID { return append([]MsgID(nil), t.order...) }

// EagerPeers / LazyPeers expose the tree partition (for inspection/tests).
func (t *Tree) EagerPeers() []ID { return keys(t.eager) }
func (t *Tree) LazyPeers() []ID  { return keys(t.lazy) }

// AddPeer integrates a new overlay neighbour (eager by default); RemovePeer
// drops a departed one. These keep the tree in step with the overlay.
func (t *Tree) AddPeer(p ID) {
	if p != t.self && !t.eager[p] && !t.lazy[p] {
		t.eager[p] = true
	}
}

func (t *Tree) RemovePeer(p ID) {
	delete(t.eager, p)
	delete(t.lazy, p)
}

// Broadcast originates a new message and returns the messages to send.
func (t *Tree) Broadcast(id MsgID, payload []byte) []TreeMessage {
	t.deliver(id, payload)
	return t.spread(id, payload, "")
}

// Receive handles an inbound Plumtree message.
func (t *Tree) Receive(m TreeMessage) []TreeMessage {
	switch m.Kind {
	case Gossip:
		return t.onGossip(m)
	case IHave:
		return t.onIHave(m)
	case Graft:
		return t.onGraft(m)
	case Prune:
		t.eager[m.From], t.lazy[m.From] = false, true
		delete(t.eager, m.From)
		return nil
	}
	return nil
}

// FireMissing issues a GRAFT for every still-missing message that has been
// advertised, promoting the chosen neighbour to eager. The simulator calls it to
// model the IHAVE timeout expiring.
func (t *Tree) FireMissing() []TreeMessage {
	var out []TreeMessage
	for id, peers := range t.missing {
		if t.Has(id) || len(peers) == 0 {
			delete(t.missing, id)
			continue
		}
		peer := peers[0]
		t.missing[id] = peers[1:]
		t.eager[peer] = true
		delete(t.lazy, peer)
		out = append(out, TreeMessage{Kind: Graft, From: t.self, To: peer, ID: id})
	}
	return out
}

func (t *Tree) onGossip(m TreeMessage) []TreeMessage {
	if t.Has(m.ID) {
		// Redundant payload: prune this edge.
		t.eager[m.From] = false
		delete(t.eager, m.From)
		t.lazy[m.From] = true
		return []TreeMessage{{Kind: Prune, From: t.self, To: m.From}}
	}
	t.deliver(m.ID, m.Payload)
	t.eager[m.From] = true // the edge we received on is a tree edge
	delete(t.lazy, m.From)
	delete(t.missing, m.ID)
	return t.spread(m.ID, m.Payload, m.From)
}

func (t *Tree) onIHave(m TreeMessage) []TreeMessage {
	if t.Has(m.ID) {
		return nil
	}
	t.missing[m.ID] = append(t.missing[m.ID], m.From)
	return nil
}

func (t *Tree) onGraft(m TreeMessage) []TreeMessage {
	t.eager[m.From] = true
	delete(t.lazy, m.From)
	if payload, ok := t.store[m.ID]; ok {
		return []TreeMessage{{Kind: Gossip, From: t.self, To: m.From, ID: m.ID, Payload: payload}}
	}
	return nil
}

// spread eager-pushes payload to eager peers and lazy-advertises to lazy peers,
// skipping the peer the message arrived from.
func (t *Tree) spread(id MsgID, payload []byte, from ID) []TreeMessage {
	var out []TreeMessage
	for p := range t.eager {
		if p != from {
			out = append(out, TreeMessage{Kind: Gossip, From: t.self, To: p, ID: id, Payload: payload})
		}
	}
	for p := range t.lazy {
		if p != from {
			out = append(out, TreeMessage{Kind: IHave, From: t.self, To: p, ID: id})
		}
	}
	return out
}

func (t *Tree) deliver(id MsgID, payload []byte) {
	if _, ok := t.store[id]; ok {
		return
	}
	t.store[id] = payload
	t.order = append(t.order, id)
}

func keys(m map[ID]bool) []ID {
	out := make([]ID, 0, len(m))
	for k, v := range m {
		if v {
			out = append(out, k)
		}
	}
	return out
}
