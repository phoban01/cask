// Package roster holds cask's authoritative cluster membership in a CASPaxos
// register. This is the stable source of truth that HRW placement and per-range
// quorums are computed over — deliberately distinct from the gossip/liveness
// view, which may flap. A node is added or removed here only by consensus, so a
// transient gossip false positive can never destabilise placement; only a
// multiply-witnessed, committed change does.
package roster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Member is one cluster node: its consensus/placement id, its address (for the
// dialer and membership plane), and its failure domain (for zone-aware placement).
type Member struct {
	NodeID uint64 `json:"node"`
	Addr   string `json:"addr"`
	Zone   string `json:"zone"`
}

// Value is the register's contents: the membership set plus a configuration
// epoch bumped on every change.
type Value struct {
	Epoch   uint64   `json:"epoch"`
	Members []Member `json:"members"`
}

// Proposer is the consensus operation the roster needs (satisfied by
// *caspaxos.Proposer and by the agent Router).
type Proposer interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// Key is the reserved register key the roster lives at.
var Key = []byte("\x00roster")

// Roster is a handle to the membership register.
type Roster struct {
	prop Proposer
	key  []byte
}

// New returns a Roster proposing through prop at the default key.
func New(prop Proposer) *Roster { return &Roster{prop: prop, key: Key} }

// Genesis installs the initial membership if the register is empty. It is a
// no-op (returning the existing value) if a roster already exists, so concurrent
// seeders converge instead of clobbering one another.
func (r *Roster) Genesis(ctx context.Context, members []Member) (Value, error) {
	return r.commit(ctx, func(cur Value, present bool) (Value, error) {
		if present {
			return cur, nil
		}
		return Value{Epoch: 1, Members: normalize(members)}, nil
	})
}

// Add inserts m (idempotent: re-adding an existing node id is a no-op).
func (r *Roster) Add(ctx context.Context, m Member) (Value, error) {
	return r.commit(ctx, func(cur Value, present bool) (Value, error) {
		if !present {
			return Value{}, fmt.Errorf("roster: not initialised")
		}
		for _, e := range cur.Members {
			if e.NodeID == m.NodeID {
				return cur, nil
			}
		}
		cur.Members = normalize(append(cur.Members, m))
		cur.Epoch++
		return cur, nil
	})
}

// Remove deletes the node with the given id (idempotent if absent).
func (r *Roster) Remove(ctx context.Context, nodeID uint64) (Value, error) {
	return r.commit(ctx, func(cur Value, present bool) (Value, error) {
		if !present {
			return Value{}, fmt.Errorf("roster: not initialised")
		}
		out := cur.Members[:0:0]
		removed := false
		for _, e := range cur.Members {
			if e.NodeID == nodeID {
				removed = true
				continue
			}
			out = append(out, e)
		}
		if !removed {
			return cur, nil
		}
		cur.Members = out
		cur.Epoch++
		return cur, nil
	})
}

// Get returns the current membership (linearizable read).
func (r *Roster) Get(ctx context.Context) (Value, error) {
	raw, err := r.prop.Propose(ctx, r.key, caspaxos.Identity)
	if err != nil {
		return Value{}, err
	}
	return decode(raw)
}

// NodeIDs returns the member node ids in sorted order — the input to HRW
// placement.
func (r *Roster) NodeIDs(ctx context.Context) ([]uint64, error) {
	v, err := r.Get(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.NodeID
	}
	return ids, nil
}

// commit applies mutate as a CASPaxos change on the register and returns the
// resulting value.
func (r *Roster) commit(ctx context.Context, mutate func(cur Value, present bool) (Value, error)) (Value, error) {
	raw, err := r.prop.Propose(ctx, r.key, func(current []byte) ([]byte, error) {
		cur, err := decode(current)
		if err != nil {
			return nil, err
		}
		next, err := mutate(cur, len(current) > 0)
		if err != nil {
			return nil, err
		}
		return encode(next)
	})
	if err != nil {
		return Value{}, err
	}
	return decode(raw)
}

func normalize(ms []Member) []Member {
	// Dedup by node id (last wins) and sort, so the encoded value is canonical.
	seen := map[uint64]Member{}
	for _, m := range ms {
		seen[m.NodeID] = m
	}
	out := make([]Member, 0, len(seen))
	for _, m := range seen {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func decode(raw []byte) (Value, error) {
	var v Value
	if len(raw) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return Value{}, fmt.Errorf("roster: decode: %w", err)
	}
	return v, nil
}

func encode(v Value) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("roster: encode: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
