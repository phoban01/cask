package ranges

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
)

// Range descriptors as consensus registers (§4.3 design C'): each range's
// descriptor is its own CASPaxos register at \x00rd/<id>, hosted uniformly on
// the roster's Core (every Core member is an acceptor for every descriptor).
// The roster remains the index (which range ids exist); the descriptor is the
// authority for where a range's data lives and whether a reconfiguration is
// in flight. Two-epoch design: the roster's epoch and each descriptor's epoch
// move independently.

// State is a descriptor register's value: the routing descriptor plus the
// lifecycle flags the orchestrator drives.
type State struct {
	Descriptor

	// Splitting serializes concurrent splits of this range: set via CAS when
	// a split begins, cleared when it completes (§4.3 Variant 1).
	Splitting bool `json:"splitting,omitempty"`
	// Tombstoned marks a range replaced by a split/merge. A client catching
	// ErrRangeChanged on this descriptor reads ReplacedBy and refetches.
	Tombstoned bool     `json:"tombstoned,omitempty"`
	ReplacedBy []uint64 `json:"replaced_by,omitempty"`
}

// DescriptorKey is the register key for range id: "\x00rd/" + big-endian id.
func DescriptorKey(id uint64) []byte {
	key := make([]byte, 0, 12)
	key = append(key, "\x00rd/"...)
	return binary.BigEndian.AppendUint64(key, id)
}

func encodeState(s State) ([]byte, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("ranges: encode descriptor: %w", err)
	}
	return raw, nil
}

func decodeState(raw []byte) (State, bool, error) {
	if len(raw) == 0 {
		return State{}, false, nil
	}
	var s State
	if err := json.Unmarshal(raw, &s); err != nil {
		return State{}, false, fmt.Errorf("ranges: decode descriptor: %w", err)
	}
	return s, true, nil
}

// Proposer is the consensus operation the descriptor store needs (satisfied
// by *caspaxos.Proposer and any router). In production it targets the
// roster's CURRENT Core, resolved at call time — the same factory-closure
// pattern the roster itself uses, so a Core reconfiguration retargets
// automatically.
type Proposer interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// Store reads and publishes descriptor registers.
type Store struct {
	prop Proposer
}

// NewStore returns a descriptor store proposing through prop.
func NewStore(prop Proposer) *Store { return &Store{prop: prop} }

// Get performs a linearizable read of range id's descriptor state.
func (s *Store) Get(ctx context.Context, id uint64) (State, bool, error) {
	raw, err := s.prop.Propose(ctx, DescriptorKey(id), caspaxos.Identity)
	if err != nil {
		return State{}, false, err
	}
	return decodeState(raw)
}

// Publish applies mutate to the current state and commits the result. mutate
// may return caspaxos.ErrConflict to abort (a failed precondition, e.g. a
// Splitting CAS); the register is then left unchanged and Publish returns
// that error.
func (s *Store) Publish(ctx context.Context, id uint64, mutate func(cur State, present bool) (State, error)) (State, error) {
	var out State
	_, err := s.prop.Propose(ctx, DescriptorKey(id), func(current []byte) ([]byte, error) {
		cur, present, err := decodeState(current)
		if err != nil {
			return nil, err
		}
		next, err := mutate(cur, present)
		if err != nil {
			return nil, err
		}
		out = next
		return encodeState(next)
	})
	if err != nil {
		return State{}, err
	}
	return out, nil
}
