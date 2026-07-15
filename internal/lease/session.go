// Package lease implements cask's leases, sessions, and locks — the headline
// coordination primitive. The model is session-gated, as in Consul: a client
// holds a SESSION with a TTL that it heartbeats, and the LOCKS it acquires are
// tied to that session rather than carrying their own expiry. So an agent keeps
// many locks alive by renewing one session (O(agents) keepalive load, not
// O(locks)), and when the agent dies its session lapses and a reaper frees all
// of its locks at once.
//
// Locks hand out a strictly monotonic FENCING token on every successful acquire.
// Because tokens only increase — and the lock register, with its token, is
// carried forward by reconfiguration — a fenced downstream resource can always
// reject a stale/zombie holder, even across owner changes and range moves. These
// are the SingleHolder and FenceMonotone properties model-checked in
// tla/Lease.tla.
//
// Time is injected (Clock returns a unix-nanos-like count), so behaviour under
// clock skew is deterministic and unit-testable.
package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/phoban01/cask/internal/buggify"
	"github.com/phoban01/cask/internal/caspaxos"
)

// errKeepAliveDropped models a keepalive RPC that was silently dropped on the
// wire (the keepalive_blackhole fault). The caller sees a failed heartbeat; the
// session may then lapse, exercising the reaper.
var errKeepAliveDropped = errors.New("lease: keepalive dropped (buggify)")

func init() {
	buggify.Register("session_drop_keepalive",
		"Sessions.KeepAlive drops the heartbeat as if the RPC were blackholed", 0.05)
}

// Proposer is the consensus operation leases need (satisfied by
// *caspaxos.Proposer and by the agent Router).
type Proposer interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// Clock returns the current time as a monotonic count (unix nanos in production).
type Clock func() int64

// Session is a client's liveness token: who owns it and when it expires.
type Session struct {
	Owner  string `json:"owner"`
	Expiry int64  `json:"expiry"`
}

// Sessions manages session registers.
type Sessions struct {
	prop Proposer
	now  Clock
}

// NewSessions returns a Sessions manager.
func NewSessions(prop Proposer, now Clock) *Sessions {
	return &Sessions{prop: prop, now: now}
}

func SessionKey(id string) []byte { return append([]byte("\x00sess\x00"), id...) }

// Grant creates or renews session id for owner with the given ttl. It fails with
// ErrHeld if a *different* owner currently holds a live session under that id.
func (s *Sessions) Grant(ctx context.Context, id, owner string, ttl int64) (Session, error) {
	return s.commit(ctx, id, func(cur Session, present bool) (Session, error) {
		now := s.now()
		if present && cur.Owner != "" && cur.Owner != owner && cur.Expiry > now {
			return Session{}, caspaxos.ErrConflict
		}
		return Session{Owner: owner, Expiry: now + ttl}, nil
	})
}

// KeepAlive extends a live session held by owner. It fails if the session has
// lapsed or belongs to someone else (the caller has lost it).
func (s *Sessions) KeepAlive(ctx context.Context, id, owner string, ttl int64) (Session, error) {
	// BUGGIFY: drop the heartbeat as if blackholed on the wire.
	if buggify.Maybe("session_drop_keepalive", 0.05) {
		return Session{}, errKeepAliveDropped
	}
	return s.commit(ctx, id, func(cur Session, present bool) (Session, error) {
		now := s.now()
		if !present || cur.Owner != owner || cur.Expiry <= now {
			return Session{}, caspaxos.ErrConflict
		}
		return Session{Owner: owner, Expiry: now + ttl}, nil
	})
}

// Revoke ends a session immediately.
func (s *Sessions) Revoke(ctx context.Context, id string) error {
	_, err := s.commit(ctx, id, func(Session, bool) (Session, error) {
		return Session{}, nil
	})
	return err
}

// Live reports whether session id is currently live (owned and unexpired), per
// the caller's clock.
func (s *Sessions) Live(ctx context.Context, id string) (bool, error) {
	cur, present, err := s.get(ctx, id)
	if err != nil {
		return false, err
	}
	return present && cur.Owner != "" && cur.Expiry > s.now(), nil
}

// Info returns the session record itself (linearizable read). Callers that
// reason about time relative to the expiry — like the ownership manager's
// takeover wait, which must outwait a lapsed holder's read window by
// MaxOffset — need the raw Expiry, not just the Live verdict.
func (s *Sessions) Info(ctx context.Context, id string) (Session, bool, error) {
	return s.get(ctx, id)
}

func (s *Sessions) get(ctx context.Context, id string) (Session, bool, error) {
	raw, err := s.prop.Propose(ctx, SessionKey(id), caspaxos.Identity)
	if err != nil {
		return Session{}, false, err
	}
	if len(raw) == 0 {
		return Session{}, false, nil
	}
	var v Session
	if err := json.Unmarshal(raw, &v); err != nil {
		return Session{}, false, fmt.Errorf("lease: decode session: %w", err)
	}
	return v, true, nil
}

func (s *Sessions) commit(ctx context.Context, id string, mutate func(cur Session, present bool) (Session, error)) (Session, error) {
	raw, err := s.prop.Propose(ctx, SessionKey(id), func(current []byte) ([]byte, error) {
		var cur Session
		present := len(current) > 0
		if present {
			if err := json.Unmarshal(current, &cur); err != nil {
				return nil, err
			}
		}
		next, err := mutate(cur, present)
		if err != nil {
			return nil, err
		}
		return marshal(next)
	})
	if err != nil {
		return Session{}, err
	}
	if len(raw) == 0 {
		return Session{}, nil
	}
	var out Session
	if err := json.Unmarshal(raw, &out); err != nil {
		return Session{}, err
	}
	return out, nil
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
