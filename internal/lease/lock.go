package lease

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/phoban01/cask/internal/caspaxos"
)

// ErrHeld means the lock is currently held by another live session.
var ErrHeld = errors.New("lease: lock held by another session")

// ErrContended means the acquire kept racing another acquirer and gave up.
var ErrContended = errors.New("lease: lock acquisition contended out")

// Lock is the register backing a named lock: which session holds it and the
// fencing token of the current grant.
type Lock struct {
	Session string `json:"session"`
	Held    bool   `json:"held"`
	Fence   uint64 `json:"fence"`
}

// Locks manages lock registers, using Sessions for holder liveness.
type Locks struct {
	prop     Proposer
	sessions *Sessions
	retries  int
}

// NewLocks returns a Locks manager that checks holder liveness via sessions.
func NewLocks(prop Proposer, sessions *Sessions) *Locks {
	return &Locks{prop: prop, sessions: sessions, retries: 8}
}

func LockKey(name string) []byte { return append([]byte("\x00lock\x00"), name...) }

// Acquire takes the named lock for sessionID and returns the fencing token of
// the grant. It returns ErrHeld if a different live session holds it. A lock
// whose holder's session has lapsed is taken over (lazy expiry). Re-acquiring a
// lock you already hold is idempotent and returns the existing token.
func (l *Locks) Acquire(ctx context.Context, name, sessionID string) (token uint64, err error) {
	key := LockKey(name)
	for attempt := 0; attempt < l.retries; attempt++ {
		cur, err := l.read(ctx, key)
		if err != nil {
			return 0, err
		}
		if cur.Held && cur.Session == sessionID {
			return cur.Fence, nil // already ours
		}
		if cur.Held && cur.Session != sessionID {
			live, err := l.sessions.Live(ctx, cur.Session)
			if err != nil {
				return 0, err
			}
			if live {
				return 0, ErrHeld
			}
			// Holder's session has lapsed; fall through to take over.
		}
		observed := cur.Fence
		newFence := observed + 1
		_, err = l.prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
			c := decodeLock(current)
			if c.Fence != observed { // raced since we read: retry from the top
				return nil, caspaxos.ErrConflict
			}
			return marshal(Lock{Session: sessionID, Held: true, Fence: newFence})
		})
		if errors.Is(err, caspaxos.ErrConflict) {
			continue // lost the race; re-read and retry
		}
		if err != nil {
			return 0, err
		}
		return newFence, nil
	}
	return 0, ErrContended
}

// Release frees the lock if held by sessionID. It is idempotent and keeps the
// fence, so the next acquire mints a strictly higher token.
func (l *Locks) Release(ctx context.Context, name, sessionID string) error {
	_, err := l.prop.Propose(ctx, LockKey(name), func(current []byte) ([]byte, error) {
		c := decodeLock(current)
		if c.Session != sessionID {
			return current, nil // not ours: no-op
		}
		return marshal(Lock{Held: false, Fence: c.Fence})
	})
	return err
}

// Owner returns the lock's holding session, whether it is held by a live
// session, and the current fence.
func (l *Locks) Owner(ctx context.Context, name string) (session string, live bool, fence uint64, err error) {
	cur, err := l.read(ctx, LockKey(name))
	if err != nil {
		return "", false, 0, err
	}
	if !cur.Held {
		return "", false, cur.Fence, nil
	}
	live, err = l.sessions.Live(ctx, cur.Session)
	if err != nil {
		return "", false, 0, err
	}
	return cur.Session, live, cur.Fence, nil
}

// freeIfDead frees the lock if it is held by a session that is no longer live.
// The reaper calls this; it is also safe to call lazily.
func (l *Locks) freeIfDead(ctx context.Context, name string) (freed bool, err error) {
	cur, err := l.read(ctx, LockKey(name))
	if err != nil || !cur.Held {
		return false, err
	}
	live, err := l.sessions.Live(ctx, cur.Session)
	if err != nil || live {
		return false, err
	}
	observed := cur.Fence
	_, err = l.prop.Propose(ctx, LockKey(name), func(current []byte) ([]byte, error) {
		c := decodeLock(current)
		if c.Fence != observed || !c.Held {
			return current, nil
		}
		return marshal(Lock{Held: false, Fence: c.Fence})
	})
	return err == nil, err
}

func (l *Locks) read(ctx context.Context, key []byte) (Lock, error) {
	raw, err := l.prop.Propose(ctx, key, caspaxos.Identity)
	if err != nil {
		return Lock{}, err
	}
	return decodeLock(raw), nil
}

func decodeLock(raw []byte) Lock {
	var c Lock
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &c)
	}
	return c
}
