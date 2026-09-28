package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
)

// ErrHeld means the lock is currently held by another live session.
var ErrHeld = errors.New("lease: lock held by another session")

// ErrContended means the acquire kept racing another acquirer and gave up.
var ErrContended = errors.New("lease: lock acquisition contended out")

// ErrNotHolder means a holder-only operation (Bump) was attempted by a session
// that does not currently hold the lock.
var ErrNotHolder = errors.New("lease: lock not held by session")

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
	backoff  func(ctx context.Context, attempt int) error
}

// LocksOption configures a Locks manager.
type LocksOption func(*Locks)

// WithAcquireBackoff installs a delay between contended Acquire/Bump retries
// (never before the first attempt). Without it, the retry budget can exhaust
// in one contention burst — the same livelock class the proposer-level
// backoff addresses, one level up.
func WithAcquireBackoff(f func(ctx context.Context, attempt int) error) LocksOption {
	return func(l *Locks) { l.backoff = f }
}

// NewLocks returns a Locks manager that checks holder liveness via sessions.
func NewLocks(prop Proposer, sessions *Sessions, opts ...LocksOption) *Locks {
	l := &Locks{prop: prop, sessions: sessions, retries: 8}
	for _, o := range opts {
		o(l)
	}
	return l
}

// pause runs the configured backoff after a lost race (attempt 0-based).
func (l *Locks) pause(ctx context.Context, attempt int) error {
	if l.backoff == nil {
		return nil
	}
	return l.backoff(ctx, attempt)
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
		// A re-entrant acquire continues the holder's tenure. The fence
		// already names this session and no other, so returning it keeps
		// fencing safe. This path also ends the retry after an unknown
		// outcome whose write landed: it returns the fence that landed.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# An acquire by the session that already holds the lock MUST return the current fence and MUST NOT mint a new one.
		if cur.Held && cur.Session == sessionID {
			return cur.Fence, nil // already ours
		}
		if cur.Held && cur.Session != sessionID {
			live, err := l.sessions.Live(ctx, cur.Session)
			if err != nil {
				return 0, err
			}
			//= docs/spec/fleet.md#5-claims-and-fencing
			//# At most one claim MUST be Bound to an object at the object's current fence.
			if live {
				return 0, ErrHeld
			}
			// Holder's session has lapsed; fall through to take over.
		}
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.
		observed := cur.Fence
		newFence := observed + 1
		_, err = l.prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
			c := decodeLock(current)
			if c.Fence != observed { // raced since we read: retry from the top
				return nil, caspaxos.ErrConflict
			}
			return marshal(Lock{Session: sessionID, Held: true, Fence: newFence})
		})
		// An unknown outcome is safe to retry here: the re-read at the top
		// of the loop fixes the outcome, finds the lock already ours if the
		// write landed, and otherwise retries the compare-and-set on Fence.
		//= docs/spec/fleet.md#3-storage-model
		//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
		if errors.Is(err, caspaxos.ErrConflict) || errors.Is(err, caspaxos.ErrUnknownOutcome) {
			if perr := l.pause(ctx, attempt); perr != nil {
				return 0, perr
			}
			continue // lost the race; re-read and retry
		}
		if err != nil {
			return 0, err
		}
		return newFence, nil
	}
	return 0, ErrContended
}

// Bump raises the fence of a lock currently held by sessionID to at least
// minFence (and always by at least one), returning the new token. Ownership
// managers use it when the register layer reports an epoch at or above the
// current fence (caspaxos.EpochBehindError — a full proposer jumped into a
// synthetic epoch after preempting the owner): the next TakeOwnership must run
// at a strictly higher epoch, and only the holder can raise the fence without
// releasing the lock. The fence stays strictly monotonic (the FenceMonotone
// property in tla/Lease.tla), so downstream fenced resources are unaffected —
// they simply see a fresher token from the same holder.
func (l *Locks) Bump(ctx context.Context, name, sessionID string, minFence uint64) (token uint64, err error) {
	key := LockKey(name)
	var unknownFence uint64 // fence of a bump whose outcome is unknown
	for attempt := 0; attempt < l.retries; attempt++ {
		cur, err := l.read(ctx, key)
		if err != nil {
			return 0, err
		}
		if !cur.Held || cur.Session != sessionID {
			return 0, ErrNotHolder
		}
		if unknownFence != 0 && cur.Fence >= unknownFence {
			return cur.Fence, nil // the earlier bump landed; do not bump twice
		}
		observed := cur.Fence
		newFence := max(observed+1, minFence)
		_, err = l.prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
			c := decodeLock(current)
			if c.Fence != observed || !c.Held || c.Session != sessionID {
				return nil, caspaxos.ErrConflict // raced since we read: retry from the top
			}
			return marshal(Lock{Session: sessionID, Held: true, Fence: newFence})
		})
		if errors.Is(err, caspaxos.ErrUnknownOutcome) {
			unknownFence = newFence
			err = caspaxos.ErrConflict // re-read, then compare-and-set again
		}
		if errors.Is(err, caspaxos.ErrConflict) {
			if perr := l.pause(ctx, attempt); perr != nil {
				return 0, perr
			}
			continue
		}
		if err != nil {
			return 0, err
		}
		return newFence, nil
	}
	return 0, ErrContended
}

// ErrSeedConflict means Seed found a lock register that it cannot raise to
// the seed without losing a live holder or a fence.
var ErrSeedConflict = errors.New("lease: lock register conflicts with the seed")

// Seed sets up a lock register that another system minted fences for, so
// that cask continues its fences. The import of a cutover uses it.
//
// After Seed, the register fence is at least fence. When sessionID is not
// empty, sessionID holds the lock at exactly fence. The caller must grant
// that session first; otherwise a concurrent Acquire sees a holder that is
// not live and takes the lock over.
//
// Seed never lowers a fence and never takes a lock from a live holder. A
// holder whose session has lapsed is free, as it is for Acquire, but only
// when the seed raises the fence above the lapsed holder's fence. Seed
// returns ErrSeedConflict when the register holds a live lock at a lower
// fence, or when sessionID must hold the lock and the register has moved
// past fence or names another holder at fence. A second Seed with the same
// arguments changes nothing.
func (l *Locks) Seed(ctx context.Context, name, sessionID string, fence uint64) error {
	key := LockKey(name)
	for attempt := 0; attempt < l.retries; attempt++ {
		cur, err := l.read(ctx, key)
		if err != nil {
			return err
		}
		write, err := l.seedNeedsWrite(ctx, name, cur, sessionID, fence)
		if err != nil || !write {
			return err
		}
		// The seed only raises the fence, so a later Acquire mints above
		// every fence the other system minted.
		observed := cur
		next := Lock{Session: sessionID, Held: sessionID != "", Fence: fence}
		_, err = l.prop.Propose(ctx, key, func(current []byte) ([]byte, error) {
			if decodeLock(current) != observed {
				return nil, caspaxos.ErrConflict // raced since we read: retry from the top
			}
			return marshal(next)
		})
		// The re-read at the top finds the seed when an unknown outcome
		// landed, and compares and sets again otherwise.
		if errors.Is(err, caspaxos.ErrConflict) || errors.Is(err, caspaxos.ErrUnknownOutcome) {
			if perr := l.pause(ctx, attempt); perr != nil {
				return perr
			}
			continue
		}
		return err
	}
	return ErrContended
}

// CheckSeed reads the register and returns ErrSeedConflict when Seed with
// the same arguments would conflict. It writes nothing. A caller checks
// before it grants the session that the seed names, so a seed that cannot
// succeed leaves no live session behind. The register can still change
// between CheckSeed and Seed.
func (l *Locks) CheckSeed(ctx context.Context, name, sessionID string, fence uint64) error {
	cur, err := l.read(ctx, LockKey(name))
	if err != nil {
		return err
	}
	_, err = l.seedNeedsWrite(ctx, name, cur, sessionID, fence)
	return err
}

// seedNeedsWrite decides a seed against the register cur. It reports
// whether Seed must write, or returns ErrSeedConflict.
func (l *Locks) seedNeedsWrite(ctx context.Context, name string, cur Lock, sessionID string, fence uint64) (bool, error) {
	conflict := func() (bool, error) {
		return false, fmt.Errorf("%w: %q has %+v, seed is fence %d for session %q",
			ErrSeedConflict, name, cur, fence, sessionID)
	}
	switch {
	case cur.Fence > fence && sessionID == "":
		return false, nil // cask minted above the seed already
	case cur.Fence == fence && (sessionID == "" || (cur.Held && cur.Session == sessionID)):
		return false, nil // seeded already
	case cur.Fence >= fence:
		// Taking the lock here would keep or lower the fence.
		return conflict()
	case cur.Held:
		live, err := l.sessions.Live(ctx, cur.Session)
		if err != nil {
			return false, err
		}
		if live {
			return conflict()
		}
		// The holder lapsed. The seed takes over at a higher fence, so
		// a receiver rejects the lapsed holder's effects.
	}
	return true, nil
}

// Release frees the lock if held by sessionID. It is idempotent and keeps the
// fence, so the next acquire mints a strictly higher token.
func (l *Locks) Release(ctx context.Context, name, sessionID string) error {
	// The change checks the holder first, so it is safe to run again after
	// an unknown outcome.
	_, err := caspaxos.ProposeResolving(ctx, l.prop, LockKey(name), func(current []byte) ([]byte, error) {
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
	_, err = caspaxos.ProposeResolving(ctx, l.prop, LockKey(name), func(current []byte) ([]byte, error) {
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
