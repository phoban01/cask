package lease

import "context"

// Reaper proactively frees locks whose holding session has lapsed. Lazy expiry
// (in Acquire) already guarantees an expired lock is never *observed* as held —
// the reaper exists only for timeliness, so a forgotten lock is released even if
// nothing else touches it. Correctness therefore never depends on the reaper
// running; a flaky reaper degrades cleanup latency, not safety.
//
// To avoid every node reaping at once, a single soft-leader does the work,
// elected by holding a well-known lock under its own session. Losing the
// leadership lock simply hands reaping to another node; nothing breaks if there
// is briefly no leader.
type Reaper struct {
	locks      *Locks
	leaderName string
}

// NewReaper returns a Reaper using leaderLock as the soft-leadership lock.
func NewReaper(locks *Locks, leaderLock string) *Reaper {
	return &Reaper{locks: locks, leaderName: leaderLock}
}

// AcquireLeadership tries to become the reaping leader for sessionID. It returns
// true if this node now holds (or already held) the leadership lock.
func (r *Reaper) AcquireLeadership(ctx context.Context, sessionID string) (bool, error) {
	_, err := r.locks.Acquire(ctx, r.leaderName, sessionID)
	if err == ErrHeld {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Reap frees every lock in names whose holding session is no longer live, and
// returns the names it freed. Intended to be run by the leader over the locks it
// is responsible for (the owner-side key scan that enumerates them lands with the
// pebble store).
func (r *Reaper) Reap(ctx context.Context, names []string) ([]string, error) {
	var freed []string
	for _, name := range names {
		ok, err := r.locks.freeIfDead(ctx, name)
		if err != nil {
			return freed, err
		}
		if ok {
			freed = append(freed, name)
		}
	}
	return freed, nil
}
