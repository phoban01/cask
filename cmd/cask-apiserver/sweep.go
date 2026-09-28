package main

// The index sweep repairs index entries that a crash left behind.
//
// A mutation writes the object register and then the index register. A
// crash between the two leaves the index behind the object. The next
// index write for that name repairs it, but a name may get no next write.
// The sweep finds such names and writes their entries
// (storage.Sweep). quint/fleet.qnt models the crash and the sweep.
//
// The sweep must see every committed object register. A committed
// register is on a majority of the core, so the sweep lists the keys on
// a majority of the core and takes the union. Two majorities of one core
// share a voter, so the union names every committed register.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/mvcc"
)

// sweepResources are the resource types whose index the sweep repairs.
var sweepResources = []string{"devices", "deviceclaims"}

// indexSweeper runs the index sweep for every resource type.
type indexSweeper struct {
	kv   *mvcc.KV
	list storage.KeyLister
	log  *slog.Logger
}

// sweepOnce lists the keys once and sweeps every resource type.
func (s *indexSweeper) sweepOnce(ctx context.Context) error {
	keys, err := s.list(ctx)
	if err != nil {
		return fmt.Errorf("index sweep: list keys: %w", err)
	}
	listed := func(context.Context) ([][]byte, error) { return keys, nil }
	var errs []error
	for _, r := range sweepResources {
		n, err := storage.Sweep(ctx, s.kv, r, listed)
		if n > 0 {
			s.log.Info("index sweep repaired entries", "resource", r, "names", n)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// sweepAtStartup runs the sweep until one pass succeeds or ctx ends. It
// waits retry between failed passes. Call it before the server serves.
func (s *indexSweeper) sweepAtStartup(ctx context.Context, retry time.Duration) error {
	//= docs/spec/fleet.md#3-storage-model
	//# The extension server MUST reconcile the index register against the object registers at startup.
	for {
		err := s.sweepOnce(ctx)
		if err == nil {
			return nil
		}
		s.log.Warn("index sweep at startup", "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last: %v)", ctx.Err(), err)
		case <-time.After(retry):
		}
	}
}

// listDataKeys lists the data keys on a majority of the current core.
// During a core change it lists a majority of the old core and a
// majority of the new core, because a register may be on either.
func (m *membership) listDataKeys(ctx context.Context) ([][]byte, error) {
	v, ok := m.current()
	if !ok || len(v.Core) == 0 {
		return nil, errors.New("membership: no roster core yet")
	}
	groups := [][]uint64{v.Core}
	if v.Joint != nil {
		groups = [][]uint64{v.Joint.Old, v.Joint.New}
	}
	union := map[string]struct{}{}
	for _, ids := range groups {
		if err := m.listMajority(ctx, ids, v.ConfigGen, union); err != nil {
			return nil, err
		}
	}
	out := make([][]byte, 0, len(union))
	for k := range union {
		out = append(out, []byte(k))
	}
	return out, nil
}

// listMajority asks every voter in ids at once. It adds the keys of the
// first majority that answers to union. gen is the ConfigGen of this
// member's view. A voter that holds a newer roster value shows that the
// view is stale: the core may have moved, so the listing fails.
func (m *membership) listMajority(ctx context.Context, ids []uint64, gen uint64, union map[string]struct{}) error {
	need := len(ids)/2 + 1
	lctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the stragglers once a majority answered

	type answer struct {
		id  uint64
		l   keyListing
		err error
	}
	answers := make(chan answer, len(ids))
	for _, id := range ids {
		go func(id uint64) {
			l, err := m.nodeKeys(lctx, id)
			answers <- answer{id, l, err}
		}(id)
	}
	got := 0
	var lastErr error
	for range ids {
		var a answer
		select {
		case a = <-answers:
		case <-ctx.Done():
			return ctx.Err()
		}
		if a.err != nil {
			lastErr = a.err
			continue
		}
		if a.l.Gen > gen {
			return fmt.Errorf("membership: node %d holds roster ConfigGen %d, this view has %d", a.id, a.l.Gen, gen)
		}
		for _, k := range a.l.Keys {
			union[string(k)] = struct{}{}
		}
		if got++; got >= need {
			return nil
		}
	}
	return fmt.Errorf("membership: %d of %d voters listed their keys, need %d (last error: %v)", got, len(ids), need, lastErr)
}
