package main

// The promote and demote endpoints change the voter set.
//
//	POST /admin/promote   body: [2, 3]   participants to make voters
//	POST /admin/demote    body: [2, 3]   voters to make participants
//
// Both serve on the consensus listener (--listen-consensus), next to the
// acceptor and the roster endpoints. Each request applies its whole list in
// one core change (changeCoreBy), so one call grows the voters from one to
// three and never passes through two.
//
// Only the driver runs a core change. A member that is not the driver
// answers 307 Temporary Redirect with the driver's address in Location, and
// does not forward the request itself. A client that follows redirects
// (curl -L) reaches the driver with the same method and body.
//
// Status codes:
//
//	200  the change is released; the body has the new core and cfg_gen
//	307  this member is not the driver; Location names the driver
//	400  the body is not a JSON list of member ids
//	405  the method is not POST
//	409  the request breaks a voter rule; the core does not change
//	503  no roster yet, another core change is in flight, or the change did
//	     not finish; the driver resumes an unfinished change on its own

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/roster"
)

const (
	promotePath = "/admin/promote"
	demotePath  = "/admin/demote"
)

// maxVoters is the largest voter count the spec allows.
const maxVoters = 5

// errCoreChangeInFlight refuses a new change while a joint core is in
// flight. The driver finishes that change first.
var errCoreChangeInFlight = errors.New("membership: a core change is in flight; retry when GET /roster shows no joint core")

// refusedError is a request that breaks a voter rule. The core does not
// change.
type refusedError struct{ msg string }

func (e *refusedError) Error() string { return e.msg }

func refuse(format string, args ...any) error {
	return &refusedError{msg: fmt.Sprintf(format, args...)}
}

// voterChange is the answer to a promote or demote that the driver released.
type voterChange struct {
	Core      []uint64 `json:"core"`
	ConfigGen uint64   `json:"cfg_gen"`
}

// planVoters returns the core that a promote (promote true) or a demote of
// ids gives on the roster value cur. It refuses a request that breaks a
// voter rule.
func planVoters(cur roster.Value, ids []uint64, promote bool) ([]uint64, error) {
	if cur.Joint != nil {
		return nil, errCoreChangeInFlight
	}
	if len(ids) == 0 {
		return nil, refuse("name at least one member id")
	}
	ids = slices.Clone(ids)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	members := map[uint64]bool{}
	for _, x := range cur.Members {
		members[x.NodeID] = true
	}
	target := slices.Clone(cur.Core)
	for _, id := range ids {
		voter := slices.Contains(cur.Core, id)
		if promote {
			//= docs/spec/fleet.md#6-membership
			//# A promote request MUST name only participants.
			switch {
			case !members[id]:
				return nil, refuse("node %d is not a roster member", id)
			case voter:
				return nil, refuse("node %d is already a voter", id)
			}
			target = append(target, id)
			continue
		}
		//= docs/spec/fleet.md#6-membership
		//# A demote request MUST name only voters.
		if !voter {
			return nil, refuse("node %d is not a voter", id)
		}
		target = slices.DeleteFunc(target, func(x uint64) bool { return x == id })
	}
	slices.Sort(target)
	if len(target) == 0 {
		return nil, refuse("the change would leave no voters")
	}
	//= docs/spec/fleet.md#6-membership
	//# The voter count MUST be odd.
	if len(target)%2 == 0 {
		return nil, refuse("the change would give %d voters; the voter count must be odd", len(target))
	}
	//= docs/spec/fleet.md#6-membership
	//# The voter count MUST NOT exceed five.
	if len(target) > maxVoters {
		return nil, refuse("the change would give %d voters; the voter count must not exceed %d", len(target), maxVoters)
	}
	return target, nil
}

// serveVoterChange serves POST /admin/promote (promote true) and
// POST /admin/demote.
func (m *membership) serveVoterChange(promote bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// The endpoints share the consensus listener. With the consensus
		// TLS flags, only a client with a certificate from the fleet CA
		// reaches them. Without the flags, anyone who can reach the
		// listener can change the voter set.
		//= docs/spec/fleet.md#8-security
		//= type=exception
		//= reason=promote and demote are open when the consensus TLS flags are absent; tracked in issue #158
		//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
			return
		}
		var ids []uint64
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&ids); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be a JSON list of member ids, e.g. [2, 3]: " + err.Error()})
			return
		}
		cur, ok := m.current()
		if !ok || len(cur.Core) == 0 {
			m.retryLater(w, errors.New("membership: no roster yet"))
			return
		}
		if !cluster.IsDriver(m.self.NodeID, cur.Core) {
			m.redirectToDriver(w, r, cur)
			return
		}

		//= docs/spec/fleet.md#6-membership
		//# The voter set MUST change only by joint-consensus reconfiguration of the roster.
		//= docs/spec/fleet.md#6-membership
		//# A promote or demote request MUST apply its whole list of member ids in one core change.
		var target []uint64
		v, err := m.changeCoreBy(r.Context(), func(cur roster.Value) ([]uint64, error) {
			t, err := planVoters(cur, ids, promote)
			target = t
			return t, err
		})
		var refused *refusedError
		switch {
		case errors.As(err, &refused):
			writeJSON(w, http.StatusConflict, map[string]string{"error": refused.msg})
			return
		case errors.Is(err, errNotDriver):
			// The core moved while the request waited for the run loop.
			if now, ok := m.current(); ok && !cluster.IsDriver(m.self.NodeID, now.Core) {
				m.redirectToDriver(w, r, now)
				return
			}
			m.retryLater(w, err)
			return
		case err != nil:
			m.retryLater(w, fmt.Errorf("core change did not finish; the driver resumes it, check GET /roster: %w", err))
			return
		}
		if v.Joint != nil || !slices.Equal(v.Core, target) {
			m.retryLater(w, fmt.Errorf("membership: core is %v, not %v; retry", v.Core, target))
			return
		}
		writeJSON(w, http.StatusOK, voterChange{Core: v.Core, ConfigGen: v.ConfigGen})
	}
}

// redirectToDriver answers 307 with the driver's consensus address. The
// member does not forward the request, so a request never loops between
// members that disagree about the driver.
func (m *membership) redirectToDriver(w http.ResponseWriter, r *http.Request, cur roster.Value) {
	addr := cluster.DriverAddr(cur.Core, cur.Members)
	if addr == "" {
		m.retryLater(w, fmt.Errorf("membership: no address for the driver of core %v", cur.Core))
		return
	}
	// On a TLS listener, point at https, so a tool such as curl keeps TLS
	// on the redirect. A member client runs TLS below HTTP, so it follows
	// an https URL too.
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	w.Header().Set("Location", scheme+"://"+addr+r.URL.Path)
	writeJSON(w, http.StatusTemporaryRedirect, map[string]string{
		"error":  fmt.Sprintf("node %d is not the driver; send the request to the driver", m.self.NodeID),
		"driver": addr,
	})
}

func (m *membership) retryLater(w http.ResponseWriter, err error) {
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
}
