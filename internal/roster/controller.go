package roster

import "context"

// Discovery surfaces candidate cluster members. Seed/DNS-SRV and mDNS are
// concrete implementations; the controller depends only on this interface so the
// discovery mechanism can change without touching membership logic.
type Discovery interface {
	Discover(ctx context.Context) ([]Member, error)
}

// SeedDiscovery is a fixed set of members (a static seed list). mDNS and DNS-SRV
// adapters implement the same interface and are wired in at the transport layer.
type SeedDiscovery []Member

// Discover returns the seed members.
func (s SeedDiscovery) Discover(context.Context) ([]Member, error) { return []Member(s), nil }

// Controller reconciles the consensus roster toward what discovery has found and
// away from what the failure detector has firmly condemned. Crucially, the
// `down` set it acts on is the cut detector's *threshold-crossed* output, not raw
// gossip suspicion — so the roster only changes on stable, multiply-witnessed
// evidence.
type Controller struct {
	r  *Roster
	rf int // register replication factor; 0 disables core reconfiguration
}

// NewController returns a Controller over r that reconciles membership only
// (no register-core reconfiguration).
func NewController(r *Roster) *Controller { return &Controller{r: r} }

// EnableCoreReconfig makes Reconcile also drive the register's acceptor set
// (the Core) toward desiredCore(members, rf): growing it as the cluster fills
// out and replacing a condemned core member. Returns c for chaining.
func (c *Controller) EnableCoreReconfig(rf int) *Controller { c.rf = rf; return c }

// Reconcile adds any discovered member not yet in the roster and removes any
// node id in down that is still present. It returns the resulting roster value.
// Adds and removes are idempotent, so repeated reconciliation is safe.
func (c *Controller) Reconcile(ctx context.Context, discovered []Member, down []uint64) (Value, error) {
	cur, err := c.r.Get(ctx)
	if err != nil {
		return Value{}, err
	}
	present := map[uint64]bool{}
	for _, m := range cur.Members {
		present[m.NodeID] = true
	}

	for _, m := range discovered {
		if !present[m.NodeID] {
			if _, err := c.r.Add(ctx, m); err != nil {
				return Value{}, err
			}
			present[m.NodeID] = true
		}
	}
	for _, id := range down {
		if present[id] {
			if _, err := c.r.Remove(ctx, id); err != nil {
				return Value{}, err
			}
			present[id] = false
		}
	}

	cur, err = c.r.Get(ctx)
	if err != nil {
		return Value{}, err
	}
	// Tail step: track the register's own acceptor set toward the deterministic
	// target. Removal was processed above, so a condemned node is never selected
	// into the new core. A no-op when the core already matches (the common case).
	//
	// Only ONE node drives reconfiguration — the highest-id current core member —
	// so concurrent reconcile loops don't duel on the register and livelock. The
	// choice is deterministic from the (consensus-read) core, so every node agrees
	// on it. If that driver dies, the failure detector's Remove (run by any node,
	// above) strips it from the core, handing the role to the next-highest member.
	if c.rf > 0 && len(cur.Core) > 0 && c.r.self == maxID(cur.Core) {
		target := nextCore(cur.Core, cur.Members, c.rf)
		if !idsEqual(cur.Core, target) {
			v, err := c.r.Reconfigure(ctx, target)
			if err != nil {
				// The membership change above is already committed; surface it so
				// callers can publish it (a newly-added member must see itself to
				// start serving — without that, a reconfiguration ONTO it can never
				// reach quorum). The reconfiguration retries on the next tick.
				return cur, nil
			}
			return v, nil
		}
	}
	return cur, nil
}

// maxID returns the largest id in a non-empty set.
func maxID(ids []uint64) uint64 {
	var hi uint64
	for _, id := range ids {
		if id > hi {
			hi = id
		}
	}
	return hi
}
