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
	r *Roster
}

// NewController returns a Controller over r.
func NewController(r *Roster) *Controller { return &Controller{r: r} }

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
	return c.r.Get(ctx)
}
