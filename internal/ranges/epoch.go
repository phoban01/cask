package ranges

import "context"

// The claimed-epoch context value carries the range-descriptor epoch a
// proposer believes it is routing under, across the generic AcceptorClient
// seam and onto the wire (§4.1). It rides context rather than the interface
// because the pure consensus core is deliberately range-unaware — epochs are
// a ROUTING concern, checked at the transport boundary: the router stamps its
// descriptor's epoch, the serving node compares it against its own current
// descriptor, and a stale proposer gets caspaxos.ErrRangeChanged instead of a
// silent round against acceptors that may no longer host the key.

type epochKey struct{}

// WithClaimedEpoch returns ctx carrying the proposer's believed descriptor
// epoch for the range it is routing to.
func WithClaimedEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, epochKey{}, epoch)
}

// ClaimedEpoch returns the epoch stamped by WithClaimedEpoch, if any.
func ClaimedEpoch(ctx context.Context) (uint64, bool) {
	e, ok := ctx.Value(epochKey{}).(uint64)
	return e, ok
}
