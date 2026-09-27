package transport

// This is the permanent ConnectRPC transport (protobuf over the Connect
// protocol), replacing the interim HTTP/JSON path in http.go. It implements the
// same [caspaxos.AcceptorClient] interface and serves the same acceptor, so the
// consensus core and the simulator are untouched.
//
// The client is constructed from a connect.HTTPClient (satisfied by
// *http.Client), which is the single seam an overlay transport such as Nebula
// plugs into: inject an *http.Client whose Transport.DialContext rides the
// overlay and every consensus RPC follows, with no change here.

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"connectrpc.com/connect"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"

	caskv1 "github.com/phoban01/cask/gen/cask/v1"
	"github.com/phoban01/cask/gen/cask/v1/caskv1connect"
)

// epochHeader carries the proposer's claimed range-descriptor epoch (§4.1).
// It rides a header rather than the protobuf schema so both transports share
// one mechanism and the wire types stay untouched.
const epochHeader = "Cask-Range-Epoch"

// EpochOf resolves the CURRENT descriptor epoch for a key on the serving
// node (from its roster/placement view). ok=false disables the check (e.g.
// the static --peers topology, which has no descriptor epochs).
type EpochOf func(key []byte) (uint64, bool)

// HandlerOption configures a transport handler.
type HandlerOption func(*handlerOpts)

type handlerOpts struct{ epochOf EpochOf }

// WithEpochOf enables stale-proposer rejection: a request claiming an epoch
// OLDER than epochOf(key) fails with caspaxos.ErrRangeChanged before touching
// the acceptor, telling the proposer to refresh its routing (§4.1). Requests
// claiming no epoch, or a newer one (the SERVER is behind — harmless, the
// acceptor itself is range-agnostic), pass through.
func WithEpochOf(f EpochOf) HandlerOption {
	return func(o *handlerOpts) { o.epochOf = f }
}

// checkEpoch applies the §4.1 rule; nil means proceed.
func (o *handlerOpts) checkEpoch(key []byte, claimed string) error {
	if o.epochOf == nil || claimed == "" {
		return nil
	}
	cur, ok := o.epochOf(key)
	if !ok {
		return nil
	}
	c, err := strconv.ParseUint(claimed, 10, 64)
	if err != nil {
		return nil // malformed claim: ignore rather than invent failures
	}
	if c < cur {
		return caspaxos.ErrRangeChanged
	}
	return nil
}

// ConnectHandler returns the route prefix and http.Handler exposing acc's
// Prepare/Accept over ConnectRPC. Register it on a mux: mux.Handle(path, h).
func ConnectHandler(acc caspaxos.AcceptorClient, opts ...HandlerOption) (string, http.Handler) {
	h := &acceptorHandler{acc: acc}
	for _, o := range opts {
		o(&h.opts)
	}
	return caskv1connect.NewAcceptorServiceHandler(h)
}

type acceptorHandler struct {
	caskv1connect.UnimplementedAcceptorServiceHandler
	acc  caspaxos.AcceptorClient
	opts handlerOpts
}

func (h *acceptorHandler) Prepare(ctx context.Context, req *connect.Request[caskv1.PrepareRequest]) (*connect.Response[caskv1.PrepareResponse], error) {
	if err := h.opts.checkEpoch(req.Msg.GetKey(), req.Header().Get(epochHeader)); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	reply, err := h.acc.Prepare(claimCtx(ctx, req.Header().Get(epochHeader)), req.Msg.GetKey(), ballotFromPB(req.Msg.GetBallot()))
	if err != nil {
		return nil, acceptorErr(err)
	}
	return connect.NewResponse(&caskv1.PrepareResponse{
		Promised: reply.Promised,
		Conflict: ballotToPB(reply.Conflict),
		Accepted: ballotToPB(reply.Accepted),
		Value:    reply.Value,
	}), nil
}

func (h *acceptorHandler) Accept(ctx context.Context, req *connect.Request[caskv1.AcceptRequest]) (*connect.Response[caskv1.AcceptResponse], error) {
	if err := h.opts.checkEpoch(req.Msg.GetKey(), req.Header().Get(epochHeader)); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	reply, err := h.acc.Accept(claimCtx(ctx, req.Header().Get(epochHeader)), req.Msg.GetKey(), ballotFromPB(req.Msg.GetBallot()), req.Msg.GetValue())
	if err != nil {
		return nil, acceptorErr(err)
	}
	return connect.NewResponse(&caskv1.AcceptResponse{
		Accepted: reply.Accepted,
		Conflict: ballotToPB(reply.Conflict),
	}), nil
}

// stampEpoch copies the claimed epoch (if the router stamped one on ctx)
// onto the outgoing request, and rangeChangedErr maps the server's
// FailedPrecondition back to the typed sentinel the proposer recognizes.
// claimCtx puts the caller's claimed epoch on the server-side context, so
// an acceptor that checks the claim itself (atomically with the operation)
// can read it with ranges.ClaimedEpoch. A missing or malformed claim leaves
// ctx unchanged.
func claimCtx(ctx context.Context, claimed string) context.Context {
	if claimed == "" {
		return ctx
	}
	c, err := strconv.ParseUint(claimed, 10, 64)
	if err != nil {
		return ctx
	}
	return ranges.WithClaimedEpoch(ctx, c)
}

// acceptorErr maps an acceptor error to a Connect error. A fenced request
// stays FailedPrecondition, so the client sees caspaxos.ErrRangeChanged.
func acceptorErr(err error) error {
	if errors.Is(err, caspaxos.ErrRangeChanged) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func stampEpoch[T any](ctx context.Context, req *connect.Request[T]) {
	if e, ok := ranges.ClaimedEpoch(ctx); ok {
		req.Header().Set(epochHeader, strconv.FormatUint(e, 10))
	}
}

func rangeChangedErr(err error) error {
	if connect.CodeOf(err) == connect.CodeFailedPrecondition {
		return caspaxos.ErrRangeChanged
	}
	return err
}

// ConnectClient is an [caspaxos.AcceptorClient] that talks to a remote
// acceptor's ConnectRPC handler.
type ConnectClient struct {
	rpc caskv1connect.AcceptorServiceClient
}

// NewConnectClient returns a client for the acceptor served at baseURL. If hc is
// nil, http.DefaultClient is used. Pass a custom http.Client to route over an
// overlay transport.
func NewConnectClient(baseURL string, hc connect.HTTPClient) *ConnectClient {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &ConnectClient{rpc: caskv1connect.NewAcceptorServiceClient(hc, baseURL)}
}

func (c *ConnectClient) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	req := connect.NewRequest(&caskv1.PrepareRequest{
		Key:    key,
		Ballot: ballotToPB(b),
	})
	stampEpoch(ctx, req)
	resp, err := c.rpc.Prepare(ctx, req)
	if err != nil {
		// A range-changed rejection is typed (aborts the round); anything else
		// is an unreachable acceptor, counted as a non-vote.
		return caspaxos.PrepareReply{}, rangeChangedErr(err)
	}
	return caspaxos.PrepareReply{
		Promised: resp.Msg.GetPromised(),
		Conflict: ballotFromPB(resp.Msg.GetConflict()),
		Accepted: ballotFromPB(resp.Msg.GetAccepted()),
		Value:    resp.Msg.GetValue(),
	}, nil
}

func (c *ConnectClient) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	req := connect.NewRequest(&caskv1.AcceptRequest{
		Key:    key,
		Ballot: ballotToPB(b),
		Value:  val,
	})
	stampEpoch(ctx, req)
	resp, err := c.rpc.Accept(ctx, req)
	if err != nil {
		return caspaxos.AcceptReply{}, rangeChangedErr(err)
	}
	return caspaxos.AcceptReply{
		Accepted: resp.Msg.GetAccepted(),
		Conflict: ballotFromPB(resp.Msg.GetConflict()),
	}, nil
}

// ballotToPB / ballotFromPB translate between the pure core's Ballot and the
// wire type. A nil wire ballot decodes to the zero ballot (a never-touched
// register), matching caspaxos.ZeroBallot.
func ballotToPB(b caspaxos.Ballot) *caskv1.Ballot {
	return &caskv1.Ballot{Counter: b.Counter, NodeId: b.NodeID}
}

func ballotFromPB(b *caskv1.Ballot) caspaxos.Ballot {
	if b == nil {
		return caspaxos.Ballot{}
	}
	return caspaxos.Ballot{Counter: b.GetCounter(), NodeID: b.GetNodeId()}
}
