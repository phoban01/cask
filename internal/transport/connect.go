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
	"net/http"

	"connectrpc.com/connect"

	"github.com/phoban01/cask/internal/caspaxos"

	caskv1 "github.com/phoban01/cask/gen/cask/v1"
	"github.com/phoban01/cask/gen/cask/v1/caskv1connect"
)

// ConnectHandler returns the route prefix and http.Handler exposing acc's
// Prepare/Accept over ConnectRPC. Register it on a mux: mux.Handle(path, h).
func ConnectHandler(acc *caspaxos.Acceptor) (string, http.Handler) {
	return caskv1connect.NewAcceptorServiceHandler(&acceptorHandler{acc: acc})
}

type acceptorHandler struct {
	caskv1connect.UnimplementedAcceptorServiceHandler
	acc *caspaxos.Acceptor
}

func (h *acceptorHandler) Prepare(ctx context.Context, req *connect.Request[caskv1.PrepareRequest]) (*connect.Response[caskv1.PrepareResponse], error) {
	reply, err := h.acc.Prepare(ctx, req.Msg.GetKey(), ballotFromPB(req.Msg.GetBallot()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&caskv1.PrepareResponse{
		Promised: reply.Promised,
		Conflict: ballotToPB(reply.Conflict),
		Accepted: ballotToPB(reply.Accepted),
		Value:    reply.Value,
	}), nil
}

func (h *acceptorHandler) Accept(ctx context.Context, req *connect.Request[caskv1.AcceptRequest]) (*connect.Response[caskv1.AcceptResponse], error) {
	reply, err := h.acc.Accept(ctx, req.Msg.GetKey(), ballotFromPB(req.Msg.GetBallot()), req.Msg.GetValue())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&caskv1.AcceptResponse{
		Accepted: reply.Accepted,
		Conflict: ballotToPB(reply.Conflict),
	}), nil
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
	resp, err := c.rpc.Prepare(ctx, connect.NewRequest(&caskv1.PrepareRequest{
		Key:    key,
		Ballot: ballotToPB(b),
	}))
	if err != nil {
		return caspaxos.PrepareReply{}, err // unreachable acceptor: counted as a non-vote
	}
	return caspaxos.PrepareReply{
		Promised: resp.Msg.GetPromised(),
		Conflict: ballotFromPB(resp.Msg.GetConflict()),
		Accepted: ballotFromPB(resp.Msg.GetAccepted()),
		Value:    resp.Msg.GetValue(),
	}, nil
}

func (c *ConnectClient) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	resp, err := c.rpc.Accept(ctx, connect.NewRequest(&caskv1.AcceptRequest{
		Key:    key,
		Ballot: ballotToPB(b),
		Value:  val,
	}))
	if err != nil {
		return caspaxos.AcceptReply{}, err
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
