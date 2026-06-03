package transport

// Network is the substrate inter-node RPC rides on. It abstracts the two seams
// a transport needs — where the server listens, and how clients dial — so the
// consensus transport (ConnectRPC) is identical whether it runs on the host's
// TCP stack or on an encrypted overlay such as Nebula.
//
// The default is [TCP], the host network. A Nebula backend (see nebula.go)
// implements the same interface by returning an overlay listener and an
// *http.Client whose connections ride the overlay; ConnectRPC neither knows nor
// cares which it is given.

import (
	"context"
	"net"
	"net/http"
)

// Network provides the listener the RPC server binds and the HTTP client peers
// are dialed through.
type Network interface {
	// Listen returns the listener the inter-node RPC server is served on.
	Listen(ctx context.Context, address string) (net.Listener, error)
	// HTTPClient returns the client used to construct AcceptorClients; its
	// connections ride this network.
	HTTPClient() *http.Client
}

// TCP is the default Network: the host's TCP stack, no overlay.
type TCP struct{}

// Listen binds address on the host TCP stack.
func (TCP) Listen(ctx context.Context, address string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", address)
}

// HTTPClient returns a client using the default host dialer.
func (TCP) HTTPClient() *http.Client { return &http.Client{} }
