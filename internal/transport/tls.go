package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
)

// TLS wraps a Network so that every consensus connection runs mutual TLS.
// Listen wraps the inner listener with Server, and HTTPClient runs a TLS
// handshake with Client on every connection it dials through the inner
// network. Build Server and Client with internal/mtls.
//
// TLS sits below HTTP, so peer URLs keep the http scheme on every network.
// The Network decides whether the bytes on the wire are encrypted, not the
// URL. An https URL also works: the client does not add a second layer.
type TLS struct {
	Net    Network
	Server *tls.Config
	Client *tls.Config
}

// Listen binds address on the inner network and requires a TLS handshake
// with a verified client certificate on every connection.
func (t TLS) Listen(ctx context.Context, address string) (net.Listener, error) {
	//= docs/spec/fleet.md#8-security
	//# Consensus traffic between members MUST use mutual TLS.
	if t.Server == nil || t.Server.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, fmt.Errorf("transport: TLS listener needs a server config that requires client certificates")
	}
	ln, err := t.Net.Listen(ctx, address)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, t.Server), nil
}

// HTTPClient returns a copy of the inner network's client whose connections
// run a TLS handshake with the client config before the first request.
func (t TLS) HTTPClient() *http.Client {
	//= docs/spec/fleet.md#8-security
	//# Consensus traffic between members MUST use mutual TLS.
	inner := t.Net.HTTPClient()
	var tr *http.Transport
	switch it := inner.Transport.(type) {
	case nil:
		tr = http.DefaultTransport.(*http.Transport).Clone()
	case *http.Transport:
		tr = it.Clone()
	default:
		panic(fmt.Sprintf("transport: TLS cannot wrap a %T round tripper", it))
	}
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	cfg := t.Client
	handshake := func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		c := tls.Client(raw, cfg.Clone())
		if err := c.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("transport: TLS handshake with %s: %w", addr, err)
		}
		return c, nil
	}
	tr.DialContext = handshake
	tr.DialTLSContext = handshake
	// Consensus traffic goes straight to the peer. An HTTP proxy from the
	// environment would see only TLS bytes and cannot route them.
	tr.Proxy = nil
	tr.ForceAttemptHTTP2 = false
	out := *inner
	out.Transport = tr
	return &out
}
