package transport_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/transport"
)

// A peer that connects to a mutual TLS consensus listener and then sends
// nothing is cut off within ReadHeaderTimeout. This holds before the
// ClientHello and after a complete handshake with no request.
func TestConsensusServerClosesSilentConnections(t *testing.T) {
	ca := testCA(t)
	srvCfg := tlsMember(t, ca, "a")
	nw := transport.TLS{Net: transport.TCP{}, Server: srvCfg.Server, Client: srvCfg.Client}
	ln, err := nw.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := transport.NewServer(http.NotFoundHandler())
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	addr := ln.Addr().String()
	peer := tlsMember(t, ca, "b")

	for _, tc := range []struct {
		name string
		dial func(t *testing.T) net.Conn
	}{
		{"no ClientHello", func(t *testing.T) net.Conn {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			return c
		}},
		{"handshake, then no request", func(t *testing.T) net.Conn {
			c, err := tls.Dial("tcp", addr, peer.Client)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Handshake(); err != nil {
				t.Fatal(err)
			}
			return c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := tc.dial(t)
			defer c.Close()
			start := time.Now()
			limit := transport.ReadHeaderTimeout + 2*time.Second
			_ = c.SetReadDeadline(start.Add(limit))
			_, err := c.Read(make([]byte, 1))
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("connection still open after %v; want the server to close it", limit)
			}
			if err == nil {
				t.Fatal("read data from a silent connection; want the server to close it")
			}
			if err != io.EOF {
				t.Logf("closed with %v", err)
			}
			if d := time.Since(start); d < transport.ReadHeaderTimeout/2 {
				t.Fatalf("closed after %v; want the deadline, not an early error", d)
			}
		})
	}
}
