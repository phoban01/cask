package transport

import (
	"net/http"
	"time"
)

// Deadlines for a consensus server. A peer that opens a connection must
// finish the TLS handshake and send its request headers within
// ReadHeaderTimeout. An idle keep-alive connection closes after
// IdleTimeout. Without them, a client that connects and sends nothing
// holds a connection and a goroutine for ever, before any certificate
// check.
const (
	ReadHeaderTimeout = 5 * time.Second
	IdleTimeout       = 2 * time.Minute
)

// NewServer returns an http.Server for a consensus listener that serves h
// with the deadlines above. net/http applies the smallest of its read and
// write deadlines to the TLS handshake of a *tls.Conn, so on a TLS
// listener ReadHeaderTimeout also bounds the handshake.
func NewServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       IdleTimeout,
	}
}
