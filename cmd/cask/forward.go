package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"

	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
)

// forwardedHeader marks a proxied request so it is never re-forwarded: the
// receiving node handles it locally (fast path if it owns, gated full path if
// not). One hop converges or fails retryably — no proxy loops.
const forwardedHeader = "X-Cask-Forwarded"

// forwarder proxies client API operations to the range-owner's node over the
// overlay (M7, docs/plans/quepaxa-learnings-implementation.md). This is what
// makes the writes-via-owner discipline hold cluster-wide: a write entering
// through any node reaches the owner's router, whose fast path updates — or
// whose full path invalidates — the owner's read cache, keeping zero-RTT
// owned reads linearizable. Forwarding reads is a latency optimization on top
// (the owner serves them without a consensus round); a local full-round read
// is always a correct fallback.
//
// When the owner is unreachable, the caller falls through to the LOCAL path,
// where the router's FullPathGate applies: a live owner lease turns into
// owner.ErrOwnerLive (HTTP 503, retryable) rather than a silent write around
// the owner's cache — you cannot commit around a live read lease.
// addrBook resolves a node id to its overlay address (*overlayDialer in
// production; a fixed map in tests).
type addrBook interface {
	addr(node uint64) (string, bool)
}

type forwarder struct {
	self  uint64
	hc    *http.Client
	book  addrBook
	snap  *rosterSnap
	log   *slog.Logger
}

func newForwarder(self uint64, hc *http.Client, book addrBook, snap *rosterSnap, log *slog.Logger) *forwarder {
	return &forwarder{self: self, hc: hc, book: book, snap: snap, log: log}
}

// ownerAddr resolves the overlay address of the range-owner hint for key.
// Empty when this node is the hint itself (handle locally) or the address is
// unknown (fall through to the gated local path).
func (f *forwarder) ownerAddr(key []byte) string {
	val, ok := f.snap.load()
	if !ok {
		return ""
	}
	d, ok := placeRange(val).Lookup(key)
	if !ok {
		return ""
	}
	hint, ok := placement.Owner(ranges.RangeKey(d.ID), d.Replicas)
	if !ok || hint == f.self {
		return ""
	}
	addr, ok := f.book.addr(hint)
	if !ok {
		return ""
	}
	return addr
}

// maybeForward proxies r to the key's owner node and relays the response.
// false means the caller must handle the request locally: this node is the
// owner hint, the request was already forwarded once, the owner's address is
// unknown, or the owner did not answer (the local path's gate then decides
// whether proceeding is safe).
func (f *forwarder) maybeForward(w http.ResponseWriter, r *http.Request, key []byte) bool {
	if f == nil || r.Header.Get(forwardedHeader) != "" {
		return false
	}
	addr := f.ownerAddr(key)
	if addr == "" {
		return false
	}

	// Buffer the body so a failed forward can still be handled locally.
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return true
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+addr+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header = r.Header.Clone()
	req.Header.Set(forwardedHeader, "1")

	resp, err := f.hc.Do(req)
	if err != nil {
		f.log.Warn("owner forward failed; handling locally", "addr", addr, "err", err)
		return false
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return true
}
