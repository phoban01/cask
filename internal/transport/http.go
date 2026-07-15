// Package transport carries CASPaxos messages between nodes.
//
// This is an interim HTTP/JSON transport so a real multi-process range group
// works today. It is deliberately behind the same [caspaxos.AcceptorClient]
// interface the simulator uses, so the planned ConnectRPC (protobuf, multi
// channel) transport drops in without touching the consensus core — it is
// gated only on adding protobuf codegen (`buf`) to the dev toolchain.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
)

// wire request/response envelopes. []byte fields marshal as base64 JSON.

type prepareReq struct {
	Key    []byte          `json:"key"`
	Ballot caspaxos.Ballot `json:"ballot"`
}

type acceptReq struct {
	Key    []byte          `json:"key"`
	Ballot caspaxos.Ballot `json:"ballot"`
	Value  []byte          `json:"value"`
}

const (
	pathPrepare = "/v1/prepare"
	pathAccept  = "/v1/accept"
)

// Handler returns an http.Handler exposing acc's Prepare/Accept over JSON.
func Handler(acc *caspaxos.Acceptor, opts ...HandlerOption) http.Handler {
	var o handlerOpts
	for _, opt := range opts {
		opt(&o)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(pathPrepare, func(w http.ResponseWriter, r *http.Request) {
		var req prepareReq
		if !decode(w, r, &req) {
			return
		}
		if err := o.checkEpoch(req.Key, r.Header.Get(epochHeader)); err != nil {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		reply, err := acc.Prepare(r.Context(), req.Key, req.Ballot)
		writeJSON(w, reply, err)
	})
	mux.HandleFunc(pathAccept, func(w http.ResponseWriter, r *http.Request) {
		var req acceptReq
		if !decode(w, r, &req) {
			return
		}
		if err := o.checkEpoch(req.Key, r.Header.Get(epochHeader)); err != nil {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		reply, err := acc.Accept(r.Context(), req.Key, req.Ballot, req.Value)
		writeJSON(w, reply, err)
	})
	return mux
}

// Client is an AcceptorClient that talks to a remote acceptor's HTTP handler.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a Client for the acceptor served at baseURL. If hc is nil,
// http.DefaultClient is used.
func NewClient(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{base: baseURL, http: hc}
}

func (c *Client) Prepare(ctx context.Context, key []byte, b caspaxos.Ballot) (caspaxos.PrepareReply, error) {
	var out caspaxos.PrepareReply
	err := c.call(ctx, pathPrepare, prepareReq{Key: key, Ballot: b}, &out)
	return out, err
}

func (c *Client) Accept(ctx context.Context, key []byte, b caspaxos.Ballot, val []byte) (caspaxos.AcceptReply, error) {
	var out caspaxos.AcceptReply
	err := c.call(ctx, pathAccept, acceptReq{Key: key, Ballot: b, Value: val}, &out)
	return out, err
}

func (c *Client) call(ctx context.Context, path string, req, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if e, ok := ranges.ClaimedEpoch(ctx); ok {
		httpReq.Header.Set(epochHeader, strconv.FormatUint(e, 10))
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err // unreachable acceptor: counted as a non-vote by the proposer
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed {
		return caspaxos.ErrRangeChanged // stale routing: abort the round, refresh
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
		return fmt.Errorf("transport: %s -> %s: %s", path, resp.Status, b)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, payload any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
