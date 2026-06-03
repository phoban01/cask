package transport_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// Reproduces the binary's exact wiring (mvcc.KV over a ConnectRPC RF=3 group)
// to confirm a read-modify-write — Put then CAS then Get — behaves over the
// wire as it does in-process.
func TestMVCCCASOverConnect(t *testing.T) {
	ctx := context.Background()

	const rf = 3
	clients := make([]caspaxos.AcceptorClient, rf)
	for i := 0; i < rf; i++ {
		acc := caspaxos.NewAcceptor(store.NewMem())
		path, h := transport.ConnectHandler(acc)
		mux := http.NewServeMux()
		mux.Handle(path, h)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		clients[i] = transport.NewConnectClient(srv.URL, srv.Client())
	}

	prop := caspaxos.NewProposer(7, clients)
	clock := hlc.New(func() int64 { return time.Now().UnixNano() })
	kv := mvcc.New(prop, clock, 7)

	if _, err := kv.Put(ctx, []byte("greeting"), []byte("hello-over-connect")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, found, err := kv.Get(ctx, []byte("greeting"))
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if string(got) != "hello-over-connect" {
		t.Fatalf("get = %q, want hello-over-connect", got)
	}

	// CAS against the current value must succeed and replace it.
	if _, err := kv.CAS(ctx, []byte("greeting"), []byte("hello-over-connect"), []byte("cas-won")); err != nil {
		t.Fatalf("CAS against current value: %v", err)
	}
	got, _, err = kv.Get(ctx, []byte("greeting"))
	if err != nil {
		t.Fatalf("get after CAS: %v", err)
	}
	if string(got) != "cas-won" {
		t.Fatalf("after CAS get = %q, want cas-won", got)
	}
}
