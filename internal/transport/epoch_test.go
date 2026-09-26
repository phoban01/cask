package transport_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// Both transports enforce the §4.1 rule: a request claiming a range epoch
// OLDER than the serving node's is rejected with caspaxos.ErrRangeChanged
// before touching the acceptor; fresh, newer, or absent claims pass through.
func TestEpochCheck(t *testing.T) {
	const current = uint64(5)
	epochOf := transport.WithEpochOf(func([]byte) (uint64, bool) { return current, true })

	mk := map[string]func(t *testing.T) caspaxos.AcceptorClient{
		"connect": func(t *testing.T) caspaxos.AcceptorClient {
			path, h := transport.ConnectHandler(caspaxos.NewAcceptor(store.NewMem()), epochOf)
			mux := http.NewServeMux()
			mux.Handle(path, h)
			ts := httptest.NewServer(mux)
			t.Cleanup(ts.Close)
			return transport.NewConnectClient(ts.URL, nil)
		},
		"http": func(t *testing.T) caspaxos.AcceptorClient {
			ts := httptest.NewServer(transport.Handler(caspaxos.NewAcceptor(store.NewMem()), epochOf))
			t.Cleanup(ts.Close)
			return transport.NewClient(ts.URL, nil)
		},
	}

	for name, build := range mk {
		t.Run(name, func(t *testing.T) {
			client := build(t)
			b := caspaxos.Ballot{Counter: 1, NodeID: 1}

			// Stale claim: rejected as the typed sentinel, on both phases.
			stale := ranges.WithClaimedEpoch(context.Background(), current-1)
			if _, err := client.Prepare(stale, []byte("k"), b); !errors.Is(err, caspaxos.ErrRangeChanged) {
				t.Fatalf("stale prepare err = %v, want ErrRangeChanged", err)
			}
			if _, err := client.Accept(stale, []byte("k"), b, []byte("v")); !errors.Is(err, caspaxos.ErrRangeChanged) {
				t.Fatalf("stale accept err = %v, want ErrRangeChanged", err)
			}

			// A current claim proceeds to the acceptor.
			fresh := ranges.WithClaimedEpoch(context.Background(), current)
			if reply, err := client.Prepare(fresh, []byte("k"), b); err != nil || !reply.Promised {
				t.Fatalf("fresh prepare = %+v err=%v, want promised", reply, err)
			}

			// A NEWER claim (the server is behind) and an absent claim both
			// pass: the acceptor itself is range-agnostic.
			newer := ranges.WithClaimedEpoch(context.Background(), current+1)
			if _, err := client.Accept(newer, []byte("k"), b, []byte("v")); err != nil {
				t.Fatalf("newer-claim accept err = %v, want nil", err)
			}
			if _, err := client.Prepare(context.Background(), []byte("k2"), b); err != nil {
				t.Fatalf("claimless prepare err = %v, want nil", err)
			}
		})
	}
}
