package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// newTestMember serves a member's acceptor and roster endpoints on a local
// port, the way main does with --listen-consensus.
func newTestMember(t *testing.T, id uint64, bootstrap bool, seeds []string, joinTimeout time.Duration, opts ...func(*membershipConfig)) *membership {
	t.Helper()
	m, _ := newTestMemberServer(t, id, bootstrap, seeds, joinTimeout, opts...)
	return m
}

// newTestMemberServer is newTestMember that also returns the server, so a
// test can stop the member. Each opt changes the config before the member
// starts.
func newTestMemberServer(t *testing.T, id uint64, bootstrap bool, seeds []string, joinTimeout time.Duration, opts ...func(*membershipConfig)) (*membership, *http.Server) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	cfg := membershipConfig{
		ID:          id,
		Advertise:   ln.Addr().String(),
		Bootstrap:   bootstrap,
		Seeds:       seeds,
		Local:       caspaxos.NewAcceptor(st),
		HTTP:        transport.TCP{}.HTTPClient(),
		Store:       st,
		Interval:    20 * time.Millisecond,
		JoinTimeout: joinTimeout,
	}
	for _, o := range opts {
		o(&cfg)
	}
	m := newMembership(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := &http.Server{Handler: hangable(t, m, m.handler())}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return m, srv
}

func memberIDs(v roster.Value) []uint64 {
	var ids []uint64
	for _, m := range v.Members {
		ids = append(ids, m.NodeID)
	}
	return ids
}

// Two apiservers form one roster: the first founds it with --bootstrap, the
// second joins through --seed. The spec forbids two voters, so the joiner is
// a participant: it is a roster member but not in the core.
func TestBootstrapAndSeedFormRosterOfTwo(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The extension server MUST join the fleet through the dynamic roster path.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	founder := newTestMember(t, 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	joiner := newTestMember(t, 2, false, []string{founder.cfg.Advertise}, 10*time.Second)
	if err := joiner.start(ctx); err != nil {
		t.Fatalf("joiner: %v", err)
	}

	for name, m := range map[string]*membership{"founder": founder, "joiner": joiner} {
		v, ok := m.current()
		if !ok {
			t.Fatalf("%s: no roster", name)
		}
		if got := memberIDs(v); !slices.Equal(got, []uint64{1, 2}) {
			t.Errorf("%s: members = %v, want [1 2]", name, got)
		}
		if !slices.Equal(v.Core, []uint64{1}) {
			t.Errorf("%s: core = %v, want [1]: the joiner must be a participant, not a second voter", name, v.Core)
		}
	}

	// The participant proposes against the founder's core, and the founder
	// reads the value back.
	key := []byte("k")
	if _, err := joiner.Propose(ctx, key, func([]byte) ([]byte, error) { return []byte("v"), nil }); err != nil {
		t.Fatalf("participant propose: %v", err)
	}
	got, err := founder.Propose(ctx, key, caspaxos.Identity)
	if err != nil {
		t.Fatalf("founder read: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("founder read %q, want %q", got, "v")
	}
}

// A member started with --seed never founds a roster. With no live member to
// join, it fails instead.
func TestSeedWithoutLiveMemberFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()

	m := newTestMember(t, 2, false, []string{dead}, 200*time.Millisecond)
	if err := m.start(context.Background()); err == nil {
		t.Fatal("join with no live seed succeeded; want an error")
	}
	if _, ok := m.current(); ok {
		t.Fatal("a seed-only member founded a roster")
	}
}
