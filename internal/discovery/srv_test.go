package discovery

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/transport/nebula"
)

// fakeResolver returns canned SRV and A/AAAA answers.
type fakeResolver struct {
	srvErr error
	srvs   []*net.SRV
	hosts  map[string][]netip.Addr // target (no trailing dot) -> ips
}

func (f fakeResolver) LookupSRV(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
	if f.srvErr != nil {
		return "", nil, f.srvErr
	}
	return "", f.srvs, nil
}

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	ips, ok := f.hosts[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	return ips, nil
}

func wantID(t *testing.T, s string) uint64 {
	t.Helper()
	return nebula.NodeIDFromIP(netip.MustParseAddr(s))
}

func TestSRVResolvesOverlayMembers(t *testing.T) {
	r := fakeResolver{
		srvs: []*net.SRV{
			{Target: "cask-a.cask.svc.cluster.local.", Port: 8001},
			{Target: "cask-b.cask.svc.cluster.local.", Port: 8001},
			{Target: "dead.cask.svc.cluster.local.", Port: 8001}, // unresolvable
		},
		hosts: map[string][]netip.Addr{
			"cask-a.cask.svc.cluster.local": {netip.MustParseAddr("10.42.0.7")},
			"cask-b.cask.svc.cluster.local": {netip.MustParseAddr("10.42.0.9")},
		},
	}
	d := SRVDiscovery{Service: "cask", Proto: "tcp", Domain: "cask.svc.cluster.local", Resolver: r}

	ms, err := d.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 { // the unresolvable target is skipped, not fatal
		t.Fatalf("got %d members, want 2: %+v", len(ms), ms)
	}
	byID := map[uint64]roster.Member{}
	for _, m := range ms {
		byID[m.NodeID] = m
	}
	a := byID[wantID(t, "10.42.0.7")]
	if a.Addr != "10.42.0.7:8001" {
		t.Fatalf("member a addr = %q, want 10.42.0.7:8001", a.Addr)
	}
	if a.Zone != "" {
		t.Fatalf("member a zone = %q, want empty (unknown from DNS)", a.Zone)
	}
	if _, ok := byID[wantID(t, "10.42.0.9")]; !ok {
		t.Fatal("member b missing")
	}
}

// A 4-in-6 AAAA answer must yield the SAME node id as the plain IPv4 form, or
// the node's identity would disagree with the rest of the cluster.
func TestSRVUnmapsV4in6(t *testing.T) {
	r := fakeResolver{
		srvs:  []*net.SRV{{Target: "n.example.", Port: 8001}},
		hosts: map[string][]netip.Addr{"n.example": {netip.MustParseAddr("::ffff:10.42.0.7")}},
	}
	ms, err := SRVDiscovery{Service: "cask", Proto: "tcp", Domain: "example", Resolver: r}.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 {
		t.Fatalf("got %d members, want 1", len(ms))
	}
	if ms[0].NodeID != wantID(t, "10.42.0.7") {
		t.Fatalf("4-in-6 id = %d, want same as v4 %d", ms[0].NodeID, wantID(t, "10.42.0.7"))
	}
	if ms[0].Addr != "10.42.0.7:8001" {
		t.Fatalf("addr = %q, want canonical v4 10.42.0.7:8001", ms[0].Addr)
	}
}

func TestSRVPropagatesLookupError(t *testing.T) {
	r := fakeResolver{srvErr: errors.New("dns down")}
	_, err := SRVDiscovery{Service: "cask", Proto: "tcp", Domain: "x", Resolver: r}.Discover(context.Background())
	if err == nil {
		t.Fatal("expected SRV lookup error to propagate")
	}
}

func TestUnionDedupAndPriority(t *testing.T) {
	ctx := context.Background()
	id := wantID(t, "10.42.0.7")
	seed := roster.SeedDiscovery{{NodeID: id, Addr: "10.42.0.7:8001", Zone: "aws"}}
	srv := fakeResolver{
		srvs:  []*net.SRV{{Target: "n.example.", Port: 8001}, {Target: "m.example.", Port: 8001}},
		hosts: map[string][]netip.Addr{"n.example": {netip.MustParseAddr("10.42.0.7")}, "m.example": {netip.MustParseAddr("10.42.0.8")}},
	}
	u := Union{seed, SRVDiscovery{Service: "cask", Proto: "tcp", Domain: "example", Resolver: srv}}

	ms, err := u.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("got %d members, want 2 (deduped): %+v", len(ms), ms)
	}
	for _, m := range ms {
		if m.NodeID == id && m.Zone != "aws" {
			t.Fatalf("earlier source should win: zone = %q, want aws", m.Zone)
		}
	}
}

// One source erroring still yields the others' members.
func TestUnionToleratesPartialFailure(t *testing.T) {
	broken := SRVDiscovery{Service: "cask", Proto: "tcp", Domain: "x", Resolver: fakeResolver{srvErr: errors.New("dns down")}}
	good := roster.SeedDiscovery{{NodeID: 5, Addr: "10.42.0.5:8001"}}
	ms, err := Union{broken, good}.Discover(context.Background())
	if err != nil {
		t.Fatalf("union should tolerate one broken source: %v", err)
	}
	if len(ms) != 1 || ms[0].NodeID != 5 {
		t.Fatalf("got %+v, want member 5", ms)
	}
}
