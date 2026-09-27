package nebula_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/slackhq/nebula/cert"
	"github.com/slackhq/nebula/cert_test"
	yaml "go.yaml.in/yaml/v3"

	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/transport/nebula"
)

// SeedMembers derives the local member (with its zone from cert groups) plus the
// lighthouse hosts (zone-less, to be refined later), keyed and sorted by an
// IP-derived NodeID.
func TestSeedMembersHybrid(t *testing.T) {
	ca, _, caKey, _ := cert_test.NewTestCaCert(
		cert.Version2, cert.Curve_CURVE25519,
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil, nil, nil,
	)
	// Local node 10.0.0.5 in zone us-west. The node cert reuses the CA's
	// validity window. A second call to time.Now() can cross a whole-second
	// boundary, and then the node cert expires after the CA and Sign panics
	// (#113).
	_, _, key, certPEM := cert_test.NewTestCert(
		cert.Version2, cert.Curve_CURVE25519, ca, caKey, "self",
		ca.NotBefore(), ca.NotAfter(),
		[]netip.Prefix{netip.MustParsePrefix("10.0.0.5/24")}, nil, []string{"zone:us-west"},
	)
	caPEM, _ := ca.MarshalPEM()

	conf := map[string]any{
		"pki":        map[string]any{"ca": string(caPEM), "cert": string(certPEM), "key": string(key)},
		"lighthouse": map[string]any{"hosts": []string{"10.0.0.1", "10.0.0.2"}},
	}
	raw, _ := yaml.Marshal(conf)

	members, err := nebula.SeedMembers(string(raw), 8001)
	if err != nil {
		t.Fatalf("SeedMembers: %v", err)
	}

	want := []roster.Member{
		{NodeID: 0x0A000001, Addr: "10.0.0.1:8001", Zone: ""},        // 10.0.0.1
		{NodeID: 0x0A000002, Addr: "10.0.0.2:8001", Zone: ""},        // 10.0.0.2
		{NodeID: 0x0A000005, Addr: "10.0.0.5:8001", Zone: "us-west"}, // self
	}
	if len(members) != len(want) {
		t.Fatalf("got %d members, want %d: %+v", len(members), len(want), members)
	}
	for i, m := range members {
		if m != want[i] {
			t.Errorf("member[%d] = %+v, want %+v", i, m, want[i])
		}
	}

	// It must satisfy roster.Discovery via SeedDiscovery.
	var d roster.Discovery = roster.SeedDiscovery(members)
	got, err := d.Discover(context.Background())
	if err != nil || len(got) != len(want) {
		t.Fatalf("SeedDiscovery.Discover: got %d err=%v", len(got), err)
	}
}
