package nebula

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/slackhq/nebula/cert"
	yaml "go.yaml.in/yaml/v3"
)

// A CA survives a marshal/reload round-trip and still mints valid node certs.
func TestCARoundTripMints(t *testing.T) {
	ca, err := CreateCA("cask-ca")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := ca.MarshalCA()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadCA(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	spec := NodeSpec{Name: "n1", OverlayIP: netip.MustParseAddr("10.42.0.5"), Zone: "aws"}
	nodeCertPEM, _, err := reloaded.MintNodeCert(spec)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := cert.UnmarshalCertificateFromPEM(nodeCertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name() != "n1" {
		t.Fatalf("cert name = %q, want n1", c.Name())
	}
	if got := c.Networks(); len(got) != 1 || got[0].Bits() != 24 {
		t.Fatalf("cert networks = %v, want one /24", got)
	}
	if zoneFromGroups(c.Groups()) != "aws" {
		t.Fatalf("cert zone = %q, want aws", zoneFromGroups(c.Groups()))
	}
}

// An IP-advertise (loopback) config emits NO static_map block; lighthouse.hosts
// holds the overlay IP. A hostname advertise DOES emit static_map.
func TestStaticMapGating(t *testing.T) {
	lhIP := netip.MustParseAddr("10.42.0.1")
	mkConfigs := func(adv string) map[string]any {
		specs := []NodeSpec{
			{Name: "lh", OverlayIP: lhIP, Zone: "z1", UDP: "0.0.0.0:4242", Advertise: adv, Lighthouse: true},
			{Name: "m", OverlayIP: netip.MustParseAddr("10.42.0.2"), Zone: "z2", UDP: "0.0.0.0:4242"},
		}
		out, err := GenerateConfigs(specs)
		if err != nil {
			t.Fatal(err)
		}
		var member map[string]any
		if err := yaml.Unmarshal([]byte(out["m"]), &member); err != nil {
			t.Fatal(err)
		}
		return member
	}

	ip := mkConfigs("203.0.113.7:4242")
	if _, ok := ip["static_map"]; ok {
		t.Fatal("IP advertise must not emit a static_map block")
	}
	lh := ip["lighthouse"].(map[string]any)
	hosts := lh["hosts"].([]any)
	if len(hosts) != 1 || hosts[0].(string) != "10.42.0.1" {
		t.Fatalf("lighthouse.hosts = %v, want overlay IP [10.42.0.1]", hosts)
	}
	shm := ip["static_host_map"].(map[string]any)
	if got := shm["10.42.0.1"].([]any)[0].(string); !strings.HasPrefix(got, "203.0.113.7") {
		t.Fatalf("static_host_map underlay = %q, want the advertised IP", got)
	}

	host := mkConfigs("lighthouse.example.com:4242")
	sm, ok := host["static_map"].(map[string]any)
	if !ok {
		t.Fatal("hostname advertise must emit a static_map block")
	}
	if sm["network"] != "ip" {
		t.Fatalf("static_map.network = %v, want ip", sm["network"])
	}
	// lighthouse.hosts is STILL the overlay IP; the hostname only appears in the underlay map.
	lh2 := host["lighthouse"].(map[string]any)
	if h := lh2["hosts"].([]any)[0].(string); h != "10.42.0.1" {
		t.Fatalf("lighthouse.hosts = %q, want overlay IP 10.42.0.1", h)
	}
	if got := host["static_host_map"].(map[string]any)["10.42.0.1"].([]any)[0].(string); got != "lighthouse.example.com:4242" {
		t.Fatalf("static_host_map underlay = %q, want the hostname", got)
	}
}
