package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/phoban01/cask/internal/transport/nebula"
)

func testMinter(t *testing.T) *minter {
	t.Helper()
	ca, err := nebula.CreateCA("cask-ca")
	if err != nil {
		t.Fatal(err)
	}
	prefix := netip.MustParsePrefix("10.42.0.0/16")
	return &minter{
		ca:       ca,
		token:    "s3cret",
		prefix:   prefix,
		lhHosts:  []string{"10.42.0.1"},
		lhStatic: map[string][]string{"10.42.0.1": {"203.0.113.1:4242"}},
		srv:      "_cask._tcp.example",
		log:      slog.New(slog.NewTextHandler(io_Discard{}, nil)),
	}
}

type io_Discard struct{}

func (io_Discard) Write(p []byte) (int, error) { return len(p), nil }

func post(t *testing.T, m *minter, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, "/mint", bytes.NewReader(b))
	w := httptest.NewRecorder()
	m.handleMint(w, r)
	return w
}

func TestMintRejectsBadToken(t *testing.T) {
	m := testMinter(t)
	w := post(t, m, mintReq{Token: "wrong", Zone: "aws", Role: "replica"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("bad token: status %d, want 401", w.Code)
	}
}

func TestMintIssuesUsableIdentity(t *testing.T) {
	m := testMinter(t)
	w := post(t, m, mintReq{Token: "s3cret", Zone: "aws", Role: "replica"})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", w.Code)
	}
	var resp mintResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	ip, err := netip.ParseAddr(resp.OverlayIP)
	if err != nil {
		t.Fatalf("bad overlay_ip %q: %v", resp.OverlayIP, err)
	}
	if !m.prefix.Contains(ip) {
		t.Fatalf("overlay ip %s not in prefix %s", ip, m.prefix)
	}
	if resp.Role != "replica" {
		t.Fatalf("role = %q, want replica", resp.Role)
	}
	if len(resp.LighthouseHosts) != 1 || resp.LighthouseHosts[0] != "10.42.0.1" {
		t.Fatalf("lighthouse_hosts = %v", resp.LighthouseHosts)
	}
	if resp.DiscoverySRV != "_cask._tcp.example" {
		t.Fatalf("discovery_srv = %q", resp.DiscoverySRV)
	}
	// The minted node config can be built and the cert chains to the returned CA.
	spec := nebula.NodeSpec{Name: resp.Name, OverlayIP: ip, Zone: "aws", UDP: "0.0.0.0:4242"}
	if _, err := nebula.BuildNebulaConfig(spec, []byte(resp.CA), []byte(resp.Cert), []byte(resp.Key), resp.LighthouseHosts, resp.StaticHostMap); err != nil {
		t.Fatalf("build config from mint response: %v", err)
	}
}

func TestMintRandomIdentities(t *testing.T) {
	m := testMinter(t)
	seen := map[string]bool{}
	for i := 0; i < 16; i++ {
		w := post(t, m, mintReq{Token: "s3cret", Role: "replica"})
		var resp mintResp
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if seen[resp.OverlayIP] {
			t.Fatalf("mint reused overlay ip %s (should be random/distinct)", resp.OverlayIP)
		}
		seen[resp.OverlayIP] = true
	}
}

func TestMintDefaultsRoleToReplica(t *testing.T) {
	m := testMinter(t)
	w := post(t, m, mintReq{Token: "s3cret", Role: "bogus"})
	var resp mintResp
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Role != "replica" {
		t.Fatalf("unknown role normalised to %q, want replica", resp.Role)
	}
}
