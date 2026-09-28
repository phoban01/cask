package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mtls"
	"github.com/phoban01/cask/internal/transport"
)

// memberTLS issues a consensus identity for name from ca.
func memberTLS(t *testing.T, ca *mtls.CA, name string) *mtls.Config {
	t.Helper()
	cert, key, err := ca.Issue(name, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mtls.New(cert, key, ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// Three apiservers with consensus certificates from one CA form a fleet,
// grow the core to three voters through /admin/promote, and commit. Every
// acceptor RPC, roster call, key listing, and admin call on the way runs
// over mutual TLS. A client without a certificate, with a certificate from
// another CA, or without TLS cannot read the roster, join, promote, or
// demote.
func TestFleetOfThreeOverMutualTLS(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca, err := mtls.NewCA("fleet-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	network := func(name string) transport.Network { return consensusNetwork(memberTLS(t, ca, name)) }

	founder, _ := newTestMemberOn(t, network("east"), 1, true, nil, 10*time.Second)
	if err := founder.start(ctx); err != nil {
		t.Fatalf("founder: %v", err)
	}
	ms := []*membership{founder}
	for i, name := range []string{"west", "north"} {
		j, _ := newTestMemberOn(t, network(name), uint64(i+2), false, []string{founder.cfg.Advertise}, 10*time.Second)
		if err := j.start(ctx); err != nil {
			t.Fatalf("joiner %s: %v", name, err)
		}
		ms = append(ms, j)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to see three members", func() bool { return hasMembers(m, []uint64{1, 2, 3}) })
	}

	key := []byte("fleet/devices/gpu-7")
	if _, err := ms[1].Propose(ctx, key, caspaxos.Write([]byte("v1"))); err != nil {
		t.Fatalf("participant write on a core of one: %v", err)
	}
	// Promote through a participant with a member client. The participant
	// is not the driver, so it redirects to the founder over https, and
	// the client follows the redirect over mutual TLS.
	resp, err := ms[1].cfg.HTTP.Do(mustRequest(t, http.MethodPost, "http://"+ms[1].cfg.Advertise+promotePath, []byte("[2, 3]")))
	if err != nil {
		t.Fatalf("promote over mutual TLS: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Request.URL.Scheme != "https" || resp.Request.URL.Host != founder.cfg.Advertise {
		t.Fatalf("promote = %s at %s; want 200 from the founder after an https redirect", resp.Status, resp.Request.URL)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to see a core of three", func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}
	if _, err := ms[2].Propose(ctx, key, func(cur []byte) ([]byte, error) { return append(cur, "+v2"...), nil }); err != nil {
		t.Fatalf("write on a core of three: %v", err)
	}
	got, err := ms[1].Propose(ctx, key, caspaxos.Identity)
	if err != nil || string(got) != "v1+v2" {
		t.Fatalf("read = %q, %v; want v1+v2", got, err)
	}

	// A peer outside the fleet CA reaches none of the consensus endpoints.
	rogue := memberTLS(t, mustCA(t), "rogue")
	otherCA := rogue.Client.Clone()
	// Trust the server, so only the server's check can refuse.
	otherCA.VerifyConnection = nil
	noCert := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true} //nolint:gosec // the test targets the server check.
	base := "http://" + founder.cfg.Advertise
	// Positive control: a member's own client reads both endpoints.
	for _, path := range []string{"/roster", dataKeysPath} {
		resp, err := ms[2].cfg.HTTP.Do(mustRequest(t, http.MethodGet, base+path, nil))
		if err != nil {
			t.Fatalf("member GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("member GET %s: %s", path, resp.Status)
		}
	}
	for name, hc := range map[string]*http.Client{
		"no certificate": transport.TLS{Net: transport.TCP{}, Client: noCert}.HTTPClient(),
		"another CA":     transport.TLS{Net: transport.TCP{}, Client: otherCA}.HTTPClient(),
		"plaintext":      transport.TCP{}.HTTPClient(),
	} {
		t.Run(name, func(t *testing.T) {
			for _, req := range []*http.Request{
				mustRequest(t, http.MethodGet, base+"/roster", nil),
				mustRequest(t, http.MethodGet, base+dataKeysPath, nil),
				mustRequest(t, http.MethodPost, base+"/roster/join", []byte(`{"node":99,"addr":"127.0.0.1:1"}`)),
				mustRequest(t, http.MethodPost, base+demotePath, []byte("[2, 3]")),
				mustRequest(t, http.MethodPost, base+promotePath, []byte("[2, 3]")),
			} {
				resp, err := hc.Do(req)
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
						t.Fatalf("%s %s: status %s; want the server to refuse the client", req.Method, req.URL.Path, resp.Status)
					}
				}
			}
			c := transport.NewConnectClient(base, hc)
			if _, err := c.Prepare(ctx, key, caspaxos.Ballot{Counter: 1 << 40, NodeID: 99}); err == nil {
				t.Fatal("prepare succeeded; want the server to refuse the client")
			}
		})
	}
	time.Sleep(3 * founder.cfg.Interval)
	if !hasMembers(founder, []uint64{1, 2, 3}) {
		v, _ := founder.current()
		t.Fatalf("members = %v after refused joins; want [1 2 3]", memberIDs(v))
	}
	if !hasCore(founder, []uint64{1, 2, 3}) {
		v, _ := founder.current()
		t.Fatalf("core = %v after refused demotes; want [1 2 3]", v.Core)
	}
}

func mustCA(t *testing.T) *mtls.CA {
	t.Helper()
	ca, err := mtls.NewCA("other-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func mustRequest(t *testing.T, method, url string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return req
}
