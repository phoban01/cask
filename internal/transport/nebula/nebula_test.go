package nebula_test

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/slackhq/nebula/cert"
	"github.com/slackhq/nebula/cert_test"
	yaml "go.yaml.in/yaml/v3"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
	"github.com/phoban01/cask/internal/transport/nebula"
)

type m = map[string]any

// newNode mirrors nebula's own service test harness: it mints a node cert
// signed by ca for the given overlay IP and builds a Nebula-backed Network from
// a config merged with overrides.
func newNode(t *testing.T, ca cert.Certificate, caKey []byte, name string, ip netip.Addr, overrides m) *nebula.Network {
	t.Helper()
	before := time.Now().Add(-time.Hour)
	after := time.Now().Add(time.Hour)
	_, _, key, certPEM := cert_test.NewTestCert(
		cert.Version2, cert.Curve_CURVE25519, ca, caKey, name, before, after,
		[]netip.Prefix{netip.PrefixFrom(ip, 24)}, nil, nil,
	)
	caPEM, err := ca.MarshalPEM()
	if err != nil {
		t.Fatal(err)
	}

	conf := m{
		"pki": m{"ca": string(caPEM), "cert": string(certPEM), "key": string(key)},
		"firewall": m{
			"outbound": []m{{"proto": "any", "port": "any", "host": "any"}},
			"inbound":  []m{{"proto": "any", "port": "any", "host": "any"}},
		},
		"timers":     m{"pending_deletion_interval": 2, "connection_alive_interval": 2},
		"handshakes": m{"try_interval": "100ms"},
	}
	for k, v := range overrides {
		conf[k] = v
	}
	raw, err := yaml.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	net, err := nebula.New(string(raw), nil)
	if err != nil {
		t.Fatalf("build nebula network %q: %v", name, err)
	}
	t.Cleanup(func() { net.Close() })
	return net
}

// TestConsensusOverNebula stands up a real two-node Nebula overlay (lighthouse a
// + member b), serves a CASPaxos acceptor over ConnectRPC on a's overlay IP, and
// drives a Propose from b dialed through b's overlay HTTP client. It proves the
// Network seam carries the consensus transport over an actual encrypted tunnel,
// not just that it compiles.
func TestConsensusOverNebula(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up real Nebula tunnels; skipped in -short")
	}
	if raceEnabled {
		t.Skip("nebula's own handshake manager/lighthouse race under -race; backend is exercised in non-race builds")
	}

	ca, _, caKey, _ := cert_test.NewTestCaCert(
		cert.Version2, cert.Curve_CURVE25519,
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil, nil, nil,
	)

	aIP := netip.MustParseAddr("10.0.0.1")
	a := newNode(t, ca, caKey, "a", aIP, m{
		"static_host_map": m{},
		"lighthouse":      m{"am_lighthouse": true},
		"listen":          m{"host": "127.0.0.1", "port": 4243},
	})
	b := newNode(t, ca, caKey, "b", netip.MustParseAddr("10.0.0.2"), m{
		"static_host_map": m{"10.0.0.1": []string{"127.0.0.1:4243"}},
		"lighthouse":      m{"hosts": []string{"10.0.0.1"}, "interval": 1},
		"listen":          m{"host": "127.0.0.1", "port": 0},
	})

	// Serve a single-acceptor consensus group on a's overlay IP.
	acc := caspaxos.NewAcceptor(store.NewMem())
	path, h := transport.ConnectHandler(acc)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	ln, err := a.Listen(context.Background(), ":9000")
	if err != nil {
		t.Fatalf("overlay listen: %v", err)
	}
	go http.Serve(ln, mux) //nolint:errcheck // closed via t.Cleanup

	// b dials a over the overlay. The first call drives the Nebula handshake, so
	// allow generous time for the tunnel to establish.
	hc := b.HTTPClient()
	hc.Timeout = 25 * time.Second
	client := transport.NewConnectClient("http://10.0.0.1:9000", hc)
	prop := caspaxos.NewProposer(1, []caspaxos.AcceptorClient{client})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := prop.Propose(ctx, []byte("k"), caspaxos.Write([]byte("hello-overlay"))); err != nil {
		t.Fatalf("propose over nebula overlay: %v", err)
	}
	got, err := prop.Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil {
		t.Fatalf("read over nebula overlay: %v", err)
	}
	if string(got) != "hello-overlay" {
		t.Fatalf("read = %q, want hello-overlay", got)
	}
}
