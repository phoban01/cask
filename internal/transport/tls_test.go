package transport_test

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mtls"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

// tlsMember issues a member identity from ca and returns its configs.
func tlsMember(t *testing.T, ca *mtls.CA, name string) *mtls.Config {
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

func testCA(t *testing.T) *mtls.CA {
	t.Helper()
	ca, err := mtls.NewCA("test-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

// serveTLS serves an acceptor over ConnectRPC on a mutual TLS listener and
// returns its base URL.
func serveTLS(t *testing.T, cfg *mtls.Config) string {
	t.Helper()
	url, _ := serveTLSLog(t, cfg)
	return url
}

// serverLog collects the error log of an http.Server.
type serverLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *serverLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *serverLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// serveTLSLog is serveTLS that also returns the server's error log, where
// the server reports each refused handshake.
func serveTLSLog(t *testing.T, cfg *mtls.Config) (string, *serverLog) {
	t.Helper()
	nw := transport.TLS{Net: transport.TCP{}, Server: cfg.Server, Client: cfg.Client}
	ln, err := nw.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(transport.ConnectHandler(caspaxos.NewAcceptor(store.NewMem())))
	logs := &serverLog{}
	srv := &http.Server{Handler: mux, ErrorLog: log.New(logs, "", 0)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String(), logs
}

// Three acceptors behind mutual TLS listeners agree on a value.
func TestProposeOverMutualTLS(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca := testCA(t)
	var urls []string
	for _, name := range []string{"a", "b", "c"} {
		urls = append(urls, serveTLS(t, tlsMember(t, ca, name)))
	}
	self := tlsMember(t, ca, "proposer")
	hc := transport.TLS{Net: transport.TCP{}, Server: self.Server, Client: self.Client}.HTTPClient()
	var clients []caspaxos.AcceptorClient
	for _, u := range urls {
		clients = append(clients, transport.NewConnectClient(u, hc))
	}
	p := caspaxos.NewProposer(1, clients)
	if _, err := p.Propose(ctx, []byte("k"), caspaxos.Write([]byte("v"))); err != nil {
		t.Fatalf("propose over mutual TLS: %v", err)
	}
	got, err := caspaxos.NewProposer(2, clients).Propose(ctx, []byte("k"), caspaxos.Identity)
	if err != nil || string(got) != "v" {
		t.Fatalf("read = %q, %v; want v", got, err)
	}
}

// A client with no certificate, or with a certificate from another CA,
// cannot reach the acceptor. A plaintext client cannot either.
func TestMutualTLSRefusesUnknownClients(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	ca := testCA(t)
	url, logs := serveTLSLog(t, tlsMember(t, ca, "a"))
	rogue := tlsMember(t, testCA(t), "rogue")

	noCert := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true} //nolint:gosec // the test targets the server check.
	otherCA := rogue.Client.Clone()
	otherCA.VerifyConnection = nil // trust the server, so only the server's check can refuse
	// Each case names the refusal the server logs for its handshake. The
	// client error text depends on timing: in TLS 1.3 the client can
	// finish its side of the handshake before the server's alert arrives.
	for _, tc := range []struct {
		name      string
		hc        *http.Client
		serverLog string
	}{
		{"no certificate", transport.TLS{Net: transport.TCP{}, Client: noCert}.HTTPClient(), "client didn't provide a certificate"},
		{"another CA", transport.TLS{Net: transport.TCP{}, Client: otherCA}.HTTPClient(), "failed to verify certificate"},
		{"plaintext", transport.TCP{}.HTTPClient(), "client sent an HTTP request to an HTTPS server"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c := transport.NewConnectClient(url, tc.hc)
			// A refused prepare comes back as an error, which the proposer
			// counts as a missing vote.
			if _, err := c.Prepare(ctx, []byte("k"), caspaxos.Ballot{Counter: 1, NodeID: 9}); err == nil {
				t.Fatal("prepare succeeded; want the server to refuse the client")
			}
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(logs.String(), tc.serverLog) {
				if time.Now().After(deadline) {
					t.Fatalf("server log %q does not contain %q", logs.String(), tc.serverLog)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	// A client from another CA also refuses this server, before it sends a
	// request.
	c := transport.NewConnectClient(url, transport.TLS{Net: transport.TCP{}, Client: rogue.Client}.HTTPClient())
	_, err := c.Prepare(context.Background(), []byte("k"), caspaxos.Ballot{Counter: 1, NodeID: 9})
	if err == nil || !strings.Contains(err.Error(), "mtls: peer certificate") {
		t.Fatalf("err = %v, want the client to refuse the server certificate", err)
	}
}

// A listener without a client certificate requirement is refused, so a
// wrong config cannot turn the check off.
func TestTLSListenerNeedsClientAuth(t *testing.T) {
	cfg := tlsMember(t, testCA(t), "a")
	weak := cfg.Server.Clone()
	weak.ClientAuth = tls.VerifyClientCertIfGiven
	if _, err := (transport.TLS{Net: transport.TCP{}, Server: weak}).Listen(context.Background(), "127.0.0.1:0"); err == nil {
		t.Fatal("listen succeeded without RequireAndVerifyClientCert")
	}
}
