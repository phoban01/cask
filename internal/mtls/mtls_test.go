package mtls

import (
	"context"
	"crypto/tls"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// member is one issued identity in PEM form.
type member struct{ cert, key, ca []byte }

func issue(t *testing.T, ca *CA, name string) member {
	t.Helper()
	c, k, err := ca.Issue(name, "127.0.0.1", name)
	if err != nil {
		t.Fatal(err)
	}
	return member{cert: c, key: k, ca: ca.CertPEM()}
}

func newCA(t *testing.T) *CA {
	t.Helper()
	ca, err := NewCA("test-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func TestRegisterParsesFlags(t *testing.T) {
	var f Files
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	f.Register(fs)
	err := fs.Parse([]string{"--consensus-cert=c.pem", "--consensus-key=k.pem", "--consensus-ca=ca.pem"})
	if err != nil {
		t.Fatal(err)
	}
	if f != (Files{Cert: "c.pem", Key: "k.pem", CA: "ca.pem"}) {
		t.Fatalf("files = %+v", f)
	}
}

func TestNewBuildsServerAndClientConfigs(t *testing.T) {
	m := issue(t, newCA(t), "east")
	cfg, err := New(m.cert, m.key, m.ca)
	if err != nil {
		t.Fatal(err)
	}
	s, c := cfg.Server, cfg.Client
	if s.MinVersion != tls.VersionTLS13 || c.MinVersion != tls.VersionTLS13 {
		t.Errorf("min versions = %x, %x; want TLS 1.3", s.MinVersion, c.MinVersion)
	}
	if s.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("server ClientAuth = %v, want RequireAndVerifyClientCert", s.ClientAuth)
	}
	if s.ClientCAs == nil || len(s.Certificates) != 1 {
		t.Error("server config lacks the CA pool or the member certificate")
	}
	if c.VerifyConnection == nil || len(c.Certificates) != 1 {
		t.Error("client config lacks the chain check or the member certificate")
	}
}

// Two members of one CA complete a handshake with the built configs.
func TestConfigsHandshake(t *testing.T) {
	ca := newCA(t)
	a, b := issue(t, ca, "east"), issue(t, ca, "west")
	srv, err := New(a.cert, a.key, a.ca)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := New(b.cert, b.key, b.ca)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(srv.Server, cli.Client); err != nil {
		t.Fatalf("handshake: %v", err)
	}
}

// A server from another CA fails the client's chain check.
func TestClientRejectsServerFromOtherCA(t *testing.T) {
	a, b := issue(t, newCA(t), "east"), issue(t, newCA(t), "rogue")
	cli, err := New(a.cert, a.key, a.ca)
	if err != nil {
		t.Fatal(err)
	}
	rogue, err := New(b.cert, b.key, b.ca)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(rogue.Server, cli.Client); err == nil {
		t.Fatal("client accepted a server certificate from another CA")
	}
}

func handshake(server, client *tls.Config) error {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		s := tls.Server(c1, server)
		err := s.HandshakeContext(ctx)
		if err == nil {
			// TLS 1.3 checks the client certificate after the client
			// finishes; a read surfaces the server's verdict.
			_, err = s.Write([]byte("x"))
		}
		errc <- err
	}()
	c := tls.Client(c2, client)
	err := c.HandshakeContext(ctx)
	if err == nil {
		_, err = c.Read(make([]byte, 1))
	}
	c2.Close()
	if serr := <-errc; err == nil {
		err = serr
	}
	return err
}

func TestNewRejectsBadInput(t *testing.T) {
	ca := newCA(t)
	a, b := issue(t, ca, "east"), issue(t, ca, "west")
	other := issue(t, newCA(t), "rogue")
	for name, tc := range map[string]struct {
		cert, key, ca []byte
		want          string
	}{
		"key of another member": {a.cert, b.key, a.ca, "certificate and key"},
		"cert from another CA":  {other.cert, other.key, a.ca, "does not chain"},
		"empty CA":              {a.cert, a.key, nil, "no PEM certificate"},
		"missing cert":          {nil, a.key, a.ca, "certificate and key"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(tc.cert, tc.key, tc.ca)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadRejectsMissingFiles(t *testing.T) {
	dir := t.TempDir()
	m := issue(t, newCA(t), "east")
	cert, key, ca := write(t, dir, "tls.crt", m.cert), write(t, dir, "tls.key", m.key), write(t, dir, "ca.crt", m.ca)
	if _, err := Load(Files{Cert: cert, Key: key, CA: ca}); err != nil {
		t.Fatalf("load: %v", err)
	}
	for name, f := range map[string]Files{
		"no key":       {Cert: cert, CA: ca},
		"no CA":        {Cert: cert, Key: key},
		"absent file":  {Cert: cert, Key: filepath.Join(dir, "nope"), CA: ca},
		"swapped pair": {Cert: key, Key: cert, CA: ca},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(f); err == nil {
				t.Fatal("load succeeded; want an error")
			}
		})
	}
}

func TestSetupWithoutFlagsIsPlaintext(t *testing.T) {
	cfg, err := Setup(Files{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || cfg != nil {
		t.Fatalf("Setup() = %v, %v; want nil, nil", cfg, err)
	}
	if _, err := Setup(Files{Cert: "c.pem"}, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("Setup with one flag succeeded; want an error")
	}
}

func write(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
