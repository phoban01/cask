package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/mtls"
)

// mainArgsEnv carries the arguments of a child process that runs main.
// The test binary starts itself with it set, so a test starts a real
// member through the same flag parsing and checks as the binary.
const mainArgsEnv = "CASK_APISERVER_TEST_MAIN_ARGS"

func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv(mainArgsEnv); ok {
		os.Args = append([]string{"cask-apiserver"}, strings.Split(args, "\n")...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// syncBuffer is a bytes.Buffer that the child's stderr copier and the
// test read at the same time.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// child is a process that runs main.
type child struct {
	cmd    *exec.Cmd
	stderr *syncBuffer
	done   chan error
}

// startMain runs main in a child process with args. The test kills the
// child when it ends.
func startMain(t *testing.T, args ...string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), mainArgsEnv+"="+strings.Join(args, "\n"))
	m := &child{cmd: cmd, stderr: &syncBuffer{}, done: make(chan error, 1)}
	cmd.Stderr = m.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { m.done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-m.done
	})
	return m
}

// waitLog waits until the child logs want. It fails when the child exits
// first.
func (m *child) waitLog(t *testing.T, want string) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for !strings.Contains(m.stderr.String(), want) {
		select {
		case err := <-m.done:
			m.done <- err
			t.Fatalf("member exited (%v) before it logged %q; stderr:\n%s", err, want, m.stderr.String())
		case <-deadline:
			t.Fatalf("member did not log %q; stderr:\n%s", want, m.stderr.String())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// waitExit waits for the child to exit and returns its exit code.
func (m *child) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case err := <-m.done:
		m.done <- err
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0
	case <-time.After(20 * time.Second):
		t.Fatalf("member did not exit; stderr:\n%s", m.stderr.String())
		return -1
	}
}

// freeAddr returns a loopback address with a port that no one listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// founderArgs are the flags of a founding member with a consensus listener
// on addr. The legacy mux keeps the child serving without a kube-apiserver.
func founderArgs(t *testing.T, addr string) []string {
	return []string{
		"--cluster=east", "--id=1", "--bootstrap",
		"--listen-consensus=" + addr, "--advertise-consensus=" + addr,
		"--legacy-http", "--listen=" + freeAddr(t),
	}
}

// getRoster reads /roster on addr with hc and returns the status code.
func getRoster(hc *http.Client, url string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/roster", nil)
	if err != nil {
		return 0, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// A member with a consensus listener and no consensus TLS flags does not
// start. It names the flags that fix it.
func TestStartRefusesPlaintextConsensus(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
	for name, args := range map[string][]string{
		"bootstrap": founderArgs(t, freeAddr(t)),
		"cask-peers": {
			"--cluster=east", "--id=1", "--listen-consensus=127.0.0.1:1",
			"--advertise-consensus=127.0.0.1:1", "--cask-peers=127.0.0.1:1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := startMain(t, args...)
			if code := m.waitExit(t); code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, m.stderr.String())
			}
			out := m.stderr.String()
			for _, want := range []string{"needs mutual TLS", "--consensus-cert", "--insecure-consensus"} {
				if !strings.Contains(out, want) {
					t.Errorf("stderr lacks %q:\n%s", want, out)
				}
			}
			if strings.Contains(out, "embedded cask acceptor serving") {
				t.Errorf("member served consensus before it refused:\n%s", out)
			}
		})
	}
}

// With --insecure-consensus, the member starts, serves the roster in
// plaintext, and logs a warning.
func TestStartWithInsecureConsensus(t *testing.T) {
	addr := freeAddr(t)
	m := startMain(t, append(founderArgs(t, addr), "--insecure-consensus")...)
	m.waitLog(t, "serving the legacy mux")
	if !strings.Contains(m.stderr.String(), "CONSENSUS TRAFFIC IS PLAINTEXT AND UNAUTHENTICATED") {
		t.Errorf("no plaintext warning; stderr:\n%s", m.stderr.String())
	}
	code, err := getRoster(http.DefaultClient, "http://"+addr)
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET /roster = %d, %v; want 200", code, err)
	}
}

// With the three consensus TLS flags, the member starts and serves the
// roster over mutual TLS: a member of the fleet CA reads it, and a client
// without a certificate does not.
func TestStartWithConsensusTLS(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	ca, err := mtls.NewCA("fleet-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cert, key, err := ca.Issue("east", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	m := startMain(t, append(founderArgs(t, addr),
		"--consensus-cert="+write("tls.crt", cert),
		"--consensus-key="+write("tls.key", key),
		"--consensus-ca="+write("ca.crt", ca.CertPEM()))...)
	m.waitLog(t, "serving the legacy mux")
	if !strings.Contains(m.stderr.String(), "mtls=true") {
		t.Errorf("acceptor does not serve over mutual TLS; stderr:\n%s", m.stderr.String())
	}

	peer := memberTLS(t, ca, "west")
	code, err := getRoster(&http.Client{Transport: &http.Transport{TLSClientConfig: peer.Client}}, "https://"+addr)
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET /roster as a fleet member = %d, %v; want 200", code, err)
	}
	anon := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, // the check under test is the server's
		MinVersion:         tls.VersionTLS13,
	}}}
	if code, err := getRoster(anon, "https://"+addr); err == nil {
		t.Fatalf("GET /roster without a client certificate = %d; want an error", code)
	}
	if code, err := getRoster(http.DefaultClient, "http://"+addr); err == nil && code == http.StatusOK {
		t.Fatal("GET /roster in plaintext = 200; want a refusal")
	}
}
