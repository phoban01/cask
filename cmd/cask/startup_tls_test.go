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
// The test binary starts itself with it set, so a test starts a real node
// through the same flag parsing and checks as the binary.
const mainArgsEnv = "CASK_TEST_MAIN_ARGS"

func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv(mainArgsEnv); ok {
		os.Args = append([]string{"cask"}, strings.Split(args, "\n")...)
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
	stderr *syncBuffer
	done   chan error
}

// startMain runs main in a child process with args. The test kills the
// child when it ends.
func startMain(t *testing.T, args ...string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), mainArgsEnv+"="+strings.Join(args, "\n"))
	c := &child{stderr: &syncBuffer{}, done: make(chan error, 1)}
	cmd.Stderr = c.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { c.done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-c.done
	})
	return c
}

// waitExit waits for the child to exit and returns its exit code.
func (c *child) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case err := <-c.done:
		c.done <- err
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0
	case <-time.After(20 * time.Second):
		t.Fatalf("node did not exit; stderr:\n%s", c.stderr.String())
		return -1
	}
}

// waitGet repeats GET url with hc until it returns a status. It fails when
// the child exits first or no status comes in time.
func (c *child) waitGet(t *testing.T, hc *http.Client, url string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		code, err := get(hc, url)
		if err == nil {
			return code
		}
		select {
		case exitErr := <-c.done:
			c.done <- exitErr
			t.Fatalf("node exited (%v); stderr:\n%s", exitErr, c.stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s: %v; stderr:\n%s", url, err, c.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func get(hc *http.Client, url string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

// A static node serves consensus on --listen. Without the consensus TLS
// flags it does not start, and it names the flags that fix it. In overlay
// mode, the TLS flags and --insecure-consensus do not apply.
func TestStartRefusesPlaintextConsensus(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# Consensus traffic between members MUST use mutual TLS.
	for name, tc := range map[string]struct {
		args []string
		want []string
	}{
		"static": {
			args: []string{"--id=1", "--listen=" + freeAddr(t)},
			want: []string{"needs mutual TLS", "--consensus-cert", "--insecure-consensus"},
		},
		"overlay with --insecure-consensus": {
			args: []string{"--nebula-config=unused.yml", "--insecure-consensus"},
			want: []string{"the Nebula overlay authenticates peers"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := startMain(t, tc.args...)
			if code := c.waitExit(t); code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, c.stderr.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(c.stderr.String(), want) {
					t.Errorf("stderr lacks %q:\n%s", want, c.stderr.String())
				}
			}
		})
	}
}

// With --insecure-consensus, a static node starts, serves in plaintext,
// and logs a warning.
func TestStartWithInsecureConsensus(t *testing.T) {
	addr := freeAddr(t)
	c := startMain(t, "--id=1", "--listen="+addr, "--insecure-consensus")
	if code := c.waitGet(t, http.DefaultClient, "http://"+addr+"/kv/missing"); code != http.StatusNotFound {
		t.Fatalf("GET /kv/missing = %d, want 404", code)
	}
	if !strings.Contains(c.stderr.String(), "CONSENSUS TRAFFIC IS PLAINTEXT AND UNAUTHENTICATED") {
		t.Errorf("no plaintext warning; stderr:\n%s", c.stderr.String())
	}
}

// With the three consensus TLS flags, a static node starts and serves over
// mutual TLS: a member of the fleet CA reaches it, and a client without a
// certificate does not.
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
	cert, key, err := ca.Issue("node-1", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	c := startMain(t, "--id=1", "--listen="+addr,
		"--consensus-cert="+write("tls.crt", cert),
		"--consensus-key="+write("tls.key", key),
		"--consensus-ca="+write("ca.crt", ca.CertPEM()))

	pc, pk, err := ca.Issue("node-2", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := mtls.New(pc, pk, ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: peer.Client}}
	if code := c.waitGet(t, hc, "https://"+addr+"/kv/missing"); code != http.StatusNotFound {
		t.Fatalf("GET /kv/missing as a fleet member = %d, want 404", code)
	}
	anon := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, // the check under test is the server's
		MinVersion:         tls.VersionTLS13,
	}}}
	if code, err := get(anon, "https://"+addr+"/kv/missing"); err == nil {
		t.Fatalf("GET without a client certificate = %d; want an error", code)
	}
	if code, err := get(http.DefaultClient, "http://"+addr+"/kv/missing"); err == nil && code == http.StatusNotFound {
		t.Fatal("GET in plaintext = 404 from the handler; want a refusal")
	}
}
