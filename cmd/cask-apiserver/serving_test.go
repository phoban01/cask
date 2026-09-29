package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/mtls"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// servingName is the name the kube-apiserver checks when it dials the
// APIService backend.
const servingName = "cask-apiserver.cask-system.svc"

// readCert reads the first certificate of a PEM file.
func readCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatalf("%s: no PEM block", path)
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The serving certificate names every DNS name of the Service, and the
// serving CA verifies it for server use.
func TestGenServingCerts(t *testing.T) {
	dir := t.TempDir()
	if err := genServingCerts([]string{"--out", dir}); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("ca.crt holds no certificate")
	}
	cert := readCert(t, filepath.Join(dir, "tls.crt"))
	for _, name := range []string{
		"cask-apiserver",
		"cask-apiserver.cask-system",
		"cask-apiserver.cask-system.svc",
		"cask-apiserver.cask-system.svc.cluster.local",
	} {
		_, err := cert.Verify(x509.VerifyOptions{
			DNSName:   name,
			Roots:     roots,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")); err != nil {
		t.Fatalf("key pair: %v", err)
	}
	if err := genServingCerts([]string{"--out", dir, "extra"}); err == nil {
		t.Fatal("a positional argument: want an error")
	}
}

// --service and --namespace take a DNS label only, so a name cannot add a
// path or a wrong subject name.
func TestGenServingCertsRejectsBadNames(t *testing.T) {
	dir := t.TempDir()
	for _, bad := range []string{"", "../x", "a/b", `a\b`, "a.b", "..", "Upper"} {
		for _, flag := range []string{"--service", "--namespace"} {
			if err := genServingCerts([]string{"--out", dir, flag, bad}); err == nil {
				t.Errorf("%s %q: want an error", flag, bad)
			}
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("rejected names wrote %d files", len(entries))
	}
}

// The serving key is 0600 even when a looser file was there before.
func TestGenServingCertsKeyMode(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(key, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := genServingCerts([]string{"--out", dir}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %v, want 0600", st.Mode().Perm())
	}
}

// A client that trusts only the serving CA, and checks the Service name
// as the kube-apiserver does, reaches the server. A client that trusts
// another CA does not.
func TestServingCertVerifiedByCA(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	dir := t.TempDir()
	if err := genServingCerts([]string{"--out", dir}); err != nil {
		t.Fatal(err)
	}
	var opts *serverOptions
	ts := startTestServerWith(t, "east", false, func(o *serverOptions) {
		o.recommended.SecureServing.ServerCert.CertKey.CertFile = filepath.Join(dir, "tls.crt")
		o.recommended.SecureServing.ServerCert.CertKey.KeyFile = filepath.Join(dir, "tls.key")
		opts = o
	})
	if opts.selfSigned {
		t.Error("server with --tls-cert-file reports a self-signed certificate")
	}

	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	get := func(caPEM []byte) error {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			t.Fatal("no CA certificate")
		}
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: servingName, MinVersion: tls.VersionTLS12},
		}}
		resp, err := hc.Get(ts.url + "/readyz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("readyz = %d: %s", resp.StatusCode, body)
		}
		return nil
	}
	if err := get(caPEM); err != nil {
		t.Fatalf("client that trusts the serving CA: %v", err)
	}

	other, err := mtls.NewCA("other-ca", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	err = get(other.CertPEM())
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("client that trusts another CA: err = %v, want an unknown authority error", err)
	}
}

// With no --tls-cert-file the server falls back to a self-signed
// certificate and says so, so main can warn.
func TestServingCertSelfSignedFallback(t *testing.T) {
	var opts *serverOptions
	startTestServerWith(t, "east", false, func(o *serverOptions) { opts = o })
	if !opts.selfSigned {
		t.Fatal("server without --tls-cert-file does not report a self-signed certificate")
	}
}

// A certificate without its key, or a key without its certificate, is an
// error, not a silent fallback to a self-signed certificate.
func TestServingCertNeedsBothFiles(t *testing.T) {
	for _, set := range []func(o *serverOptions){
		func(o *serverOptions) { o.recommended.SecureServing.ServerCert.CertKey.CertFile = "/x/tls.crt" },
		func(o *serverOptions) { o.recommended.SecureServing.ServerCert.CertKey.KeyFile = "/x/tls.key" },
	} {
		o := newServerOptions()
		set(o)
		if _, err := o.newFleetServer("east", nil, nil); err == nil || !strings.Contains(err.Error(), "both") {
			t.Errorf("err = %v, want an error that asks for both files", err)
		}
	}
}

// The demo manifest, which the e2e suite also deploys, registers the
// APIService with the serving CA and without insecureSkipTLSVerify, and
// starts the apiserver with the serving certificate.
func TestDemoAPIServiceHasCABundle(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	f, err := os.Open(filepath.Join("..", "..", "demo", "kind", "manifests", "apiserver.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	var apiService, sts *unstructured.Unstructured
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: obj}
		switch u.GetKind() {
		case "APIService":
			apiService = u
		case "StatefulSet":
			sts = u
		}
	}
	if apiService == nil || sts == nil {
		t.Fatal("manifest has no APIService or no StatefulSet")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(apiService.Object, "spec", "insecureSkipTLSVerify"); found {
		t.Error("APIService sets insecureSkipTLSVerify")
	}
	if b, _, _ := unstructured.NestedString(apiService.Object, "spec", "caBundle"); b != "__SERVING_CA__" {
		t.Errorf("caBundle = %q, want the serving CA placeholder", b)
	}
	containers, _, _ := unstructured.NestedSlice(sts.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		t.Fatal("StatefulSet has no container")
	}
	args, _, _ := unstructured.NestedStringSlice(containers[0].(map[string]any), "args")
	for _, want := range []string{
		"--tls-cert-file=/etc/cask/serving/tls.crt",
		"--tls-private-key-file=/etc/cask/serving/tls.key",
	} {
		if !slices.Contains(args, want) {
			t.Errorf("apiserver args lack %s", want)
		}
	}
}
