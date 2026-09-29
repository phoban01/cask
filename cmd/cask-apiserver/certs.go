package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/phoban01/cask/internal/mtls"
	"k8s.io/apimachinery/pkg/util/validation"
)

// genConsensusCerts implements `cask-apiserver gen-consensus-certs`. It
// makes a fleet CA and one member certificate per name, and writes ca.crt,
// <name>.crt, and <name>.key to --out. It does not write the CA key, so a
// later member needs a new CA. The kind demo uses it; a real fleet uses its
// own CA.
func genConsensusCerts(args []string) error {
	flags := flag.NewFlagSet("gen-consensus-certs", flag.ContinueOnError)
	out := flags.String("out", ".", "directory to write ca.crt, <name>.crt, and <name>.key to")
	ttl := flags.Duration("ttl", 365*24*time.Hour, "validity of the CA and the member certificates")
	if err := flags.Parse(args); err != nil {
		return err
	}
	names := flags.Args()
	if len(names) == 0 {
		return fmt.Errorf("usage: cask-apiserver gen-consensus-certs [--out dir] name...")
	}
	for _, name := range names {
		// A name becomes a file name in --out. It must not reach outside
		// --out, and "ca" would overwrite the CA certificate.
		if name == "." || name == ".." || name == "ca" || strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") {
			return fmt.Errorf("bad member name %q: use a plain name such as east", name)
		}
	}
	ca, err := mtls.NewCA("cask-consensus-ca", *ttl)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "ca.crt"), ca.CertPEM(), 0o644); err != nil {
		return err
	}
	for _, name := range names {
		cert, key, err := ca.Issue(name, name)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, name+".crt"), cert, 0o644); err != nil {
			return err
		}
		if err := writeKey(filepath.Join(*out, name+".key"), key); err != nil {
			return err
		}
	}
	return nil
}

// genServingCerts implements `cask-apiserver gen-serving-certs`. It makes
// a serving CA and one serving certificate for the Service of the
// apiserver, and writes ca.crt, tls.crt, and tls.key to --out. The
// APIService carries ca.crt as its caBundle, so the kube-apiserver can
// verify the apiserver. It does not write the CA key. The kind demo and
// the e2e suite use it; a real cluster can use any CA, such as
// cert-manager.
func genServingCerts(args []string) error {
	flags := flag.NewFlagSet("gen-serving-certs", flag.ContinueOnError)
	out := flags.String("out", ".", "directory to write ca.crt, tls.crt, and tls.key to")
	ttl := flags.Duration("ttl", 365*24*time.Hour, "validity of the CA and the serving certificate")
	service := flags.String("service", "cask-apiserver", "name of the Service in front of the apiserver")
	namespace := flags.String("namespace", "cask-system", "namespace of the Service")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("usage: cask-apiserver gen-serving-certs [--out dir] [--service name] [--namespace name]")
	}
	// Each name becomes part of a DNS name in the certificate. A DNS label
	// has no dot, no slash, and no backslash.
	for _, n := range []struct{ flag, value string }{{"service", *service}, {"namespace", *namespace}} {
		if errs := validation.IsDNS1123Label(n.value); len(errs) > 0 {
			return fmt.Errorf("bad --%s %q: %s", n.flag, n.value, strings.Join(errs, "; "))
		}
	}
	ca, cert, key, err := issueServing(*service, *namespace, *ttl)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "ca.crt"), ca, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "tls.crt"), cert, 0o644); err != nil {
		return err
	}
	return writeKey(filepath.Join(*out, "tls.key"), key)
}

// issueServing makes a serving CA and a serving certificate and key for
// the Service namespace/service, all in PEM form. The certificate names
// every DNS name of the Service. The kube-apiserver dials an APIService
// backend as <service>.<namespace>.svc and checks that name.
func issueServing(service, namespace string, ttl time.Duration) (caPEM, certPEM, keyPEM []byte, err error) {
	//= docs/spec/fleet.md#8-security
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	ca, err := mtls.NewCA("cask-serving-ca", ttl)
	if err != nil {
		return nil, nil, nil, err
	}
	svc := service + "." + namespace + ".svc"
	certPEM, keyPEM, err = ca.Issue(svc, service, service+"."+namespace, svc, svc+".cluster.local")
	if err != nil {
		return nil, nil, nil, err
	}
	return ca.CertPEM(), certPEM, keyPEM, nil
}

// writeKey writes a private key readable by its owner only. os.WriteFile
// keeps the mode of a file that exists, so writeKey removes it first and
// creates the new file exclusively.
func writeKey(path string, key []byte) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(key); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
