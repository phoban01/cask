// Package mtls builds the mutual TLS configs for consensus traffic between
// fleet members.
//
// Every member holds one certificate and key, signed by one fleet CA. The
// member presents the same certificate as a server and as a client. The
// server side requires a client certificate that chains to the CA. The
// client side checks that the server certificate chains to the CA.
//
// The client does not check the server host name. Members reach each other
// by node IP or overlay address, and those change. The trust rule is: a
// certificate that the fleet CA signed is a member. Keep the CA key off the
// members.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
)

// Files names the PEM files that hold a member's consensus identity.
type Files struct {
	Cert string // the member certificate (--consensus-cert)
	Key  string // the private key of Cert (--consensus-key)
	CA   string // the fleet CA certificate (--consensus-ca)
}

// Register adds --consensus-cert, --consensus-key, and --consensus-ca to fs.
func (f *Files) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Cert, "consensus-cert", "", "PEM certificate this member presents on consensus traffic, as server and as client; set with --consensus-key and --consensus-ca")
	fs.StringVar(&f.Key, "consensus-key", "", "PEM private key for --consensus-cert")
	fs.StringVar(&f.CA, "consensus-ca", "", "PEM fleet CA; a peer must present a certificate that this CA signed")
}

// Any reports whether at least one of the three files is set.
func (f Files) Any() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

// Config is the pair of TLS configs for one member.
type Config struct {
	Server *tls.Config // for the consensus listener
	Client *tls.Config // for every call to a peer
}

// Setup loads the configs named by f. With no file set, it logs a warning
// and returns nil: consensus traffic then stays plaintext. With some but
// not all files set, it fails, so a typo never turns TLS off.
func Setup(f Files, log *slog.Logger) (*Config, error) {
	if !f.Any() {
		log.Warn("CONSENSUS TRAFFIC IS PLAINTEXT AND UNAUTHENTICATED: anyone who reaches the consensus port can read and write fleet state; set --consensus-cert, --consensus-key, and --consensus-ca")
		return nil, nil
	}
	return Load(f)
}

// Load reads the three files and builds the configs. All three must be set.
func Load(f Files) (*Config, error) {
	if f.Cert == "" || f.Key == "" || f.CA == "" {
		return nil, errors.New("mtls: set --consensus-cert, --consensus-key, and --consensus-ca together")
	}
	cert, err := os.ReadFile(f.Cert)
	if err != nil {
		return nil, fmt.Errorf("mtls: read certificate: %w", err)
	}
	key, err := os.ReadFile(f.Key)
	if err != nil {
		return nil, fmt.Errorf("mtls: read key: %w", err)
	}
	ca, err := os.ReadFile(f.CA)
	if err != nil {
		return nil, fmt.Errorf("mtls: read CA: %w", err)
	}
	return New(cert, key, ca)
}

// New builds the configs from PEM data. It fails when the key does not
// match the certificate, when the CA holds no certificate, or when the CA
// did not sign the certificate for both server and client use.
func New(certPEM, keyPEM, caPEM []byte) (*Config, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("mtls: certificate and key: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("mtls: CA file holds no PEM certificate")
	}
	// A member is a server and a client, so its own certificate must pass
	// both checks that its peers run.
	inter := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("mtls: intermediate certificate: %w", err)
		}
		inter.AddCert(c)
	}
	for _, use := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		opts := x509.VerifyOptions{Roots: pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{use}}
		if _, err := pair.Leaf.Verify(opts); err != nil {
			return nil, fmt.Errorf("mtls: certificate does not chain to the CA: %w", err)
		}
	}

	server := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	client := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		// The default check also matches the host name, which this package
		// does not use (see the package comment). VerifyConnection runs the
		// chain check against the CA instead, so no server passes without
		// a certificate that the CA signed.
		InsecureSkipVerify: true, //nolint:gosec // VerifyConnection checks the chain.
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyPeer(cs, pool, x509.ExtKeyUsageServerAuth)
		},
	}
	return &Config{Server: server, Client: client}, nil
}

// verifyPeer checks that the peer certificate chains to roots for use.
func verifyPeer(cs tls.ConnectionState, roots *x509.CertPool, use x509.ExtKeyUsage) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("mtls: peer sent no certificate")
	}
	inter := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		inter.AddCert(c)
	}
	opts := x509.VerifyOptions{Roots: roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{use}}
	if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
		return fmt.Errorf("mtls: peer certificate: %w", err)
	}
	return nil
}
