package nebula

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/slackhq/nebula/cert"
	yaml "go.yaml.in/yaml/v3"
	"golang.org/x/crypto/curve25519"
)

// caTTL is the validity window for a generated CA, and certTTL for node certs.
const (
	caTTL   = 10 * 365 * 24 * time.Hour
	certTTL = 365 * 24 * time.Hour
)

// NodeSpec describes one node to generate a Nebula identity and config for.
type NodeSpec struct {
	Name       string     // node name (also the output config key)
	OverlayIP  netip.Addr // address on the overlay, e.g. 10.42.0.1
	Zone       string     // failure domain, emitted as the "zone:<z>" cert group
	UDP        string     // underlay listen (bind) address, e.g. "0.0.0.0:4242"
	Advertise  string     // public underlay address peers dial via static_host_map; falls back to UDP when empty (lighthouses only). May be a DNS name, which Nebula re-resolves.
	Lighthouse bool       // whether this node is a lighthouse (bootstrap + rendezvous)
	PrefixBits int        // overlay network prefix length for the cert; 0 => 24
}

func (n NodeSpec) prefixBits() int {
	if n.PrefixBits == 0 {
		return 24
	}
	return n.PrefixBits
}

// CA is a Nebula certificate authority: the signing certificate plus its private
// key. Unlike GenerateConfigs (which mints an ephemeral CA), a CA can be
// persisted and reloaded, so a long-lived signer (e.g. `cask mint`) issues node
// certs that all chain to one stable root.
type CA struct {
	Cert cert.Certificate
	key  []byte // ed25519 signing private key
}

// CreateCA mints a fresh CA valid for caTTL.
func CreateCA(name string) (*CA, error) {
	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("nebula: ca keypair: %w", err)
	}
	tbs := &cert.TBSCertificate{
		Curve: cert.Curve_CURVE25519, Version: cert.Version2, Name: name,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(caTTL), PublicKey: caPub, IsCA: true,
	}
	caCert, err := tbs.Sign(nil, cert.Curve_CURVE25519, caPriv)
	if err != nil {
		return nil, fmt.Errorf("nebula: sign ca: %w", err)
	}
	return &CA{Cert: caCert, key: caPriv}, nil
}

// MarshalCA returns the CA's PEM-encoded certificate and private key, for
// persistence.
func (ca *CA) MarshalCA() (certPEM, keyPEM []byte, err error) {
	certPEM, err = ca.Cert.MarshalPEM()
	if err != nil {
		return nil, nil, fmt.Errorf("nebula: marshal ca cert: %w", err)
	}
	keyPEM = cert.MarshalSigningPrivateKeyToPEM(cert.Curve_CURVE25519, ca.key)
	return certPEM, keyPEM, nil
}

// LoadCA reconstructs a CA from its PEM-encoded certificate and private key.
func LoadCA(certPEM, keyPEM []byte) (*CA, error) {
	c, _, err := cert.UnmarshalCertificateFromPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("nebula: load ca cert: %w", err)
	}
	key, _, _, err := cert.UnmarshalSigningPrivateKeyFromPEM(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("nebula: load ca key: %w", err)
	}
	return &CA{Cert: c, key: key}, nil
}

// CAPEM returns just the CA certificate PEM (handed to nodes as pki.ca).
func (ca *CA) CAPEM() ([]byte, error) { return ca.Cert.MarshalPEM() }

// MintNodeCert signs a node certificate for spec valid for certTTL, returning
// the certificate and its private key in PEM.
func (ca *CA) MintNodeCert(spec NodeSpec) (certPEM, keyPEM []byte, err error) {
	pub, priv := x25519Keypair()
	var groups []string
	if spec.Zone != "" {
		groups = []string{zoneGroupPrefix + spec.Zone}
	}
	tbs := &cert.TBSCertificate{
		Version: cert.Version2, Curve: cert.Curve_CURVE25519, Name: spec.Name,
		Networks:  []netip.Prefix{netip.PrefixFrom(spec.OverlayIP, spec.prefixBits())},
		Groups:    groups,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(certTTL), PublicKey: pub,
	}
	c, err := tbs.Sign(ca.Cert, cert.Curve_CURVE25519, ca.key)
	if err != nil {
		return nil, nil, fmt.Errorf("nebula: sign %s: %w", spec.Name, err)
	}
	certPEM, err = c.MarshalPEM()
	if err != nil {
		return nil, nil, fmt.Errorf("nebula: marshal %s: %w", spec.Name, err)
	}
	keyPEM = cert.MarshalPrivateKeyToPEM(cert.Curve_CURVE25519, priv)
	return certPEM, keyPEM, nil
}

// BuildNebulaConfig assembles a ready-to-run Nebula YAML config for spec, given
// its PEM material and the cluster's lighthouse rendezvous data: lhHosts is the
// list of lighthouse overlay IPs, and lhStatic maps each lighthouse overlay IP
// to its dialable underlay address(es). A lighthouse omits itself from both.
//
// When any static_host_map value is a DNS name (rather than an IP), a static_map
// block is emitted so Nebula re-resolves it robustly across IPv4/IPv6 and over
// slower public DNS. No relay keys are ever emitted — cask relies on
// lighthouse-coordinated hole-punching, leaving the network path to operators.
func BuildNebulaConfig(spec NodeSpec, caPEM, certPEM, keyPEM []byte, lhHosts []string, lhStatic map[string][]string) (string, error) {
	host, portStr, err := net.SplitHostPort(spec.UDP)
	if err != nil {
		return "", fmt.Errorf("nebula: udp addr %q: %w", spec.UDP, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("nebula: udp port %q: %w", portStr, err)
	}

	lighthouse := map[string]any{}
	static := map[string][]string{}
	if spec.Lighthouse {
		lighthouse["am_lighthouse"] = true
		// Peer lighthouses (every lighthouse except self). cask no longer derives
		// consensus genesis from this list, but it remains the underlay rendezvous
		// set so the mesh forms; a lighthouse must still know the others.
		var peers []string
		self := spec.OverlayIP.String()
		for _, h := range lhHosts {
			if h != self {
				peers = append(peers, h)
			}
		}
		if len(peers) > 0 {
			lighthouse["hosts"] = peers
			lighthouse["interval"] = 10
		}
		for ip, addr := range lhStatic {
			if ip != self {
				static[ip] = addr
			}
		}
	} else {
		lighthouse["am_lighthouse"] = false
		lighthouse["hosts"] = lhHosts
		lighthouse["interval"] = 10
		for ip, addr := range lhStatic {
			static[ip] = addr
		}
	}

	conf := map[string]any{
		"pki":             map[string]any{"ca": string(caPEM), "cert": string(certPEM), "key": string(keyPEM)},
		"static_host_map": static,
		"lighthouse":      lighthouse,
		"listen":          map[string]any{"host": host, "port": port},
		"punchy":          map[string]any{"punch": true},
		"firewall": map[string]any{
			"outbound": []map[string]any{{"proto": "any", "port": "any", "host": "any"}},
			"inbound":  []map[string]any{{"proto": "any", "port": "any", "host": "any"}},
		},
	}
	// If any rendezvous address is a DNS name, let Nebula re-resolve it: default
	// network is ip4 (drops IPv6-only underlays) and the 250ms default timeout is
	// tight for public DNS.
	if hasHostname(static) {
		conf["static_map"] = map[string]any{"network": "ip", "lookup_timeout": "1s"}
	}

	raw, err := yaml.Marshal(conf)
	if err != nil {
		return "", fmt.Errorf("nebula: marshal config %s: %w", spec.Name, err)
	}
	return string(raw), nil
}

// hasHostname reports whether any static_host_map value is a DNS name rather
// than a literal IP.
func hasHostname(static map[string][]string) bool {
	for _, addrs := range static {
		for _, a := range addrs {
			host, _, err := net.SplitHostPort(a)
			if err != nil {
				host = a
			}
			if net.ParseIP(host) == nil {
				return true
			}
		}
	}
	return false
}

// GenerateConfigs mints a fresh (ephemeral) CA and one signed certificate per
// node, returning a ready-to-run Nebula YAML config per node (keyed by node
// name). It is the offline path used by `cask gen-certs`; runtime enrollment
// uses the CA primitives above. Lighthouses are marked am_lighthouse; every
// other node points at all lighthouses so the cluster rendezvouses through them.
func GenerateConfigs(nodes []NodeSpec) (map[string]string, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("nebula: no nodes specified")
	}
	ca, err := CreateCA("cask-ca")
	if err != nil {
		return nil, err
	}
	caPEM, err := ca.CAPEM()
	if err != nil {
		return nil, err
	}

	// Lighthouse rendezvous data, shared by every member. Peers dial a lighthouse
	// at its Advertise address (its public underlay address); UDP is only the bind
	// address. Advertise falls back to UDP for loopback demos.
	var lhHosts []string
	lhStatic := map[string][]string{}
	for _, n := range nodes {
		if n.Lighthouse {
			adv := n.Advertise
			if adv == "" {
				adv = n.UDP
			}
			lhHosts = append(lhHosts, n.OverlayIP.String())
			lhStatic[n.OverlayIP.String()] = []string{adv}
		}
	}

	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		certPEM, keyPEM, err := ca.MintNodeCert(n)
		if err != nil {
			return nil, err
		}
		yml, err := BuildNebulaConfig(n, caPEM, certPEM, keyPEM, lhHosts, lhStatic)
		if err != nil {
			return nil, err
		}
		out[n.Name] = yml
	}
	return out, nil
}

func x25519Keypair() (pub, priv []byte) {
	priv = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, priv); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	pub, err := curve25519.X25519(priv, curve25519.Basepoint)
	if err != nil {
		panic(err)
	}
	return pub, priv
}
