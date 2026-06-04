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

// NodeSpec describes one node to generate a Nebula identity and config for.
type NodeSpec struct {
	Name       string     // node name (also the output config key)
	OverlayIP  netip.Addr // address on the overlay, e.g. 10.42.0.1
	Zone       string     // failure domain, emitted as the "zone:<z>" cert group
	UDP        string     // underlay listen (bind) address, e.g. "0.0.0.0:4242"
	Advertise  string     // public underlay address peers dial via static_host_map; falls back to UDP when empty (lighthouses only)
	Lighthouse bool       // whether this node is a lighthouse (bootstrap + rendezvous)
}

// GenerateConfigs mints a fresh CA and one signed certificate per node, and
// returns a ready-to-run Nebula YAML config per node (keyed by node name). This
// is the one place cask creates Nebula PKI; it lets the self-forming overlay be
// demoed without the external nebula-cert tool. The CA private key is used only
// here and never persisted.
//
// Lighthouses are marked am_lighthouse; every other node is given a
// static_host_map and lighthouse.hosts pointing at all lighthouses, so the
// cluster rendezvouses through them.
func GenerateConfigs(nodes []NodeSpec) (map[string]string, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("nebula: no nodes specified")
	}
	before := time.Now().Add(-time.Hour)
	after := time.Now().Add(365 * 24 * time.Hour)

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("nebula: ca keypair: %w", err)
	}
	caTBS := &cert.TBSCertificate{
		Curve: cert.Curve_CURVE25519, Version: cert.Version2, Name: "cask-ca",
		NotBefore: before, NotAfter: after, PublicKey: caPub, IsCA: true,
	}
	caCert, err := caTBS.Sign(nil, cert.Curve_CURVE25519, caPriv)
	if err != nil {
		return nil, fmt.Errorf("nebula: sign ca: %w", err)
	}
	caPEM, err := caCert.MarshalPEM()
	if err != nil {
		return nil, fmt.Errorf("nebula: marshal ca: %w", err)
	}

	// Lighthouse rendezvous data, shared by every member. Peers dial a lighthouse
	// at its Advertise address (its public underlay address); UDP is only the bind
	// address, so for a cloud lighthouse UDP="0.0.0.0:4242" while Advertise is the
	// reachable "<public-ip>:4242". Advertise falls back to UDP for loopback demos.
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
		pub, priv := x25519Keypair()
		var groups []string
		if n.Zone != "" {
			groups = []string{zoneGroupPrefix + n.Zone}
		}
		tbs := &cert.TBSCertificate{
			Version: cert.Version2, Curve: cert.Curve_CURVE25519, Name: n.Name,
			Networks:  []netip.Prefix{netip.PrefixFrom(n.OverlayIP, 24)},
			Groups:    groups,
			NotBefore: before, NotAfter: after, PublicKey: pub,
		}
		c, err := tbs.Sign(caCert, cert.Curve_CURVE25519, caPriv)
		if err != nil {
			return nil, fmt.Errorf("nebula: sign %s: %w", n.Name, err)
		}
		certPEM, err := c.MarshalPEM()
		if err != nil {
			return nil, fmt.Errorf("nebula: marshal %s: %w", n.Name, err)
		}
		keyPEM := cert.MarshalPrivateKeyToPEM(cert.Curve_CURVE25519, priv)

		host, portStr, err := net.SplitHostPort(n.UDP)
		if err != nil {
			return nil, fmt.Errorf("nebula: udp addr %q: %w", n.UDP, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("nebula: udp port %q: %w", portStr, err)
		}

		lighthouse := map[string]any{}
		static := map[string][]string{}
		if n.Lighthouse {
			lighthouse["am_lighthouse"] = true
			for ip, addr := range lhStatic {
				if ip != n.OverlayIP.String() {
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
		raw, err := yaml.Marshal(conf)
		if err != nil {
			return nil, fmt.Errorf("nebula: marshal config %s: %w", n.Name, err)
		}
		out[n.Name] = string(raw)
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
