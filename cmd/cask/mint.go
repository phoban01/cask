package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/phoban01/cask/internal/transport/nebula"
)

// mintReq is a node's enrollment request to `cask mint`.
type mintReq struct {
	Token string `json:"token"`
	Zone  string `json:"zone"`
	Role  string `json:"role"` // "replica" or "client"
}

// mintResp is the minted identity plus everything a node needs to build its
// Nebula config and find the cluster.
type mintResp struct {
	CA              string              `json:"ca"`
	Cert            string              `json:"cert"`
	Key             string              `json:"key"`
	Name            string              `json:"name"`
	OverlayIP       string              `json:"overlay_ip"`
	Role            string              `json:"role"`
	LighthouseHosts []string            `json:"lighthouse_hosts"`
	StaticHostMap   map[string][]string `json:"static_host_map"`
	DiscoverySRV    string              `json:"discovery_srv,omitempty"`
}

// minter is the stateless enrollment authority: a persistent CA plus the cluster
// rendezvous facts to hand back. It holds NO per-node state — identities are
// assigned by random overlay IP, so the same minter never needs to remember who
// it minted for (a re-mint is simply a new identity).
type minter struct {
	ca       *nebula.CA
	token    string
	prefix   netip.Prefix        // overlay address space to draw random IPs from
	lhHosts  []string            // lighthouse overlay IPs
	lhStatic map[string][]string // lighthouse overlay IP -> public underlay addr(s)
	srv      string              // discovery SRV name to advertise, if any
	log      *slog.Logger
}

// handleMint validates the token and issues a fresh random identity.
func (m *minter) handleMint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req mintReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.Token), []byte(m.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	role := req.Role
	if role != "client" {
		role = "replica"
	}

	ip, err := randomHostIP(m.prefix)
	if err != nil {
		http.Error(w, "address pool exhausted", http.StatusInternalServerError)
		return
	}
	name := fmt.Sprintf("cask-%08x", ipHostBits(ip, m.prefix))
	spec := nebula.NodeSpec{
		Name:       name,
		OverlayIP:  ip,
		Zone:       req.Zone,
		PrefixBits: m.prefix.Bits(),
	}
	certPEM, keyPEM, err := m.ca.MintNodeCert(spec)
	if err != nil {
		m.log.Error("mint node cert", "err", err)
		http.Error(w, "mint failed", http.StatusInternalServerError)
		return
	}
	caPEM, err := m.ca.CAPEM()
	if err != nil {
		http.Error(w, "mint failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(mintResp{
		CA:              string(caPEM),
		Cert:            string(certPEM),
		Key:             string(keyPEM),
		Name:            name,
		OverlayIP:       ip.String(),
		Role:            role,
		LighthouseHosts: m.lhHosts,
		StaticHostMap:   m.lhStatic,
		DiscoverySRV:    m.srv,
	})
}

// LoadOrCreateCA loads a persisted CA from dir, creating and persisting a fresh
// one if none exists. The key is written 0600.
func LoadOrCreateCA(dir string) (*nebula.CA, error) {
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")
	certPEM, cerr := os.ReadFile(certPath)
	keyPEM, kerr := os.ReadFile(keyPath)
	if cerr == nil && kerr == nil {
		return nebula.LoadCA(certPEM, keyPEM)
	}
	ca, err := nebula.CreateCA("cask-ca")
	if err != nil {
		return nil, err
	}
	cp, kp, err := ca.MarshalCA()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(certPath, cp, 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, kp, 0o600); err != nil {
		return nil, err
	}
	return ca, nil
}

// randomHostIP returns a uniformly random usable host address in prefix,
// avoiding the network/all-ones/lighthouse(.1) hosts.
func randomHostIP(prefix netip.Prefix) (netip.Addr, error) {
	prefix = prefix.Masked()
	addr := prefix.Addr()
	if !addr.Is4() {
		return netip.Addr{}, fmt.Errorf("only IPv4 overlay prefixes are supported")
	}
	hostBits := 32 - prefix.Bits()
	if hostBits < 2 {
		return netip.Addr{}, fmt.Errorf("overlay prefix /%d too small", prefix.Bits())
	}
	base := binary.BigEndian.Uint32(as4(addr))
	mask := uint32(1)<<uint(hostBits) - 1
	for tries := 0; tries < 1000; tries++ {
		host := randUint32() & mask
		if host == 0 || host == 1 || host == mask { // network, lighthouse, broadcast
			continue
		}
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], base|host)
		return netip.AddrFrom4(b), nil
	}
	return netip.Addr{}, fmt.Errorf("could not find a free host address")
}

func ipHostBits(ip netip.Addr, prefix netip.Prefix) uint32 {
	mask := uint32(1)<<uint(32-prefix.Bits()) - 1
	return binary.BigEndian.Uint32(as4(ip)) & mask
}

func as4(a netip.Addr) []byte {
	b := a.As4()
	return b[:]
}

func randUint32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}

// runMint implements `cask mint`: it loads/creates a persistent CA, self-mints a
// Nebula lighthouse identity (am_lighthouse, no relay), brings up the lighthouse
// for underlay rendezvous, and serves POST /mint on a PUBLIC host listener so
// nodes can enroll before they are on the overlay.
func runMint(args []string) {
	fs := flag.NewFlagSet("mint", flag.ExitOnError)
	var (
		listen   = fs.String("listen", ":8088", "public address to serve POST /mint on")
		token    = fs.String("token", "", "shared enrollment token (required)")
		caDir    = fs.String("ca-dir", "/opt/cask/ca", "directory holding the persistent CA")
		public   = fs.String("public-addr", "", "the lighthouse's public underlay address peers dial, e.g. <ip>:4242 (required)")
		prefixS  = fs.String("overlay-prefix", "10.42.0.0/16", "overlay address space to assign node IPs from")
		udp      = fs.String("udp", "0.0.0.0:4242", "lighthouse underlay bind address")
		srv      = fs.String("discovery-srv", "", "DNS-SRV name to advertise to nodes for peer discovery")
	)
	_ = fs.Parse(args)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *token == "" || *public == "" {
		log.Error("mint requires --token and --public-addr")
		os.Exit(1)
	}
	prefix, err := netip.ParsePrefix(*prefixS)
	if err != nil {
		log.Error("bad --overlay-prefix", "err", err)
		os.Exit(1)
	}
	ca, err := LoadOrCreateCA(*caDir)
	if err != nil {
		log.Error("load/create CA", "err", err)
		os.Exit(1)
	}

	// The lighthouse is host .1 of the prefix.
	lhIP := prefix.Masked().Addr().Next()
	lhHosts := []string{lhIP.String()}
	lhStatic := map[string][]string{lhIP.String(): {*public}}

	// Self-mint the lighthouse identity and bring up the lighthouse (no Listen:
	// it is pure Nebula rendezvous, not a cask acceptor).
	lhSpec := nebula.NodeSpec{Name: "lighthouse", OverlayIP: lhIP, Zone: "lighthouse", UDP: *udp, Advertise: *public, Lighthouse: true, PrefixBits: prefix.Bits()}
	certPEM, keyPEM, err := ca.MintNodeCert(lhSpec)
	if err != nil {
		log.Error("mint lighthouse cert", "err", err)
		os.Exit(1)
	}
	caPEM, _ := ca.CAPEM()
	lhConfig, err := nebula.BuildNebulaConfig(lhSpec, caPEM, certPEM, keyPEM, lhHosts, lhStatic)
	if err != nil {
		log.Error("build lighthouse config", "err", err)
		os.Exit(1)
	}
	if _, err := nebula.New(lhConfig, log); err != nil {
		log.Error("start lighthouse overlay", "err", err)
		os.Exit(1)
	}

	m := &minter{ca: ca, token: *token, prefix: prefix, lhHosts: lhHosts, lhStatic: lhStatic, srv: *srv, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/mint", m.handleMint)
	log.Info("cask mint serving", "listen", *listen, "lighthouse", lhIP, "public", *public, "prefix", prefix)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("mint listen", "err", err)
		os.Exit(1)
	}
	if err := http.Serve(ln, mux); err != nil {
		log.Error("mint server stopped", "err", err)
		os.Exit(1)
	}
}
