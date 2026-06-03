package nebula

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/slackhq/nebula/cert"
	"github.com/slackhq/nebula/config"

	"github.com/phoban01/cask/internal/roster"
)

// zoneGroupPrefix is the Nebula cert group that carries a node's failure domain,
// e.g. a cert in group "zone:us-east" places the node in zone "us-east".
const zoneGroupPrefix = "zone:"

// SeedMembers derives cask roster seed members from a Nebula config (the hybrid
// strategy): the node's own identity — overlay IP for the NodeID, cert group for
// the Zone — plus the lighthouse hosts it is configured to reach. This is only a
// bootstrap seed; cask's roster controller grows and corrects the membership by
// consensus as the cluster forms.
//
// caskPort is the port cask serves consensus RPC on over the overlay; it is
// combined with each overlay IP to form the dialable Member.Addr.
//
// Peers discovered via the lighthouse have no Zone here (their cert is not known
// until a handshake); they seed with an empty zone and are refined later. The
// local node, whose cert we hold, gets its real zone immediately.
func SeedMembers(yamlConfig string, caskPort int) ([]roster.Member, error) {
	var c config.C
	if err := c.LoadString(yamlConfig); err != nil {
		return nil, fmt.Errorf("nebula: load config: %w", err)
	}

	byID := map[uint64]roster.Member{}

	// Local node, from our own certificate: overlay IP + zone group.
	self, err := selfMember(&c, caskPort)
	if err != nil {
		return nil, err
	}
	if self.Addr != "" {
		byID[self.NodeID] = self
	}

	// Lighthouse hosts: reachable overlay IPs to seed the roster from.
	for _, h := range c.GetStringSlice("lighthouse.hosts", nil) {
		ip, err := netip.ParseAddr(h)
		if err != nil {
			return nil, fmt.Errorf("nebula: lighthouse host %q: %w", h, err)
		}
		id := nodeIDFromIP(ip)
		if _, ok := byID[id]; ok {
			continue // already have it (likely ourselves)
		}
		byID[id] = roster.Member{NodeID: id, Addr: overlayAddr(ip, caskPort)}
	}

	out := make([]roster.Member, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

// GenesisMembers returns the initial consensus roster: the cluster's lighthouse
// nodes. This set is identical on every node (members list the same lighthouses;
// a lighthouse includes itself), so all nodes install the same genesis roster
// against the same acceptor set — the prerequisite for a safe bootstrap. Non
// lighthouse nodes are NOT in genesis; they join afterward via roster.Add.
func GenesisMembers(yamlConfig string, caskPort int) ([]roster.Member, error) {
	var c config.C
	if err := c.LoadString(yamlConfig); err != nil {
		return nil, fmt.Errorf("nebula: load config: %w", err)
	}

	byID := map[uint64]roster.Member{}
	for _, h := range c.GetStringSlice("lighthouse.hosts", nil) {
		ip, err := netip.ParseAddr(h)
		if err != nil {
			return nil, fmt.Errorf("nebula: lighthouse host %q: %w", h, err)
		}
		id := nodeIDFromIP(ip)
		byID[id] = roster.Member{NodeID: id, Addr: overlayAddr(ip, caskPort)}
	}
	// A lighthouse does not list itself in lighthouse.hosts, so add it here; the
	// result then matches what every member computes from its hosts list.
	if c.GetBool("lighthouse.am_lighthouse", false) {
		self, err := selfMember(&c, caskPort)
		if err != nil {
			return nil, err
		}
		if self.Addr != "" {
			byID[self.NodeID] = roster.Member{NodeID: self.NodeID, Addr: self.Addr} // zone refined on join
		}
	}

	out := make([]roster.Member, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

// SelfMember returns the local node's roster Member derived from its own Nebula
// certificate: NodeID from the overlay IP, Zone from the "zone:" cert group. The
// NodeID matches what every other node computes for this one, so placement and
// dialing agree across the cluster.
func SelfMember(yamlConfig string, caskPort int) (roster.Member, error) {
	var c config.C
	if err := c.LoadString(yamlConfig); err != nil {
		return roster.Member{}, fmt.Errorf("nebula: load config: %w", err)
	}
	return selfMember(&c, caskPort)
}

func selfMember(c *config.C, caskPort int) (roster.Member, error) {
	certPEM := c.GetString("pki.cert", "")
	if certPEM == "" {
		return roster.Member{}, nil
	}
	crt, _, err := cert.UnmarshalCertificateFromPEM([]byte(certPEM))
	if err != nil {
		return roster.Member{}, fmt.Errorf("nebula: parse pki.cert: %w", err)
	}
	nets := crt.Networks()
	if len(nets) == 0 {
		return roster.Member{}, fmt.Errorf("nebula: pki.cert has no overlay network")
	}
	ip := nets[0].Addr()
	id := nodeIDFromIP(ip)
	return roster.Member{NodeID: id, Addr: overlayAddr(ip, caskPort), Zone: zoneFromGroups(crt.Groups())}, nil
}

// nodeIDFromIP maps an overlay IP to a stable node id. An IPv4 overlay address
// packs losslessly into the low 32 bits; an IPv6 address is folded into 64 bits.
// Either way the mapping is deterministic and collision-free within one overlay.
func nodeIDFromIP(ip netip.Addr) uint64 {
	if ip.Is4() {
		b := ip.As4()
		return uint64(binary.BigEndian.Uint32(b[:]))
	}
	b := ip.As16()
	return binary.BigEndian.Uint64(b[8:]) ^ binary.BigEndian.Uint64(b[:8])
}

// zoneFromGroups returns the failure domain encoded in a "zone:<name>" cert
// group, or "" if the cert carries none.
func zoneFromGroups(groups []string) string {
	for _, g := range groups {
		if z, ok := strings.CutPrefix(g, zoneGroupPrefix); ok {
			return z
		}
	}
	return ""
}

func overlayAddr(ip netip.Addr, port int) string {
	return ip.String() + ":" + strconv.Itoa(port)
}
