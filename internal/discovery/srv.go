// Package discovery provides roster.Discovery adapters that source candidate
// cluster members from DNS. The reconcile loop consumes these to grow the
// consensus roster; membership is never removed by absence from discovery (only
// the failure detector removes), so a discovery source going dark is safe.
package discovery

import (
	"context"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/transport/nebula"
)

// srvResolver is the subset of *net.Resolver this package needs, extracted so a
// fake can be injected in tests. *net.Resolver satisfies it.
type srvResolver interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// SRVDiscovery resolves cask members from DNS SRV records. Each SRV target's
// A/AAAA record resolves to a member's Nebula OVERLAY IP (cask identity keys off
// the overlay IP), and the SRV port is the cask consensus port. Records are
// published by the operator's DNS zone or a Kubernetes headless service; cask
// only consumes them.
type SRVDiscovery struct {
	Service  string      // e.g. "cask" -> _cask._tcp.<domain>
	Proto    string      // e.g. "tcp"
	Domain   string      // e.g. "cask.svc.cluster.local"
	Resolver srvResolver // nil => net.DefaultResolver
}

// Discover returns the members named by the SRV record set. Unresolvable
// targets are skipped (a partial result beats failing the whole sweep); a
// LookupSRV error propagates so the caller can treat the tick as transient.
func (d SRVDiscovery) Discover(ctx context.Context) ([]roster.Member, error) {
	r := d.Resolver
	if r == nil {
		r = net.DefaultResolver
	}
	_, srvs, err := r.LookupSRV(ctx, d.Service, d.Proto, d.Domain)
	if err != nil {
		return nil, err
	}
	byID := map[uint64]roster.Member{}
	for _, srv := range srvs {
		target := strings.TrimSuffix(srv.Target, ".") // SRV targets are FQDNs with a trailing dot
		if target == "" {
			continue
		}
		ips, err := r.LookupNetIP(ctx, "ip", target)
		if err != nil || len(ips) == 0 {
			continue
		}
		for _, ip := range ips {
			// Collapse a 4-in-6 form to canonical IPv4: NodeIDFromIP packs IPv4
			// and folds IPv6 differently, so without Unmap the same host would
			// hash to a different id than every other node computes for it.
			ip = ip.Unmap()
			id := nebula.NodeIDFromIP(ip)
			if _, ok := byID[id]; ok {
				continue
			}
			byID[id] = roster.Member{
				NodeID: id,
				Addr:   net.JoinHostPort(ip.String(), strconv.Itoa(int(srv.Port))),
				// Zone is unknown from DNS; the node's own cert-derived zone
				// overwrites this (last-write-wins) when it self-joins.
			}
		}
	}
	return sortedMembers(byID), nil
}

// Union combines several discovery sources, deduping members by NodeID (earlier
// sources win, so a source carrying richer info — e.g. a real zone — should be
// listed first). It returns an error only when every source failed AND nothing
// was found, so one broken source never starves the reconcile loop of the rest.
type Union []roster.Discovery

func (u Union) Discover(ctx context.Context) ([]roster.Member, error) {
	byID := map[uint64]roster.Member{}
	var firstErr error
	for _, d := range u {
		ms, err := d.Discover(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, m := range ms {
			if _, ok := byID[m.NodeID]; !ok {
				byID[m.NodeID] = m
			}
		}
	}
	if len(byID) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return sortedMembers(byID), nil
}

func sortedMembers(byID map[uint64]roster.Member) []roster.Member {
	out := make([]roster.Member, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}
