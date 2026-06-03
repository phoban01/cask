package placement

import "sort"

// Node is a placement candidate: its id and its failure domain (zone/rack/AZ).
type Node struct {
	ID   uint64
	Zone string
}

// TargetReplicas chooses rf replicas for the range identified by rangeKey,
// spreading them across as many distinct failure domains as possible. Within the
// constraints, higher-HRW nodes win, so placement stays stable and computable
// locally. With Z zones and rf replicas the result spans min(Z, rf) zones as
// evenly as possible — so a single zone failure can never take a whole quorum.
func TargetReplicas(rangeKey []byte, nodes []Node, rf int) []uint64 {
	if rf > len(nodes) {
		rf = len(nodes)
	}
	// Bucket nodes by zone, each bucket ordered by descending HRW score.
	byZone := map[string][]Node{}
	for _, n := range nodes {
		byZone[n.Zone] = append(byZone[n.Zone], n)
	}
	zones := make([]string, 0, len(byZone))
	for z, ns := range byZone {
		sort.Slice(ns, func(i, j int) bool { return hrwLess(rangeKey, ns[i], ns[j]) })
		byZone[z] = ns
		zones = append(zones, z)
	}
	// Order zones by their best (highest-HRW) node, so zone selection is stable.
	sort.Slice(zones, func(i, j int) bool {
		return hrwLess(rangeKey, byZone[zones[i]][0], byZone[zones[j]][0])
	})

	// Round-robin across zones, taking the next-best node from each, until full.
	cursor := map[string]int{}
	var out []uint64
	for len(out) < rf {
		progressed := false
		for _, z := range zones {
			if len(out) == rf {
				break
			}
			i := cursor[z]
			if i < len(byZone[z]) {
				out = append(out, byZone[z][i].ID)
				cursor[z] = i + 1
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return out
}

// hrwLess orders so the higher HRW score comes first (descending).
func hrwLess(key []byte, a, b Node) bool {
	sa, sb := score(key, a.ID), score(key, b.ID)
	if sa != sb {
		return sa > sb
	}
	return a.ID < b.ID
}

// NeedsReconfig decides whether a range with the given current replicas should be
// reconfigured, applying HYSTERESIS: a healthy assignment is left alone even if
// HRW would now prefer slightly different nodes, so the cluster does not thrash
// on cosmetic membership churn. It returns a target replica set and true only
// when reconfiguration is actually warranted:
//
//   - a current replica is no longer a member (dead/removed), or
//   - the range is under-replicated (fewer than rf live replicas), or
//   - the current placement spans fewer failure domains than the target would.
func NeedsReconfig(rangeKey []byte, current []uint64, members []Node, rf int) (target []uint64, need bool) {
	target = TargetReplicas(rangeKey, members, rf)

	memberZone := map[uint64]string{}
	for _, m := range members {
		memberZone[m.ID] = m.Zone
	}

	live := 0
	curZones := map[string]bool{}
	for _, id := range current {
		z, ok := memberZone[id]
		if !ok {
			return target, true // a replica left the roster: must reconfigure
		}
		live++
		curZones[z] = true
	}
	if live < rf && live < len(members) {
		return target, true // under-replicated while capacity exists
	}

	targetZones := map[string]bool{}
	for _, id := range target {
		targetZones[memberZone[id]] = true
	}
	if len(curZones) < len(targetZones) {
		return target, true // a better failure-domain spread is available
	}
	return target, false
}
