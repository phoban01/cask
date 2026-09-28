package migrate

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// LockSeed is the lock state that Import seeds for one device before it
// writes any object.
type LockSeed struct {
	// Device is the device name. Its lock guards every claim on it.
	Device string
	// Fence is the highest fence that the export recorded for Device.
	Fence uint64
	// Claim and Cluster name the Bound claim that holds the lock at
	// Fence. Claim is empty when no claim holds it: the lock is then
	// seeded released.
	Claim   string
	Cluster string
	// TTLSeconds is the claim's session TTL, from its spec.
	TTLSeconds int64
}

// Locks seeds claim locks. The API server implements it over its lease
// sessions and locks.
type Locks interface {
	// Seed seeds the lock of s.Device. For a seed with a Claim, it must
	// grant the claim's session before it seeds the lock.
	Seed(ctx context.Context, s LockSeed) error
	// Renew extends the session of the claim that s restores. It must
	// fail, and not grant again, when the session has lapsed.
	Renew(ctx context.Context, s LockSeed) error
}

// LostReason is the status reason of a Bound claim that Import writes as
// Lost.
const LostReason = "cutover: the import restores a Bound claim only when it alone holds the highest recorded fence"

// fenceOf reads a fence field. A missing field is fence 0.
func fenceOf(o *unstructured.Unstructured, fields ...string) (uint64, error) {
	v, found, err := unstructured.NestedFieldNoCopy(o.Object, fields...)
	if err != nil || !found || v == nil {
		return 0, err
	}
	var f int64
	switch n := v.(type) {
	case int64:
		f = n
	case float64:
		f = int64(n)
		if float64(f) != n {
			return 0, fmt.Errorf("migrate: %s %q: %v is not a whole number", o.GetKind(), o.GetName(), n)
		}
	default:
		return 0, fmt.Errorf("migrate: %s %q: fence is %T, not a number", o.GetKind(), o.GetName(), v)
	}
	if f < 0 {
		return 0, fmt.Errorf("migrate: %s %q: fence %d is negative", o.GetKind(), o.GetName(), f)
	}
	return uint64(f), nil
}

// planFences finds the highest fence the export recorded for each device
// and decides which Bound claims keep their lock. It writes every other
// Bound claim as Lost, in objects, so the caller passes copies. It returns one seed for each device
// with a recorded fence, in name order.
func planFences(objects []*unstructured.Unstructured) ([]LockSeed, error) {
	type lease struct {
		claim, cluster string
		fence          uint64
	}
	highest := map[string]uint64{}
	advertised := map[string]lease{} // the Device status lease
	var bound []*unstructured.Unstructured

	// The export keeps status as the API returned it. The Device status
	// carries the advertised lease fence and lastFence, and each claim
	// status carries the claim's fence.
	for _, o := range objects {
		switch o.GetKind() {
		case "Device":
			f, err := fenceOf(o, "status", "lease", "fence")
			if err != nil {
				return nil, err
			}
			// A release clears the lease but keeps its fence in lastFence.
			// The claim may be gone, so lastFence is the only record of
			// that fence.
			//= docs/spec/fleet.md#7-migration
			//# The fences that the export records for a Device MUST include the lastFence in its status.
			kept, err := fenceOf(o, "status", "lastFence")
			if err != nil {
				return nil, err
			}
			claim, _, _ := unstructured.NestedString(o.Object, "status", "lease", "claim")
			cluster, _, _ := unstructured.NestedString(o.Object, "status", "lease", "cluster")
			advertised[o.GetName()] = lease{claim: claim, cluster: cluster, fence: f}
			highest[o.GetName()] = max(highest[o.GetName()], f, kept)
		case "DeviceClaim":
			f, err := fenceOf(o, "status", "fence")
			if err != nil {
				return nil, err
			}
			device, _, _ := unstructured.NestedString(o.Object, "spec", "deviceName")
			if device != "" {
				highest[device] = max(highest[device], f)
			}
			if phase, _, _ := unstructured.NestedString(o.Object, "status", "phase"); phase == "Bound" {
				bound = append(bound, o)
			}
		}
	}

	// A claim holds the highest fence alone when it is the only Bound
	// claim at that fence, and the Device status names no other holder
	// at that fence.
	type key struct {
		device string
		fence  uint64
	}
	atFence := map[key]int{}
	for _, o := range bound {
		device, _, _ := unstructured.NestedString(o.Object, "spec", "deviceName")
		f, _ := fenceOf(o, "status", "fence")
		atFence[key{device, f}]++
	}
	holder := map[string]LockSeed{}
	for _, o := range bound {
		device, _, _ := unstructured.NestedString(o.Object, "spec", "deviceName")
		cluster, _, _ := unstructured.NestedString(o.Object, "status", "cluster")
		f, _ := fenceOf(o, "status", "fence")
		adv, hasDevice := advertised[device]
		//= docs/spec/fleet.md#7-migration
		//# The import MUST restore a Bound claim as Bound only when that claim alone holds the highest fence that the export recorded for its object.
		alone := device != "" && cluster != "" && f > 0 &&
			f == highest[device] && atFence[key{device, f}] == 1 &&
			(!hasDevice || adv.fence != f || (adv.claim == o.GetName() && adv.cluster == cluster))
		if alone {
			ttl, _, _ := unstructured.NestedInt64(o.Object, "spec", "ttlSeconds")
			holder[device] = LockSeed{Claim: o.GetName(), Cluster: cluster, TTLSeconds: ttl}
			continue
		}
		// Cask holds no session from the source. A claim that does not
		// get the lock is not Bound, so the import says so.
		//= docs/spec/fleet.md#7-migration
		//# The import MUST write a Bound claim as Lost when it does not restore that claim as Bound.
		if err := unstructured.SetNestedField(o.Object, "Lost", "status", "phase"); err != nil {
			return nil, err
		}
		if err := unstructured.SetNestedField(o.Object, LostReason, "status", "reason"); err != nil {
			return nil, err
		}
	}

	var seeds []LockSeed
	for device, f := range highest {
		if f == 0 {
			continue
		}
		//= docs/spec/fleet.md#7-migration
		//# The import MUST NOT seed an object's lock at a fence below the highest fence that the export recorded for that object.
		s := holder[device]
		s.Device, s.Fence = device, f
		seeds = append(seeds, s)
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i].Device < seeds[j].Device })
	return seeds, nil
}
