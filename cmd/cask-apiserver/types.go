// Command cask-apiserver is a Kubernetes-style API extension server backed by
// cask (docs/k8s-aggregation.md): one API group, fleet.cask.dev/v1alpha1,
// served identically from every cluster in a fleet, so a fleet-scoped
// resource created anywhere is visible — strongly consistently — everywhere.
//
// The demo model is a finite fleet of DEVICES that pods in local clusters
// lease through DEVICECLAIMS. Devices carry arbitrary metadata; the property
// that matters is that AT MOST ONE lease exists per device globally. That is
// not enforced by this server's logic — it is inherited from cask's fenced
// locks (the SingleHolder and FenceMonotone properties model-checked in
// tla/Lease.tla): binding a claim IS acquiring the device's lock, and the
// fencing token in the claim's status is what workloads present downstream,
// making a zombie holder's late actions rejectable.
package main

import (
	"encoding/json"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
)

// The group and version come from the v1alpha1 scheme.
const (
	apiGroup    = v1alpha1.GroupName
	apiVersion  = v1alpha1.Version
	groupPrefix = "/apis/" + apiGroup + "/" + apiVersion
)

// ObjectMeta is the subset of Kubernetes object metadata the prototype
// serves. ResourceVersion is the index sequence at which the type index
// recorded the object version. An update checks it against the index entry
// and then compares and sets on the object sequence the entry records.
type ObjectMeta struct {
	Name            string            `json:"name"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// TypeMeta mirrors k8s type identification.
type TypeMeta struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

// Device is one leasable unit of the global fleet inventory.
type Device struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       DeviceSpec   `json:"spec"`
	Status     DeviceStatus `json:"status,omitempty"`
}

type DeviceSpec struct {
	// Model/Zone plus free-form attributes: the metadata is descriptive;
	// the lease is the contract.
	Model      string            `json:"model,omitempty"`
	Zone       string            `json:"zone,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type DeviceStatus struct {
	// Phase is Available or Leased; derived from the device's lock.
	Phase string `json:"phase,omitempty"`
	// Lease describes the current holder when Phase == Leased.
	Lease *LeaseRef `json:"lease,omitempty"`
	// LastFence is the highest fence the device has advertised. A release
	// keeps it, and no write lowers it. The cutover export reads it, so
	// the import seeds the lock above a fence whose claim is gone.
	LastFence uint64 `json:"lastFence,omitempty"`
}

// LeaseRef identifies a lease holder and its fencing token.
type LeaseRef struct {
	Cluster string `json:"cluster"`
	Claim   string `json:"claim"`
	// Fence is the monotonic fencing token minted by the acquisition.
	// Downstream systems MUST prefer the highest fence they have seen: a
	// holder presenting a lower token is a zombie.
	Fence uint64 `json:"fence"`
}

// DeviceClaim is a local cluster's request to lease one named device.
type DeviceClaim struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       DeviceClaimSpec   `json:"spec"`
	Status     DeviceClaimStatus `json:"status,omitempty"`
}

type DeviceClaimSpec struct {
	// DeviceName is the device to lease (explicit in v1alpha1; selectors are
	// future work).
	DeviceName string `json:"deviceName"`
	// TTLSeconds is the lease session TTL; the serving apiserver renews it
	// while the claim exists. Default 30.
	TTLSeconds int64 `json:"ttlSeconds,omitempty"`
}

const (
	ClaimPending = "Pending" // device lock held elsewhere; will retry
	ClaimBound   = "Bound"   // this claim holds the device's global lease
	ClaimLost    = "Lost"    // held it, then the session lapsed or was superseded

	DeviceAvailable = "Available"
	DeviceLeased    = "Leased"
)

type DeviceClaimStatus struct {
	Phase string `json:"phase,omitempty"`
	// Cluster records which cluster's apiserver manages this claim.
	Cluster string `json:"cluster,omitempty"`
	// Fence is the claim's fencing token while Bound. Zero otherwise.
	Fence uint64 `json:"fence,omitempty"`
	// Reason is a human-readable note (why Pending/Lost).
	Reason string `json:"reason,omitempty"`
}

// List wrappers (k8s list shapes).
type DeviceList struct {
	TypeMeta `json:",inline"`
	Items    []Device `json:"items"`
}

type DeviceClaimList struct {
	TypeMeta `json:",inline"`
	Items    []DeviceClaim `json:"items"`
}

// WatchEvent is the k8s watch stream framing (one JSON object per line).
type WatchEvent struct {
	Type   string          `json:"type"` // ADDED | MODIFIED | DELETED
	Object json.RawMessage `json:"object"`
}

func deviceTypeMeta() TypeMeta {
	return TypeMeta{APIVersion: apiGroup + "/" + apiVersion, Kind: "Device"}
}
func claimTypeMeta() TypeMeta {
	return TypeMeta{APIVersion: apiGroup + "/" + apiVersion, Kind: "DeviceClaim"}
}
