package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Device is one leasable unit of the fleet inventory. At most one
// DeviceClaim holds the lease on a Device at a time.
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
type Device struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DeviceSpec   `json:"spec,omitempty"`
	Status DeviceStatus `json:"status,omitempty"`
}

// NamespaceScoped reports false: a Device has no namespace.
func (*Device) NamespaceScoped() bool {
	//= docs/spec/fleet.md#2-resources
	//# Every fleet resource MUST be cluster-scoped.
	return false
}

// DeviceSpec describes a device. The fields are descriptive; the lease is
// the contract.
type DeviceSpec struct {
	Model      string            `json:"model,omitempty"`
	Zone       string            `json:"zone,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Device phases.
const (
	DeviceAvailable = "Available"
	DeviceLeased    = "Leased"
)

// DeviceStatus reports the lease state of a device.
type DeviceStatus struct {
	// Phase is Available or Leased. The device's cask lock sets it.
	Phase string `json:"phase,omitempty"`
	// Lease names the current holder when Phase is Leased.
	Lease *LeaseRef `json:"lease,omitempty"`
}

// LeaseRef names a lease holder and its fencing token.
type LeaseRef struct {
	Cluster string `json:"cluster"`
	Claim   string `json:"claim"`
	// Fence is the monotonic fencing token that the acquisition minted.
	// A receiver rejects an effect with a lower fence than it has seen.
	Fence uint64 `json:"fence"`
}

// DeviceList is a list of Devices.
//
// +kubebuilder:object:root=true
type DeviceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Device `json:"items"`
}

// DeviceClaim is a cluster's request to lease one named Device.
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
type DeviceClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DeviceClaimSpec   `json:"spec,omitempty"`
	Status DeviceClaimStatus `json:"status,omitempty"`
}

// NamespaceScoped reports false: a DeviceClaim has no namespace.
func (*DeviceClaim) NamespaceScoped() bool {
	//= docs/spec/fleet.md#2-resources
	//# Every fleet resource MUST be cluster-scoped.
	return false
}

// DeviceClaimSpec names the device to lease.
type DeviceClaimSpec struct {
	// DeviceName is the device to lease.
	DeviceName string `json:"deviceName"`
	// TTLSeconds is the lease session TTL. The serving extension server
	// renews the session while the claim exists. The default is 30.
	TTLSeconds int64 `json:"ttlSeconds,omitempty"`
}

// DeviceClaim phases.
const (
	// ClaimPending means another claim holds the device lock.
	ClaimPending = "Pending"
	// ClaimBound means this claim holds the device lease.
	ClaimBound = "Bound"
	// ClaimLost means the claim held the lease, then its session lapsed.
	ClaimLost = "Lost"
)

// DeviceClaimStatus reports the state of a claim.
type DeviceClaimStatus struct {
	Phase string `json:"phase,omitempty"`
	// Cluster names the cluster whose extension server manages the claim.
	Cluster string `json:"cluster,omitempty"`
	// Fence is the claim's fencing token while Bound. It is zero otherwise.
	Fence uint64 `json:"fence,omitempty"`
	// Reason says why the claim is Pending or Lost.
	Reason string `json:"reason,omitempty"`
}

// DeviceClaimList is a list of DeviceClaims.
//
// +kubebuilder:object:root=true
type DeviceClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []DeviceClaim `json:"items"`
}
