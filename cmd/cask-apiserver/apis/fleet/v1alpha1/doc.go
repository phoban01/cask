// Package v1alpha1 holds the fleet.cask.dev/v1alpha1 API types: Device and
// DeviceClaim. Both kinds are cluster-scoped.
//
// +kubebuilder:object:generate=true
// +groupName=fleet.cask.dev
package v1alpha1

//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.1 object paths=.
