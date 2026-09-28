// Package storage adapts cask to the storage.Interface of the Kubernetes
// generic API server. Each object lives in one cask register, and each
// resource type has one index register (docs/spec/fleet.md, section 3).
//
// This file is a skeleton. Every method returns ErrNotImplemented. Issues
// #28 to #35 fill in the methods one at a time.
package storage

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// ErrNotImplemented is the error that each method of the skeleton returns.
var ErrNotImplemented = errors.New("cask storage: not implemented")

// Store is a storage.Interface backed by cask registers.
type Store struct{}

var _ apistorage.Interface = (*Store)(nil)

// New returns a Store.
func New() *Store {
	return &Store{}
}

// Versioner returns nil until the Versioner lands (#35).
func (*Store) Versioner() apistorage.Versioner {
	return nil
}

// Create returns ErrNotImplemented.
func (*Store) Create(_ context.Context, _ string, _, _ runtime.Object, _ uint64) error {
	//= docs/spec/fleet.md#3-storage-model
	//= type=exception
	//= reason=the storage.Interface seam does not write registers yet; tracked in issue #29
	//# Each object MUST be stored in one cask register keyed by resource type and name.
	return ErrNotImplemented
}

// Delete returns ErrNotImplemented.
func (*Store) Delete(_ context.Context, _ string, _ runtime.Object, _ *apistorage.Preconditions,
	_ apistorage.ValidateObjectFunc, _ runtime.Object, _ apistorage.DeleteOptions) error {
	return ErrNotImplemented
}

// Watch returns ErrNotImplemented.
func (*Store) Watch(_ context.Context, _ string, _ apistorage.ListOptions) (watch.Interface, error) {
	return nil, ErrNotImplemented
}

// Get returns ErrNotImplemented.
func (*Store) Get(_ context.Context, _ string, _ apistorage.GetOptions, _ runtime.Object) error {
	return ErrNotImplemented
}

// GetList returns ErrNotImplemented.
func (*Store) GetList(_ context.Context, _ string, _ apistorage.ListOptions, _ runtime.Object) error {
	return ErrNotImplemented
}

// GuaranteedUpdate returns ErrNotImplemented.
func (*Store) GuaranteedUpdate(_ context.Context, _ string, _ runtime.Object, _ bool,
	_ *apistorage.Preconditions, _ apistorage.UpdateFunc, _ runtime.Object) error {
	return ErrNotImplemented
}

// Stats returns ErrNotImplemented.
func (*Store) Stats(context.Context) (apistorage.Stats, error) {
	return apistorage.Stats{}, ErrNotImplemented
}

// ReadinessCheck returns ErrNotImplemented, so the store never reports
// ready.
func (*Store) ReadinessCheck() error {
	return ErrNotImplemented
}

// RequestWatchProgress returns ErrNotImplemented.
func (*Store) RequestWatchProgress(context.Context) error {
	return ErrNotImplemented
}

// GetCurrentResourceVersion returns ErrNotImplemented.
func (*Store) GetCurrentResourceVersion(context.Context) (uint64, error) {
	return 0, ErrNotImplemented
}

// EnableResourceSizeEstimation returns ErrNotImplemented.
func (*Store) EnableResourceSizeEstimation(apistorage.KeysFunc) error {
	return ErrNotImplemented
}

// CompactRevision returns 0: the store has seen no compaction.
func (*Store) CompactRevision() int64 {
	return 0
}
