// Package storage adapts cask to the storage.Interface of the Kubernetes
// generic API server. Each object lives in one cask register, and each
// resource type has one index register (docs/spec/fleet.md, section 3).
//
// Methods that are not implemented yet return ErrNotImplemented. Issues
// #28 to #35 fill in the methods one at a time.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/phoban01/cask/internal/mvcc"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// ErrNotImplemented is the error that each unimplemented method returns.
var ErrNotImplemented = errors.New("cask storage: not implemented")

// Store is a storage.Interface backed by cask registers. One Store serves
// one resource type.
type Store struct {
	kv       *mvcc.KV
	codec    runtime.Codec
	resource string
	// versioner sets and reads resourceVersion on objects. Versioner()
	// does not expose it until #35.
	versioner apistorage.APIObjectVersioner
}

var _ apistorage.Interface = (*Store)(nil)

// New returns a Store for resource. It keeps objects in kv and encodes
// them with codec.
func New(kv *mvcc.KV, codec runtime.Codec, resource string) *Store {
	return &Store{kv: kv, codec: codec, resource: resource}
}

// objectKey maps a storage key to the object's register key and name. The
// generic registry builds a cluster-scoped key as "<prefix>/<name>", so
// the name is the last path element.
func (s *Store) objectKey(key string) ([]byte, string, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# Each object MUST be stored in one cask register keyed by resource type and name.
	name := key[strings.LastIndexByte(key, '/')+1:]
	if name == "" {
		return nil, "", fmt.Errorf("cask storage: key %q names no object", key)
	}
	return ObjectKey(s.resource, name), name, nil
}

// head returns the object register's head version. ok is false when the
// register is absent or its head is a tombstone.
func (s *Store) head(ctx context.Context, reg []byte) (v mvcc.Version, ok bool, err error) {
	chain, err := s.kv.History(ctx, reg)
	if err != nil {
		return mvcc.Version{}, false, err
	}
	n := len(chain.Versions)
	if n == 0 || chain.Versions[n-1].Tombstone {
		return mvcc.Version{}, false, nil
	}
	return chain.Versions[n-1], true, nil
}

// decode decodes raw into out and sets its resourceVersion to seq.
func (s *Store) decode(key string, raw []byte, seq uint64, out runtime.Object) error {
	//= docs/spec/fleet.md#3-storage-model
	//# An object's resourceVersion MUST be the sequence of its object register.
	if _, _, err := s.codec.Decode(raw, nil, out); err != nil {
		return apistorage.NewCorruptObjError(key, err)
	}
	return s.versioner.UpdateObject(out, seq)
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

// Get reads the object at key into out. Every Get is a linearizable read
// of the register head, so it meets any "not older than" bound in
// opts.ResourceVersion. The resourceVersion of out is the register
// sequence.
func (s *Store) Get(ctx context.Context, key string, opts apistorage.GetOptions, out runtime.Object) error {
	reg, _, err := s.objectKey(key)
	if err != nil {
		return err
	}
	v, ok, err := s.head(ctx, reg)
	if err != nil {
		return err
	}
	if !ok {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return apistorage.NewKeyNotFoundError(key, 0)
	}
	return s.decode(key, v.Value, v.Seq, out)
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
