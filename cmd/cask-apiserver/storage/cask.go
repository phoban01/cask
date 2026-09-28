// Package storage adapts cask to the storage.Interface of the Kubernetes
// generic API server. Each object lives in one cask register, and each
// resource type has one index register (docs/spec/fleet.md, section 3).
//
// Methods that are not implemented yet return ErrNotImplemented. Issues
// #28 to #35 fill in the methods one at a time.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/phoban01/cask/internal/caspaxos"
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

// encode clears the resourceVersion of obj and encodes it. The register
// sequence is the resourceVersion, so the stored bytes never carry one.
func (s *Store) encode(obj runtime.Object) ([]byte, error) {
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return nil, err
	}
	return runtime.Encode(s.codec, obj)
}

// Create stores obj at key if no live object is there, records it in the
// index, and decodes the stored object into out. It returns the storage
// KeyExists error when a live object is there. The store has no TTL, so
// ttl must be 0.
func (s *Store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	reg, name, err := s.objectKey(key)
	if err != nil {
		return err
	}
	if ttl != 0 {
		return fmt.Errorf("cask storage: create %q: TTL is not supported", key)
	}
	if rv, err := s.versioner.ObjectResourceVersion(obj); err != nil {
		return err
	} else if rv != 0 {
		return apistorage.ErrResourceVersionSetOnCreate
	}
	raw, err := s.encode(obj)
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A create MUST use a compare-and-set that requires the object register to be absent.
	v, err := s.kv.CAS(ctx, reg, nil, raw)
	if errors.Is(err, caspaxos.ErrConflict) {
		return apistorage.NewKeyExistsError(key, 0)
	}
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	if err := WriteIndex(ctx, s.kv, s.resource, name); err != nil {
		// The object is committed. The next index write for this name,
		// or the sweep, records it.
		return fmt.Errorf("cask storage: create %q: object written, index write failed: %w", key, err)
	}
	if out == nil {
		return nil
	}
	return s.decode(key, raw, v.Seq, out)
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

// newLike returns a new, empty object of the same type as obj.
func newLike(obj runtime.Object) runtime.Object {
	return reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
}

// GuaranteedUpdate reads the object at key, runs tryUpdate on it, and
// writes the result with a compare-and-set on the sequence it read. When
// another write lands first, it reads the object again and runs tryUpdate
// on the fresh copy. It stops when a write lands, tryUpdate or
// preconditions fail, or ctx ends. The written object goes into
// destination.
//
// The generic registry puts the client's resourceVersion check in
// tryUpdate, so a stale client resourceVersion fails there on the fresh
// read. cachedExistingObject is not used: every attempt reads the
// register.
func (s *Store) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool,
	preconditions *apistorage.Preconditions, tryUpdate apistorage.UpdateFunc, _ runtime.Object) error {
	reg, name, err := s.objectKey(key)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		v, ok, err := s.head(ctx, reg)
		if err != nil {
			return err
		}
		existing := newLike(destination)
		var seq uint64 // 0 asks CASSeq for an absent register
		if ok {
			if err := s.decode(key, v.Value, v.Seq, existing); err != nil {
				return err
			}
			seq = v.Seq
		} else if !ignoreNotFound {
			return apistorage.NewKeyNotFoundError(key, 0)
		}
		// A failed precondition is a storage InvalidObj error. The
		// registry returns it to the client as 409 Conflict.
		//= docs/spec/fleet.md#3-storage-model
		//# An update whose compare-and-set fails MUST return a conflict.
		if err := preconditions.Check(key, existing); err != nil {
			return err
		}
		updated, ttl, err := tryUpdate(existing, apistorage.ResponseMeta{ResourceVersion: seq})
		if err != nil {
			return err
		}
		if ttl != nil && *ttl != 0 {
			return fmt.Errorf("cask storage: update %q: TTL is not supported", key)
		}
		raw, err := s.encode(updated)
		if err != nil {
			return err
		}
		if ok && bytes.Equal(raw, v.Value) {
			// Nothing changed. Skip the write, as the etcd store does.
			return s.decode(key, v.Value, v.Seq, destination)
		}
		//= docs/spec/fleet.md#3-storage-model
		//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
		//= docs/spec/fleet.md#3-storage-model
		//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
		nv, err := s.kv.CASSeq(ctx, reg, seq, raw)
		if errors.Is(err, caspaxos.ErrConflict) {
			// Another write landed after the read. Read again and run
			// tryUpdate on the fresh copy.
			//= docs/spec/fleet.md#3-storage-model
			//# The extension server MUST re-read an object before it retries a write that returned a conflict.
			continue
		}
		if err != nil {
			return err
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A mutation MUST write the object register before the index register.
		if err := WriteIndex(ctx, s.kv, s.resource, name); err != nil {
			return fmt.Errorf("cask storage: update %q: object written, index write failed: %w", key, err)
		}
		return s.decode(key, raw, nv.Seq, destination)
	}
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
