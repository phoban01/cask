// Package storage adapts cask to the storage.Interface of the Kubernetes
// generic API server. Each object lives in one cask register, and each
// resource type has one index register (docs/spec/fleet.md, section 3).
//
// Get, Create, GuaranteedUpdate, Delete, GetList, and Watch are
// implemented. The other methods return ErrNotImplemented.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mvcc"
	"k8s.io/apimachinery/pkg/runtime"
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
	// versioner sets and reads resourceVersion on objects and lists.
	versioner apistorage.APIObjectVersioner
	// newFunc returns a new, empty object of the resource type. Watch
	// uses it for bookmark events.
	newFunc func() runtime.Object

	// watchPoll is how often a watch reads the index history when no
	// local write wakes it. Writes from other clusters reach a watch
	// through this read.
	watchPoll time.Duration
	// bookmarkEvery is how often a watch that allows bookmarks sends
	// one when it has sent nothing else.
	bookmarkEvery time.Duration

	mu sync.Mutex
	// changed is closed and replaced after each index write through this
	// Store, to wake the watches.
	changed chan struct{}
}

var _ apistorage.Interface = (*Store)(nil)

// New returns a Store for resource. It keeps objects in kv and encodes
// them with codec. newFunc returns a new, empty object of the resource
// type.
func New(kv *mvcc.KV, codec runtime.Codec, resource string, newFunc func() runtime.Object) *Store {
	return &Store{
		kv:            kv,
		codec:         codec,
		resource:      resource,
		newFunc:       newFunc,
		watchPoll:     250 * time.Millisecond,
		bookmarkEvery: time.Minute,
		changed:       make(chan struct{}),
	}
}

// writeIndex runs WriteIndex for name and then wakes the watches.
func (s *Store) writeIndex(ctx context.Context, name string) error {
	if err := WriteIndex(ctx, s.kv, s.resource, name); err != nil {
		return err
	}
	s.mu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	return nil
}

// indexChanged returns a channel that is closed at the next index write
// through this Store.
func (s *Store) indexChanged() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
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

// Versioner returns the Versioner that the store uses. A resourceVersion
// is a decimal sequence. On an object it is the sequence of the object
// register. On a list it is the sequence of the index register.
// ParseResourceVersion reads "" and "0" as 0. It rejects a value that is
// not a decimal number with a storage InvalidError.
func (s *Store) Versioner() apistorage.Versioner {
	//= docs/spec/fleet.md#3-storage-model
	//# An object's resourceVersion MUST be the sequence of its object register.
	return s.versioner
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
	if err := s.writeIndex(ctx, name); err != nil {
		// The object is committed. The next index write for this name,
		// or the sweep, records it.
		return fmt.Errorf("cask storage: create %q: object written, index write failed: %w", key, err)
	}
	if out == nil {
		return nil
	}
	return s.decode(key, raw, v.Seq, out)
}

// Delete removes the object at key and decodes the deleted object into
// out. The resourceVersion of out is the sequence of the tombstone.
//
// Delete reads the object, checks the preconditions, and runs
// validateDeletion. It then tombstones the register with a
// compare-and-set on the sequence it read, and removes the name from the
// index. When another write lands first, it reads the object again and
// repeats the checks. cachedExistingObject is not used: every attempt
// reads the register.
func (s *Store) Delete(ctx context.Context, key string, out runtime.Object, preconditions *apistorage.Preconditions,
	validateDeletion apistorage.ValidateObjectFunc, _ runtime.Object, opts apistorage.DeleteOptions) error {
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
		if !ok {
			return apistorage.NewKeyNotFoundError(key, 0)
		}
		existing := newLike(out)
		readable := true
		if err := s.decode(key, v.Value, v.Seq, existing); err != nil {
			if !opts.IgnoreStoreReadError {
				return err
			}
			// The caller asked to delete an object it cannot decode.
			// There is no object to check.
			readable = false
		}
		if readable {
			if err := preconditions.Check(key, existing); err != nil {
				return err
			}
			if validateDeletion != nil {
				if err := validateDeletion(ctx, existing); err != nil {
					return err
				}
			}
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A delete MUST tombstone the object register before it removes the name from the index register.
		tomb, err := s.kv.DeleteSeq(ctx, reg, v.Seq)
		if errors.Is(err, caspaxos.ErrConflict) {
			// Another write landed after the read. The checks ran on an
			// old copy, so read again and repeat them.
			//= docs/spec/fleet.md#3-storage-model
			//# The extension server MUST re-read an object before it retries a write that returned a conflict.
			continue
		}
		if err != nil {
			return err
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A mutation MUST write the object register before the index register.
		if err := s.writeIndex(ctx, name); err != nil {
			return fmt.Errorf("cask storage: delete %q: object tombstoned, index write failed: %w", key, err)
		}
		if !readable {
			return runtime.SetZeroValue(out)
		}
		return s.decode(key, v.Value, tomb.Seq, out)
	}
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
		if err := s.writeIndex(ctx, name); err != nil {
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
