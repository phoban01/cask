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

// writeIndex runs WriteIndex for name and then wakes the watches. It
// returns the entry that the index holds for name, and whether the index
// names it.
func (s *Store) writeIndex(ctx context.Context, name string) (Entry, bool, error) {
	e, live, err := WriteIndex(ctx, s.kv, s.resource, name)
	if err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	return e, live, nil
}

// repairIndex runs after a compare-and-set on an object register failed.
// The register may hold a write that the index does not record yet, from
// another writer or from a mutation whose index write did not complete.
// The index write catches the index up, so the next read of the entry
// sees that write.
func (s *Store) repairIndex(ctx context.Context, name string) error {
	//= docs/spec/fleet.md#3-storage-model
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	_, _, err := s.writeIndex(ctx, name)
	return err
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

// entry reads the index entry of name. ok is false when the index does
// not name it.
func (s *Store) entry(ctx context.Context, name string) (e Entry, ok bool, err error) {
	idx, err := ReadIndex(ctx, s.kv, s.resource)
	if err != nil {
		return Entry{}, false, err
	}
	e, ok = idx.Entries[name]
	return e, ok, nil
}

// indexed reads the object version that the entry e records.
func (s *Store) indexed(ctx context.Context, name string, e Entry) ([]byte, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A get MUST read the object at the object register sequence that the index entry records.
	v, found, err := s.kv.GetAt(ctx, ObjectKey(s.resource, name), e.Obj)
	if err != nil {
		return nil, err
	}
	if !found || v.Tombstone {
		// The index never records a sequence the object register does
		// not hold. A missing version was compacted away.
		return nil, apistorage.NewInternalError(fmt.Errorf(
			"cask storage: %s %q has no live version at object sequence %d", s.resource, name, e.Obj))
	}
	return v.Value, nil
}

// decode decodes raw into out and sets its resourceVersion to rv, an
// index sequence.
func (s *Store) decode(key string, raw []byte, rv uint64, out runtime.Object) error {
	//= docs/spec/fleet.md#3-storage-model
	//# An object's resourceVersion MUST be the index sequence at which the index register recorded that object version.
	if _, _, err := s.codec.Decode(raw, nil, out); err != nil {
		return apistorage.NewCorruptObjError(key, err)
	}
	return s.versioner.UpdateObject(out, rv)
}

// Versioner returns the Versioner that the store uses. A resourceVersion
// is a decimal index sequence. On an object it is the index sequence at
// which the index recorded that object version. On a list it is the
// sequence of the index register. ParseResourceVersion reads "" and "0"
// as 0. It rejects a value that is not a decimal number with a storage
// InvalidError.
func (s *Store) Versioner() apistorage.Versioner {
	//= docs/spec/fleet.md#4-list-and-watch
	//# Every resourceVersion that a get, a list, or a watch event reports MUST be an index sequence.
	return s.versioner
}

// encode clears the resourceVersion of obj and encodes it. The index
// sequence is the resourceVersion, so the stored bytes never carry one.
func (s *Store) encode(obj runtime.Object) ([]byte, error) {
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return nil, err
	}
	return runtime.Encode(s.codec, obj)
}

// written decodes into out the result of a write of raw at object
// sequence seq. e and live are what the index write returned.
//
// Usually the index records seq, and out is raw at the entry's index
// sequence. Another writer can land a newer version before this index
// write runs. The index then records that version, and raw has no index
// sequence of its own. out is then the version the index records, so the
// object and its resourceVersion always agree. When the index no longer
// names the object, out is raw at the index sequence of the removal. No
// entry ever carries that index sequence, so a precondition on it fails.
func (s *Store) written(ctx context.Context, key, name string, seq uint64, raw []byte, e Entry, live bool,
	out runtime.Object) error {
	if live && e.Obj != seq {
		var err error
		if raw, err = s.indexed(ctx, name, e); err != nil {
			return err
		}
	}
	return s.decode(key, raw, e.Idx, out)
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
		// The live object may not be in the index yet. Record it, so a
		// get agrees with this answer.
		if err := s.repairIndex(ctx, name); err != nil {
			return err
		}
		return apistorage.NewKeyExistsError(key, 0)
	}
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation MUST write the object register before the index register.
	e, live, err := s.writeIndex(ctx, name)
	if err != nil {
		// The object is committed. The next index write for this name,
		// or the sweep, records it.
		return fmt.Errorf("cask storage: create %q: object written, index write failed: %w", key, err)
	}
	if out == nil {
		return nil
	}
	return s.written(ctx, key, name, v.Seq, raw, e, live, out)
}

// Delete removes the object at key and decodes the deleted object into
// out. The resourceVersion of out is the index sequence at which the
// index removed the name.
//
// Delete reads the index entry and the object version it records, checks
// the preconditions, and runs validateDeletion. It then tombstones the
// register with a compare-and-set on the recorded object sequence, and
// removes the name from the index. When the register moved past the
// entry, it catches the index up, reads again, and repeats the checks.
// cachedExistingObject is not used: every attempt reads the index.
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
		e, ok, err := s.entry(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return apistorage.NewKeyNotFoundError(key, 0)
		}
		raw, err := s.indexed(ctx, name, e)
		if err != nil {
			return err
		}
		existing := newLike(out)
		readable := true
		if err := s.decode(key, raw, e.Idx, existing); err != nil {
			if !opts.IgnoreStoreReadError {
				return err
			}
			// The caller asked to delete an object it cannot decode.
			// There is no object to check.
			readable = false
		}
		if readable {
			//= docs/spec/fleet.md#3-storage-model
			//# A resourceVersion precondition MUST be checked against the index sequence of the object's index entry.
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
		_, err = s.kv.DeleteSeq(ctx, reg, e.Obj)
		if errors.Is(err, caspaxos.ErrConflict) {
			// The register holds a write that the entry does not record.
			// The checks ran on an old copy, so catch the index up, read
			// again, and repeat them.
			//= docs/spec/fleet.md#3-storage-model
			//# The extension server MUST re-read an object before it retries a write that returned a conflict.
			if err := s.repairIndex(ctx, name); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A mutation MUST write the object register before the index register.
		after, live, err := s.writeIndex(ctx, name)
		if err != nil {
			return fmt.Errorf("cask storage: delete %q: object tombstoned, index write failed: %w", key, err)
		}
		if !readable {
			return runtime.SetZeroValue(out)
		}
		rv := after.Idx
		if live {
			// A create landed after the tombstone and the index records
			// it. There is no removal step. Keep the resourceVersion of
			// the deleted version, which no later entry carries.
			rv = e.Idx
		}
		return s.decode(key, raw, rv, out)
	}
}

// Get reads the object at key into out. It reads the index entry and then
// the object version that the entry records, so an object written but
// not yet indexed is not found. Every Get is a linearizable read of the
// index register, so it meets any "not older than" bound in
// opts.ResourceVersion. The resourceVersion of out is the index sequence
// of the entry.
func (s *Store) Get(ctx context.Context, key string, opts apistorage.GetOptions, out runtime.Object) error {
	_, name, err := s.objectKey(key)
	if err != nil {
		return err
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A get MUST serve an object only once the index register records it.
	e, ok, err := s.entry(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return apistorage.NewKeyNotFoundError(key, 0)
	}
	raw, err := s.indexed(ctx, name, e)
	if err != nil {
		return err
	}
	return s.decode(key, raw, e.Idx, out)
}

// newLike returns a new, empty object of the same type as obj.
func newLike(obj runtime.Object) runtime.Object {
	return reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
}

// GuaranteedUpdate reads the index entry at key and the object version
// it records, runs tryUpdate on it, and writes the result with a
// compare-and-set on the recorded object sequence. When the register
// moved past the entry, it catches the index up, reads again, and runs
// tryUpdate on the fresh copy. It stops when a write lands, tryUpdate or
// preconditions fail, or ctx ends. The written object goes into
// destination.
//
// The object that tryUpdate gets carries the entry's index sequence as
// resourceVersion. The generic registry puts the client's resourceVersion
// check in tryUpdate, so a stale client resourceVersion fails there on
// the fresh read. cachedExistingObject is not used: every attempt reads
// the index.
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
		e, ok, err := s.entry(ctx, name)
		if err != nil {
			return err
		}
		existing := newLike(destination)
		var cur []byte
		if ok {
			if cur, err = s.indexed(ctx, name, e); err != nil {
				return err
			}
			if err := s.decode(key, cur, e.Idx, existing); err != nil {
				return err
			}
		} else if !ignoreNotFound {
			return apistorage.NewKeyNotFoundError(key, 0)
		}
		// A failed precondition is a storage InvalidObj error. The
		// registry returns it to the client as 409 Conflict.
		//= docs/spec/fleet.md#3-storage-model
		//# A resourceVersion precondition MUST be checked against the index sequence of the object's index entry.
		//= docs/spec/fleet.md#3-storage-model
		//# An update whose compare-and-set fails MUST return a conflict.
		if err := preconditions.Check(key, existing); err != nil {
			return err
		}
		// e is the zero Entry when the index does not name the object.
		// Obj 0 then asks CASSeq for an absent or tombstoned register.
		updated, ttl, err := tryUpdate(existing, apistorage.ResponseMeta{ResourceVersion: e.Idx})
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
		if ok && bytes.Equal(raw, cur) {
			// Nothing changed. Skip the write, as the etcd store does.
			return s.decode(key, cur, e.Idx, destination)
		}
		// The client's resourceVersion matched the entry's index
		// sequence. The entry maps it to one object sequence, so the
		// compare-and-set on that object sequence fails if any write
		// landed since that version.
		//= docs/spec/fleet.md#3-storage-model
		//# An update MUST use a compare-and-set on the resourceVersion the client supplied.
		//= docs/spec/fleet.md#3-storage-model
		//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
		nv, err := s.kv.CASSeq(ctx, reg, e.Obj, raw)
		if errors.Is(err, caspaxos.ErrConflict) {
			// The register holds a write that the entry does not record.
			// Catch the index up, read again, and run tryUpdate on the
			// fresh copy.
			//= docs/spec/fleet.md#3-storage-model
			//# The extension server MUST re-read an object before it retries a write that returned a conflict.
			if err := s.repairIndex(ctx, name); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		//= docs/spec/fleet.md#3-storage-model
		//# A mutation MUST write the object register before the index register.
		after, live, err := s.writeIndex(ctx, name)
		if err != nil {
			return fmt.Errorf("cask storage: update %q: object written, index write failed: %w", key, err)
		}
		return s.written(ctx, key, name, nv.Seq, raw, after, live, destination)
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
