package storage

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/phoban01/cask/internal/mvcc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// The messages match the etcd store, so clients see the same 410 Gone.
const (
	listExpired     = "The resourceVersion for the provided list is too old."
	continueExpired = "The provided continue parameter is too old to display a consistent list result. " +
		"You can start a new list without the continue parameter, or use the continue token in this " +
		"response to retrieve the remainder of the results. Continuing with the provided token results " +
		"in an inconsistent list - objects that were created, modified, or deleted between the time " +
		"the first chunk was returned and now may show up in the list."
)

// indexAt returns the entries of the index version seq in chain. ok is
// false when chain no longer holds that version. Sequence 0 is the empty
// index before the first index write.
func indexAt(chain mvcc.Chain, seq uint64) (entries map[string]uint64, ok bool, err error) {
	if seq == 0 {
		if chain.CompactedBelow > 1 {
			return nil, false, nil
		}
		return map[string]uint64{}, true, nil
	}
	for _, v := range chain.Versions {
		if v.Seq != seq {
			continue
		}
		if v.Tombstone {
			return map[string]uint64{}, true, nil
		}
		entries, err := decodeIndex(v.Value)
		return entries, err == nil, err
	}
	return nil, false, nil
}

// headSeq returns the sequence of the newest version in chain, or 0.
func headSeq(chain mvcc.Chain) uint64 {
	if n := len(chain.Versions); n > 0 {
		return chain.Versions[n-1].Seq
	}
	return 0
}

// objectAt decodes the object name at sequence seq into a new object like
// proto. The resourceVersion of the result is seq.
func (s *Store) objectAt(ctx context.Context, name string, seq uint64, proto runtime.Object) (runtime.Object, error) {
	v, found, err := s.kv.GetAt(ctx, ObjectKey(s.resource, name), seq)
	if err != nil {
		return nil, err
	}
	if !found || v.Tombstone {
		// The index never records a sequence the object register does
		// not hold. A missing version was compacted away.
		return nil, apistorage.NewInternalError(fmt.Errorf(
			"cask storage: %s %q has no live version at index sequence %d", s.resource, name, seq))
	}
	obj := newLike(proto)
	if err := s.decode(string(ObjectKey(s.resource, name)), v.Value, seq, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// GetList reads the index register once and lists the objects it names.
//
// The list resourceVersion is the index sequence. Each item is the object
// at the sequence the index recorded, so the whole list is the state at
// one index version. With ResourceVersionMatch Exact, or with a continue
// token, GetList serves an older index version from the index history. It
// returns 410 Gone when that version is compacted. A resourceVersion
// above the current index sequence gets a "too large resource version"
// error, which the client retries.
//
// Limit and Continue page through the names in sorted order. Every page
// reads the index version of the first page.
func (s *Store) GetList(ctx context.Context, key string, opts apistorage.ListOptions, listObj runtime.Object) error {
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	items, err := conversion.EnforcePtr(listPtr)
	if err != nil || items.Kind() != reflect.Slice {
		return fmt.Errorf("cask storage: list %q: need a pointer to a slice: %v", key, err)
	}
	proto, ok := reflect.New(items.Type().Elem()).Interface().(runtime.Object)
	if !ok {
		return fmt.Errorf("cask storage: list %q: item type %s is not a runtime.Object", key, items.Type().Elem())
	}

	// Every fleet resource is cluster-scoped, so a recursive key is the
	// resource prefix and a non-recursive key names one object.
	keyPrefix := key
	if opts.Recursive && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	withRev, continueKey, err := apistorage.ValidateListOptions(keyPrefix, s.versioner, opts)
	if err != nil {
		return err
	}
	minRV, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	if opts.ResourceVersionMatch == metav1.ResourceVersionMatchExact && minRV == 0 {
		return apierrors.NewBadRequest("resourceVersionMatch=Exact needs a nonzero resourceVersion")
	}

	//= docs/spec/fleet.md#4-list-and-watch
	//# A list MUST return every object that the index register names at the index sequence the list reports.
	chain, err := s.kv.History(ctx, IndexKey(s.resource))
	if err != nil {
		return err
	}
	current := headSeq(chain)
	listRV := current
	if withRev > 0 {
		listRV = uint64(withRev)
	}
	if listRV < minRV || listRV > current {
		// Not older than minRV, or an exact version this cluster has
		// not seen yet.
		return apistorage.NewTooLargeResourceVersionError(max(minRV, listRV), current, 1)
	}
	entries, ok, err := indexAt(chain, listRV)
	if err != nil {
		return fmt.Errorf("cask storage: decode %s index at %d: %w", s.resource, listRV, err)
	}
	if !ok {
		if continueKey != "" {
			return continueFromLatest(continueKey, keyPrefix)
		}
		return apierrors.NewResourceExpired(listExpired)
	}

	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	if !opts.Recursive {
		name := key[strings.LastIndexByte(key, '/')+1:]
		names = names[:0]
		if _, ok := entries[name]; ok {
			names = append(names, name)
		}
	}
	if continueKey != "" {
		start := strings.TrimPrefix(continueKey, keyPrefix)
		names = names[sort.SearchStrings(names, start):]
	}

	limit := opts.Predicate.Limit
	var next string
	var remaining *int64
	for i, name := range names {
		obj, err := s.objectAt(ctx, name, entries[name], proto)
		if err != nil {
			return err
		}
		match, err := opts.Predicate.Matches(obj)
		if err != nil {
			return err
		}
		if match {
			items.Set(reflect.Append(items, reflect.ValueOf(obj).Elem()))
		}
		if limit > 0 && int64(items.Len()) == limit && i+1 < len(names) {
			// The next page starts just after this name.
			next, err = apistorage.EncodeContinue(keyPrefix+name+"\x00", keyPrefix, int64(listRV))
			if err != nil {
				return err
			}
			if opts.Predicate.Empty() {
				n := int64(len(names) - i - 1)
				remaining = &n
			}
			break
		}
	}
	//= docs/spec/fleet.md#3-storage-model
	//# A list's resourceVersion MUST be the sequence of the index register.
	if listRV == 0 {
		// No index write has happened yet. The versioner refuses 0, so
		// set the list resourceVersion directly. A watch from "0" starts
		// with the current state, so no change is lost.
		lm, err := meta.ListAccessor(listObj)
		if err != nil {
			return err
		}
		lm.SetResourceVersion("0")
		lm.SetContinue(next)
		lm.SetRemainingItemCount(remaining)
		return nil
	}
	return s.versioner.UpdateList(listObj, listRV, next, remaining)
}

// continueFromLatest returns 410 Gone for a continue token whose index
// version is compacted. Like the etcd store, it carries a new token that
// continues from the same key at the latest index version.
func continueFromLatest(continueKey, keyPrefix string) error {
	status := apierrors.NewResourceExpired(continueExpired)
	token, err := apistorage.EncodeContinue(continueKey, keyPrefix, -1)
	if err != nil {
		return status
	}
	status.ErrStatus.ListMeta.Continue = token
	return status
}
