package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/mvcc"
)

// ObjectKey names the register that holds one object.
func ObjectKey(resource, name string) []byte {
	return fmt.Appendf(nil, "fleet/%s/%s", resource, name)
}

// IndexKey names the index register of one resource type.
func IndexKey(resource string) []byte {
	return fmt.Appendf(nil, "fleet/%s.index", resource)
}

// Index is one read of a resource type's index register.
type Index struct {
	// Seq is the sequence of the index register. It is the list
	// resourceVersion.
	Seq uint64
	// Entries maps each object name to the object sequence that the index
	// recorded.
	Entries map[string]uint64
}

// ReadIndex reads the index register of resource.
func ReadIndex(ctx context.Context, kv *mvcc.KV, resource string) (Index, error) {
	chain, err := kv.History(ctx, IndexKey(resource))
	if err != nil {
		return Index{}, err
	}
	idx := Index{Entries: map[string]uint64{}}
	n := len(chain.Versions)
	if n == 0 {
		return idx, nil
	}
	head := chain.Versions[n-1]
	idx.Seq = head.Seq
	if head.Tombstone {
		return idx, nil
	}
	entries, err := decodeIndex(head.Value)
	if err != nil {
		return Index{}, fmt.Errorf("cask storage: decode %s index: %w", resource, err)
	}
	idx.Entries = entries
	return idx, nil
}

func decodeIndex(raw []byte) (map[string]uint64, error) {
	entries := map[string]uint64{}
	if len(raw) == 0 {
		return entries, nil
	}
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// indexWriteRetries bounds how often WriteIndex retries after another
// index writer changed the register first.
const indexWriteRetries = 32

// WriteIndex makes the index entry for name agree with the object
// register. It records the object's current sequence, or removes the name
// when the object is tombstoned or absent. Call it after the object write
// of every create, update, and delete.
//
// WriteIndex reads the index before the object. Object sequences only
// grow, so the sequence it records is never lower than the entry it
// read. The compare-and-set on the index then makes sure no other writer
// recorded a newer entry in between. An entry therefore never goes
// backwards and never passes the object register.
func WriteIndex(ctx context.Context, kv *mvcc.KV, resource, name string) error {
	//= docs/spec/fleet.md#3-storage-model
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	//= docs/spec/fleet.md#3-storage-model
	//# The index register MUST NOT record a sequence higher than the object register holds.
	//= docs/spec/fleet.md#3-storage-model
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	for range indexWriteRetries {
		cur, found, err := kv.Get(ctx, IndexKey(resource))
		if err != nil {
			return err
		}
		entries, err := decodeIndex(cur)
		if err != nil {
			return fmt.Errorf("cask storage: decode %s index: %w", resource, err)
		}
		seq, live, err := objectHead(ctx, kv, resource, name)
		if err != nil {
			return err
		}
		if live {
			entries[name] = seq
		} else {
			delete(entries, name)
		}
		// encoding/json sorts map keys, so equal entries give equal bytes.
		next, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		var expected []byte
		if found {
			expected = cur
		}
		if bytes.Equal(expected, next) || (!found && len(entries) == 0) {
			return nil
		}
		_, err = kv.CAS(ctx, IndexKey(resource), expected, next)
		if err == nil {
			return nil
		}
		if !errors.Is(err, caspaxos.ErrConflict) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return fmt.Errorf("cask storage: %s index write for %q lost %d races", resource, name, indexWriteRetries)
}

// objectHead returns the sequence of the object's head version and
// whether that version is live.
func objectHead(ctx context.Context, kv *mvcc.KV, resource, name string) (seq uint64, live bool, err error) {
	chain, err := kv.History(ctx, ObjectKey(resource, name))
	if err != nil {
		return 0, false, err
	}
	n := len(chain.Versions)
	if n == 0 {
		return 0, false, nil
	}
	head := chain.Versions[n-1]
	return head.Seq, head.Live(), nil
}
