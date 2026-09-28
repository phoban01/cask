package storage

import (
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

// Entry is the index entry of one object.
type Entry struct {
	// Obj is the object register sequence that the index recorded. A
	// write to the object compares and sets on it.
	Obj uint64 `json:"o"`
	// Idx is the index sequence at which the index recorded Obj. It is
	// the object's resourceVersion.
	Idx uint64 `json:"i"`
}

// Index is one read of a resource type's index register.
type Index struct {
	// Seq is the sequence of the index register. It is the list
	// resourceVersion.
	Seq uint64
	// Entries maps each object name to its index entry.
	Entries map[string]Entry
}

// ReadIndex reads the index register of resource.
func ReadIndex(ctx context.Context, kv *mvcc.KV, resource string) (Index, error) {
	chain, err := kv.History(ctx, IndexKey(resource))
	if err != nil {
		return Index{}, err
	}
	idx, _, err := indexHead(chain)
	if err != nil {
		return Index{}, fmt.Errorf("cask storage: decode %s index: %w", resource, err)
	}
	return idx, nil
}

// indexHead decodes the newest version in chain. live is false when the
// register is absent or its head is a tombstone.
func indexHead(chain mvcc.Chain) (idx Index, live bool, err error) {
	idx.Entries = map[string]Entry{}
	n := len(chain.Versions)
	if n == 0 {
		return idx, false, nil
	}
	head := chain.Versions[n-1]
	idx.Seq = head.Seq
	if head.Tombstone {
		return idx, false, nil
	}
	if idx.Entries, err = decodeIndex(head.Value); err != nil {
		return Index{}, false, err
	}
	return idx, true, nil
}

func decodeIndex(raw []byte) (map[string]Entry, error) {
	entries := map[string]Entry{}
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
// It returns the entry that the index holds for name after the call, and
// whether the index names it. When the index does not name it, the
// entry's Idx is the index sequence of the removal, or the index sequence
// that WriteIndex read when there was nothing to remove.
//
// WriteIndex reads the index before the object. Object sequences only
// grow, so the sequence it records is never lower than the entry it
// read. The compare-and-set on the index sequence then makes sure no
// other writer changed the index in between. An entry therefore never
// goes backwards and never passes the object register.
func WriteIndex(ctx context.Context, kv *mvcc.KV, resource, name string) (Entry, bool, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	//= docs/spec/fleet.md#3-storage-model
	//# The index register MUST NOT record a sequence higher than the object register holds.
	//= docs/spec/fleet.md#3-storage-model
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	for range indexWriteRetries {
		chain, err := kv.History(ctx, IndexKey(resource))
		if err != nil {
			return Entry{}, false, err
		}
		idx, live, err := indexHead(chain)
		if err != nil {
			return Entry{}, false, fmt.Errorf("cask storage: decode %s index: %w", resource, err)
		}
		seq, objLive, err := objectHead(ctx, kv, resource, name)
		if err != nil {
			return Entry{}, false, err
		}
		cur, had := idx.Entries[name]
		switch {
		case objLive && had && cur.Obj == seq:
			return cur, true, nil
		case !objLive && !had:
			return Entry{Idx: idx.Seq}, false, nil
		case objLive:
			//= docs/spec/fleet.md#3-storage-model
			//# Each index entry MUST record both the object register sequence and the index sequence at which the index register recorded it.
			idx.Entries[name] = Entry{Obj: seq, Idx: idx.Seq + 1}
		default:
			delete(idx.Entries, name)
		}
		next, err := json.Marshal(idx.Entries)
		if err != nil {
			return Entry{}, false, err
		}
		// The compare-and-set is on the index sequence, not the value. An
		// index value can repeat, for example after a create and a
		// delete of the same name, but a sequence never does. CASSeq
		// with 0 matches an absent or tombstoned register.
		expect := idx.Seq
		if !live {
			expect = 0
		}
		v, err := kv.CASSeq(ctx, IndexKey(resource), expect, next)
		if err == nil {
			if v.Seq != idx.Seq+1 {
				return Entry{}, false, fmt.Errorf("cask storage: %s index write landed at %d, want %d",
					resource, v.Seq, idx.Seq+1)
			}
			if objLive {
				return idx.Entries[name], true, nil
			}
			return Entry{Idx: v.Seq}, false, nil
		}
		if !errors.Is(err, caspaxos.ErrConflict) {
			return Entry{}, false, err
		}
		if err := ctx.Err(); err != nil {
			return Entry{}, false, err
		}
	}
	return Entry{}, false, fmt.Errorf("cask storage: %s index write for %q lost %d races", resource, name, indexWriteRetries)
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
