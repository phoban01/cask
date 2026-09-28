package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/phoban01/cask/internal/backoff"
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

// ReadIndex reads the index register of resource. It retries a read that
// lost its round.
func ReadIndex(ctx context.Context, kv *mvcc.KV, resource string) (Index, error) {
	chain, err := indexHistory(ctx, kv, resource)
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

// indexWriteRetries bounds how often WriteIndex retries. It retries after
// another index writer changed the register first, and after a round
// lost to other proposers.
const indexWriteRetries = 32

// contentionBackoff spaces the retries after a lost round. A round loses
// when other rounds on the same register preempt it, so an immediate
// retry often meets the same rounds again.
var contentionBackoff = backoff.FullJitter(time.Millisecond, 50*time.Millisecond)

// WriteIndex makes the index entry for name agree with the object
// register. It records the object's current sequence, or removes the name
// when the object is tombstoned or absent. Call it after the object write
// of every create, update, and delete.
//
// It returns the entry that the index holds for name after the call, and
// whether the index names it. When the index does not name it, the
// entry's Idx is the index sequence of the removal, or the index sequence
// that WriteIndex read when there was nothing to remove.
func WriteIndex(ctx context.Context, kv *mvcc.KV, resource, name string) (Entry, bool, error) {
	w, err := writeIndex(ctx, kv, resource, name)
	return w.entry, w.live, err
}

// indexWrite is the result of one index write for a name.
type indexWrite struct {
	// entry is the entry that the index holds for the name after the
	// write. When the index does not name it, only entry.Idx is set: the
	// index sequence of the removal, or the index sequence the write read.
	entry Entry
	// live is true when the index names the object.
	live bool
	// wrote is true when this write changed the index.
	wrote bool
	// removed is the entry that this write removed. It is set only when
	// wrote is true and live is false.
	removed Entry
}

// writeIndex is WriteIndex. It also reports whether it changed the index.
//
// It retries an attempt that another index writer beat, and an attempt
// that lost its round. A lost round can end in ErrUnknownOutcome: the
// compare-and-set on the index may or may not land. The retry is still
// safe. Each attempt reads the index and the object head again, computes
// the entry from that head, and compares and sets on the index sequence
// it read. The read is itself a round on the index register. It either
// chooses the lost write or chooses a value without it, and after that
// the lost write can never land. If the lost write landed, the attempt
// finds nothing to do. If not, the attempt writes the entry.
//
// When an earlier attempt landed, wrote is false. So a caller that needs
// the index sequence of its own write looks the step up in the index
// history when wrote is false, as Store.written and Store.Delete do.
func writeIndex(ctx context.Context, kv *mvcc.KV, resource, name string) (indexWrite, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# When the index write of a mutation did not complete, the next index write for that object MUST record the object register's current sequence.
	//= docs/spec/fleet.md#3-storage-model
	//# A mutation whose object write committed MUST retry its index write when that index write loses a round or has an unknown outcome.
	var lost error
	lostRounds := 0
	for range indexWriteRetries {
		w, err := writeIndexOnce(ctx, kv, resource, name)
		switch {
		case err == nil:
			return w, nil
		case errors.Is(err, caspaxos.ErrConflict):
			// Another index writer changed the register after the read.
		case caspaxos.LostRound(err):
			lost = err
			if err := contentionBackoff(ctx, lostRounds); err != nil {
				return indexWrite{}, err
			}
			lostRounds++
		default:
			return indexWrite{}, err
		}
		if err := ctx.Err(); err != nil {
			return indexWrite{}, err
		}
	}
	if lost != nil {
		return indexWrite{}, fmt.Errorf("cask storage: %s index write for %q failed %d times, %d on lost rounds: %w",
			resource, name, indexWriteRetries, lostRounds, lost)
	}
	return indexWrite{}, fmt.Errorf("cask storage: %s index write for %q lost %d races", resource, name, indexWriteRetries)
}

// writeIndexOnce is one attempt of writeIndex. It returns
// caspaxos.ErrConflict when another index writer changed the register
// after the read.
//
// It reads the index before the object. Object sequences only grow, so
// the sequence it records is never lower than the entry it read. The
// compare-and-set on the index sequence then makes sure no other writer
// changed the index in between. An entry therefore never goes backwards
// and never passes the object register.
func writeIndexOnce(ctx context.Context, kv *mvcc.KV, resource, name string) (indexWrite, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# Each resource type MUST have one index register that maps every object name to that object's latest sequence.
	//= docs/spec/fleet.md#3-storage-model
	//# The index register MUST NOT record a sequence higher than the object register holds.
	chain, err := kv.History(ctx, IndexKey(resource))
	if err != nil {
		return indexWrite{}, err
	}
	idx, live, err := indexHead(chain)
	if err != nil {
		return indexWrite{}, fmt.Errorf("cask storage: decode %s index: %w", resource, err)
	}
	seq, objLive, err := objectHead(ctx, kv, resource, name)
	if err != nil {
		return indexWrite{}, err
	}
	cur, had := idx.Entries[name]
	switch {
	case objLive && had && cur.Obj == seq:
		return indexWrite{entry: cur, live: true}, nil
	case !objLive && !had:
		return indexWrite{entry: Entry{Idx: idx.Seq}}, nil
	case objLive:
		//= docs/spec/fleet.md#3-storage-model
		//# Each index entry MUST record both the object register sequence and the index sequence at which the index register recorded it.
		idx.Entries[name] = Entry{Obj: seq, Idx: idx.Seq + 1}
	default:
		delete(idx.Entries, name)
	}
	next, err := json.Marshal(idx.Entries)
	if err != nil {
		return indexWrite{}, err
	}
	// The compare-and-set is on the index sequence, not the value. An
	// index value can repeat, for example after a create and a delete of
	// the same name, but a sequence never does. CASSeq with 0 matches an
	// absent or tombstoned register.
	expect := idx.Seq
	if !live {
		expect = 0
	}
	v, err := kv.CASSeq(ctx, IndexKey(resource), expect, next)
	if err != nil {
		return indexWrite{}, err
	}
	if v.Seq != idx.Seq+1 {
		return indexWrite{}, fmt.Errorf("cask storage: %s index write landed at %d, want %d",
			resource, v.Seq, idx.Seq+1)
	}
	if objLive {
		return indexWrite{entry: idx.Entries[name], live: true, wrote: true}, nil
	}
	return indexWrite{entry: Entry{Idx: v.Seq}, wrote: true, removed: cur}, nil
}

// indexHistory reads the index history of resource. It retries a read
// that lost its round, so a mutation whose writes committed does not fail
// on the read that finds its own index step.
func indexHistory(ctx context.Context, kv *mvcc.KV, resource string) (mvcc.Chain, error) {
	return readHistory(ctx, kv, IndexKey(resource))
}

// readHistory reads the history of the register at key. It retries a
// read that lost its round.
func readHistory(ctx context.Context, kv *mvcc.KV, key []byte) (mvcc.Chain, error) {
	return retryRead(ctx, func() (mvcc.Chain, error) { return kv.History(ctx, key) })
}

// retryRead runs read, one read round on a register. While the round
// loses, it waits a jittered backoff and runs read again. It gives up
// after indexWriteRetries attempts and returns the last error.
//
// A read round proposes the value it found, unchanged. So a retry never
// applies a change twice. A lost read round ends in ErrPreempted, or in
// ErrUnknownOutcome when its accept phase failed. After an unknown
// outcome, some acceptors may hold the write-back of the read. That
// value is one the register held, or one that another proposer was
// already writing. A later round may choose it, and a later read then
// sees it, as it would anyway. So an unknown outcome of a read is a lost
// read, and the retry is safe.
func retryRead[T any](ctx context.Context, read func() (T, error)) (T, error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A read of the index register or an object register MUST retry when its round loses or has an unknown outcome.
	return caspaxos.RetryLost(ctx, indexWriteRetries, contentionBackoff, read)
}

// ErrNotRecorded means that the retained index history holds no entry for
// a write that committed. It happens only when the index history was
// compacted past the write. The write landed; the caller must re-read.
var ErrNotRecorded = errors.New("cask storage: the index history no longer holds the write")

// RecordedAt returns the index sequence at which the index of resource
// recorded object sequence objSeq for name. It returns ErrNotRecorded when
// the retained index history holds no such entry.
//
// A write at objSeq can be overwritten before its own index write runs.
// The overwrite compared and set on objSeq, and a writer compares and
// sets only on a sequence that the index recorded. So the index recorded
// objSeq before the overwrite, and RecordedAt finds it.
func RecordedAt(ctx context.Context, kv *mvcc.KV, resource, name string, objSeq uint64) (uint64, error) {
	chain, err := indexHistory(ctx, kv, resource)
	if err != nil {
		return 0, err
	}
	// Walk back from the head. Entries never go backwards, so stop at the
	// first entry below objSeq.
	for i := len(chain.Versions) - 1; i >= 0; i-- {
		entries, err := versionEntries(chain.Versions[i])
		if err != nil {
			return 0, fmt.Errorf("cask storage: decode %s index at %d: %w", resource, chain.Versions[i].Seq, err)
		}
		e, ok := entries[name]
		switch {
		case ok && e.Obj == objSeq:
			return e.Idx, nil
		case ok && e.Obj < objSeq:
			return 0, ErrNotRecorded
		}
	}
	return 0, ErrNotRecorded
}

// RemovedAt returns the index sequence at which the index of resource
// removed name after it recorded object sequence objSeq. A delete that
// tombstoned objSeq uses it to report its own removal step when another
// index write removed the name. It returns ErrNotRecorded when the
// retained index history holds no such step.
func RemovedAt(ctx context.Context, kv *mvcc.KV, resource, name string, objSeq uint64) (uint64, error) {
	chain, err := indexHistory(ctx, kv, resource)
	if err != nil {
		return 0, err
	}
	// Walk back from the head. removed is the sequence of the newest
	// version after i that does not name the object, while no later
	// version names it.
	var removed uint64
	for i := len(chain.Versions) - 1; i >= 0; i-- {
		v := chain.Versions[i]
		entries, err := versionEntries(v)
		if err != nil {
			return 0, fmt.Errorf("cask storage: decode %s index at %d: %w", resource, v.Seq, err)
		}
		e, ok := entries[name]
		switch {
		case !ok:
			removed = v.Seq
		case e.Obj == objSeq && removed != 0:
			return removed, nil
		case e.Obj <= objSeq:
			return 0, ErrNotRecorded
		default:
			removed = 0
		}
	}
	return 0, ErrNotRecorded
}

// versionEntries decodes the entries of one index version.
func versionEntries(v mvcc.Version) (map[string]Entry, error) {
	if v.Tombstone {
		return map[string]Entry{}, nil
	}
	return decodeIndex(v.Value)
}

// PrepareCreate readies a create of name in resource. It returns the head
// sequence of the object register that the create must pass to
// mvcc.CreateAt: 0 for an absent register, or the sequence of the
// tombstone at the head. exists is true when the head is live, and the
// create must fail.
//
// A create over a tombstone must not run while the index still names the
// deleted object. Its index write would change the entry from the old
// object to the new one in one step, and a watch would report one
// MODIFIED event with a new UID in place of a DELETED and an ADDED event.
// So when the index still names the object, PrepareCreate first writes the
// index, which records the removal, and then reads again.
//
// PrepareCreate reads the head before the index. CreateAt then succeeds
// only if the head is still that tombstone. Until the create lands the
// head stays a tombstone, so no index write can name the object again.
func PrepareCreate(ctx context.Context, kv *mvcc.KV, resource, name string) (seq uint64, exists bool, err error) {
	seq, exists, _, err = readyCreate(ctx, kv, resource, name)
	return seq, exists, err
}

// readyCreate is PrepareCreate. It also reports whether it tried to
// write the index, so the Store wakes its watches only then.
func readyCreate(ctx context.Context, kv *mvcc.KV, resource, name string) (seq uint64, exists, wrote bool, err error) {
	//= docs/spec/fleet.md#3-storage-model
	//# A create MUST NOT write over a tombstone while the index register still names the deleted object.
	//= docs/spec/fleet.md#4-list-and-watch
	//# A watch MUST report the deletion of an object and the creation of a new object with the same name as separate events.
	for range indexWriteRetries {
		seq, live, err := objectHead(ctx, kv, resource, name)
		if err != nil {
			return 0, false, wrote, err
		}
		if live {
			return seq, true, wrote, nil
		}
		idx, err := ReadIndex(ctx, kv, resource)
		if err != nil {
			return 0, false, wrote, err
		}
		if _, named := idx.Entries[name]; !named {
			return seq, false, wrote, nil
		}
		wrote = true
		if _, err := writeIndex(ctx, kv, resource, name); err != nil {
			return 0, false, wrote, err
		}
	}
	return 0, false, wrote, fmt.Errorf("cask storage: %s create of %q lost %d races", resource, name, indexWriteRetries)
}

// objectHead returns the sequence of the object's head version and
// whether that version is live. It retries a read that lost its round.
func objectHead(ctx context.Context, kv *mvcc.KV, resource, name string) (seq uint64, live bool, err error) {
	chain, err := readHistory(ctx, kv, ObjectKey(resource, name))
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
