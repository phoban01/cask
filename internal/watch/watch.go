// Package watch turns the MVCC version chain into a resumable change feed. A
// watcher streams the versions of a key (or key prefix) after a cursor, and can
// resume from a past revision as long as history has not been compacted below
// it — in which case it gets ErrCompacted and must re-list current state (the
// same contract etcd offers). The agent tier fans one upstream subscription out
// to many client watchers, so the storage tier sees O(agents) subscriptions
// rather than O(clients).
//
// Watchers are pull-based (Poll): a real deployment drives Poll from the range
// owner's commit notifications, but pulling keeps the logic pure and lets the
// simulator control timing deterministically.
package watch

import (
	"context"
	"errors"
	"sort"

	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
)

// ErrCompacted means a watcher's resume point is older than the retained
// history; the client must re-read current state and resume from there.
var ErrCompacted = errors.New("watch: requested revision has been compacted")

// Event is one observed version of a key.
type Event struct {
	Key     []byte
	Version mvcc.Version
}

// HistoryReader reads a key's retained version chain. *mvcc.KV satisfies it.
type HistoryReader interface {
	History(ctx context.Context, key []byte) (mvcc.Chain, error)
}

// KeyLister enumerates the keys under a prefix (the range owner's local scan).
type KeyLister func(ctx context.Context, prefix []byte) ([][]byte, error)

// KeyWatcher streams a single key's versions after a sequence-number cursor.
type KeyWatcher struct {
	src    HistoryReader
	key    []byte
	cursor uint64 // highest seq already delivered
}

// NewKeyWatcher watches key, resuming after fromSeq (0 = from the beginning).
func NewKeyWatcher(src HistoryReader, key []byte, fromSeq uint64) *KeyWatcher {
	return &KeyWatcher{src: src, key: append([]byte(nil), key...), cursor: fromSeq}
}

// Cursor returns the highest sequence number delivered so far.
func (w *KeyWatcher) Cursor() uint64 { return w.cursor }

// Poll returns the versions committed since the cursor, in order, and advances
// the cursor past them. It returns ErrCompacted if the cursor has fallen below
// the retained history.
func (w *KeyWatcher) Poll(ctx context.Context) ([]Event, error) {
	chain, err := w.src.History(ctx, w.key)
	if err != nil {
		return nil, err
	}
	if w.cursor+1 < chain.CompactedBelow {
		return nil, ErrCompacted
	}
	var out []Event
	for _, v := range chain.Versions {
		if v.Seq > w.cursor {
			out = append(out, Event{Key: w.key, Version: v})
			w.cursor = v.Seq
		}
	}
	return out, nil
}

// PrefixWatcher streams versions of every key under a prefix, ordered by HLC, so
// a client sees a single coherent stream across the keys in a range.
type PrefixWatcher struct {
	src    HistoryReader
	lister KeyLister
	prefix []byte
	cursor hlc.Timestamp // highest HLC already delivered
}

// NewPrefixWatcher watches prefix, resuming after fromHLC.
func NewPrefixWatcher(src HistoryReader, lister KeyLister, prefix []byte, fromHLC hlc.Timestamp) *PrefixWatcher {
	return &PrefixWatcher{src: src, lister: lister, prefix: append([]byte(nil), prefix...), cursor: fromHLC}
}

// Cursor returns the highest HLC delivered so far.
func (w *PrefixWatcher) Cursor() hlc.Timestamp { return w.cursor }

// Poll returns all versions across the prefix's keys with HLC after the cursor,
// ordered by HLC, and advances the cursor.
func (w *PrefixWatcher) Poll(ctx context.Context) ([]Event, error) {
	keys, err := w.lister(ctx, w.prefix)
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, key := range keys {
		chain, err := w.src.History(ctx, key)
		if err != nil {
			return nil, err
		}
		for _, v := range chain.Versions {
			if w.cursor.Less(v.HLC) {
				out = append(out, Event{Key: key, Version: v})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version.HLC.Less(out[j].Version.HLC) })
	if n := len(out); n > 0 {
		w.cursor = out[n-1].Version.HLC
	}
	return out, nil
}

// SafeCompactPoint returns the lowest sequence number it is safe to compact a
// key up to: the smaller of the retention target and one past the oldest active
// watcher cursor, so no live watcher is starved. cursors are the active
// KeyWatcher cursors for the key; with none, retention alone applies.
func SafeCompactPoint(retentionFrom uint64, cursors []uint64) uint64 {
	keep := retentionFrom
	for _, c := range cursors {
		// A watcher at cursor c still needs versions with Seq > c, so we may only
		// compact strictly at or below c+1's predecessor — i.e. keep from <= c+1.
		if c+1 < keep {
			keep = c + 1
		}
	}
	return keep
}
