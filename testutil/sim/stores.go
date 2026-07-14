package sim

import (
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/store"
)

// MemStores returns a store factory that builds n fresh in-memory acceptor
// stores per scenario — the default backing for the gate. (Pebble-backed
// durable stores arrive with §3.0; the slow_fsync fault wraps these via
// faults.SlowStore.)
func MemStores(n int) func(seed int64) []caspaxos.Storage {
	return func(int64) []caspaxos.Storage {
		out := make([]caspaxos.Storage, n)
		for i := range out {
			out[i] = store.NewMem()
		}
		return out
	}
}
