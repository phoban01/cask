package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/mvcc"
)

// storageCheckTimeout bounds the consensus read of one readiness check. It
// is below the one-second default timeout of a kubelet probe.
const storageCheckTimeout = 800 * time.Millisecond

// storageCheck is a readyz check. It passes only when a linearizable read
// of the Device index register completes, so the server reports not ready
// while it cannot reach a majority of the acceptors. The read proposes no
// change. It checks each request again, so readiness follows the storage
// both ways.
type storageCheck struct {
	kv      *mvcc.KV
	timeout time.Duration
}

func newStorageCheck(kv *mvcc.KV) storageCheck {
	return storageCheck{kv: kv, timeout: storageCheckTimeout}
}

// Name is the check name under /readyz.
func (storageCheck) Name() string { return "cask-storage" }

// Check reads the Device index register through consensus. ReadIndex
// retries a read that lost its round to another proposer.
func (c storageCheck) Check(r *http.Request) error {
	//= docs/spec/fleet.md#2-resources
	//# The extension server MUST report not ready until its storage is reachable.
	ctx, cancel := context.WithTimeout(r.Context(), c.timeout)
	defer cancel()
	if _, err := storage.ReadIndex(ctx, c.kv, "devices"); err != nil {
		return fmt.Errorf("cask storage is not reachable: %w", err)
	}
	return nil
}
