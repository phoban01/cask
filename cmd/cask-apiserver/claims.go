package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/phoban01/cask/internal/lease"
)

// claimController binds DeviceClaims to devices. A binding IS holding the
// device's cask lock under the claim's session: cask's SingleHolder property
// (tla/Lease.tla) is what makes the lease globally exclusive across every
// cluster's apiserver — this controller contains no mutual-exclusion logic
// of its own, only bookkeeping.
//
// Reconciliation is deliberately pull-based and idempotent (reconcileOnce),
// driven by a ticker in production and directly by tests, so every scenario
// — contention, takeover, zombie fencing — is deterministic under test.
type claimController struct {
	cluster string
	store   *fleetStore
	log     *slog.Logger
}

// reconcileOnce advances every claim this cluster manages by one step:
// Pending claims try to acquire their device's lock; Bound claims renew
// their session (and detect having been superseded); deleted claims are
// handled by release() at delete time.
func (c *claimController) reconcileOnce(ctx context.Context) {
	names, raws, rvs, err := c.store.list(ctx, "deviceclaims")
	if err != nil {
		c.log.Warn("claim list", "err", err)
		return
	}
	for i := range names {
		var claim DeviceClaim
		if err := json.Unmarshal(raws[i], &claim); err != nil {
			c.log.Warn("claim decode", "claim", names[i], "err", err)
			continue
		}
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A controller MUST reconcile only claims whose status names its own cluster.
		if claim.Status.Cluster != c.cluster {
			continue // another cluster's apiserver manages this claim
		}
		if err := c.reconcileClaim(ctx, claim, rvs[i]); err != nil {
			c.log.Warn("claim reconcile", "claim", claim.Name, "err", err)
		}
	}
}

func (c *claimController) reconcileClaim(ctx context.Context, claim DeviceClaim, rv uint64) error {
	switch claim.Status.Phase {
	case ClaimBound:
		return c.renew(ctx, claim, rv)
	case ClaimLost:
		return nil // terminal: the workload must create a new claim
	default:
		return c.tryBind(ctx, claim, rv)
	}
}

// tryBind attempts to take the device's global lease for a Pending claim.
func (c *claimController) tryBind(ctx context.Context, claim DeviceClaim, rv uint64) error {
	device := claim.Spec.DeviceName
	if _, _, err := c.store.get(ctx, "devices", device); errors.Is(err, errNotFound) {
		return c.setClaimStatus(ctx, claim, rv, DeviceClaimStatus{
			Phase: ClaimPending, Cluster: c.cluster, Reason: fmt.Sprintf("device %q does not exist", device),
		})
	} else if err != nil {
		return err
	}

	ttl := claim.Spec.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	session := claimSessionID(c.cluster, claim.Name)
	if _, err := c.store.sessions.Grant(ctx, session, session, ttl*1_000_000_000); err != nil {
		return fmt.Errorf("session grant: %w", err)
	}

	//= docs/spec/fleet.md#5-claims-and-fencing
	//# Binding a claim to an object MUST be the acquisition of that object's cask lock under the claim's session.
	fence, err := c.store.locks.Acquire(ctx, deviceLockName(device), session)
	switch {
	case errors.Is(err, lease.ErrHeld):
		// The single-global-lease property doing its job: someone else —
		// possibly in another cluster — holds this device. Stay Pending.
		return c.setClaimStatus(ctx, claim, rv, DeviceClaimStatus{
			Phase: ClaimPending, Cluster: c.cluster, Reason: "device leased elsewhere",
		})
	case err != nil:
		return fmt.Errorf("acquire: %w", err)
	}

	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A Bound claim MUST carry its fence in its status.
	if err := c.setClaimStatus(ctx, claim, rv, DeviceClaimStatus{
		Phase: ClaimBound, Cluster: c.cluster, Fence: fence,
	}); err != nil {
		return err
	}
	return c.setDeviceLease(ctx, device, &LeaseRef{Cluster: c.cluster, Claim: claim.Name, Fence: fence})
}

// renew keeps a Bound claim's session alive and detects being superseded.
func (c *claimController) renew(ctx context.Context, claim DeviceClaim, rv uint64) error {
	ttl := claim.Spec.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	session := claimSessionID(c.cluster, claim.Name)
	if _, err := c.store.sessions.KeepAlive(ctx, session, session, ttl*1_000_000_000); err != nil {
		// The session lapsed (or the keepalive lost): this claim's lease is
		// gone, and a successor may already hold a HIGHER fence. Terminal.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# When a claim's session lapses, the controller MUST set the claim to Lost.
		return c.setClaimStatus(ctx, claim, rv, DeviceClaimStatus{
			Phase: ClaimLost, Cluster: c.cluster, Fence: claim.Status.Fence,
			Reason: "lease session lapsed; a successor may hold a higher fence",
		})
	}
	return nil
}

// release drops a claim's lease (called when the claim object is deleted).
func (c *claimController) release(ctx context.Context, claim DeviceClaim) {
	if claim.Status.Cluster != c.cluster || claim.Status.Phase != ClaimBound {
		return
	}
	session := claimSessionID(c.cluster, claim.Name)
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# Deleting a Bound claim MUST release the object's lock.
	if err := c.store.locks.Release(ctx, deviceLockName(claim.Spec.DeviceName), session); err != nil {
		c.log.Warn("lock release", "device", claim.Spec.DeviceName, "err", err)
	}
	if err := c.setDeviceLease(ctx, claim.Spec.DeviceName, nil); err != nil && !errors.Is(err, errNotFound) {
		c.log.Warn("device status clear", "device", claim.Spec.DeviceName, "err", err)
	}
}

// setClaimStatus CAS-updates a claim's status against the version reconciled.
func (c *claimController) setClaimStatus(ctx context.Context, claim DeviceClaim, rv uint64, status DeviceClaimStatus) error {
	if claim.Status == status {
		return nil
	}
	claim.Status = status
	claim.ResourceVersion = ""
	raw, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	if _, err := c.store.update(ctx, "deviceclaims", claim.Name, raw, rv); errors.Is(err, errConflict) {
		//= docs/spec/fleet.md#3-storage-model
		//# The extension server MUST re-read an object before it retries a write that returned a conflict.
		//= docs/spec/fleet.md#3-storage-model
		//# A retried write MUST be a compare-and-set, never a blind reapplication of a change.
		return nil // the claim moved (user update / delete); next tick re-reads
	} else if err != nil {
		return err
	}
	return nil
}

// setDeviceLease updates a device's advertised lease (nil = Available). The
// authoritative lease is the LOCK; this is observability for kubectl.
func (c *claimController) setDeviceLease(ctx context.Context, name string, ref *LeaseRef) error {
	raw, rv, err := c.store.get(ctx, "devices", name)
	if err != nil {
		return err
	}
	var dev Device
	if err := json.Unmarshal(raw, &dev); err != nil {
		return err
	}
	// Fencing on the STATUS write itself: never regress the advertised fence
	// — a zombie's stale clear/downgrade must not mask a live higher lease.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A status write MUST NOT lower an advertised fence.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A receiver MUST reject an effect whose fence is lower than the highest fence it has accepted for that object.
	if ref != nil && dev.Status.Lease != nil && dev.Status.Lease.Fence > ref.Fence {
		return nil
	}
	if ref == nil {
		dev.Status = DeviceStatus{Phase: DeviceAvailable}
	} else {
		dev.Status = DeviceStatus{Phase: DeviceLeased, Lease: ref}
	}
	dev.ResourceVersion = ""
	next, err := json.Marshal(dev)
	if err != nil {
		return err
	}
	if _, err := c.store.update(ctx, "devices", name, next, rv); errors.Is(err, errConflict) {
		return nil // raced another status writer; next reconcile converges
	} else if err != nil {
		return err
	}
	return nil
}
