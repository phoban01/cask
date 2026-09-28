package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/lease"
)

// claimStore is what the claim controller reads and writes. Each object
// carries its resourceVersion. An update is a compare-and-set on that
// resourceVersion: it returns errConflict when the object moved, and
// errNotFound when it is gone. The generic server backs it with the cask
// storage (storageClaims); the legacy mux backs it with fleetStore
// (legacyClaims).
type claimStore interface {
	listClaims(ctx context.Context) ([]v1alpha1.DeviceClaim, error)
	getDevice(ctx context.Context, name string) (*v1alpha1.Device, error)
	updateClaim(ctx context.Context, claim *v1alpha1.DeviceClaim) error
	updateDevice(ctx context.Context, dev *v1alpha1.Device) error
}

// claimController binds DeviceClaims to devices. A binding IS holding the
// device's cask lock under the claim's session: cask's SingleHolder property
// (quint/lease.qnt) is what makes the lease globally exclusive across every
// cluster's apiserver — this controller contains no mutual-exclusion logic
// of its own, only bookkeeping.
//
// Reconciliation is deliberately pull-based and idempotent (reconcileOnce),
// driven by a ticker in production and directly by tests, so every scenario
// — contention, takeover, zombie fencing — is deterministic under test.
type claimController struct {
	cluster  string
	store    claimStore
	sessions *lease.Sessions
	locks    *lease.Locks
	log      *slog.Logger
}

// reconcileOnce advances every claim this cluster manages by one step:
// Pending claims try to acquire their device's lock; Bound claims renew
// their session (and detect having been superseded); deleted claims are
// handled by release() at delete time.
func (c *claimController) reconcileOnce(ctx context.Context) {
	claims, err := c.store.listClaims(ctx)
	if err != nil {
		c.log.Warn("claim list", "err", err)
		return
	}
	for i := range claims {
		claim := &claims[i]
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A controller MUST reconcile only claims whose status names its own cluster.
		if claim.Status.Cluster != c.cluster {
			continue // another cluster's apiserver manages this claim
		}
		if err := c.reconcileClaim(ctx, claim); err != nil {
			c.log.Warn("claim reconcile", "claim", claim.Name, "err", err)
		}
	}
}

func (c *claimController) reconcileClaim(ctx context.Context, claim *v1alpha1.DeviceClaim) error {
	switch claim.Status.Phase {
	case v1alpha1.ClaimBound:
		return c.renew(ctx, claim)
	case v1alpha1.ClaimLost:
		return nil // terminal: the workload must create a new claim
	default:
		return c.tryBind(ctx, claim)
	}
}

// tryBind attempts to take the device's global lease for a Pending claim.
func (c *claimController) tryBind(ctx context.Context, claim *v1alpha1.DeviceClaim) error {
	device := claim.Spec.DeviceName
	if _, err := c.store.getDevice(ctx, device); errors.Is(err, errNotFound) {
		return c.setClaimStatus(ctx, claim, v1alpha1.DeviceClaimStatus{
			Phase: v1alpha1.ClaimPending, Cluster: c.cluster, Reason: fmt.Sprintf("device %q does not exist", device),
		})
	} else if err != nil {
		return err
	}

	ttl := claim.Spec.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	session := claimSessionID(c.cluster, claim.Name)
	if _, err := c.sessions.Grant(ctx, session, session, ttl*1_000_000_000); err != nil {
		return fmt.Errorf("session grant: %w", err)
	}

	//= docs/spec/fleet.md#5-claims-and-fencing
	//# Binding a claim to an object MUST be the acquisition of that object's cask lock under the claim's session.
	fence, err := c.locks.Acquire(ctx, deviceLockName(device), session)
	switch {
	case errors.Is(err, lease.ErrHeld):
		// The single-global-lease property doing its job: someone else —
		// possibly in another cluster — holds this device. Stay Pending.
		return c.setClaimStatus(ctx, claim, v1alpha1.DeviceClaimStatus{
			Phase: v1alpha1.ClaimPending, Cluster: c.cluster, Reason: "device leased elsewhere",
		})
	case err != nil:
		return fmt.Errorf("acquire: %w", err)
	}

	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A Bound claim MUST carry its fence in its status.
	if err := c.setClaimStatus(ctx, claim, v1alpha1.DeviceClaimStatus{
		Phase: v1alpha1.ClaimBound, Cluster: c.cluster, Fence: fence,
	}); err != nil {
		return err
	}
	return c.setDeviceLease(ctx, device, &v1alpha1.LeaseRef{Cluster: c.cluster, Claim: claim.Name, Fence: fence})
}

// renew keeps a Bound claim's session alive and detects being superseded.
func (c *claimController) renew(ctx context.Context, claim *v1alpha1.DeviceClaim) error {
	ttl := claim.Spec.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	session := claimSessionID(c.cluster, claim.Name)
	if _, err := c.sessions.KeepAlive(ctx, session, session, ttl*1_000_000_000); err != nil {
		// The session lapsed (or the keepalive lost): this claim's lease is
		// gone, and a successor may already hold a HIGHER fence. Terminal.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# When a claim's session lapses, the controller MUST set the claim to Lost.
		return c.setClaimStatus(ctx, claim, v1alpha1.DeviceClaimStatus{
			Phase: v1alpha1.ClaimLost, Cluster: c.cluster, Fence: claim.Status.Fence,
			Reason: "lease session lapsed; a successor may hold a higher fence",
		})
	}
	return nil
}

// release drops a claim's lease. The server calls it before it deletes the
// claim object. An error means the device status does not keep the
// claim's fence yet, so the delete must not go ahead.
func (c *claimController) release(ctx context.Context, claim *v1alpha1.DeviceClaim) error {
	own := claim.Status.Cluster == c.cluster
	if own && claim.Status.Phase == v1alpha1.ClaimBound {
		session := claimSessionID(c.cluster, claim.Name)
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# Deleting a Bound claim MUST release the object's lock.
		if err := c.locks.Release(ctx, deviceLockName(claim.Spec.DeviceName), session); err != nil {
			c.log.Warn("lock release", "device", claim.Spec.DeviceName, "err", err)
		}
	}
	if claim.Status.Fence == 0 {
		return nil
	}
	// The claim object goes next, and with it the claim's fence. The
	// device keeps the fence first, so the cutover export still sees it.
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# Deleting a claim MUST NOT complete before the lastFence of its Device is at least the claim's fence.
	err := c.releaseDeviceLease(ctx, claim.Spec.DeviceName, claim.Status.Fence, own)
	if errors.Is(err, errNotFound) {
		return nil // no device, so no status to keep the fence in
	}
	return err
}

// statusRetries bounds the compare-and-set retries of a device status write.
const statusRetries = 5

// updateDeviceStatus reads the device and compares and sets the status
// that next returns. next returns false when no write is needed. A
// conflict makes it read the device again and retry.
func (c *claimController) updateDeviceStatus(ctx context.Context, name string,
	next func(v1alpha1.DeviceStatus) (v1alpha1.DeviceStatus, bool)) error {
	var err error
	for range statusRetries {
		var dev *v1alpha1.Device
		//= docs/spec/fleet.md#3-storage-model
		//# The extension server MUST re-read an object before it retries a write that returned a conflict.
		if dev, err = c.store.getDevice(ctx, name); err != nil {
			return err
		}
		status, write := next(dev.Status)
		if !write {
			return nil
		}
		dev.Status = status
		if err = c.store.updateDevice(ctx, dev); !errors.Is(err, errConflict) {
			return err
		}
	}
	return err
}

// keptFence is the highest fence that status shows: its lastFence or its
// lease fence. A device written before lastFence existed has only the
// lease fence.
func keptFence(st v1alpha1.DeviceStatus) uint64 {
	if st.Lease != nil {
		return max(st.LastFence, st.Lease.Fence)
	}
	return st.LastFence
}

// setDeviceLease advertises ref as the device's lease. The authoritative
// lease is the LOCK; this is observability for kubectl and the fence
// record that the cutover export reads.
func (c *claimController) setDeviceLease(ctx context.Context, name string, ref *v1alpha1.LeaseRef) error {
	return c.updateDeviceStatus(ctx, name, func(st v1alpha1.DeviceStatus) (v1alpha1.DeviceStatus, bool) {
		// Fencing on the STATUS write itself: never regress the advertised
		// fence — a zombie's stale write must not mask a live higher lease,
		// and must not re-advertise a fence that a release already let go.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A status write MUST NOT lower an advertised fence.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A receiver MUST reject an effect whose fence is lower than the highest fence it has accepted for that object.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A write to a Device MUST NOT lower its lastFence.
		kept := keptFence(st)
		if ref.Fence < kept || (ref.Fence == kept && (st.Lease == nil || *st.Lease != *ref)) {
			// No counter counts this rejection, and none counts lease expiries.
			//= docs/spec/fleet.md#9-operations
			//= type=exception
			//= reason=no metrics yet; tracked in issue #63
			//# Cask MUST expose counters for lease expiries and for rejected fence regressions.
			return st, false
		}
		if st.Lease != nil && *st.Lease == *ref && st.LastFence == ref.Fence {
			return st, false // already advertised
		}
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A Device status MUST keep in its lastFence field the highest fence that the Device has advertised.
		return v1alpha1.DeviceStatus{Phase: v1alpha1.DeviceLeased, Lease: ref, LastFence: ref.Fence}, true
	})
}

// releaseDeviceLease records a released claim's fence in the device's
// lastFence. With clear, it also clears the lease when no higher fence
// holds it.
func (c *claimController) releaseDeviceLease(ctx context.Context, name string, fence uint64, clear bool) error {
	return c.updateDeviceStatus(ctx, name, func(st v1alpha1.DeviceStatus) (v1alpha1.DeviceStatus, bool) {
		// A release keeps the fence. It clears only the lease.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A write to a Device MUST NOT lower its lastFence.
		next := v1alpha1.DeviceStatus{Phase: st.Phase, Lease: st.Lease, LastFence: max(keptFence(st), fence)}
		// A zombie's release must not clear a successor's higher lease.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# A status write MUST NOT lower an advertised fence.
		if clear && st.Lease != nil && st.Lease.Fence <= fence {
			next.Lease = nil
		}
		if next.Lease == nil {
			next.Phase = v1alpha1.DeviceAvailable
		}
		return next, next.Phase != st.Phase || next.Lease != st.Lease || next.LastFence != st.LastFence
	})
}

// setClaimStatus CAS-updates a claim's status against the version
// reconciled, which the claim's resourceVersion names.
func (c *claimController) setClaimStatus(ctx context.Context, claim *v1alpha1.DeviceClaim, status v1alpha1.DeviceClaimStatus) error {
	if claim.Status == status {
		return nil
	}
	next := claim.DeepCopy()
	next.Status = status
	if err := c.store.updateClaim(ctx, next); errors.Is(err, errConflict) {
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

// run drives claim binding on an interval until ctx ends.
func (c *claimController) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.reconcileOnce(ctx)
		}
	}
}
