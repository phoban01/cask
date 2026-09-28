package main

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"k8s.io/apimachinery/pkg/runtime"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// storageClaims is the claimStore of the generic server. It reads and
// writes through the same cask stores that the registry serves, so a
// status write wakes the watches of that store. A status write goes
// straight to storage, not through the registry: the controller owns the
// status, and the write is a compare-and-set on the resourceVersion it
// read.
type storageClaims struct {
	devices, claims *storage.Store
}

var _ claimStore = storageClaims{}

// storageErr maps a storage error to the errors the controller expects.
func storageErr(err error) error {
	switch {
	case err == nil:
		return nil
	case apistorage.IsNotFound(err):
		return errNotFound
	case apistorage.IsInvalidObj(err), apistorage.IsConflict(err):
		// A failed resourceVersion precondition is an InvalidObj error.
		return errConflict
	}
	return err
}

func (s storageClaims) listClaims(ctx context.Context) ([]v1alpha1.DeviceClaim, error) {
	var list v1alpha1.DeviceClaimList
	opts := apistorage.ListOptions{Recursive: true, Predicate: apistorage.Everything}
	if err := s.claims.GetList(ctx, "/deviceclaims", opts, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (s storageClaims) getDevice(ctx context.Context, name string) (*v1alpha1.Device, error) {
	var d v1alpha1.Device
	if err := s.devices.Get(ctx, "/devices/"+name, apistorage.GetOptions{}, &d); err != nil {
		return nil, storageErr(err)
	}
	return &d, nil
}

func (s storageClaims) updateClaim(ctx context.Context, c *v1alpha1.DeviceClaim) error {
	return casUpdate(ctx, s.claims, "/deviceclaims/"+c.Name, c, &v1alpha1.DeviceClaim{})
}

func (s storageClaims) updateDevice(ctx context.Context, d *v1alpha1.Device) error {
	return casUpdate(ctx, s.devices, "/devices/"+d.Name, d, &v1alpha1.Device{})
}

// casUpdate writes obj at key if the stored object still has the
// resourceVersion of obj. It never creates the object.
func casUpdate(ctx context.Context, st *storage.Store, key string, obj, out runtime.Object) error {
	rv, err := st.Versioner().ObjectResourceVersion(obj)
	if err != nil {
		return err
	}
	s := strconv.FormatUint(rv, 10)
	pre := &apistorage.Preconditions{ResourceVersion: &s}
	err = st.GuaranteedUpdate(ctx, key, out, false, pre,
		func(runtime.Object, apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
			return obj, nil, nil
		}, nil)
	return storageErr(err)
}

// legacyClaims is the claimStore of the legacy mux, over fleetStore. It
// keeps the legacy stored shape: the objects carry no uid and no
// creationTimestamp.
type legacyClaims struct {
	fs *fleetStore
}

var _ claimStore = legacyClaims{}

func (l legacyClaims) listClaims(ctx context.Context) ([]v1alpha1.DeviceClaim, error) {
	_, raws, rvs, err := l.fs.list(ctx, "deviceclaims")
	if err != nil {
		return nil, err
	}
	out := make([]v1alpha1.DeviceClaim, 0, len(raws))
	for i := range raws {
		var c v1alpha1.DeviceClaim
		if err := json.Unmarshal(raws[i], &c); err != nil {
			continue
		}
		c.ResourceVersion = formatRV(rvs[i])
		out = append(out, c)
	}
	return out, nil
}

func (l legacyClaims) getDevice(ctx context.Context, name string) (*v1alpha1.Device, error) {
	raw, rv, err := l.fs.get(ctx, "devices", name)
	if err != nil {
		return nil, err
	}
	var d v1alpha1.Device
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	d.ResourceVersion = formatRV(rv)
	return &d, nil
}

func (l legacyClaims) updateClaim(ctx context.Context, c *v1alpha1.DeviceClaim) error {
	rv, err := parseRV(c.ResourceVersion)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(DeviceClaim{
		TypeMeta:   claimTypeMeta(),
		ObjectMeta: ObjectMeta{Name: c.Name, Labels: c.Labels},
		Spec:       DeviceClaimSpec{DeviceName: c.Spec.DeviceName, TTLSeconds: c.Spec.TTLSeconds},
		Status: DeviceClaimStatus{
			Phase: c.Status.Phase, Cluster: c.Status.Cluster, Fence: c.Status.Fence, Reason: c.Status.Reason,
		},
	})
	if err != nil {
		return err
	}
	_, _, err = l.fs.update(ctx, "deviceclaims", c.Name, raw, rv)
	return err
}

func (l legacyClaims) updateDevice(ctx context.Context, d *v1alpha1.Device) error {
	rv, err := parseRV(d.ResourceVersion)
	if err != nil {
		return err
	}
	dev := Device{
		TypeMeta:   deviceTypeMeta(),
		ObjectMeta: ObjectMeta{Name: d.Name, Labels: d.Labels},
		Spec:       DeviceSpec{Model: d.Spec.Model, Zone: d.Spec.Zone, Attributes: d.Spec.Attributes},
		Status:     DeviceStatus{Phase: d.Status.Phase, LastFence: d.Status.LastFence},
	}
	if l := d.Status.Lease; l != nil {
		dev.Status.Lease = &LeaseRef{Cluster: l.Cluster, Claim: l.Claim, Fence: l.Fence}
	}
	raw, err := json.Marshal(dev)
	if err != nil {
		return err
	}
	_, _, err = l.fs.update(ctx, "devices", d.Name, raw, rv)
	return err
}
