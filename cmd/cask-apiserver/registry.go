package main

// The registry serves Device and DeviceClaim through the generic registry
// of k8s.io/apiserver. Each resource has one registry.Store over the cask
// storage.Interface for that resource type. The registry does the
// Kubernetes parts: request decoding, strategies, validation, metadata
// such as uid and creationTimestamp, and the resourceVersion checks. The
// cask store does the storage parts: object and index registers,
// index-sequence resourceVersions, and watch.

import (
	"context"
	"errors"
	"fmt"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/caspaxos"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
)

// caskRESTOptions gives each registry the cask store of its resource. It
// replaces the etcd storage that the generic server uses by default.
type caskRESTOptions struct {
	codec  runtime.Codec
	stores map[string]*storage.Store
	// beforeClaimDelete runs inside each DeviceClaim delete, on the claim
	// that the delete read, before the delete tombstones it. An error
	// stops the delete.
	beforeClaimDelete func(context.Context, *v1alpha1.DeviceClaim) error
}

var _ generic.RESTOptionsGetter = caskRESTOptions{}

// GetRESTOptions returns options whose decorator hands back the cask
// store of resource. The store is not wrapped in a watch cache: every
// read goes to the registers.
func (g caskRESTOptions) GetRESTOptions(resource schema.GroupResource, _ runtime.Object) (generic.RESTOptions, error) {
	st, ok := g.stores[resource.Resource]
	if !ok {
		return generic.RESTOptions{}, fmt.Errorf("cask-apiserver: no cask store for %s", resource)
	}
	decorator := func(*storagebackend.ConfigForResource, string, func(runtime.Object) (string, error),
		func() runtime.Object, func() runtime.Object, apistorage.AttrFunc, apistorage.IndexerFuncs,
		*cache.Indexers) (apistorage.Interface, factory.DestroyFunc, error) {
		if resource.Resource == "deviceclaims" && g.beforeClaimDelete != nil {
			return claimDeletes{staleWrites{st}, g.beforeClaimDelete}, func() {}, nil
		}
		return staleWrites{st}, func() {}, nil
	}
	return generic.RESTOptions{
		StorageConfig: &storagebackend.ConfigForResource{
			Config:        storagebackend.Config{Codec: g.codec, EncodeVersioner: v1alpha1.SchemeGroupVersion},
			GroupResource: resource,
		},
		Decorator:               decorator,
		ResourcePrefix:          "/" + resource.Resource,
		DeleteCollectionWorkers: 1,
	}, nil
}

// statusREST serves the status subresource of a resource. It shares the
// store of the main resource, with a strategy that keeps the spec.
type statusREST struct {
	store *genericregistry.Store
}

var (
	_ rest.Getter  = (*statusREST)(nil)
	_ rest.Updater = (*statusREST)(nil)
)

// New returns an empty object of the resource.
func (r *statusREST) New() runtime.Object { return r.store.New() }

// Destroy does nothing: the main resource owns the store.
func (r *statusREST) Destroy() {}

// Get returns the object with its status.
func (r *statusREST) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, opts)
}

// Update writes the status of the object. A status update never creates
// an object.
func (r *statusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, _ bool,
	opts *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, opts)
}

// fleetStrategy holds what the Device and DeviceClaim strategies share.
type fleetStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NamespaceScoped reports false: every fleet resource is cluster-scoped.
func (fleetStrategy) NamespaceScoped() bool {
	//= docs/spec/fleet.md#2-resources
	//# Every fleet resource MUST be cluster-scoped.
	return false
}

// AllowCreateOnUpdate reports false: an update of a missing object fails.
func (fleetStrategy) AllowCreateOnUpdate() bool { return false }

// AllowUnconditionalUpdate reports true, as for a CRD: an update without
// a resourceVersion replaces the object. An update with one must match.
func (fleetStrategy) AllowUnconditionalUpdate() bool { return true }

// Canonicalize does nothing.
func (fleetStrategy) Canonicalize(runtime.Object) {}

// WarningsOnCreate returns no warnings.
func (fleetStrategy) WarningsOnCreate(context.Context, runtime.Object) []string { return nil }

// WarningsOnUpdate returns no warnings.
func (fleetStrategy) WarningsOnUpdate(context.Context, runtime.Object, runtime.Object) []string {
	return nil
}

// deviceStrategy creates and updates Devices.
type deviceStrategy struct{ fleetStrategy }

// PrepareForCreate starts a Device as Available. The claim controller
// owns the status.
func (deviceStrategy) PrepareForCreate(_ context.Context, obj runtime.Object) {
	obj.(*v1alpha1.Device).Status = v1alpha1.DeviceStatus{Phase: v1alpha1.DeviceAvailable}
}

// PrepareForUpdate keeps the status: the status subresource writes it.
// So a client update never lowers lastFence or the lease fence.
func (deviceStrategy) PrepareForUpdate(_ context.Context, obj, old runtime.Object) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A write to a Device MUST NOT lower its lastFence.
	obj.(*v1alpha1.Device).Status = old.(*v1alpha1.Device).Status
}

// Validate accepts every Device. The generic registry checks the name.
func (deviceStrategy) Validate(context.Context, runtime.Object) field.ErrorList { return nil }

// ValidateUpdate accepts every Device update.
func (deviceStrategy) ValidateUpdate(context.Context, runtime.Object, runtime.Object) field.ErrorList {
	return nil
}

// deviceStatusStrategy updates the status of a Device and keeps its spec.
type deviceStatusStrategy struct{ deviceStrategy }

// PrepareForUpdate keeps the spec, and keeps lastFence at least at the
// highest fence that the stored status shows.
func (deviceStatusStrategy) PrepareForUpdate(_ context.Context, obj, old runtime.Object) {
	d, o := obj.(*v1alpha1.Device), old.(*v1alpha1.Device)
	d.Spec = o.Spec
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A write to a Device MUST NOT lower its lastFence.
	d.Status.LastFence = max(d.Status.LastFence, keptFence(o.Status))
}

// ValidateUpdate refuses a client status write that lowers the advertised
// lease fence or drops the lease. The claim controller writes the lease
// straight to storage, so its release path does not come here.
func (deviceStatusStrategy) ValidateUpdate(_ context.Context, obj, old runtime.Object) field.ErrorList {
	d, o := obj.(*v1alpha1.Device), old.(*v1alpha1.Device)
	if o.Status.Lease == nil {
		if d.Status.Lease != nil && d.Status.Lease.Fence < keptFence(o.Status) {
			return field.ErrorList{field.Invalid(field.NewPath("status", "lease", "fence"), d.Status.Lease.Fence,
				fmt.Sprintf("below the device's lastFence %d", keptFence(o.Status)))}
		}
		return nil
	}
	//= docs/spec/fleet.md#5-claims-and-fencing
	//# A status write MUST NOT lower an advertised fence.
	if d.Status.Lease == nil {
		return field.ErrorList{field.Forbidden(field.NewPath("status", "lease"),
			"only the claim controller releases a lease")}
	}
	if d.Status.Lease.Fence < keptFence(o.Status) {
		return field.ErrorList{field.Invalid(field.NewPath("status", "lease", "fence"), d.Status.Lease.Fence,
			fmt.Sprintf("below the advertised fence %d", keptFence(o.Status)))}
	}
	return nil
}

// claimStrategy creates and updates DeviceClaims. cluster names the
// cluster whose extension server admits a new claim.
type claimStrategy struct {
	fleetStrategy
	cluster string
}

// PrepareForCreate starts a claim as Pending and stamps this cluster on
// it. The claim controller of this cluster manages it from then on.
func (s claimStrategy) PrepareForCreate(_ context.Context, obj runtime.Object) {
	obj.(*v1alpha1.DeviceClaim).Status = v1alpha1.DeviceClaimStatus{Phase: v1alpha1.ClaimPending, Cluster: s.cluster}
}

// PrepareForUpdate keeps the status: the status subresource writes it.
func (claimStrategy) PrepareForUpdate(_ context.Context, obj, old runtime.Object) {
	obj.(*v1alpha1.DeviceClaim).Status = old.(*v1alpha1.DeviceClaim).Status
}

// Validate requires spec.deviceName.
func (claimStrategy) Validate(_ context.Context, obj runtime.Object) field.ErrorList {
	c := obj.(*v1alpha1.DeviceClaim)
	if c.Spec.DeviceName == "" {
		return field.ErrorList{field.Required(field.NewPath("spec", "deviceName"), "name the device to lease")}
	}
	return nil
}

// ValidateUpdate keeps spec.deviceName fixed. A Bound claim holds the lock
// of its device, and a delete releases the lock of the device it names.
func (s claimStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	errs := s.Validate(ctx, obj)
	c, o := obj.(*v1alpha1.DeviceClaim), old.(*v1alpha1.DeviceClaim)
	if c.Spec.DeviceName != o.Spec.DeviceName {
		errs = append(errs, field.Invalid(field.NewPath("spec", "deviceName"), c.Spec.DeviceName, "the field is immutable"))
	}
	return errs
}

// claimStatusStrategy updates the status of a DeviceClaim and keeps its
// spec.
type claimStatusStrategy struct{ claimStrategy }

// PrepareForUpdate keeps the spec.
func (claimStatusStrategy) PrepareForUpdate(_ context.Context, obj, old runtime.Object) {
	obj.(*v1alpha1.DeviceClaim).Spec = old.(*v1alpha1.DeviceClaim).Spec
}

// ValidateUpdate accepts the status: the spec does not change.
func (claimStatusStrategy) ValidateUpdate(context.Context, runtime.Object, runtime.Object) field.ErrorList {
	return nil
}

// newFleetREST returns the REST storage of the group, keyed by resource
// path as the generic server installs it.
func newFleetREST(typer runtime.ObjectTyper, getter generic.RESTOptionsGetter, cluster string) (map[string]rest.Storage, error) {
	// The registry filters a list and a watch on metadata.name, and the
	// cask watch sends bookmarks. No test covers informers or bookmarks
	// through the server yet.
	//= docs/spec/fleet.md#2-resources
	//= type=exception
	//= reason=no test of informers and watch bookmarks through the generic server; tracked in issue #91
	//# A client MUST be able to use kubectl, client-go informers, field selectors, and watch bookmarks against fleet resources without fleet-specific code.
	base := fleetStrategy{ObjectTyper: typer, NameGenerator: names.SimpleNameGenerator}

	dev := deviceStrategy{base}
	devices := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &v1alpha1.Device{} },
		NewListFunc:               func() runtime.Object { return &v1alpha1.DeviceList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("devices"),
		SingularQualifiedResource: v1alpha1.Resource("device"),
		CreateStrategy:            dev,
		UpdateStrategy:            dev,
		DeleteStrategy:            dev,
		TableConvertor:            rest.NewDefaultTableConvertor(v1alpha1.Resource("devices")),
	}
	if err := devices.CompleteWithOptions(&generic.StoreOptions{RESTOptions: getter}); err != nil {
		return nil, fmt.Errorf("devices registry: %w", err)
	}
	deviceStatus := *devices
	deviceStatus.UpdateStrategy = deviceStatusStrategy{dev}

	claim := claimStrategy{fleetStrategy: base, cluster: cluster}
	claims := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &v1alpha1.DeviceClaim{} },
		NewListFunc:               func() runtime.Object { return &v1alpha1.DeviceClaimList{} },
		DefaultQualifiedResource:  v1alpha1.Resource("deviceclaims"),
		SingularQualifiedResource: v1alpha1.Resource("deviceclaim"),
		CreateStrategy:            claim,
		UpdateStrategy:            claim,
		DeleteStrategy:            claim,
		TableConvertor:            rest.NewDefaultTableConvertor(v1alpha1.Resource("deviceclaims")),
	}
	if err := claims.CompleteWithOptions(&generic.StoreOptions{RESTOptions: getter}); err != nil {
		return nil, fmt.Errorf("deviceclaims registry: %w", err)
	}
	claimStatus := *claims
	claimStatus.UpdateStrategy = claimStatusStrategy{claim}

	return map[string]rest.Storage{
		"devices":             devices,
		"devices/status":      &statusREST{store: &deviceStatus},
		"deviceclaims":        claims,
		"deviceclaims/status": &statusREST{store: &claimStatus},
	}, nil
}

// staleWrites is the cask store as the registry sees it. A write that a
// voter fenced because the core changed under it did not apply. The
// client gets 503 with Retry-After for it, not 500.
type staleWrites struct {
	*storage.Store
}

// Create creates the object. See storage.Store.Create.
func (s staleWrites) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	return retryable(s.Store.Create(ctx, key, obj, out, ttl))
}

// Delete deletes the object. See storage.Store.Delete.
func (s staleWrites) Delete(ctx context.Context, key string, out runtime.Object, pre *apistorage.Preconditions,
	validate apistorage.ValidateObjectFunc, cached runtime.Object, opts apistorage.DeleteOptions) error {
	return retryable(s.Store.Delete(ctx, key, out, pre, validate, cached, opts))
}

// GuaranteedUpdate updates the object. See storage.Store.GuaranteedUpdate.
func (s staleWrites) GuaranteedUpdate(ctx context.Context, key string, dest runtime.Object, ignoreNotFound bool,
	pre *apistorage.Preconditions, tryUpdate apistorage.UpdateFunc, cached runtime.Object) error {
	return retryable(s.Store.GuaranteedUpdate(ctx, key, dest, ignoreNotFound, pre, tryUpdate, cached))
}

// claimDeletes is the DeviceClaim store as the registry sees it. A delete
// runs before on the claim it read, after the preconditions pass and
// before it tombstones the claim. A dry run never reaches it: the
// registry's dry-run storage answers without calling Delete.
type claimDeletes struct {
	staleWrites
	before func(context.Context, *v1alpha1.DeviceClaim) error
}

// Delete deletes the claim once before returns nil for it. When the
// claim moves under the delete, the store reads it again and runs before
// on the new copy.
func (s claimDeletes) Delete(ctx context.Context, key string, out runtime.Object, pre *apistorage.Preconditions,
	validate apistorage.ValidateObjectFunc, cached runtime.Object, opts apistorage.DeleteOptions) error {
	check := func(ctx context.Context, obj runtime.Object) error {
		if validate != nil {
			if err := validate(ctx, obj); err != nil {
				return err
			}
		}
		c, ok := obj.(*v1alpha1.DeviceClaim)
		if !ok {
			return nil
		}
		// The device keeps the claim's fence before the claim goes. If it
		// does not, the delete fails and the client retries.
		//= docs/spec/fleet.md#5-claims-and-fencing
		//# Deleting a claim MUST NOT complete before the lastFence of its Device is at least the claim's fence.
		err := s.before(ctx, c)
		if errors.Is(err, errConflict) {
			return apierrors.NewConflict(v1alpha1.Resource("deviceclaims"), c.Name, err)
		}
		return err
	}
	return s.staleWrites.Delete(ctx, key, out, pre, check, cached, opts)
}

// retryable turns a write that a voter fenced into 503 Service Unavailable
// with a Retry-After of one second. The client retries after the member's
// view of the core catches up.
func retryable(err error) error {
	if !errors.Is(err, caspaxos.ErrRangeChanged) {
		return err
	}
	//= docs/spec/fleet.md#6-membership
	//# The extension server MUST answer a data write that a voter rejected as stale with a retryable status.
	status := apierrors.NewServiceUnavailable(err.Error())
	status.ErrStatus.Details = &metav1.StatusDetails{RetryAfterSeconds: 1}
	return status
}
