package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the API group of the fleet resources.
const GroupName = "fleet.cask.dev"

// Version is the only served version of the group.
const Version = "v1alpha1"

// SchemeGroupVersion is the group and version of the types in this package.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// InternalGroupVersion is the internal version that the generic server
// decodes into. v1alpha1 is the only version, so the internal version
// reuses the v1alpha1 Go types and needs no conversion functions.
var InternalGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder registers the types with a scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, addInternalTypes)
	// AddToScheme adds the external and internal versions to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Kind returns the group-qualified kind for kind.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource returns the group-qualified resource for resource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

func objects() []runtime.Object {
	return []runtime.Object{
		&Device{}, &DeviceList{},
		&DeviceClaim{}, &DeviceClaimList{},
	}
}

func addKnownTypes(scheme *runtime.Scheme) error {
	// This scheme is the one definition of the group, version, and kinds
	// that the server serves: fleet.cask.dev/v1alpha1, Device and
	// DeviceClaim. A source CRD uses the same names.
	//= docs/spec/fleet.md#7-migration
	//# The extension server MUST serve the same API group, version, and kinds that the CRD served.
	scheme.AddKnownTypes(SchemeGroupVersion, objects()...)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func addInternalTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(InternalGroupVersion, objects()...)
	return nil
}
