package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestKindsAreClusterScoped(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# Every fleet resource MUST be cluster-scoped.
	for _, obj := range []interface {
		runtime.Object
		NamespaceScoped() bool
	}{&Device{}, &DeviceClaim{}} {
		if obj.NamespaceScoped() {
			t.Errorf("%T is namespace-scoped", obj)
		}
	}
}
