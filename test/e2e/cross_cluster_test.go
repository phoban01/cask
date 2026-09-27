//go:build e2e

package e2e

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// TestCrossClusterRead creates a Device through east and reads it through
// west. Both reads go through each cluster's own kube-apiserver and the
// aggregation layer, with the generic client-go dynamic client.
func TestCrossClusterRead(t *testing.T) {
	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# Cask MUST serve its resources through the Kubernetes aggregation layer as an APIService.

	//= docs/spec/fleet.md#2-resources
	//= type=test
	//# A client MUST be able to use kubectl, client-go informers, field selectors, and watch bookmarks against fleet resources without fleet-specific code.
	const name = "e2e-cross-cluster-read"
	spec := map[string]any{"model": "h100", "zone": "rack-12"}

	f := features.New("cross-cluster read").
		Assess("a Device created in east reads the same in west",
			func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				east, err := dynamicClient(ctx, "east")
				if err != nil {
					t.Fatal(err)
				}
				west, err := dynamicClient(ctx, "west")
				if err != nil {
					t.Fatal(err)
				}

				dev := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "fleet.cask.dev/v1alpha1",
					"kind":       "Device",
					"metadata":   map[string]any{"name": name},
					"spec":       spec,
				}}
				created, err := east.Resource(deviceGVR).Create(ctx, dev, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("create in east: %v", err)
				}
				t.Cleanup(func() {
					_ = east.Resource(deviceGVR).Delete(context.Background(), name, metav1.DeleteOptions{})
				})

				got, err := west.Resource(deviceGVR).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("get in west: %v", err)
				}
				if got.GetResourceVersion() != created.GetResourceVersion() {
					t.Errorf("resourceVersion: west %q, east %q",
						got.GetResourceVersion(), created.GetResourceVersion())
				}
				if got.GetUID() != created.GetUID() {
					t.Errorf("uid: west %q, east %q", got.GetUID(), created.GetUID())
				}
				gotSpec, _, _ := unstructured.NestedMap(got.Object, "spec")
				if !equality.Semantic.DeepEqual(gotSpec, spec) {
					t.Errorf("spec: west %v, want %v", gotSpec, spec)
				}
				return ctx
			}).
		Feature()

	testenv.Test(t, f)
}
