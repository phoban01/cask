//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// zombieTTL is the claim session TTL in seconds. It is short, so the
// session of the stopped holder lapses soon.
const zombieTTL = 10

// deviceLease is the lease that a Device advertises in its status.
type deviceLease struct {
	cluster, claim string
	fence          int64
}

// readLease returns the advertised lease of a Device. found is false when
// the device advertises no lease.
func readLease(u *unstructured.Unstructured) (l deviceLease, found bool) {
	m, found, _ := unstructured.NestedMap(u.Object, "status", "lease")
	if !found {
		return deviceLease{}, false
	}
	l.cluster, _ = m["cluster"].(string)
	l.claim, _ = m["claim"].(string)
	l.fence, _, _ = unstructured.NestedInt64(m, "fence")
	return l, true
}

// scaleApiserver sets the replica count of the cask-apiserver StatefulSet
// in one cluster.
func scaleApiserver(ctx context.Context, r *resources.Resources, replicas int32) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, "cask-apiserver", "cask-system", sts); err != nil {
		return err
	}
	sts.Spec.Replicas = &replicas
	return r.Update(ctx, sts)
}

// apiserverGone reports whether no cask-apiserver pod is left in a cluster.
func apiserverGone(ctx context.Context, r *resources.Resources) (bool, error) {
	var pods corev1.PodList
	err := r.WithNamespace("cask-system").List(ctx, &pods,
		resources.WithLabelSelector("app=cask-apiserver"))
	if err != nil {
		return false, nil
	}
	return len(pods.Items) == 0, nil
}

// createClaim creates a DeviceClaim with the zombie TTL through one client.
func createClaim(ctx context.Context, c dynamic.Interface, name, device string) error {
	claim := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.cask.dev/v1alpha1",
		"kind":       "DeviceClaim",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"deviceName": device, "ttlSeconds": int64(zombieTTL)},
	}}
	if _, err := c.Resource(deviceClaimGVR).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create claim %s: %w", name, err)
	}
	return nil
}

// boundAt returns the fence of a claim when the claim is Bound in the given
// cluster with a positive fence and the device advertises that lease.
func boundAt(ctx context.Context, c dynamic.Interface, claim, cluster, device string) (int64, error) {
	u, err := c.Resource(deviceClaimGVR).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	s := readClaim(u)
	if s.phase != "Bound" || s.cluster != cluster || s.fence <= 0 {
		return 0, fmt.Errorf("claim %s: %+v, want Bound in %s with a fence", claim, s, cluster)
	}
	d, err := c.Resource(deviceGVR).Get(ctx, device, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	l, found := readLease(d)
	want := deviceLease{cluster: cluster, claim: claim, fence: s.fence}
	if !found || l != want {
		return 0, fmt.Errorf("device lease %+v (found %t), want %+v", l, found, want)
	}
	return s.fence, nil
}

// logApiserversOnFailure logs the tail of every cask-apiserver log when
// the test fails. Finish destroys the clusters at the end of the run, so
// the logs are gone after that.
func logApiserversOnFailure(ctx context.Context, t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx := context.WithoutCancel(ctx)
		tail := int64(200)
		for _, logical := range logicalClusters {
			rc, err := restConfig(ctx, logical)
			if err != nil {
				t.Logf("%s logs: %v", logical, err)
				continue
			}
			cs, err := kubernetes.NewForConfig(rc)
			if err != nil {
				t.Logf("%s logs: %v", logical, err)
				continue
			}
			raw, err := cs.CoreV1().Pods("cask-system").
				GetLogs("cask-apiserver-0", &corev1.PodLogOptions{TailLines: &tail}).
				DoRaw(ctx)
			if err != nil {
				t.Logf("%s logs: %v", logical, err)
				continue
			}
			t.Logf("%s cask-apiserver logs:\n%s", logical, raw)
		}
	})
}

// TestZombieFenceRejection follows the zombie act of demo/kind/demo.sh.
// East binds a claim at fence f. East's apiserver stops, so nothing renews
// the claim's session. The session lapses, and a claim from west binds at
// a fence above f. East comes back as a zombie. Its claim becomes Lost,
// and the device never advertises east's lower fence again.
func TestZombieFenceRejection(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Every successful acquisition MUST mint a fence strictly greater than every fence previously minted for that object.

	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# When a claim's session lapses, the controller MUST set the claim to Lost.

	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A status write MUST NOT lower an advertised fence.

	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A receiver MUST reject an effect whose fence is lower than the highest fence it has accepted for that object.
	const device = "e2e-zombie-fence"
	const zombieClaim = device + "-east"
	const successorClaim = device + "-west"

	f := features.New("zombie fence rejection").
		Assess("a stopped holder loses its claim and cannot lower the fence",
			func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				logApiserversOnFailure(ctx, t)
				east, err := dynamicClient(ctx, "east")
				if err != nil {
					t.Fatal(err)
				}
				west, err := dynamicClient(ctx, "west")
				if err != nil {
					t.Fatal(err)
				}
				rc, err := restConfig(ctx, "east")
				if err != nil {
					t.Fatal(err)
				}
				eastRes, err := resources.New(rc)
				if err != nil {
					t.Fatal(err)
				}

				dev := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "fleet.cask.dev/v1alpha1",
					"kind":       "Device",
					"metadata":   map[string]any{"name": device},
					"spec":       map[string]any{"model": "h100", "zone": "rack-9"},
				}}
				if _, err := east.Resource(deviceGVR).Create(ctx, dev, metav1.CreateOptions{}); err != nil {
					t.Fatalf("create device: %v", err)
				}
				// Cleanups run last in, first out, so the device goes last.
				// Each claim is deleted through the cluster that admitted
				// it, because only that cluster releases its lock.
				t.Cleanup(func() {
					_ = west.Resource(deviceGVR).Delete(context.Background(), device, metav1.DeleteOptions{})
				})

				// Step 1: east binds a claim at fence f.
				if err := createClaim(ctx, east, zombieClaim, device); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = east.Resource(deviceClaimGVR).Delete(context.Background(), zombieClaim, metav1.DeleteOptions{})
				})
				var zf int64
				var lastErr error
				err = poll(ctx, func(ctx context.Context) (bool, error) {
					zf, lastErr = boundAt(ctx, east, zombieClaim, "east", device)
					return lastErr == nil, nil
				})
				if err != nil {
					t.Fatalf("east claim did not bind: %v (last: %v)", err, lastErr)
				}
				t.Logf("east bound %s at fence %d", zombieClaim, zf)

				// Step 2: stop east's apiserver. Nothing renews the east
				// session now. West and north keep a quorum. This cleanup
				// runs before the east claim and device cleanups, so east
				// serves again for them and for the next test.
				if err := scaleApiserver(ctx, eastRes, 0); err != nil {
					t.Fatalf("scale east to 0: %v", err)
				}
				t.Cleanup(func() {
					// waitReady finds the cluster in the context, so keep
					// the values of ctx but not its cancellation.
					ctx := context.WithoutCancel(ctx)
					if err := scaleApiserver(ctx, eastRes, 1); err != nil {
						t.Errorf("cleanup: scale east to 1: %v", err)
						return
					}
					if err := waitReady(ctx, "east", eastRes); err != nil {
						t.Errorf("cleanup: east not ready: %v", err)
					}
				})
				if err := poll(ctx, func(ctx context.Context) (bool, error) {
					return apiserverGone(ctx, eastRes)
				}); err != nil {
					t.Fatalf("east apiserver did not stop: %v", err)
				}
				t.Log("east apiserver stopped")

				// Step 3: west claims the device. The claim binds only after
				// the east session lapses, and at a fence above f.
				if err := createClaim(ctx, west, successorClaim, device); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = west.Resource(deviceClaimGVR).Delete(context.Background(), successorClaim, metav1.DeleteOptions{})
				})
				var wf int64
				err = poll(ctx, func(ctx context.Context) (bool, error) {
					wf, lastErr = boundAt(ctx, west, successorClaim, "west", device)
					return lastErr == nil, nil
				})
				if err != nil {
					t.Fatalf("west claim did not bind: %v (last: %v)", err, lastErr)
				}
				if wf <= zf {
					t.Fatalf("west fence %d, want > east fence %d", wf, zf)
				}
				t.Logf("west bound %s at fence %d", successorClaim, wf)

				// From here on, every read of the device must show west's
				// lease at fence wf.
				holds := func(ctx context.Context) error {
					d, err := west.Resource(deviceGVR).Get(ctx, device, metav1.GetOptions{})
					if err != nil {
						return err
					}
					l, found := readLease(d)
					want := deviceLease{cluster: "west", claim: successorClaim, fence: wf}
					if !found || l != want {
						return fmt.Errorf("device lease %+v (found %t), want %+v", l, found, want)
					}
					return nil
				}

				// Step 4: restart east. It comes back as a zombie whose
				// stored claim still says Bound at fence f.
				if err := scaleApiserver(ctx, eastRes, 1); err != nil {
					t.Fatalf("scale east to 1: %v", err)
				}
				restarted := make(chan error, 1)
				go func() { restarted <- waitReady(ctx, "east", eastRes) }()
			restart:
				for {
					select {
					case err := <-restarted:
						if err != nil {
							t.Fatalf("east did not come back: %v", err)
						}
						break restart
					case <-time.After(500 * time.Millisecond):
						if err := holds(ctx); err != nil {
							t.Fatalf("while east restarted: %v", err)
						}
					}
				}
				t.Log("east apiserver serves again")

				// Step 5: east's claim becomes Lost and keeps its old fence.
				err = poll(ctx, func(ctx context.Context) (bool, error) {
					if err := holds(ctx); err != nil {
						return false, err
					}
					u, err := east.Resource(deviceClaimGVR).Get(ctx, zombieClaim, metav1.GetOptions{})
					if err != nil {
						lastErr = err
						return false, nil
					}
					s := readClaim(u)
					if s.phase != "Lost" || s.cluster != "east" || s.fence != zf {
						lastErr = fmt.Errorf("east claim %+v, want Lost in east at fence %d", s, zf)
						return false, nil
					}
					return true, nil
				})
				if err != nil {
					t.Fatalf("east claim did not become Lost: %v (last: %v)", err, lastErr)
				}
				t.Logf("east claim %s is Lost at fence %d", zombieClaim, zf)

				// The result holds for several reconcile ticks on both
				// sides. West stays Bound and the device keeps fence wf.
				deadline := time.Now().Add(holdFor)
				for time.Now().Before(deadline) {
					if err := holds(ctx); err != nil {
						t.Fatalf("after east came back: %v", err)
					}
					if _, err := boundAt(ctx, east, successorClaim, "west", device); err != nil {
						t.Fatalf("after east came back: %v", err)
					}
					u, err := east.Resource(deviceClaimGVR).Get(ctx, zombieClaim, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if s := readClaim(u); s.phase != "Lost" {
						t.Fatalf("east claim left Lost: %+v", s)
					}
					time.Sleep(time.Second)
				}
				return ctx
			}).
		Feature()

	testenv.Test(t, f)
}
