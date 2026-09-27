//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// deviceClaimGVR is the fleet DeviceClaim resource.
var deviceClaimGVR = schema.GroupVersionResource{
	Group: "fleet.cask.dev", Version: "v1alpha1", Resource: "deviceclaims",
}

// leasedElsewhere is the reason a Pending claim shows when another claim
// holds the device's lock.
const leasedElsewhere = "device leased elsewhere"

// holdFor is how long the test watches the settled result for a change.
const holdFor = 10 * time.Second

// claimState is the part of a DeviceClaim status that the race test reads.
type claimState struct {
	phase, cluster, reason string
	fence                  int64
}

func readClaim(u *unstructured.Unstructured) claimState {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	cluster, _, _ := unstructured.NestedString(u.Object, "status", "cluster")
	reason, _, _ := unstructured.NestedString(u.Object, "status", "reason")
	fence, _, _ := unstructured.NestedInt64(u.Object, "status", "fence")
	return claimState{phase: phase, cluster: cluster, reason: reason, fence: fence}
}

// TestClaimRace creates a DeviceClaim for one Device through east and
// another through west at the same time. Exactly one claim binds. The
// other stays Pending because the device is leased elsewhere.
func TestClaimRace(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# At most one claim MUST be Bound to an object at the object's current fence.

	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# A Bound claim MUST carry its fence in its status.
	const device = "e2e-claim-race"
	racers := []string{"east", "west"}
	claimName := func(logical string) string { return device + "-" + logical }

	f := features.New("claim race").
		Assess("two claims race for one device and exactly one binds",
			func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				clients := make(map[string]dynamic.Interface, len(racers))
				for _, logical := range racers {
					c, err := dynamicClient(ctx, logical)
					if err != nil {
						t.Fatal(err)
					}
					clients[logical] = c
				}
				east := clients["east"]

				dev := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "fleet.cask.dev/v1alpha1",
					"kind":       "Device",
					"metadata":   map[string]any{"name": device},
					"spec":       map[string]any{"model": "h100", "zone": "rack-7"},
				}}
				if _, err := east.Resource(deviceGVR).Create(ctx, dev, metav1.CreateOptions{}); err != nil {
					t.Fatalf("create device: %v", err)
				}
				// Cleanups run last in, first out, so the claims go before
				// the device. Each claim is deleted through the cluster that
				// admitted it, because only that cluster releases its lock.
				t.Cleanup(func() {
					_ = east.Resource(deviceGVR).Delete(context.Background(), device, metav1.DeleteOptions{})
				})

				// Release both creates at once.
				start := make(chan struct{})
				errs := make(chan error, len(racers))
				var wg sync.WaitGroup
				for _, logical := range racers {
					claim := &unstructured.Unstructured{Object: map[string]any{
						"apiVersion": "fleet.cask.dev/v1alpha1",
						"kind":       "DeviceClaim",
						"metadata":   map[string]any{"name": claimName(logical)},
						"spec":       map[string]any{"deviceName": device},
					}}
					c := clients[logical]
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						if _, err := c.Resource(deviceClaimGVR).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
							errs <- fmt.Errorf("create claim %s: %w", claim.GetName(), err)
						}
					}()
				}
				close(start)
				wg.Wait()
				close(errs)
				for _, logical := range racers {
					name, c := claimName(logical), clients[logical]
					t.Cleanup(func() {
						_ = c.Resource(deviceClaimGVR).Delete(context.Background(), name, metav1.DeleteOptions{})
					})
				}
				for err := range errs {
					t.Fatal(err)
				}

				// settled reads both claims and the device. It returns the
				// winner when one claim is Bound with a positive fence, the
				// other is Pending with the leased-elsewhere reason, and the
				// device advertises the winner's lease.
				settled := func(ctx context.Context) (string, error) {
					states := make(map[string]claimState, len(racers))
					for _, logical := range racers {
						u, err := east.Resource(deviceClaimGVR).Get(ctx, claimName(logical), metav1.GetOptions{})
						if err != nil {
							return "", err
						}
						states[logical] = readClaim(u)
					}
					var bound []string
					for _, logical := range racers {
						s := states[logical]
						if s.cluster != logical {
							return "", fmt.Errorf("claim %s: status cluster %q, want %q",
								claimName(logical), s.cluster, logical)
						}
						if s.phase == "Bound" {
							bound = append(bound, logical)
						}
					}
					switch len(bound) {
					case 0:
						return "", fmt.Errorf("no claim is Bound yet: %+v", states)
					case 1:
					default:
						return "", fmt.Errorf("more than one claim is Bound: %+v", states)
					}
					winner := bound[0]
					for _, logical := range racers {
						if logical == winner {
							continue
						}
						if s := states[logical]; s.phase != "Pending" || s.reason != leasedElsewhere {
							return "", fmt.Errorf("loser %s: phase %q reason %q, want Pending %q",
								logical, s.phase, s.reason, leasedElsewhere)
						}
					}
					w := states[winner]
					if w.fence <= 0 {
						return "", fmt.Errorf("winner %s: fence %d, want > 0", winner, w.fence)
					}

					d, err := east.Resource(deviceGVR).Get(ctx, device, metav1.GetOptions{})
					if err != nil {
						return "", err
					}
					lease, found, _ := unstructured.NestedMap(d.Object, "status", "lease")
					if !found {
						return "", fmt.Errorf("device advertises no lease yet")
					}
					fence, _, _ := unstructured.NestedInt64(lease, "fence")
					if lease["cluster"] != winner || lease["claim"] != claimName(winner) || fence != w.fence {
						return "", fmt.Errorf("device lease %v, want cluster %q claim %q fence %d",
							lease, winner, claimName(winner), w.fence)
					}
					return winner, nil
				}

				var winner string
				var lastErr error
				err := poll(ctx, func(ctx context.Context) (bool, error) {
					winner, lastErr = settled(ctx)
					return lastErr == nil, nil
				})
				if err != nil {
					t.Fatalf("race did not settle: %v (last: %v)", err, lastErr)
				}
				t.Logf("winner: %s", winner)

				// The loser stays Pending while the winner holds the lease.
				// Several reconcile ticks pass in this window.
				deadline := time.Now().Add(holdFor)
				for time.Now().Before(deadline) {
					got, err := settled(ctx)
					if err != nil {
						t.Fatalf("result changed after it settled: %v", err)
					}
					if got != winner {
						t.Fatalf("winner changed from %s to %s", winner, got)
					}
					time.Sleep(time.Second)
				}
				return ctx
			}).
		Feature()

	testenv.Test(t, f)
}
