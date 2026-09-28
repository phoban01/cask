package main

import (
	"context"
	"testing"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// A claim delete runs the release first. When the release fails, the
// claim stays, so its fence is never lost with it.
func TestClaimDeleteStopsWhenReleaseFails(t *testing.T) {
	//= docs/spec/fleet.md#5-claims-and-fencing
	//= type=test
	//# Deleting a claim MUST NOT complete before the lastFence of its Device is at least the claim's fence.
	acc := caspaxos.NewAcceptor(store.NewMem())
	prop := caspaxos.NewProposer(1, []caspaxos.AcceptorClient{acc})
	kv := mvcc.New(prop, hlc.New(func() int64 { return time.Now().UnixNano() }), 1)
	st := fleetStores(kv)["deviceclaims"]
	ctx := context.Background()
	c := &v1alpha1.DeviceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "job"},
		Spec:       v1alpha1.DeviceClaimSpec{DeviceName: "gpu"},
		Status:     v1alpha1.DeviceClaimStatus{Phase: v1alpha1.ClaimBound, Cluster: "east", Fence: 3},
	}
	if err := st.Create(ctx, "/deviceclaims/job", c, nil, 0); err != nil {
		t.Fatal(err)
	}

	var seen uint64
	failing := claimDeletes{staleWrites{st}, func(_ context.Context, c *v1alpha1.DeviceClaim) error {
		seen = c.Status.Fence
		return errConflict
	}}
	err := failing.Delete(ctx, "/deviceclaims/job", &v1alpha1.DeviceClaim{}, nil, nil, nil, apistorage.DeleteOptions{})
	if !apierrors.IsConflict(err) {
		t.Fatalf("delete with a failed release: err = %v, want 409 Conflict", err)
	}
	if seen != 3 {
		t.Fatalf("release saw fence %d, want 3", seen)
	}
	if err := st.Get(ctx, "/deviceclaims/job", apistorage.GetOptions{}, &v1alpha1.DeviceClaim{}); err != nil {
		t.Fatalf("claim is gone after a failed release: %v", err)
	}

	ok := claimDeletes{staleWrites{st}, func(context.Context, *v1alpha1.DeviceClaim) error { return nil }}
	if err := ok.Delete(ctx, "/deviceclaims/job", &v1alpha1.DeviceClaim{}, nil, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	err = st.Get(ctx, "/deviceclaims/job", apistorage.GetOptions{}, &v1alpha1.DeviceClaim{})
	if !apistorage.IsNotFound(err) {
		t.Fatalf("get after delete: err = %v, want not found", err)
	}
}
