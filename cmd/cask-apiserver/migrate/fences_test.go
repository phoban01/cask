package migrate

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/mvcc"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fakeLocks records the seeds. With kv set, it also records whether any
// object register existed when each seed ran.
type fakeLocks struct {
	kv         *mvcc.KV
	seeds      []LockSeed
	objectSeen bool
	err        error

	renewed             []string
	renewedBeforeMarker bool
	renewErr            error
}

func (f *fakeLocks) Seed(ctx context.Context, s LockSeed) error {
	if f.kv != nil {
		for _, k := range [][]byte{storage.ObjectKey("devices", s.Device), storage.ObjectKey("deviceclaims", s.Claim)} {
			if _, found, err := f.kv.Get(ctx, k); err != nil {
				return err
			} else if found {
				f.objectSeen = true
			}
		}
	}
	f.seeds = append(f.seeds, s)
	return f.err
}

// Renew records the renewal. With kv set, it also records whether the
// marker existed at that time.
func (f *fakeLocks) Renew(ctx context.Context, s LockSeed) error {
	if f.kv != nil {
		if _, found, err := f.kv.Get(ctx, MarkerKey); err != nil {
			return err
		} else if !found {
			f.renewedBeforeMarker = true
		}
	}
	f.renewed = append(f.renewed, s.Claim)
	return f.renewErr
}

func TestImportSeedsLocksBeforeObjects(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST NOT seed an object's lock at a fence below the highest fence that the export recorded for that object.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST grant a restored claim's session and seed its lock before it writes the claim.
	kv := newKV(t)
	h, objects, err := Read(bytes.NewReader(exportFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	locks := &fakeLocks{kv: kv}
	if _, err := Import(context.Background(), kv, locks, h, objects); err != nil {
		t.Fatal(err)
	}
	const big = 9007199254740993
	want := []LockSeed{
		{Device: "dev-a", Fence: 7, Claim: "claim-a", Cluster: "east", TTLSeconds: 30},
		{Device: "dev-b", Fence: big},
		{Device: "dev-c", Fence: big},
	}
	if !reflect.DeepEqual(locks.seeds, want) {
		t.Fatalf("seeds = %+v\nwant %+v", locks.seeds, want)
	}
	if locks.objectSeen {
		t.Fatal("an object register existed before its lock was seeded")
	}
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST renew each restored claim's session after it writes the import marker.
	if !reflect.DeepEqual(locks.renewed, []string{"claim-a"}) || locks.renewedBeforeMarker {
		t.Fatalf("renewed %v, before the marker = %v; want claim-a after the marker",
			locks.renewed, locks.renewedBeforeMarker)
	}

	// A complete import handed the locks over. A second run seeds none.
	again := &fakeLocks{}
	if _, err := Import(context.Background(), kv, again, h, objects); err != nil {
		t.Fatal(err)
	}
	if len(again.seeds) != 0 {
		t.Fatalf("second import seeded %+v", again.seeds)
	}
}

// A session that lapsed during the import is counted, not fatal: the
// import is complete, and the claim's controller sets it to Lost.
func TestImportCountsLapsedSessions(t *testing.T) {
	kv := newKV(t)
	h, objects, err := Read(bytes.NewReader(exportFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	res, err := Import(context.Background(), kv, &fakeLocks{renewErr: errors.New("lapsed")}, h, objects)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Written: 4, Lapsed: 1}) {
		t.Fatalf("result = %+v, want 4 written and 1 lapsed", res)
	}
}

func TestImportWritesNothingWhenASeedFails(t *testing.T) {
	kv := newKV(t)
	h, objects, err := Read(bytes.NewReader(exportFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, kv, objects)
	boom := errors.New("seed conflict")
	if _, err := Import(context.Background(), kv, &fakeLocks{err: boom}, h, objects); !errors.Is(err, boom) {
		t.Fatalf("import err = %v, want the seed error", err)
	}
	if after := snapshot(t, kv, objects); !reflect.DeepEqual(before, after) {
		t.Fatal("an import with a failed seed wrote an object")
	}
}

func boundClaim(name, device, cluster string, fence int64) *unstructured.Unstructured {
	c := claim(name, "uid-"+name)
	_ = unstructured.SetNestedField(c.Object, device, "spec", "deviceName")
	_ = unstructured.SetNestedMap(c.Object, map[string]any{
		"phase": "Bound", "cluster": cluster, "fence": fence,
	}, "status")
	return c
}

func leasedDevice(name, claim, cluster string, fence int64) *unstructured.Unstructured {
	d := device(name, "uid-"+name)
	_ = unstructured.SetNestedMap(d.Object, map[string]any{
		"cluster": cluster, "claim": claim, "fence": fence,
	}, "status", "lease")
	return d
}

func phaseOf(t *testing.T, objects []*unstructured.Unstructured, name string) string {
	t.Helper()
	for _, o := range objects {
		if o.GetKind() == "DeviceClaim" && o.GetName() == name {
			p, _, _ := unstructured.NestedString(o.Object, "status", "phase")
			return p
		}
	}
	t.Fatalf("no claim %s", name)
	return ""
}

func TestPlanFencesKeepsOnlyTheSoleHolderOfTheHighestFence(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST restore a Bound claim as Bound only when that claim alone holds the highest fence that the export recorded for its object.
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The import MUST write a Bound claim as Lost when it does not restore that claim as Bound.
	objects := []*unstructured.Unstructured{
		// d1: the holder at 7 stays Bound. A zombie at 5 is Lost.
		leasedDevice("d1", "holder", "east", 7),
		boundClaim("holder", "d1", "east", 7),
		boundClaim("zombie", "d1", "west", 5),
		// d2: the Device advertises 9 for a claim not in the file, so the
		// Bound claim at 8 is stale.
		leasedDevice("d2", "gone", "east", 9),
		boundClaim("stale", "d2", "east", 8),
		// d3: two Bound claims share fence 4. Neither holds it alone.
		leasedDevice("d3", "twin-a", "east", 4),
		boundClaim("twin-a", "d3", "east", 4),
		boundClaim("twin-b", "d3", "west", 4),
		// d4: the Device names another holder at the claim's fence.
		leasedDevice("d4", "someone", "east", 3),
		boundClaim("impostor", "d4", "east", 3),
		// d5: no Device in the file. The claim alone holds its fence.
		boundClaim("orphan", "d5", "east", 2),
	}
	seeds, err := planFences(objects)
	if err != nil {
		t.Fatal(err)
	}
	want := []LockSeed{
		{Device: "d1", Fence: 7, Claim: "holder", Cluster: "east", TTLSeconds: 30},
		{Device: "d2", Fence: 9},
		{Device: "d3", Fence: 4},
		{Device: "d4", Fence: 3},
		{Device: "d5", Fence: 2, Claim: "orphan", Cluster: "east", TTLSeconds: 30},
	}
	if !reflect.DeepEqual(seeds, want) {
		t.Fatalf("seeds = %+v\nwant %+v", seeds, want)
	}
	for name, phase := range map[string]string{
		"holder": "Bound", "orphan": "Bound",
		"zombie": "Lost", "stale": "Lost", "twin-a": "Lost", "twin-b": "Lost", "impostor": "Lost",
	} {
		if got := phaseOf(t, objects, name); got != phase {
			t.Errorf("%s phase = %s, want %s", name, got, phase)
		}
	}
	// A Lost claim keeps its fence and cluster, and says why.
	for _, o := range objects {
		if o.GetName() != "zombie" {
			continue
		}
		st, _, _ := unstructured.NestedMap(o.Object, "status")
		if st["fence"] != int64(5) || st["cluster"] != "west" || st["reason"] != LostReason {
			t.Fatalf("zombie status = %v", st)
		}
	}
}

func TestPlanFencesRejectsANegativeFence(t *testing.T) {
	if _, err := planFences([]*unstructured.Unstructured{leasedDevice("d", "c", "east", -1)}); err == nil {
		t.Fatal("planFences accepted a negative fence")
	}
}
