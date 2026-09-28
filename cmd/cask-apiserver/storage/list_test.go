package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// everything is a predicate that matches every object.
func everything() apistorage.SelectionPredicate {
	return apistorage.SelectionPredicate{
		Label:    labels.Everything(),
		Field:    fields.Everything(),
		GetAttrs: apistorage.DefaultClusterScopedAttr,
	}
}

func listOpts() apistorage.ListOptions {
	return apistorage.ListOptions{Recursive: true, Predicate: everything()}
}

func mustList(t *testing.T, s *Store, opts apistorage.ListOptions) *v1alpha1.DeviceList {
	t.Helper()
	list := &v1alpha1.DeviceList{}
	if err := s.GetList(context.Background(), keyPrefix, opts, list); err != nil {
		t.Fatal(err)
	}
	return list
}

func listRV(t *testing.T, list *v1alpha1.DeviceList) uint64 {
	t.Helper()
	rv, err := strconv.ParseUint(list.ResourceVersion, 10, 64)
	if err != nil {
		t.Fatalf("list resourceVersion %q: %v", list.ResourceVersion, err)
	}
	return rv
}

func names(list *v1alpha1.DeviceList) []string {
	out := make([]string, len(list.Items))
	for i := range list.Items {
		out[i] = list.Items[i].Name
	}
	return out
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGetListReturnsIndexAtIndexSequence(t *testing.T) {
	//= docs/spec/fleet.md#4-list-and-watch
	//= type=test
	//# A list MUST return every object that the index register names at the index sequence the list reports.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A list's resourceVersion MUST be the sequence of the index register.
	ctx := context.Background()
	s := newTestStore(t)
	for _, n := range []string{"gpu-2", "gpu-0", "gpu-1"} {
		mustCreate(t, s, device(n, "a100"))
	}
	if err := s.GuaranteedUpdate(ctx, keyPrefix+"gpu-1", &v1alpha1.Device{}, false, nil,
		mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" }), nil); err != nil {
		t.Fatal(err)
	}
	// A write with no index write: the index still records sequence 1
	// for gpu-0, so the list must not show this version.
	putRaw(t, s, device("gpu-0", "b200"))

	list := mustList(t, s, listOpts())
	idx := mustIndex(t, s.kv, "devices")
	if got := listRV(t, list); got != idx.Seq || got != 4 {
		t.Fatalf("list resourceVersion = %d; index sequence = %d; want 4", got, idx.Seq)
	}
	if got, want := names(list), []string{"gpu-0", "gpu-1", "gpu-2"}; !equalNames(got, want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
	for i := range list.Items {
		d := &list.Items[i]
		if rvOf(t, d) != idx.Entries[d.Name] {
			t.Errorf("%s resourceVersion = %s, index records %d", d.Name, d.ResourceVersion, idx.Entries[d.Name])
		}
	}
	if m := list.Items[0].Spec.Model; m != "a100" {
		t.Errorf("gpu-0 model = %q, want the indexed a100", m)
	}
	if m := list.Items[1].Spec.Model; m != "h100" {
		t.Errorf("gpu-1 model = %q, want h100", m)
	}
}

func TestGetListEmptyStore(t *testing.T) {
	list := mustList(t, newTestStore(t), listOpts())
	if len(list.Items) != 0 || list.ResourceVersion != "0" {
		t.Fatalf("items = %d, resourceVersion = %q; want 0 items at \"0\"", len(list.Items), list.ResourceVersion)
	}
}

func TestGetListSelectors(t *testing.T) {
	s := newTestStore(t)
	for i, n := range []string{"gpu-0", "gpu-1", "gpu-2"} {
		d := device(n, "a100")
		d.Labels = map[string]string{"zone": []string{"east", "west", "east"}[i]}
		mustCreate(t, s, d)
	}

	opts := listOpts()
	opts.Predicate.Label = labels.SelectorFromSet(labels.Set{"zone": "east"})
	if got := names(mustList(t, s, opts)); !equalNames(got, []string{"gpu-0", "gpu-2"}) {
		t.Errorf("label selector: names = %v", got)
	}

	opts = listOpts()
	opts.Predicate.Field = fields.OneTermEqualSelector("metadata.name", "gpu-1")
	if got := names(mustList(t, s, opts)); !equalNames(got, []string{"gpu-1"}) {
		t.Errorf("field selector: names = %v", got)
	}

	// A non-recursive list names one object.
	one := &v1alpha1.DeviceList{}
	opts = apistorage.ListOptions{Predicate: everything()}
	if err := s.GetList(context.Background(), keyPrefix+"gpu-2", opts, one); err != nil {
		t.Fatal(err)
	}
	if got := names(one); !equalNames(got, []string{"gpu-2"}) {
		t.Errorf("non-recursive: names = %v", got)
	}
}

func TestGetListResourceVersionRules(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	mustCreate(t, s, device("gpu-0", "a100"))
	mustCreate(t, s, device("gpu-1", "a100"))
	if err := s.Delete(ctx, keyPrefix+"gpu-0", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Index sequences: 1 adds gpu-0, 2 adds gpu-1, 3 removes gpu-0.

	list := func(rv string, match metav1.ResourceVersionMatch) (*v1alpha1.DeviceList, error) {
		opts := listOpts()
		opts.ResourceVersion, opts.ResourceVersionMatch = rv, match
		out := &v1alpha1.DeviceList{}
		return out, s.GetList(ctx, keyPrefix, opts, out)
	}

	for _, tc := range []struct {
		rv    string
		match metav1.ResourceVersionMatch
		want  []string
		rvOut string
	}{
		{"", "", []string{"gpu-1"}, "3"},
		{"0", "", []string{"gpu-1"}, "3"},
		{"2", "", []string{"gpu-1"}, "3"},
		{"2", metav1.ResourceVersionMatchNotOlderThan, []string{"gpu-1"}, "3"},
		{"3", metav1.ResourceVersionMatchNotOlderThan, []string{"gpu-1"}, "3"},
		{"2", metav1.ResourceVersionMatchExact, []string{"gpu-0", "gpu-1"}, "2"},
		{"1", metav1.ResourceVersionMatchExact, []string{"gpu-0"}, "1"},
	} {
		got, err := list(tc.rv, tc.match)
		if err != nil {
			t.Errorf("rv=%q match=%q: %v", tc.rv, tc.match, err)
			continue
		}
		if !equalNames(names(got), tc.want) || got.ResourceVersion != tc.rvOut {
			t.Errorf("rv=%q match=%q: names %v at %q; want %v at %q",
				tc.rv, tc.match, names(got), got.ResourceVersion, tc.want, tc.rvOut)
		}
	}

	for _, match := range []metav1.ResourceVersionMatch{"", metav1.ResourceVersionMatchNotOlderThan, metav1.ResourceVersionMatchExact} {
		if _, err := list("4", match); !apistorage.IsTooLargeResourceVersion(err) {
			t.Errorf("rv=4 match=%q: error = %v; want too large resource version", match, err)
		}
	}
	if _, err := list("abc", ""); !apierrors.IsBadRequest(err) {
		t.Errorf("rv=abc: error = %v; want bad request", err)
	}
	if _, err := list("0", metav1.ResourceVersionMatchExact); !apierrors.IsBadRequest(err) {
		t.Errorf("rv=0 exact: error = %v; want bad request", err)
	}

	if err := s.kv.Compact(ctx, IndexKey("devices"), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := list("2", metav1.ResourceVersionMatchExact); !apierrors.IsResourceExpired(err) {
		t.Errorf("compacted rv=2 exact: error = %v; want 410 Gone", err)
	}
	if got, err := list("3", metav1.ResourceVersionMatchExact); err != nil || !equalNames(names(got), []string{"gpu-1"}) {
		t.Errorf("rv=3 exact after compaction: %v, %v", names(got), err)
	}
}

func TestGetListPages(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	all := []string{"gpu-0", "gpu-1", "gpu-2", "gpu-3", "gpu-4"}
	for _, n := range all {
		mustCreate(t, s, device(n, "a100"))
	}

	opts := listOpts()
	opts.Predicate.Limit = 2
	first := mustList(t, s, opts)
	if got := names(first); !equalNames(got, all[:2]) {
		t.Fatalf("page 1 = %v", got)
	}
	if first.Continue == "" || first.RemainingItemCount == nil || *first.RemainingItemCount != 3 {
		t.Fatalf("page 1 continue = %q, remaining = %v", first.Continue, first.RemainingItemCount)
	}

	// Writes after the first page do not show in later pages: each page
	// reads the index version of the first page.
	mustCreate(t, s, device("gpu-5", "a100"))
	if err := s.Delete(ctx, keyPrefix+"gpu-3", &v1alpha1.Device{}, nil, nil, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	var got []string
	got = append(got, names(first)...)
	cont := first.Continue
	for cont != "" {
		opts := listOpts()
		opts.Predicate.Limit = 2
		opts.Predicate.Continue = cont
		page := mustList(t, s, opts)
		if page.ResourceVersion != first.ResourceVersion {
			t.Fatalf("page resourceVersion = %s, first page = %s", page.ResourceVersion, first.ResourceVersion)
		}
		got = append(got, names(page)...)
		cont = page.Continue
	}
	if !equalNames(got, all) {
		t.Fatalf("pages = %v, want %v", got, all)
	}

	// Once the first page's index version is compacted, the token gets
	// 410 Gone and a new token that continues at the latest version.
	idx := mustIndex(t, s.kv, "devices")
	if err := s.kv.Compact(ctx, IndexKey("devices"), idx.Seq); err != nil {
		t.Fatal(err)
	}
	opts = listOpts()
	opts.Predicate.Limit = 2
	opts.Predicate.Continue = first.Continue
	err := s.GetList(ctx, keyPrefix, opts, &v1alpha1.DeviceList{})
	if !apierrors.IsResourceExpired(err) {
		t.Fatalf("compacted continue: error = %v; want 410 Gone", err)
	}
	opts.Predicate.Continue = err.(apierrors.APIStatus).Status().ListMeta.Continue
	if got := names(mustList(t, s, opts)); !equalNames(got, []string{"gpu-2", "gpu-4"}) {
		t.Fatalf("continue at latest = %v, want [gpu-2 gpu-4]", got)
	}
}
