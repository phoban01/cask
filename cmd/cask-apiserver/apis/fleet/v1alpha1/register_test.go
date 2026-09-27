package v1alpha1

import (
	"bytes"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

func TestSchemeServesCRDGroupVersionKinds(t *testing.T) {
	//= docs/spec/fleet.md#7-migration
	//= type=test
	//# The extension server MUST serve the same API group, version, and kinds that the CRD served.
	scheme := newScheme(t)
	for _, kind := range []string{"Device", "DeviceList", "DeviceClaim", "DeviceClaimList"} {
		gvk := schema.GroupVersionKind{Group: "fleet.cask.dev", Version: "v1alpha1", Kind: kind}
		if !scheme.Recognizes(gvk) {
			t.Errorf("scheme does not recognize %v", gvk)
		}
		internal := gvk.GroupKind().WithVersion(runtime.APIVersionInternal)
		if !scheme.Recognizes(internal) {
			t.Errorf("scheme does not recognize %v", internal)
		}
	}
}

func TestDeviceJSONRoundTrip(t *testing.T) {
	scheme := newScheme(t)
	codecs := serializer.NewCodecFactory(scheme)
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		t.Fatal("no JSON serializer")
	}
	encoder := codecs.EncoderForVersion(info.Serializer, SchemeGroupVersion)
	decoder := codecs.UniversalDecoder(SchemeGroupVersion)

	in := &Device{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cam-1",
			UID:               types.UID("6f1c0e52-0d5e-4c1b-9d0a-3f1f3c7d2b11"),
			ResourceVersion:   "7",
			CreationTimestamp: metav1.Unix(1_700_000_000, 0),
			Labels:            map[string]string{"zone": "eu-west"},
		},
		Spec: DeviceSpec{
			Model:      "cam-x",
			Zone:       "eu-west",
			Attributes: map[string]string{"lens": "wide"},
		},
		Status: DeviceStatus{
			Phase: DeviceLeased,
			Lease: &LeaseRef{Cluster: "cluster-a", Claim: "mapper", Fence: 3},
		},
	}

	var buf bytes.Buffer
	if err := encoder.Encode(in, &buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, want := range []string{`"apiVersion":"fleet.cask.dev/v1alpha1"`, `"kind":"Device"`} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("encoded JSON lacks %s: %s", want, buf.String())
		}
	}

	obj, gvk, err := decoder.Decode(buf.Bytes(), nil, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := SchemeGroupVersion.WithKind("Device"); *gvk != want {
		t.Errorf("decoded kind = %v, want %v", *gvk, want)
	}
	out, ok := obj.(*Device)
	if !ok {
		t.Fatalf("decoded %T, want *Device", obj)
	}
	want := in.DeepCopy()
	want.TypeMeta = metav1.TypeMeta{APIVersion: "fleet.cask.dev/v1alpha1", Kind: "Device"}
	if !equality.Semantic.DeepEqual(out, want) {
		t.Errorf("round trip changed the object:\n got %+v\nwant %+v", out, want)
	}
}
