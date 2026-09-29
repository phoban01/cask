//go:build e2e

package e2e

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// TestAPIServiceVerifiesServingCert checks that each kube-apiserver
// verifies the cask apiserver. The APIService carries a caBundle and no
// insecureSkipTLSVerify, it is Available, and the CA in the caBundle
// signs the serving certificate for the Service name.
func TestAPIServiceVerifiesServingCert(t *testing.T) {
	//= docs/spec/fleet.md#8-security
	//= type=test
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	f := features.New("verified serving certificate").
		Assess("every APIService verifies the apiserver with its caBundle",
			func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				for _, logical := range logicalClusters {
					checkServingCert(ctx, t, logical)
				}
				return ctx
			}).
		Feature()

	testenv.Test(t, f)
}

// checkServingCert checks the APIService and the serving Secret of one
// cluster.
func checkServingCert(ctx context.Context, t *testing.T, logical string) {
	t.Helper()
	dc, err := dynamicClient(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := dc.Resource(apiServiceGVR).Get(ctx, "v1alpha1.fleet.cask.dev", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("%s: get APIService: %v", logical, err)
	}
	if skip, found, _ := unstructured.NestedBool(svc.Object, "spec", "insecureSkipTLSVerify"); found || skip {
		t.Errorf("%s: APIService sets insecureSkipTLSVerify", logical)
	}
	bundle, _, _ := unstructured.NestedString(svc.Object, "spec", "caBundle")
	if bundle == "" {
		t.Fatalf("%s: APIService has no caBundle", logical)
	}
	if !apiServiceAvailable(svc) {
		t.Errorf("%s: APIService is not Available", logical)
	}
	caPEM, err := base64.StdEncoding.DecodeString(bundle)
	if err != nil {
		t.Fatalf("%s: caBundle: %v", logical, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatalf("%s: caBundle holds no certificate", logical)
	}

	rc, err := restConfig(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	r, err := resources.New(rc)
	if err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	if err := r.Get(ctx, "cask-serving-tls", "cask-system", &secret); err != nil {
		t.Fatalf("%s: get serving Secret: %v", logical, err)
	}
	block, _ := pem.Decode(secret.Data["tls.crt"])
	if block == nil {
		t.Fatalf("%s: serving Secret has no certificate", logical)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("%s: serving certificate: %v", logical, err)
	}
	// The aggregator dials <service>.<namespace>.svc and checks that name.
	_, err = cert.Verify(x509.VerifyOptions{
		DNSName:   "cask-apiserver.cask-system.svc",
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		t.Errorf("%s: caBundle does not verify the serving certificate: %v", logical, err)
	}
}
