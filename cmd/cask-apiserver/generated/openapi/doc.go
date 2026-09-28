// Package openapi holds the OpenAPI definitions of the fleet.cask.dev
// types and the Kubernetes meta types they embed. The generic server
// needs them to serve /openapi/v2 and /openapi/v3 and to track managed
// fields for server-side apply.
package openapi

//go:generate go run k8s.io/kube-openapi/cmd/openapi-gen@v0.0.0-20250910181357-589584f1c912 --output-dir . --output-pkg github.com/phoban01/cask/cmd/cask-apiserver/generated/openapi --output-file zz_generated.openapi.go --report-filename /dev/null github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1 k8s.io/apimachinery/pkg/apis/meta/v1 k8s.io/apimachinery/pkg/runtime k8s.io/apimachinery/pkg/version
