//go:build e2e

// Package e2e runs the fleet end-to-end suite against three kind clusters.
//
// The suite uses sigs.k8s.io/e2e-framework with plain testing. It does not
// use Ginkgo or envtest. Run it with:
//
//	devbox run e2e
//
// Setup creates the clusters, builds the demo image once, and deploys one
// cask-apiserver per cluster from demo/kind/manifests. See fleet_test.go.
//
// The build tag keeps `go test ./...` from starting kind.
//
// Cluster names carry a prefix so the suite does not touch the demo
// clusters. The default prefix is "e2e-". Set CASK_E2E_PREFIX to use a
// different prefix, for example to run two suites at the same time.
//
// Set CASK_E2E_KEEP_CLUSTERS=1 to keep the clusters after the run. CI sets
// it so that it can export the cluster logs. The caller must then delete
// the clusters.
package e2e

import (
	"fmt"
	"os"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/third_party/kind"
)

// defaultPrefix is the cluster name prefix when CASK_E2E_PREFIX is not set.
const defaultPrefix = "e2e-"

// prefixEnv names the environment variable that overrides defaultPrefix.
const prefixEnv = "CASK_E2E_PREFIX"

// keepEnv names the environment variable that keeps the clusters after
// the run.
const keepEnv = "CASK_E2E_KEEP_CLUSTERS"

// logicalClusters are the logical names of the fleet clusters. They match
// the cluster names in demo/kind.
var logicalClusters = []string{"east", "west", "north"}

// clusterNames maps a logical name to its kind cluster name. TestMain
// fills it before any feature runs.
var clusterNames = map[string]string{}

// testenv is the shared environment. Features in this package run in it.
var testenv env.Environment

// clusterPrefix returns the kind cluster name prefix for this run.
func clusterPrefix() string {
	if p := os.Getenv(prefixEnv); p != "" {
		return p
	}
	return defaultPrefix
}

// clusterName returns the kind cluster name for a logical cluster name.
// For example, "east" gives "e2e-east". It panics on an unknown name, so a
// typo in a feature fails at once.
func clusterName(logical string) string {
	name, ok := clusterNames[logical]
	if !ok {
		panic(fmt.Sprintf("e2e: unknown cluster %q", logical))
	}
	return name
}

func TestMain(m *testing.M) {
	//= docs/spec/fleet.md#10-verification
	//# End-to-end tests MUST use sigs.k8s.io/e2e-framework against kind clusters.

	//= docs/spec/fleet.md#10-verification
	//= type=test
	//# End-to-end tests MUST use sigs.k8s.io/e2e-framework against kind clusters.

	//= docs/spec/fleet.md#10-verification
	//# End-to-end tests MUST NOT use Ginkgo.

	//= docs/spec/fleet.md#10-verification
	//# End-to-end tests MUST NOT use envtest.
	testenv = env.NewWithConfig(envconf.New())

	prefix := clusterPrefix()
	setup := make([]env.Func, 0, len(logicalClusters))
	finish := make([]env.Func, 0, len(logicalClusters))
	for _, logical := range logicalClusters {
		name := prefix + logical
		clusterNames[logical] = name
		setup = append(setup, envfuncs.CreateCluster(kind.NewProvider(), name))
		finish = append(finish, envfuncs.DestroyCluster(name))
	}
	setup = append(setup, buildImage(), deployFleet())
	testenv.Setup(setup...)
	if os.Getenv(keepEnv) == "" {
		testenv.Finish(finish...)
	}

	os.Exit(testenv.Run(m))
}
