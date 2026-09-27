//go:build e2e

// Package e2e runs the fleet end-to-end suite against three kind clusters.
//
// The suite uses sigs.k8s.io/e2e-framework with plain testing. It does not
// use Ginkgo or envtest. Run it with:
//
//	devbox run e2e
//
// The build tag keeps `go test ./...` from starting kind.
package e2e

import (
	"os"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/third_party/kind"
)

// clusters are the kind clusters of the fleet. The names match demo/kind.
var clusters = []string{"east", "west", "north"}

// testenv is the shared environment. Features in this package run in it.
var testenv env.Environment

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

	setup := make([]env.Func, 0, len(clusters))
	finish := make([]env.Func, 0, len(clusters))
	for _, name := range clusters {
		setup = append(setup, envfuncs.CreateCluster(kind.NewProvider(), name))
		finish = append(finish, envfuncs.DestroyCluster(name))
	}
	testenv.Setup(setup...)
	testenv.Finish(finish...)

	os.Exit(testenv.Run(m))
}
