//go:build tools

package main

// These imports pin the Kubernetes libraries that the extension server
// builds on. The build tag keeps them out of the binary. Remove this file
// when the server code imports the packages directly.
import (
	_ "k8s.io/apimachinery/pkg/runtime"
	_ "k8s.io/apiserver/pkg/server"
	_ "k8s.io/client-go/kubernetes"
)
