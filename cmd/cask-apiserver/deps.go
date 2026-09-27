//go:build tools

package main

// These imports pin the Kubernetes libraries that the extension server
// will build on. The build tag keeps them out of the binary. Remove each
// import when the server code imports its package directly.
import (
	_ "k8s.io/apiserver/pkg/server"
	_ "k8s.io/client-go/kubernetes"
)
