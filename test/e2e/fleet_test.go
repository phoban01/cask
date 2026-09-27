//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
)

// The fleet deploy follows demo/kind/demo.sh. It uses the same Dockerfile
// and the same manifest with the same placeholders, so the suite tests
// what the demo runs.

// image is the tag the suite builds. It differs from the demo tag, so a
// suite run does not replace the demo image.
const image = "cask-e2e:latest"

// demoImage is the image name in the demo manifest.
const demoImage = "cask-demo:latest"

// consensusPort is the embedded acceptor port on each node IP.
const consensusPort = 8001

// firstID is the proposer id of the first apiserver, as in demo.sh.
const firstID = 101

// readyTimeout bounds each readiness wait in deployFleet.
const readyTimeout = 3 * time.Minute

// deviceGVR is the fleet Device resource.
var deviceGVR = schema.GroupVersionResource{
	Group: "fleet.cask.dev", Version: "v1alpha1", Resource: "devices",
}

// apiServiceGVR is the aggregation-layer registration of an API group.
var apiServiceGVR = schema.GroupVersionResource{
	Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices",
}

// repoRoot returns the repository root. go test runs in test/e2e.
func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(wd, "..", ".."))
}

// buildImage builds the cask-apiserver image once with the demo Dockerfile.
func buildImage() env.Func {
	return func(ctx context.Context, _ *envconf.Config) (context.Context, error) {
		root, err := repoRoot()
		if err != nil {
			return ctx, err
		}
		dockerfile := filepath.Join(root, "demo", "kind", "Dockerfile")
		cmd := exec.CommandContext(ctx, "docker", "build", "-q", "-f", dockerfile, "-t", image, root)
		if out, err := cmd.CombinedOutput(); err != nil {
			return ctx, fmt.Errorf("docker build: %w: %s", err, out)
		}
		return ctx, nil
	}
}

// restConfig returns the REST config of a kind cluster that Setup created.
func restConfig(ctx context.Context, logical string) (*rest.Config, error) {
	c, ok := envfuncs.GetClusterFromContext(ctx, clusterName(logical))
	if !ok {
		return nil, fmt.Errorf("cluster %s is not in the context", clusterName(logical))
	}
	return c.KubernetesRestConfig(), nil
}

// dynamicClient returns a client-go dynamic client for a logical cluster.
func dynamicClient(ctx context.Context, logical string) (dynamic.Interface, error) {
	rc, err := restConfig(ctx, logical)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(rc)
}

// nodeIP returns the InternalIP of the node of a one-node kind cluster.
// The kind nodes share one docker network, so each node IP is reachable
// from the other clusters.
func nodeIP(ctx context.Context, r *resources.Resources) (string, error) {
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return "", err
	}
	for _, n := range nodes.Items {
		for _, a := range n.Status.Addresses {
			if a.Type == corev1.NodeInternalIP {
				return a.Address, nil
			}
		}
	}
	return "", fmt.Errorf("no node has an InternalIP")
}

// renderManifest fills the demo manifest placeholders for one cluster, as
// demo.sh does, and points the pod at the suite image.
func renderManifest(tmpl []byte, cluster string, id int, peers string) string {
	return strings.NewReplacer(
		"__CLUSTER__", cluster,
		"__ID__", strconv.Itoa(id),
		"__CASK_PEERS__", peers,
		demoImage, image,
	).Replace(string(tmpl))
}

// deployFleet loads the image into every cluster and deploys one
// cask-apiserver per cluster. The apiservers use hostNetwork and embedded
// acceptors, so the three form one consensus group. deployFleet returns
// when every cluster serves a Device list, which needs a quorum.
func deployFleet() env.Func {
	return func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		root, err := repoRoot()
		if err != nil {
			return ctx, err
		}
		tmpl, err := os.ReadFile(filepath.Join(root, "demo", "kind", "manifests", "apiserver.yaml"))
		if err != nil {
			return ctx, err
		}

		clients := make(map[string]*resources.Resources, len(logicalClusters))
		peers := make([]string, 0, len(logicalClusters))
		for _, logical := range logicalClusters {
			ctx, err = envfuncs.LoadImageToCluster(clusterName(logical), image)(ctx, cfg)
			if err != nil {
				return ctx, fmt.Errorf("load image into %s: %w", logical, err)
			}
			rc, err := restConfig(ctx, logical)
			if err != nil {
				return ctx, err
			}
			r, err := resources.New(rc)
			if err != nil {
				return ctx, err
			}
			clients[logical] = r
			ip, err := nodeIP(ctx, r)
			if err != nil {
				return ctx, fmt.Errorf("node IP of %s: %w", logical, err)
			}
			peers = append(peers, fmt.Sprintf("%s:%d", ip, consensusPort))
		}

		for i, logical := range logicalClusters {
			// --cluster takes the logical name, as in the demo.
			m := renderManifest(tmpl, logical, firstID+i, strings.Join(peers, ","))
			h := decoder.CreateHandler(clients[logical])
			if err := decoder.DecodeEach(ctx, strings.NewReader(m), h); err != nil {
				return ctx, fmt.Errorf("apply manifest to %s: %w", logical, err)
			}
		}

		for _, logical := range logicalClusters {
			if err := waitReady(ctx, logical, clients[logical]); err != nil {
				return ctx, fmt.Errorf("%s: %w", logical, err)
			}
		}
		return ctx, nil
	}
}

// poll runs cond every two seconds until it is true or readyTimeout ends.
func poll(ctx context.Context, cond func(context.Context) (bool, error)) error {
	return wait.For(cond, wait.WithContext(ctx), wait.WithTimeout(readyTimeout),
		wait.WithInterval(2*time.Second))
}

// waitReady waits for the apiserver pod, the APIService, and a Device list
// through the kube-apiserver of one cluster.
func waitReady(ctx context.Context, logical string, r *resources.Resources) error {
	sts := &appsv1.StatefulSet{}
	err := poll(ctx, func(ctx context.Context) (bool, error) {
		if err := r.Get(ctx, "cask-apiserver", "cask-system", sts); err != nil {
			return false, nil
		}
		return sts.Status.ReadyReplicas == 1, nil
	})
	if err != nil {
		return fmt.Errorf("statefulset not ready: %w", err)
	}

	dc, err := dynamicClient(ctx, logical)
	if err != nil {
		return err
	}
	err = poll(ctx, func(ctx context.Context) (bool, error) {
		svc, err := dc.Resource(apiServiceGVR).Get(ctx, "v1alpha1.fleet.cask.dev", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return apiServiceAvailable(svc), nil
	})
	if err != nil {
		return fmt.Errorf("APIService not Available: %w", err)
	}

	var lastErr error
	err = poll(ctx, func(ctx context.Context) (bool, error) {
		_, lastErr = dc.Resource(deviceGVR).List(ctx, metav1.ListOptions{})
		return lastErr == nil, nil
	})
	if err != nil {
		return fmt.Errorf("device list not served: %w (last error: %v)", err, lastErr)
	}
	return nil
}

// apiServiceAvailable reports whether an APIService has Available=True.
func apiServiceAvailable(svc *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(svc.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]any)
		if m["type"] == "Available" && m["status"] == "True" {
			return true
		}
	}
	return false
}
