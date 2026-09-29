package main

// The extension server is a generic server from k8s.io/apiserver. It
// serves the fleet.cask.dev/v1alpha1 group through the generic registry
// over the cask storage (registry.go). The kube-apiserver of the cluster
// authenticates and authorizes each request for it: the server delegates
// both with TokenReview and SubjectAccessReview, and trusts the front
// proxy of the aggregation layer through the extension-apiserver-
// authentication ConfigMap. The generic server also serves discovery,
// OpenAPI, /healthz, /livez, /readyz, and /metrics.

import (
	"context"
	"fmt"
	"net"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/generated/openapi"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
)

var (
	// fleetScheme holds the fleet types and the meta types the generic
	// server needs.
	fleetScheme = runtime.NewScheme()
	// fleetCodecs encodes and decodes with fleetScheme.
	fleetCodecs = serializer.NewCodecFactory(fleetScheme)
)

func init() {
	utilruntime.Must(v1alpha1.AddToScheme(fleetScheme))
	// The generic server decodes list and delete options and encodes
	// status and discovery documents as unversioned meta types.
	metav1.AddToGroupVersion(fleetScheme, schema.GroupVersion{Version: "v1"})
	fleetScheme.AddUnversionedTypes(schema.GroupVersion{Group: "", Version: "v1"},
		&metav1.Status{}, &metav1.APIVersions{}, &metav1.APIGroupList{}, &metav1.APIGroup{}, &metav1.APIResourceList{})
}

// fleetCodec encodes objects for the cask registers as v1alpha1 JSON.
func fleetCodec() runtime.Codec {
	return fleetCodecs.LegacyCodec(v1alpha1.SchemeGroupVersion)
}

// serverOptions are the flags of the generic server.
type serverOptions struct {
	recommended *genericoptions.RecommendedOptions
	// delegatedAuth is false only in tests. The server then serves every
	// request without authentication or authorization.
	delegatedAuth bool
	// selfSigned is true when newFleetServer made an in-memory self-signed
	// serving certificate because no --tls-cert-file was set.
	selfSigned bool
}

// newServerOptions returns the recommended options of an aggregated
// server, less the etcd options: cask is the storage.
func newServerOptions() *serverOptions {
	o := genericoptions.NewRecommendedOptions("/registry/"+v1alpha1.GroupName, fleetCodec())
	o.Etcd = nil
	o.SecureServing.BindPort = 9443
	// With no --tls-cert-file, the server makes a self-signed certificate
	// in memory, for tests only. It writes nothing to disk.
	o.SecureServing.ServerCert.CertDirectory = ""
	o.SecureServing.ServerCert.PairName = "cask-apiserver"
	return &serverOptions{recommended: o, delegatedAuth: true}
}

// addFlags adds the generic server flags to fs.
func (o *serverOptions) addFlags(fs *pflag.FlagSet) {
	o.recommended.AddFlags(fs)
	fs.BoolVar(&o.delegatedAuth, "delegated-auth", o.delegatedAuth,
		"delegate authentication and authorization to the kube-apiserver; false serves every request "+
			"without either and is for tests only")
}

// newFleetServer builds the generic server over the cask stores of
// Device and DeviceClaim. beforeClaimDelete runs inside each claim delete,
// before the claim goes; an error stops the delete. The server reports
// ready only when every check in ready passes.
func (o *serverOptions) newFleetServer(cluster string, stores map[string]*storage.Store,
	beforeClaimDelete func(context.Context, *v1alpha1.DeviceClaim) error,
	ready ...healthz.HealthChecker) (*genericapiserver.GenericAPIServer, error) {
	//= docs/spec/fleet.md#2-resources
	//# The extension server MUST be built on the generic server in k8s.io/apiserver.
	rec := o.recommended
	if !o.delegatedAuth {
		rec.Authentication = nil
		rec.Authorization = nil
	}
	// --tls-cert-file and --tls-private-key-file name the serving
	// certificate. `cask-apiserver gen-serving-certs` makes one for the
	// Service, and the APIService carries its CA as the caBundle.
	//= docs/spec/fleet.md#8-security
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	cert := rec.SecureServing.ServerCert.CertKey
	if (cert.CertFile == "") != (cert.KeyFile == "") {
		return nil, fmt.Errorf("serving certificate: set both --tls-cert-file and --tls-private-key-file")
	}
	// Without the flags, the server makes a self-signed certificate in
	// memory. The kube-apiserver cannot verify it. In-process tests use it,
	// and main logs a warning.
	o.selfSigned = cert.CertFile == ""
	if err := rec.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost",
		[]string{"cask-apiserver", "cask-apiserver.cask-system", "cask-apiserver.cask-system.svc"},
		[]net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("serving certificate: %w", err)
	}
	if errs := rec.Validate(); len(errs) > 0 {
		return nil, fmt.Errorf("server options: %v", errs)
	}

	cfg := genericapiserver.NewRecommendedConfig(fleetCodecs)
	cfg.EffectiveVersion = compatibility.DefaultBuildEffectiveVersion()
	namer := openapinamer.NewDefinitionNamer(fleetScheme)
	cfg.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(openapi.GetOpenAPIDefinitions, namer)
	cfg.OpenAPIConfig.Info.Title = "cask"
	cfg.OpenAPIConfig.Info.Version = v1alpha1.Version
	cfg.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(openapi.GetOpenAPIDefinitions, namer)
	cfg.OpenAPIV3Config.Info.Title = "cask"
	cfg.OpenAPIV3Config.Info.Version = v1alpha1.Version
	// The server asks the kube-apiserver about each request. TokenReview
	// checks a bearer token. The front-proxy CA from the
	// extension-apiserver-authentication ConfigMap checks a proxied
	// request. SubjectAccessReview checks what the user may do.
	//= docs/spec/fleet.md#2-resources
	//# The extension server MUST delegate authentication and authorization to the local kube-apiserver.
	//= docs/spec/fleet.md#8-security
	//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
	if err := rec.ApplyTo(cfg); err != nil {
		return nil, fmt.Errorf("server config: %w", err)
	}

	srv, err := cfg.Complete().New("cask-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}
	getter := caskRESTOptions{codec: fleetCodec(), stores: stores, beforeClaimDelete: beforeClaimDelete}
	resources, err := newFleetREST(fleetScheme, getter, cluster)
	if err != nil {
		return nil, err
	}
	group := genericapiserver.NewDefaultAPIGroupInfo(v1alpha1.GroupName, fleetScheme, metav1.ParameterCodec, fleetCodecs)
	group.VersionedResourcesStorageMap[v1alpha1.Version] = resources
	// The kube-apiserver proxies the group to this server through an
	// APIService.
	//= docs/spec/fleet.md#2-resources
	//# Cask MUST serve its resources through the Kubernetes aggregation layer as an APIService.
	if err := srv.InstallAPIGroup(&group); err != nil {
		return nil, fmt.Errorf("install %s: %w", v1alpha1.GroupName, err)
	}
	// The checks join the readyz checks of the generic server. main passes
	// storageCheck, and importCheck with --expect-import.
	if err := srv.AddReadyzChecks(ready...); err != nil {
		return nil, fmt.Errorf("readyz checks: %w", err)
	}
	return srv, nil
}

// fleetStores returns one cask store per fleet resource over kv.
func fleetStores(kv *mvcc.KV) map[string]*storage.Store {
	codec := fleetCodec()
	return map[string]*storage.Store{
		"devices":      storage.New(kv, codec, "devices", func() runtime.Object { return &v1alpha1.Device{} }),
		"deviceclaims": storage.New(kv, codec, "deviceclaims", func() runtime.Object { return &v1alpha1.DeviceClaim{} }),
	}
}
