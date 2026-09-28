// Package migrate moves fleet objects from a CRD on the management
// cluster into cask.
//
// Export reads every object of an API group through the Kubernetes API
// and writes an export file. The file is JSON lines:
//
//   - Line 1 is a Header. It names the format, the group, the source etcd
//     revision, and each resource with its object count.
//   - Every other line is one object, exactly as the API returned it. The
//     objects come in header order, then in name order.
//
// The file keeps every field, including metadata.uid,
// metadata.creationTimestamp, and status. The same objects at the same
// revision give the same bytes.
//
// Import writes the objects of an export file into cask. The API server
// runs it before it serves, with --import-file.
package migrate

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// Format names the export file format. A reader rejects any other value.
const Format = "cask-export/v1"

// pageSize is the list page size. Later pages read the first page's
// snapshot.
const pageSize = 500

// Header is the first line of an export file.
type Header struct {
	// Format is always Format.
	Format string `json:"format"`
	// Group is the exported API group.
	Group string `json:"group"`
	// Revision is the highest list resourceVersion of the export. On a
	// CRD it is the source etcd revision. Clients hold resourceVersions
	// up to this value. Cask serves index sequences as resourceVersions,
	// so the import starts each index register above Revision.
	//
	// Each object line keeps its source metadata.resourceVersion, which
	// is an etcd revision, not an index sequence. The import replaces it.
	Revision uint64 `json:"revision"`
	// Resources lists the exported resources in file order.
	Resources []ResourceHeader `json:"resources"`
}

// ResourceHeader describes the objects of one resource in the file.
type ResourceHeader struct {
	Version  string `json:"version"`
	Resource string `json:"resource"`
	Kind     string `json:"kind"`
	// ListResourceVersion is the resourceVersion of this resource's list.
	ListResourceVersion uint64 `json:"listResourceVersion"`
	// Count is the number of object lines for this resource.
	Count int `json:"count"`
}

// Resource is one listable resource of the group.
type Resource struct {
	GVR        schema.GroupVersionResource
	Kind       string
	Namespaced bool
}

// Discover returns the listable resources of group at its preferred
// version, sorted by name. It leaves out subresources.
func Discover(d discovery.DiscoveryInterface, group string) ([]Resource, error) {
	groups, err := d.ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("discover groups: %w", err)
	}
	var gv string
	for _, g := range groups.Groups {
		if g.Name == group {
			gv = g.PreferredVersion.GroupVersion
		}
	}
	if gv == "" {
		return nil, fmt.Errorf("the server does not serve group %q", group)
	}
	parsed, err := schema.ParseGroupVersion(gv)
	if err != nil {
		return nil, err
	}
	list, err := d.ServerResourcesForGroupVersion(gv)
	if err != nil {
		return nil, fmt.Errorf("discover %s: %w", gv, err)
	}
	var out []Resource
	for _, r := range list.APIResources {
		if strings.Contains(r.Name, "/") || !hasVerb(r.Verbs, "list") {
			continue
		}
		out = append(out, Resource{
			GVR:        parsed.WithResource(r.Name),
			Kind:       r.Kind,
			Namespaced: r.Namespaced,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("group %s has no listable resources", gv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GVR.Resource < out[j].GVR.Resource })
	return out, nil
}

func hasVerb(verbs []string, verb string) bool {
	for _, v := range verbs {
		if v == verb {
			return true
		}
	}
	return false
}

// Export lists every object of resources through c and writes an export
// file to w. It only reads from the cluster. It refuses a namespaced
// resource and an object with a namespace.
func Export(ctx context.Context, c dynamic.Interface, group string, resources []Resource, w io.Writer) (Header, error) {
	h := Header{Format: Format, Group: group, Resources: []ResourceHeader{}}
	var objects []*unstructured.Unstructured
	for _, r := range resources {
		if r.GVR.Group != group {
			return Header{}, fmt.Errorf("resource %s is not in group %s", r.GVR, group)
		}
		if r.Namespaced {
			return Header{}, fmt.Errorf("resource %s is namespaced; every fleet resource is cluster-scoped", r.GVR)
		}
		items, rv, err := listAll(ctx, c.Resource(r.GVR))
		if err != nil {
			return Header{}, fmt.Errorf("list %s: %w", r.GVR, err)
		}
		for _, o := range items {
			if o.GetNamespace() != "" {
				return Header{}, fmt.Errorf("%s %s/%s has a namespace; every fleet resource is cluster-scoped",
					r.GVR, o.GetNamespace(), o.GetName())
			}
			// The file keeps each object whole, so the import can write
			// the uid and the creationTimestamp back unchanged. An object
			// without them cannot be preserved, so the export stops.
			//= docs/spec/fleet.md#7-migration
			//# The migration MUST preserve each object's uid.
			if o.GetUID() == "" {
				return Header{}, fmt.Errorf("%s %s has no uid", r.GVR, o.GetName())
			}
			//= docs/spec/fleet.md#7-migration
			//# The migration MUST preserve each object's creationTimestamp.
			if ts := o.GetCreationTimestamp(); ts.IsZero() {
				return Header{}, fmt.Errorf("%s %s has no creationTimestamp", r.GVR, o.GetName())
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].GetName() < items[j].GetName() })
		h.Resources = append(h.Resources, ResourceHeader{
			Version:             r.GVR.Version,
			Resource:            r.GVR.Resource,
			Kind:                r.Kind,
			ListResourceVersion: rv,
			Count:               len(items),
		})
		h.Revision = max(h.Revision, rv)
		objects = append(objects, items...)
	}

	bw := bufio.NewWriter(w)
	if err := writeLine(bw, h); err != nil {
		return Header{}, err
	}
	for _, o := range objects {
		// encoding/json sorts map keys, so the same object gives the
		// same line.
		if err := writeLine(bw, o.Object); err != nil {
			return Header{}, err
		}
	}
	if err := bw.Flush(); err != nil {
		return Header{}, err
	}
	return h, nil
}

// listAll lists every object in pages. It returns the resourceVersion of
// the first page. Later pages read the same snapshot.
func listAll(ctx context.Context, c dynamic.ResourceInterface) ([]*unstructured.Unstructured, uint64, error) {
	var (
		out  []*unstructured.Unstructured
		rv   string
		cont string
	)
	for {
		list, err := c.List(ctx, metav1.ListOptions{Limit: pageSize, Continue: cont})
		if err != nil {
			return nil, 0, err
		}
		if rv == "" {
			rv = list.GetResourceVersion()
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
		next := list.GetContinue()
		if next == "" {
			break
		}
		// A server that repeats a token would page forever.
		if next == cont {
			return nil, 0, fmt.Errorf("the server repeated continue token %q", next)
		}
		cont = next
	}
	// The list resourceVersion of a CRD is the etcd revision. The import
	// needs it as a number to seed the index sequence.
	n, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("list resourceVersion %q is not an etcd revision", rv)
	}
	return out, n, nil
}

func writeLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// Read parses an export file. It checks the format. It checks that each
// resource has exactly its header count of objects, each of its kind.
func Read(r io.Reader) (Header, []*unstructured.Unstructured, error) {
	dec := json.NewDecoder(r)
	var h Header
	if err := dec.Decode(&h); err != nil {
		return Header{}, nil, fmt.Errorf("read header: %w", err)
	}
	if h.Format != Format {
		return Header{}, nil, fmt.Errorf("format %q is not %q", h.Format, Format)
	}
	var objects []*unstructured.Unstructured
	for _, rh := range h.Resources {
		apiVersion := schema.GroupVersion{Group: h.Group, Version: rh.Version}.String()
		for i := range rh.Count {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return Header{}, nil, fmt.Errorf("read %s object %d of %d: %w", rh.Resource, i+1, rh.Count, err)
			}
			o := &unstructured.Unstructured{}
			if err := o.UnmarshalJSON(raw); err != nil {
				return Header{}, nil, fmt.Errorf("decode %s object %d: %w", rh.Resource, i+1, err)
			}
			if o.GetAPIVersion() != apiVersion || o.GetKind() != rh.Kind {
				return Header{}, nil, fmt.Errorf("%s object %d is %s %s, want %s %s",
					rh.Resource, i+1, o.GetAPIVersion(), o.GetKind(), apiVersion, rh.Kind)
			}
			objects = append(objects, o)
		}
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Header{}, nil, errors.New("export file has lines after the last counted object")
	}
	return h, objects, nil
}
