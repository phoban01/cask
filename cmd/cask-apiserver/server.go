package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// apiServer serves the fleet.cask.dev/v1alpha1 group in the shapes kubectl
// and the aggregation layer expect: discovery documents, typed CRUD with
// resourceVersion optimistic concurrency, list wrappers, and line-delimited
// watch streams.
type apiServer struct {
	cluster string
	store   *fleetStore
	claims  *claimController
	log     *slog.Logger

	// watchPoll is the poll-diff interval for watch streams (tests shrink it).
	watchPoll time.Duration
}

func newAPIServer(cluster string, store *fleetStore, log *slog.Logger) *apiServer {
	return &apiServer{
		cluster:   cluster,
		store:     store,
		claims:    &claimController{cluster: cluster, store: store, log: log},
		log:       log,
		watchPoll: 500 * time.Millisecond,
	}
}

func (s *apiServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/apis", s.serveGroupList)
	mux.HandleFunc("/apis/"+apiGroup, s.serveGroup)
	mux.HandleFunc(groupPrefix, s.serveResourceList)
	mux.HandleFunc(groupPrefix+"/devices", s.collection("devices"))
	mux.HandleFunc(groupPrefix+"/devices/", s.item("devices"))
	mux.HandleFunc(groupPrefix+"/deviceclaims", s.collection("deviceclaims"))
	mux.HandleFunc(groupPrefix+"/deviceclaims/", s.item("deviceclaims"))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

// --- discovery (what registration as an APIService requires) ---------------

func (s *apiServer) serveGroupList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": "APIGroupList", "apiVersion": "v1",
		"groups": []any{groupDoc()},
	})
}

func (s *apiServer) serveGroup(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, groupDoc())
}

func groupDoc() map[string]any {
	gv := map[string]any{"groupVersion": apiGroup + "/" + apiVersion, "version": apiVersion}
	return map[string]any{
		"kind": "APIGroup", "apiVersion": "v1", "name": apiGroup,
		"versions": []any{gv}, "preferredVersion": gv,
	}
}

func (s *apiServer) serveResourceList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": "APIResourceList", "apiVersion": "v1",
		"groupVersion": apiGroup + "/" + apiVersion,
		"resources": []any{
			map[string]any{"name": "devices", "singularName": "device", "kind": "Device",
				"namespaced": false, "verbs": []string{"create", "get", "list", "watch", "update", "delete"}},
			map[string]any{"name": "deviceclaims", "singularName": "deviceclaim", "kind": "DeviceClaim",
				"namespaced": false, "verbs": []string{"create", "get", "list", "watch", "update", "delete"}},
		},
	})
}

// --- collection and item handlers -------------------------------------------

func (s *apiServer) collection(resource string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("watch") == "true":
			s.serveWatch(w, r, resource)
		case r.Method == http.MethodGet:
			s.serveList(w, r, resource)
		case r.Method == http.MethodPost:
			s.serveCreate(w, r, resource)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (s *apiServer) item(resource string) http.HandlerFunc {
	prefix := groupPrefix + "/" + resource + "/"
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, prefix)
		if name == "" || strings.Contains(name, "/") {
			http.Error(w, "bad name", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			raw, rv, err := s.store.get(r.Context(), resource, name)
			if err != nil {
				httpStoreErr(w, err)
				return
			}
			writeRaw(w, http.StatusOK, stampRV(raw, rv))
		case http.MethodPut:
			s.serveUpdate(w, r, resource, name)
		case http.MethodDelete:
			s.serveDelete(w, r, resource, name)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (s *apiServer) serveCreate(w http.ResponseWriter, r *http.Request, resource string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name, raw, err := s.normalize(resource, body, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rv, err := s.store.create(r.Context(), resource, name, raw)
	if err != nil {
		httpStoreErr(w, err)
		return
	}
	writeRaw(w, http.StatusCreated, stampRV(raw, rv))
}

func (s *apiServer) serveUpdate(w http.ResponseWriter, r *http.Request, resource, name string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var meta struct {
		Metadata ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal(body, &meta); err != nil || meta.Metadata.Name != name {
		http.Error(w, "body/name mismatch", http.StatusBadRequest)
		return
	}
	expect, err := parseRV(meta.Metadata.ResourceVersion)
	if err != nil {
		httpStoreErr(w, err)
		return
	}
	_, raw, err := s.normalize(resource, body, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rv, err := s.store.update(r.Context(), resource, name, raw, expect)
	if err != nil {
		httpStoreErr(w, err)
		return
	}
	writeRaw(w, http.StatusOK, stampRV(raw, rv))
}

func (s *apiServer) serveDelete(w http.ResponseWriter, r *http.Request, resource, name string) {
	// Deleting a Bound claim releases its device's global lease.
	if resource == "deviceclaims" {
		if raw, _, err := s.store.get(r.Context(), resource, name); err == nil {
			var claim DeviceClaim
			if json.Unmarshal(raw, &claim) == nil {
				s.claims.release(r.Context(), claim)
			}
		}
	}
	if err := s.store.delete(r.Context(), resource, name); err != nil {
		httpStoreErr(w, err)
		return
	}
	// A metav1.Status with an application/json content-type: kubectl decodes
	// the delete response and errors ("serializer for text/plain ... doesn't
	// exist") if the content-type is sniffed instead of set — which is what a
	// premature WriteHeader before writeJSON would cause.
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Success",
		"details": map[string]any{"name": name, "group": apiGroup, "kind": resource},
	})
}

func (s *apiServer) serveList(w http.ResponseWriter, r *http.Request, resource string) {
	_, raws, rvs, err := s.store.list(r.Context(), resource)
	if err != nil {
		httpStoreErr(w, err)
		return
	}
	items := make([]json.RawMessage, len(raws))
	for i := range raws {
		items[i] = stampRV(raws[i], rvs[i])
	}
	kind := "DeviceList"
	if resource == "deviceclaims" {
		kind = "DeviceClaimList"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": kind, "apiVersion": apiGroup + "/" + apiVersion, "items": items,
	})
}

// serveWatch streams k8s watch events by poll-diffing resourceVersions — the
// interim shape §3.5's Plumtree push upgrades later. Correctness over
// promptness: every event reflects a linearizable read.
func (s *apiServer) serveWatch(w http.ResponseWriter, r *http.Request, resource string) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fl.Flush() // ship the headers now: an empty watch emits nothing else to flush them
	enc := json.NewEncoder(w)

	seen := map[string]uint64{}
	emit := func(typ string, raw json.RawMessage) bool {
		if err := enc.Encode(WatchEvent{Type: typ, Object: raw}); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	for {
		names, raws, rvs, err := s.store.list(r.Context(), resource)
		if err == nil {
			live := map[string]bool{}
			for i, name := range names {
				live[name] = true
				prev, known := seen[name]
				switch {
				case !known:
					if !emit("ADDED", stampRV(raws[i], rvs[i])) {
						return
					}
				case rvs[i] > prev:
					if !emit("MODIFIED", stampRV(raws[i], rvs[i])) {
						return
					}
				}
				seen[name] = rvs[i]
			}
			for name := range seen {
				if !live[name] {
					delete(seen, name)
					if !emit("DELETED", json.RawMessage(fmt.Sprintf(`{"metadata":{"name":%q}}`, name))) {
						return
					}
				}
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(s.watchPoll):
		}
	}
}

// normalize validates a submitted object, stamps type/cluster fields, and
// returns its storage form (resourceVersion stripped — RV lives in MVCC).
func (s *apiServer) normalize(resource string, body []byte, isCreate bool) (string, []byte, error) {
	switch resource {
	case "devices":
		var d Device
		if err := json.Unmarshal(body, &d); err != nil {
			return "", nil, err
		}
		if d.Name == "" {
			return "", nil, errors.New("metadata.name required")
		}
		d.TypeMeta = deviceTypeMeta()
		d.ResourceVersion = ""
		if isCreate {
			d.Status = DeviceStatus{Phase: DeviceAvailable}
		}
		raw, err := json.Marshal(d)
		return d.Name, raw, err
	case "deviceclaims":
		var c DeviceClaim
		if err := json.Unmarshal(body, &c); err != nil {
			return "", nil, err
		}
		if c.Name == "" || c.Spec.DeviceName == "" {
			return "", nil, errors.New("metadata.name and spec.deviceName required")
		}
		c.TypeMeta = claimTypeMeta()
		c.ResourceVersion = ""
		if isCreate {
			// Claims are managed by the cluster whose apiserver admitted them.
			c.Status = DeviceClaimStatus{Phase: ClaimPending, Cluster: s.cluster}
		}
		raw, err := json.Marshal(c)
		return c.Name, raw, err
	default:
		return "", nil, fmt.Errorf("unknown resource %q", resource)
	}
}

// runReconciler drives claim binding on an interval until ctx ends.
func (s *apiServer) runReconciler(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.claims.reconcileOnce(ctx)
		}
	}
}

// --- plumbing ---------------------------------------------------------------

// stampRV injects the storage resourceVersion into an object's metadata.
func stampRV(raw []byte, rv uint64) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	var meta map[string]any
	if err := json.Unmarshal(obj["metadata"], &meta); err != nil {
		return raw
	}
	meta["resourceVersion"] = formatRV(rv)
	mraw, err := json.Marshal(meta)
	if err != nil {
		return raw
	}
	obj["metadata"] = mraw
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

func httpStoreErr(w http.ResponseWriter, err error) {
	// kubectl decodes error bodies as metav1.Status; a text/plain body (what
	// http.Error emits) fails with "serializer for text/plain ... doesn't
	// exist". Emit a real Status so 404/409 surface as clean kubectl errors.
	code, reason := http.StatusInternalServerError, "InternalError"
	switch {
	case errors.Is(err, errNotFound):
		code, reason = http.StatusNotFound, "NotFound"
	case errors.Is(err, errConflict):
		code, reason = http.StatusConflict, "Conflict"
	}
	writeJSON(w, code, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"message": err.Error(), "reason": reason, "code": code,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, code int, raw json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}
