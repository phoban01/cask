// Command cask runs a single cask node: a CASPaxos acceptor reachable by peers,
// plus a client HTTP API for key-value and lock operations. It forms a static
// cluster from the --peers list (the permanent static-membership backend; mDNS/
// roster-driven membership layers on top). This makes the system runnable today:
//
//	cask --id 1 --listen :8001 --peers :8001,:8002,:8003
//	cask --id 2 --listen :8002 --peers :8001,:8002,:8003
//	cask --id 3 --listen :8003 --peers :8001,:8002,:8003
//
//	curl -XPUT  localhost:8001/kv/greeting -d 'hello'
//	curl        localhost:8001/kv/greeting
//	curl -XPOST 'localhost:8001/session/me?ttl=30'
//	curl -XPOST 'localhost:8001/lock/widget?session=me'   # -> {"token":1}
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/cluster"
	"github.com/phoban01/cask/internal/discovery"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mtls"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/owner"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
	"github.com/phoban01/cask/internal/transport/nebula"
)

func main() {
	// Subcommands are dispatched before flag parsing.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "gen-certs":
			genCerts(os.Args[2:])
			return
		case "mint":
			runMint(os.Args[2:])
			return
		}
	}

	var (
		id      = flag.Uint64("id", 1, "this node's id (unique per node); ignored when --nebula-config is set (derived from overlay IP)")
		listen  = flag.String("listen", ":8001", "address to serve the client KV/lock API on")
		peers   = flag.String("peers", "", "comma-separated addresses of all nodes (incl. self); empty = single node")
		tport   = flag.String("transport", "connect", "inter-node transport: connect (ConnectRPC/protobuf) or http (interim JSON)")
		nebConf = flag.String("nebula-config", "", "path to a Nebula config; when set, the cluster self-forms over the overlay instead of using --peers")
		ovPort  = flag.Int("overlay-port", 8001, "port the node serves consensus RPC on over the Nebula overlay")
		client  = flag.Bool("client-only", false, "join the overlay and route to the roster's replicas without joining consensus (no roster Add, no health monitor); requires --nebula-config")
		boot    = flag.Bool("bootstrap", false, "found a new cluster as the single initial roster member (exactly one node; every other node joins an existing cluster)")
		discSRV = flag.String("discovery-srv", "", "DNS-SRV name for dynamic peer discovery, e.g. _cask._tcp.cask.svc.cluster.local; unioned into the reconcile loop")
		seed    = flag.String("seed", "", "comma-separated overlay addresses of known cask members to bootstrap discovery from, e.g. 10.42.0.7:8001")
		mintURL = flag.String("mint", "", "URL of a `cask mint` endpoint; enroll for an identity instead of reading --nebula-config")
		token   = flag.String("token", "", "enrollment token presented to --mint")
		zone    = flag.String("zone", "", "this node's failure domain (zone), sent at enrollment")
		role    = flag.String("role", "replica", "enrollment role: replica or client")
		dataDir = flag.String("data-dir", "", "directory for durable consensus state (Pebble); empty = in-memory (state lost on restart)")
	)
	var consensusFiles mtls.Files
	consensusFiles.Register(flag.CommandLine)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	consensusTLS, err := setupConsensusTLS(consensusFiles, *nebConf != "" || *mintURL != "", log)
	if err != nil {
		log.Error("consensus TLS", "err", err)
		os.Exit(1)
	}
	var network transport.Network = transport.TCP{}
	if consensusTLS != nil {
		//= docs/spec/fleet.md#8-security
		//# Consensus traffic between members MUST use mutual TLS.
		network = transport.TLS{Net: transport.TCP{}, Server: consensusTLS.Server, Client: consensusTLS.Client}
		log.Info("consensus traffic and the client API on --listen use mutual TLS")
	}

	// Local acceptor: durable consensus state for this node, served to peers
	// over the chosen transport. With --data-dir, registers live in Pebble and
	// survive restarts (every acknowledged Store is fsynced; group commit
	// amortizes the syncs); without it, memory only — fine for tests and
	// demos, but every "safe under crash" claim needs the durable store.
	var acceptorStore caspaxos.Storage = store.NewMem()
	if *dataDir != "" {
		p, err := store.NewPebble(*dataDir, store.WithGroupCommit())
		if err != nil {
			log.Error("open data dir", "dir", *dataDir, "err", err)
			os.Exit(1)
		}
		defer p.Close()
		acceptorStore = p
		log.Info("durable store open", "dir", *dataDir)
	}
	localAcc := caspaxos.NewAcceptor(acceptorStore)
	clock := hlc.New(func() int64 { return time.Now().UnixNano() })

	// prop drives mvcc/lease: a flat proposer over --peers, or a self-forming,
	// placement-routed engine over the Nebula overlay.
	var prop engine
	nodeID := *id
	ctx := context.Background()

	// Stale-routing rejection (§4.1): the consensus handlers compare each
	// request's claimed range epoch against the CURRENT placement, resolved
	// from the roster snapshot installed once the overlay is up. Until then
	// (and forever in static --peers mode, which has no descriptor epochs)
	// the check is disabled.
	var epochSnap atomic.Pointer[cluster.Snap]
	epochOf := transport.EpochOf(func(key []byte) (uint64, bool) {
		s := epochSnap.Load()
		if s == nil {
			return 0, false
		}
		v, ok := s.Load()
		if !ok {
			return 0, false
		}
		// §4.3: current epochs come from descriptor registers (distributed
		// via the roster snapshot), falling back to roster-derived placement
		// until the genesis descriptor is seeded.
		if d, ok := rmapFromSnap(s, v).Lookup(key); ok {
			return d.Epoch, true
		}
		return 0, false
	})

	mux := http.NewServeMux()
	if *tport == "http" {
		mux.Handle("/v1/", transport.Handler(localAcc, transport.WithEpochOf(epochOf)))
	} else {
		mux.Handle(transport.ConnectHandler(localAcc, transport.WithEpochOf(epochOf)))
	}
	// §4.3 carry-forward key enumeration: the reconfiguration driver reads a
	// majority of the old replicas' key lists through this endpoint.
	mux.HandleFunc("/rangekeys", func(w http.ResponseWriter, r *http.Request) {
		l, ok := acceptorStore.(store.Lister)
		if !ok {
			http.Error(w, "store cannot enumerate keys", http.StatusNotImplemented)
			return
		}
		keys, err := l.Keys(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(keys)
	})

	// The overlay path gets its config either from a file (--nebula-config) or by
	// enrolling at a `cask mint` endpoint (--mint).
	var configYAML string
	useOverlay := false
	clientOnly := *client
	srvName := *discSRV
	switch {
	case *mintURL != "":
		cfg, co, srv, err := enroll(ctx, *mintURL, *token, *zone, *role)
		if err != nil {
			log.Error("enroll", "err", err)
			os.Exit(1)
		}
		configYAML, useOverlay = cfg, true
		clientOnly = co || *client
		if srvName == "" {
			srvName = srv
		}
	case *nebConf != "":
		raw, err := os.ReadFile(*nebConf)
		if err != nil {
			log.Error("read nebula config", "err", err)
			os.Exit(1)
		}
		configYAML, useOverlay = string(raw), true
	}

	var (
		fwd *forwarder
		mgr *owner.Manager
	)
	if useOverlay {
		disco := buildDisco(log, srvName, *seed)
		dyn, overlayLn, self, mon, snap, f, m, admin, err := nebulaCluster(ctx, log, configYAML, *ovPort, localAcc, acceptorStore, clientOnly, *boot, disco)
		if err != nil {
			log.Error("nebula cluster", "err", err)
			os.Exit(1)
		}
		prop, nodeID, fwd, mgr = dyn, self.NodeID, f, m
		epochSnap.Store(snap)
		// §4.3 lifecycle operations (driver-gated, like joins).
		if admin != nil {
			mux.HandleFunc("/admin/split", admin.serveSplit)
			mux.HandleFunc("/admin/merge", admin.serveMerge)
		}
		// Peers probe this endpoint over the overlay; its reply is both a
		// liveness heartbeat and this node's suspicion vector for cut detection.
		// A client-only node is never in the roster, so nothing probes it (mon is nil).
		if mon != nil {
			mux.HandleFunc("/health", mon.serveHealth)
		}
		// Joining peers read the current roster acceptor core from here, and
		// register through the driver here (the driver is the sole register writer).
		mux.HandleFunc("/roster", snap.Serve)
		mux.HandleFunc("/roster/join", snap.ServeJoin)
		// Consensus rides the overlay; the client API rides the host listener
		// below so operators can still curl localhost.
		go func() {
			if err := transport.NewServer(mux).Serve(overlayLn); err != nil {
				log.Error("overlay server stopped", "err", err)
			}
		}()
	} else {
		clients := buildClients(*tport, *listen, *peers, localAcc, network.HTTPClient())
		log.Info("starting", "id", *id, "listen", *listen, "transport", *tport, "replicas", len(clients))
		prop = caspaxos.NewProposer(*id, clients, caspaxos.WithBackoff(contentionBackoff()))
	}

	// With the M7 forwarder routing client writes to the range owner and the
	// FullPathGate blocking writes around a live read lease, lease-guarded
	// local reads are sound cluster-wide — enable them when a manager exists.
	var kvOpts []mvcc.KVOption
	if mgr != nil {
		kvOpts = append(kvOpts, mvcc.WithLocalReader(mgr))
	}
	kv := mvcc.New(prop, clock, nodeID, kvOpts...)
	sessions := lease.NewSessions(prop, func() int64 { return time.Now().UnixNano() })
	locks := lease.NewLocks(prop, sessions, lease.WithAcquireBackoff(contentionBackoff()))
	srv := &server{kv: kv, sessions: sessions, locks: locks, fwd: fwd, log: log}

	mux.HandleFunc("/kv/", srv.handleKV) // client KV API
	mux.HandleFunc("/cas/", srv.handleCAS)
	mux.HandleFunc("/session/", srv.handleSession)
	mux.HandleFunc("/lock/", srv.handleLock)

	ln, err := network.Listen(ctx, *listen)
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}
	if err := transport.NewServer(mux).Serve(ln); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// setupConsensusTLS returns the consensus TLS configs for the static
// topology. That topology serves consensus and the client API on --listen,
// so it refuses to start without the three consensus TLS flags, unless
// f.Insecure is set. With the flags, the listener needs a client
// certificate from the fleet CA, and every peer call presents one.
//
// In overlay mode (--nebula-config or --mint), the Nebula overlay
// authenticates peers with its own certificates. The consensus TLS flags
// and --insecure-consensus do not apply there, and it returns nil.
func setupConsensusTLS(f mtls.Files, overlay bool, log *slog.Logger) (*mtls.Config, error) {
	if overlay {
		if f.Any() || f.Insecure {
			return nil, errors.New("--consensus-cert, --consensus-key, --consensus-ca, and --insecure-consensus apply to the TCP transport; the Nebula overlay authenticates peers with its own certificates")
		}
		// Overlay mode still serves the consensus mux in plaintext on the
		// host --listen port, next to the client API.
		//= docs/spec/fleet.md#8-security
		//= type=exception
		//= reason=overlay mode also serves consensus on the host --listen port in plaintext; tracked in issue #163
		//# Consensus traffic between members MUST use mutual TLS.
		return nil, nil
	}
	//= docs/spec/fleet.md#8-security
	//# Consensus traffic between members MUST use mutual TLS.
	return mtls.Setup(f, log)
}

// engine is the consensus operation mvcc and lease drive — satisfied by both a
// flat *caspaxos.Proposer and the overlay *dynamicProposer.
type engine interface {
	Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error)
}

// buildClients returns one AcceptorClient per node: the local acceptor for our
// own address, a remote client (per the chosen transport) for every other peer.
func buildClients(mode, self, peers string, local *caspaxos.Acceptor, hc *http.Client) []caspaxos.AcceptorClient {
	if peers == "" {
		return []caspaxos.AcceptorClient{local}
	}
	dial := func(addr string) caspaxos.AcceptorClient {
		base := "http://" + httpHost(addr)
		if mode == "http" {
			return transport.NewClient(base, hc)
		}
		return transport.NewConnectClient(base, hc)
	}
	var out []caspaxos.AcceptorClient
	for _, addr := range strings.Split(peers, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if addr == self {
			out = append(out, local)
		} else {
			out = append(out, dial(addr))
		}
	}
	return out
}

// buildDisco assembles the reconcile-loop discovery from explicit seed members
// and/or a DNS-SRV name (unioned, seeds first). Returns nil when neither is set.
func buildDisco(log *slog.Logger, srvName, seedCSV string) roster.Discovery {
	var sources []roster.Discovery
	if seeds := parseSeedMembers(seedCSV); len(seeds) > 0 {
		sources = append(sources, roster.SeedDiscovery(seeds))
	}
	if srvName != "" {
		service, proto, domain, ok := parseSRVName(srvName)
		if !ok {
			log.Error("bad --discovery-srv (want _service._proto.domain)", "value", srvName)
			os.Exit(1)
		}
		sources = append(sources, discovery.SRVDiscovery{Service: service, Proto: proto, Domain: domain})
	}
	switch len(sources) {
	case 0:
		return nil
	case 1:
		return sources[0]
	default:
		return discovery.Union(sources)
	}
}

// parseSeedMembers turns "10.42.0.7:8001,10.42.0.9:8001" into roster members,
// deriving each NodeID from its overlay IP (the cluster-wide identity rule).
func parseSeedMembers(csv string) []roster.Member {
	var out []roster.Member
	for _, addr := range strings.Split(csv, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			continue
		}
		out = append(out, roster.Member{NodeID: nebula.NodeIDFromIP(ip.Unmap()), Addr: addr})
	}
	return out
}

// parseSRVName splits a DNS-SRV name "_service._proto.domain" into its parts.
func parseSRVName(s string) (service, proto, domain string, ok bool) {
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	service = strings.TrimPrefix(parts[0], "_")
	proto = strings.TrimPrefix(parts[1], "_")
	domain = parts[2]
	return service, proto, domain, service != "" && proto != "" && domain != ""
}

// httpHost turns a ":8002" or "host:8002" listen address into a dialable host.
func httpHost(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

type server struct {
	kv       *mvcc.KV
	sessions *lease.Sessions
	locks    *lease.Locks
	fwd      *forwarder // nil outside the overlay topology
	log      *slog.Logger
}

func (s *server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := []byte(strings.TrimPrefix(r.URL.Path, "/kv/"))
	// M7: route the op to the range owner — its fast path serves writes in
	// one accept round and reads with zero rounds, and its router keeps the
	// read cache coherent. Local handling below remains correct either way
	// (the FullPathGate turns an unsafe write-around into a retryable 503).
	if s.fwd.maybeForward(w, r, key) {
		return
	}
	ctx := r.Context()
	switch r.Method {
	case http.MethodGet:
		v, found, err := s.kv.Get(ctx, key)
		if err != nil {
			httpErr(w, err)
			return
		}
		if !found {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write(v)
	case http.MethodPut, http.MethodPost:
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if _, err := s.kv.Put(ctx, key, body); err != nil {
			httpErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if _, err := s.kv.Delete(ctx, key); err != nil {
			httpErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleCAS(w http.ResponseWriter, r *http.Request) {
	key := []byte(strings.TrimPrefix(r.URL.Path, "/cas/"))
	if s.fwd.maybeForward(w, r, key) {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var expect []byte
	if e := r.URL.Query().Get("expect"); e != "" {
		expect = []byte(e)
	}
	if _, err := s.kv.CAS(r.Context(), key, expect, body); err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSession(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/session/")
	id, keepalive := strings.CutSuffix(rest, "/keepalive")
	ttl := time.Duration(queryInt(r, "ttl", 30)) * time.Second
	var err error
	if keepalive {
		_, err = s.sessions.KeepAlive(r.Context(), id, id, int64(ttl))
	} else {
		_, err = s.sessions.Grant(r.Context(), id, id, int64(ttl))
	}
	if err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleLock(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/lock/")
	session := r.URL.Query().Get("session")
	ctx := r.Context()
	switch r.Method {
	case http.MethodPost:
		token, err := s.locks.Acquire(ctx, name, session)
		if err != nil {
			httpErr(w, err)
			return
		}
		json.NewEncoder(w).Encode(map[string]uint64{"token": token})
	case http.MethodDelete:
		if err := s.locks.Release(ctx, name, session); err != nil {
			httpErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func httpErr(w http.ResponseWriter, err error) {
	// A live owner lease is a transient routing condition, not a conflict:
	// the client retries (or the next forward attempt reaches the owner).
	if errors.Is(err, owner.ErrOwnerLive) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	http.Error(w, err.Error(), http.StatusConflict)
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
