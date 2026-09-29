package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"github.com/phoban01/cask/cmd/cask-apiserver/storage"
	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mtls"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
	"github.com/spf13/pflag"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gen-consensus-certs" {
		if err := genConsensusCerts(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "gen-consensus-certs:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "gen-serving-certs" {
		if err := genServingCerts(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "gen-serving-certs:", err)
			os.Exit(1)
		}
		return
	}
	var (
		legacy   = flag.Bool("legacy-http", false, "DEPRECATED, kept for one release: serve the old net/http mux on --listen instead of the generic server; it has no authentication")
		listen   = flag.String("listen", ":9443", "address to serve the legacy mux on; with --legacy-http only (the generic server uses --bind-address and --secure-port)")
		cluster  = flag.String("cluster", "", "this cluster's name (stamped on claims it manages); required")
		boot     = flag.Bool("bootstrap", false, "found a new fleet as its single founding member (exactly one apiserver in a fleet; every other one joins with --seed)")
		seed     = flag.String("seed", "", "comma-separated consensus addresses of live fleet members to join through, e.g. 10.0.0.7:9444")
		peers    = flag.String("cask-peers", "", "DEPRECATED, tests only: comma-separated static consensus addresses; use --bootstrap or --seed")
		consLn   = flag.String("listen-consensus", "", "address to serve the EMBEDDED cask acceptor and the roster endpoints on; required with --bootstrap and --seed")
		adv      = flag.String("advertise-consensus", "", "this node's consensus address as peers reach it; required with --listen-consensus")
		self     = flag.Uint64("id", 0, "node id (unique per apiserver); required with --bootstrap, --seed, and --cask-peers")
		dataDir  = flag.String("data-dir", "", "directory for durable consensus state (Pebble); empty = in-memory (an embedded acceptor that restarts empty forgets its promises — demo only)")
		sweepIv  = flag.Duration("index-sweep-interval", time.Minute, "how often to repair index entries that a crash left behind (each wait adds up to 10% jitter)")
		impFile  = flag.String("import-file", "", "cask-export/v1 file to import before serving (see cask-migrate export); safe to repeat")
		impGrace = flag.Duration("import-grace", defaultImportGrace, "extra session time for each Bound claim that the import restores; it covers the import and the start of the claim's controller")
		selfTLS  = flag.Bool("self-signed-tls", false, "serve the legacy mux over HTTPS with an in-memory self-signed cert; with --legacy-http only")
	)
	var consensusFiles mtls.Files
	consensusFiles.Register(flag.CommandLine)
	// The generic server flags are pflags. The cask flags join them, so
	// --help lists both.
	serverOpts := newServerOptions()
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	serverOpts.addFlags(pflag.CommandLine)
	pflag.Parse()
	// The runbook in docs/runbooks/majority-loss.md needs --force-new-fleet,
	// which does not exist yet. Until it lands, the apiserver has no
	// supported majority-loss recovery.
	//= docs/spec/fleet.md#9-operations
	//= type=exception
	//= reason=the runbook calls --force-new-fleet, which is missing; tracked in issue #74
	//# A majority-loss recovery procedure MUST be documented.
	//= docs/spec/fleet.md#9-operations
	//= type=exception
	//= reason=no rehearsal on kind yet; tracked in issue #97
	//# The majority-loss recovery procedure MUST be rehearsed before phase two.
	// The demo PodDisruptionBudget covers one cluster only. The order across
	// clusters is a manual step in docs/runbooks/rolling-upgrade.md. Nothing
	// stops two clusters from upgrading at the same time.
	//= docs/spec/fleet.md#9-operations
	//= type=exception
	//= reason=docs/runbooks/rolling-upgrade.md orders the upgrade by hand; nothing enforces or runs it; tracked in issue #194
	//# A rolling upgrade MUST keep a majority of voters available at all times.
	//= docs/spec/fleet.md#9-operations
	//= type=exception
	//= reason=no metrics endpoint and no quorum signal; tracked in issue #62
	//# Cask MUST expose a health signal for quorum state.
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *cluster == "" {
		log.Error("--cluster is required (e.g. --cluster eu-west-a)")
		os.Exit(1)
	}
	if *sweepIv <= 0 {
		log.Error("--index-sweep-interval must be positive", "value", *sweepIv)
		os.Exit(1)
	}
	// A member that serves or calls consensus over the network needs the
	// three consensus TLS flags. It refuses to start without them, unless
	// --insecure-consensus asks for plaintext. The process-local register
	// has no consensus traffic, so it needs neither.
	consensusTLS, err := setupConsensusTLS(consensusFiles, *consLn != "" || *peers != "" || *boot || *seed != "", log)
	if err != nil {
		log.Error("consensus TLS", "err", err)
		os.Exit(1)
	}
	if consensusTLS != nil {
		log.Info("consensus traffic uses mutual TLS", "cert", consensusFiles.Cert, "ca", consensusFiles.CA)
	}
	network := consensusNetwork(consensusTLS)

	// The storage engine. cask is designed to be embedded: with
	// --listen-consensus each apiserver carries its own acceptor and the
	// apiservers ARE the consensus fleet — no external cask processes. One
	// apiserver founds the roster with --bootstrap; the others join it with
	// --seed and start as participants. The deprecated alternatives are proposer-only mode over an existing fleet
	// (--cask-peers without --listen-consensus) and a process-local
	// single-node register for dev. Whichever way, every apiserver proposes
	// over the SAME replicas — that shared consensus is what makes one
	// device lease globally exclusive.
	ctx := context.Background()
	dynamic := *boot || *seed != ""
	switch {
	case *boot && *seed != "":
		log.Error("--bootstrap and --seed are exclusive: one apiserver founds the fleet, the others join it")
		os.Exit(1)
	case dynamic && *peers != "":
		log.Error("--cask-peers cannot be combined with --bootstrap or --seed")
		os.Exit(1)
	case dynamic && (*consLn == "" || *adv == ""):
		log.Error("--bootstrap and --seed need --listen-consensus and --advertise-consensus")
		os.Exit(1)
	}
	var (
		prop mvcc.Proposer
		// listKeys lists the data keys on a majority of the acceptors,
		// for the index sweep.
		listKeys storage.KeyLister
	)
	switch {
	case *peers == "" && !dynamic:
		log.Warn("embedded single-node cask: state is process-local and non-durable (demo mode)")
		st := store.NewMem()
		prop = caspaxos.NewProposer(1, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(st)})
		// The one acceptor is a majority.
		listKeys = st.Keys
	default:
		if *self == 0 {
			log.Error("--id required with --bootstrap, --seed, or --cask-peers (unique per apiserver)")
			os.Exit(1)
		}
		if !dynamic {
			log.Warn("--cask-peers is deprecated and kept for tests only: a static peer list cannot grow the fleet; use --bootstrap or --seed")
		}
		var (
			local *caspaxos.Acceptor
			mem   *membership
		)
		if *consLn != "" {
			if *adv == "" {
				log.Error("--advertise-consensus required with --listen-consensus (this node's entry in --cask-peers)")
				os.Exit(1)
			}
			var st caspaxos.Storage = store.NewMem()
			if *dataDir != "" {
				// The acceptor keeps its promises and accepted values in
				// Pebble under --data-dir. The demo mounts a
				// PersistentVolumeClaim there.
				//= docs/spec/fleet.md#9-operations
				//# Every voter MUST persist its acceptor state to a durable volume.
				p, err := store.NewPebble(*dataDir, store.WithGroupCommit())
				if err != nil {
					log.Error("open data dir", "dir", *dataDir, "err", err)
					os.Exit(1)
				}
				defer p.Close()
				st = p
				log.Info("durable store open", "dir", *dataDir)
			} else {
				// A voter can still start without --data-dir.
				//= docs/spec/fleet.md#9-operations
				//= type=exception
				//= reason=an embedded acceptor may run in memory; tracked in issue #95
				//# Every voter MUST persist its acceptor state to a durable volume.
				log.Warn("embedded acceptor is in-memory: a restart wipes its promises, which is unsafe for consensus — set --data-dir for anything beyond a demo")
			}
			local = caspaxos.NewAcceptor(st)
			var h http.Handler
			if dynamic {
				// Every peer call carries a context, and the client also has
				// a timeout, so no peer request can hang forever.
				hc := network.HTTPClient()
				hc.Timeout = 10 * time.Second
				mem = newMembership(membershipConfig{
					ID:        *self,
					Advertise: *adv,
					Bootstrap: *boot,
					Seeds:     parseSeeds(*seed),
					Local:     local,
					Store:     st,
					HTTP:      hc,
				}, log)
				h = mem.handler()
			} else {
				mux := http.NewServeMux()
				mux.Handle(transport.ConnectHandler(local))
				h = mux
			}
			// The listener serves the acceptor, the roster endpoints, and
			// the admin endpoints only to a peer with a certificate that
			// the fleet CA signed. setupConsensusTLS refused to start
			// without the consensus TLS flags, so the only plaintext
			// listener is one that --insecure-consensus asked for.
			//= docs/spec/fleet.md#8-security
			//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
			ln, err := network.Listen(ctx, *consLn)
			if err != nil {
				log.Error("consensus listen", "listen", *consLn, "err", err)
				os.Exit(1)
			}
			go func() {
				log.Info("embedded cask acceptor serving", "listen", *consLn, "mtls", consensusTLS != nil)
				if err := transport.NewServer(h).Serve(ln); err != nil {
					log.Error("consensus server stopped", "err", err)
					os.Exit(1)
				}
			}()
		}
		if dynamic {
			// The founder creates the roster; a joiner asks the driver to add
			// it as a participant. Both run the same code as cmd/cask.
			if err := mem.start(ctx); err != nil {
				log.Error("join fleet", "err", err)
				os.Exit(1)
			}
			prop = mem
			listKeys = mem.listDataKeys
			break
		}
		hc := network.HTTPClient()
		var clients []caspaxos.AcceptorClient
		for _, addr := range splitCSV(*peers) {
			if local != nil && addr == *adv {
				clients = append(clients, local)
				continue
			}
			clients = append(clients, transport.NewConnectClient("http://"+addr, hc))
		}
		prop = caspaxos.NewProposer(*self, clients,
			caspaxos.WithBackoff(backoff.FullJitter(5*time.Millisecond, 500*time.Millisecond)))
	}

	clock := hlc.New(func() int64 { return time.Now().UnixNano() })
	// The KV names its writes by the member id. It adds a random
	// incarnation, so a restart or a second apiserver with the same id
	// never reuses an operation id. The PID is no name: it is 1 in every
	// pod (issue #170).
	kv := mvcc.New(prop, clock, *self)
	sessions := lease.NewSessions(prop, func() int64 { return time.Now().UnixNano() })
	locks := lease.NewLocks(prop, sessions)
	// fs seeds the claim locks for an import on either path, and it is
	// the store of the legacy mux.
	fs := &fleetStore{kv: kv, sessions: sessions, locks: locks, importGrace: *impGrace}

	// Repair the index before the server serves, so no list misses an
	// object whose index write a crash lost.
	if listKeys != nil {
		sw := &indexSweeper{kv: kv, list: listKeys, log: log}
		if err := sw.sweepAtStartup(ctx, time.Second); err != nil {
			log.Error("index sweep at startup", "err", err)
			os.Exit(1)
		}
		log.Info("index sweep at startup done")
		go sw.sweepEvery(ctx, *sweepIv)
	} else {
		// A proposer over a static peer list cannot list keys on the peers.
		log.Warn("no index sweep: --cask-peers cannot list keys on a majority; use --bootstrap or --seed")
	}

	// The import runs in this process, through the same proposer as every
	// other write, before the server listens. No client write on this
	// cluster can race it, and it needs no new endpoint.
	if *impFile != "" {
		if err := importFile(ctx, kv, fs, *impFile, log); err != nil {
			log.Error("import", "file", *impFile, "err", err)
			os.Exit(1)
		}
	}

	// The import writes a marker last, but no readiness gate reads it yet,
	// so nothing checks imported owners before the server serves.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=tracked in issue #40
	//# The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no readiness gate reads the import marker; tracked in issue #40
	//# The APIService MUST NOT become available before the import has completed.
	// cask-migrate export, the import, and the cutover runbook exist. The
	// export CronJob and the rehearsal do not exist yet. The freeze is a
	// manual step in docs/runbooks/cutover.md. No code enforces it.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=docs/runbooks/cutover.md freezes writers by hand; nothing enforces or rehearses it; tracked in issue #54
	//# Writers MUST be frozen from the start of the export until the APIService is available.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=the import starts each index register at sequence 1, not above the revision; tracked in issue #51
	//# The initial index sequence for each resource type MUST be greater than the source etcd revision at export time.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=cask-migrate export runs only by hand; no CronJob yet; tracked in issue #53
	//# A continuous export of all fleet objects MUST run from the first day of phase one.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no rehearsal test on kind; tracked in issue #54
	//# The cutover MUST be rehearsed on a copy of the management cluster before it runs on the real one.
	if *legacy {
		if err := serveLegacy(ctx, *cluster, fs, *listen, *selfTLS, log); err != nil {
			log.Error("server stopped", "err", err)
			os.Exit(1)
		}
		return
	}

	// The generic server and the claim controller share one cask store
	// per resource, so a status write wakes the watches of that store.
	stores := fleetStores(kv)
	claims := &claimController{
		cluster:  *cluster,
		store:    storageClaims{devices: stores["devices"], claims: stores["deviceclaims"]},
		sessions: sessions,
		locks:    locks,
		log:      log,
	}
	// A claim delete releases the claim inside the delete of the store,
	// so the device keeps the fence before the claim goes.
	srv, err := serverOpts.newFleetServer(*cluster, stores, claims.release)
	if err != nil {
		log.Error("generic server", "err", err)
		os.Exit(1)
	}
	if serverOpts.selfSigned {
		log.Warn("no --tls-cert-file: serving a self-signed certificate that the kube-apiserver cannot verify; " +
			"for tests only, see cask-apiserver gen-serving-certs")
	}
	// SIGTERM ends the context. The server then drains its requests.
	runCtx := genericapiserver.SetupSignalContext()
	go claims.run(runCtx, 2*time.Second)
	log.Info("cask-apiserver serving", "group", apiGroup+"/"+apiVersion, "cluster", *cluster,
		"port", serverOpts.recommended.SecureServing.BindPort, "delegated-auth", serverOpts.delegatedAuth)
	if err := srv.PrepareRun().RunWithContext(runCtx); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// serveLegacy serves the legacy net/http mux on listen until it fails.
// The mux has no authentication. It stays for one release behind
// --legacy-http.
func serveLegacy(ctx context.Context, cluster string, fs *fleetStore, listen string, selfTLS bool, log *slog.Logger) error {
	srv := newAPIServer(cluster, fs, log)
	go srv.runReconciler(ctx, 2*time.Second)
	log.Warn("serving the legacy mux: no authentication, no authorization", "cluster", cluster, "listen", listen, "tls", selfTLS)
	server := &http.Server{Addr: listen, Handler: srv.routes()}
	// The cert is self-signed, so the APIService needs insecureSkipTLSVerify.
	//= docs/spec/fleet.md#8-security
	//= type=exception
	//= reason=the legacy mux behind --legacy-http has a self-signed cert only; tracked in issue #190
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	if !selfTLS {
		return server.ListenAndServe()
	}
	// The k8s aggregation layer requires extension apiservers to serve
	// TLS; the demo registers the APIService with insecureSkipTLSVerify.
	cfg, err := selfSignedTLS([]string{"cask-apiserver", "cask-apiserver.cask-system.svc", "localhost"})
	if err != nil {
		return err
	}
	server.TLSConfig = cfg
	return server.ListenAndServeTLS("", "")
}

// setupConsensusTLS returns the consensus TLS configs. When the member has
// consensus traffic (network is true), it fails without the three
// consensus TLS flags, unless f.Insecure is set. Without consensus
// traffic, it loads the flags only when some are set.
func setupConsensusTLS(f mtls.Files, network bool, log *slog.Logger) (*mtls.Config, error) {
	if !network && !f.Any() && !f.Insecure {
		return nil, nil
	}
	//= docs/spec/fleet.md#8-security
	//# Consensus traffic between members MUST use mutual TLS.
	return mtls.Setup(f, log)
}

// consensusNetwork returns the network for consensus traffic: mutual TLS
// when the member has a consensus identity, plain TCP otherwise. The
// listener and every peer client come from it: acceptor RPCs, roster
// calls, and key listings.
func consensusNetwork(cfg *mtls.Config) transport.Network {
	if cfg == nil {
		return transport.TCP{}
	}
	//= docs/spec/fleet.md#8-security
	//# Consensus traffic between members MUST use mutual TLS.
	return transport.TLS{Net: transport.TCP{}, Server: cfg.Server, Client: cfg.Client}
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if p := s[start:i]; p != "" {
				out = append(out, p)
			}
			start = i + 1
		}
	}
	return out
}

// importFile reads a cask-migrate export file and imports it into kv. It
// writes nothing when the file is invalid or conflicts with cask.
func importFile(ctx context.Context, kv *mvcc.KV, locks migrate.Locks, path string, log *slog.Logger) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h, objects, err := migrate.Read(f)
	if err != nil {
		return err
	}
	res, err := migrate.Import(ctx, kv, locks, h, objects)
	if err != nil {
		return err
	}
	log.Info("import done", "group", h.Group, "revision", h.Revision,
		"written", res.Written, "unchanged", res.Unchanged,
		"renewed", res.Renewed, "lapsed", res.Lapsed)
	if res.Lapsed > 0 {
		log.Warn("import: restored claim sessions lapsed before the import ended; those claims go to Lost", "lapsed", res.Lapsed)
	}
	return nil
}
