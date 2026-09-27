package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/phoban01/cask/internal/backoff"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/lease"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/internal/transport"
)

func main() {
	var (
		listen  = flag.String("listen", ":9443", "address to serve the API group on")
		cluster = flag.String("cluster", "", "this cluster's name (stamped on claims it manages); required")
		boot    = flag.Bool("bootstrap", false, "found a new fleet as its single founding member (exactly one apiserver in a fleet; every other one joins with --seed)")
		seed    = flag.String("seed", "", "comma-separated consensus addresses of live fleet members to join through, e.g. 10.0.0.7:9444")
		peers   = flag.String("cask-peers", "", "DEPRECATED, tests only: comma-separated static consensus addresses; use --bootstrap or --seed")
		consLn  = flag.String("listen-consensus", "", "address to serve the EMBEDDED cask acceptor and the roster endpoints on; required with --bootstrap and --seed")
		adv     = flag.String("advertise-consensus", "", "this node's consensus address as peers reach it; required with --listen-consensus")
		self    = flag.Uint64("id", 0, "node id (unique per apiserver); required with --bootstrap, --seed, and --cask-peers")
		dataDir = flag.String("data-dir", "", "directory for durable consensus state (Pebble); empty = in-memory (an embedded acceptor that restarts empty forgets its promises — demo only)")
		selfTLS = flag.Bool("self-signed-tls", false, "serve HTTPS with an in-memory self-signed cert (required for k8s API aggregation; pair with insecureSkipTLSVerify on the APIService)")
	)
	flag.Parse()
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
	// The demo PodDisruptionBudget covers one cluster only. Nothing stops
	// two clusters from upgrading at the same time.
	//= docs/spec/fleet.md#9-operations
	//= type=exception
	//= reason=no fleet-wide upgrade order; tracked in issue #96
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
	var prop mvcc.Proposer
	switch {
	case *peers == "" && !dynamic:
		log.Warn("embedded single-node cask: state is process-local and non-durable (demo mode)")
		prop = caspaxos.NewProposer(1, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(store.NewMem())})
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
				cfg := membershipConfig{
					ID:        *self,
					Advertise: *adv,
					Bootstrap: *boot,
					Seeds:     parseSeeds(*seed),
					Local:     local,
					HTTP:      transport.TCP{}.HTTPClient(),
				}
				if l, ok := st.(store.Lister); ok {
					cfg.Keys = l.Keys
				}
				mem = newMembership(cfg, log)
				h = mem.handler()
			} else {
				mux := http.NewServeMux()
				mux.Handle(transport.ConnectHandler(local))
				h = mux
			}
			// The consensus listener and the peer dialer use plain HTTP.
			// Anyone who can reach --listen-consensus can propose.
			//= docs/spec/fleet.md#8-security
			//= type=exception
			//= reason=consensus traffic is plaintext; tracked in issues #47 and #48
			//# Consensus traffic between members MUST use mutual TLS.
			//= docs/spec/fleet.md#8-security
			//= type=exception
			//= reason=no client certificate on the consensus listener and no delegated auth on the API; tracked in issues #48 and #38
			//# The cask client API and control endpoints MUST NOT be reachable outside the pod without authentication.
			go func() {
				log.Info("embedded cask acceptor serving", "listen", *consLn)
				if err := http.ListenAndServe(*consLn, h); err != nil {
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
			break
		}
		hc := transport.TCP{}.HTTPClient()
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
	kv := mvcc.New(prop, clock, uint64(os.Getpid()))
	sessions := lease.NewSessions(prop, func() int64 { return time.Now().UnixNano() })
	locks := lease.NewLocks(prop, sessions)
	fs := &fleetStore{kv: kv, sessions: sessions, locks: locks}
	//= docs/spec/fleet.md#3-storage-model
	//= type=exception
	//= reason=no index sweep runs at startup yet; tracked in issue #36
	//# The extension server MUST reconcile the index register against the object registers at startup.

	srv := newAPIServer(*cluster, fs, log)
	go srv.runReconciler(ctx, 2*time.Second)

	// The server has no import path and no readiness gate yet, so nothing
	// checks imported owners before it serves.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=tracked in issue #40
	//# The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no import marker and no readiness gate; tracked in issue #40
	//# The APIService MUST NOT become available before the import has completed.
	// There is no cask-migrate command yet. The export, the import, the
	// cutover runbook, and the rehearsal do not exist.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no cutover runbook yet; tracked in issue #52
	//# Writers MUST be frozen from the start of the export until the APIService is available.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no import command to seed the index; tracked in issue #51
	//# The initial index sequence for each resource type MUST be greater than the source etcd revision at export time.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no export command and no CronJob; tracked in issues #49 and #53
	//# A continuous export of all fleet objects MUST run from the first day of phase one.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=no rehearsal test on kind; tracked in issue #54
	//# The cutover MUST be rehearsed on a copy of the management cluster before it runs on the real one.
	log.Info("cask-apiserver serving", "group", apiGroup+"/"+apiVersion, "cluster", *cluster, "listen", *listen, "tls", *selfTLS)
	server := &http.Server{Addr: *listen, Handler: srv.routes()}
	var err error
	// The cert is self-signed, so the APIService needs insecureSkipTLSVerify.
	//= docs/spec/fleet.md#8-security
	//= type=exception
	//= reason=self-signed cert only; tracked in issue #41
	//# The extension server MUST serve HTTPS with a certificate the kube-apiserver can verify.
	if *selfTLS {
		// The k8s aggregation layer requires extension apiservers to serve
		// TLS; the demo registers the APIService with insecureSkipTLSVerify.
		if server.TLSConfig, err = selfSignedTLS([]string{"cask-apiserver", "cask-apiserver.cask-system.svc", "localhost"}); err == nil {
			err = server.ListenAndServeTLS("", "")
		}
	} else {
		err = server.ListenAndServe()
	}
	if err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
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
