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
		peers   = flag.String("cask-peers", "", "comma-separated consensus addresses of the cask fleet; empty = embedded single-node cask (demo/dev)")
		consLn  = flag.String("listen-consensus", "", "address to serve an EMBEDDED cask acceptor on; with --cask-peers, this apiserver IS one of the fleet's consensus nodes")
		adv     = flag.String("advertise-consensus", "", "this node's address as it appears in --cask-peers (requests to it stay in-process); required with --listen-consensus")
		self    = flag.Uint64("id", 0, "proposer id (unique per apiserver); required with --cask-peers")
		dataDir = flag.String("data-dir", "", "directory for durable consensus state (Pebble); empty = in-memory (an embedded acceptor that restarts empty forgets its promises — demo only)")
		selfTLS = flag.Bool("self-signed-tls", false, "serve HTTPS with an in-memory self-signed cert (required for k8s API aggregation; pair with insecureSkipTLSVerify on the APIService)")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *cluster == "" {
		log.Error("--cluster is required (e.g. --cluster eu-west-a)")
		os.Exit(1)
	}

	// The storage engine. cask is designed to be embedded: with
	// --listen-consensus each apiserver carries its own acceptor and the
	// apiservers ARE the consensus fleet — no external cask processes. The
	// alternatives are proposer-only mode over an existing fleet
	// (--cask-peers without --listen-consensus) and a process-local
	// single-node register for dev. Whichever way, every apiserver proposes
	// over the SAME replicas — that shared consensus is what makes one
	// device lease globally exclusive.
	var prop mvcc.Proposer
	switch {
	case *peers == "":
		log.Warn("embedded single-node cask: state is process-local and non-durable (demo mode)")
		prop = caspaxos.NewProposer(1, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(store.NewMem())})
	default:
		if *self == 0 {
			log.Error("--id required with --cask-peers (unique per apiserver)")
			os.Exit(1)
		}
		var local *caspaxos.Acceptor
		if *consLn != "" {
			if *adv == "" {
				log.Error("--advertise-consensus required with --listen-consensus (this node's entry in --cask-peers)")
				os.Exit(1)
			}
			var st caspaxos.Storage = store.NewMem()
			if *dataDir != "" {
				p, err := store.NewPebble(*dataDir, store.WithGroupCommit())
				if err != nil {
					log.Error("open data dir", "dir", *dataDir, "err", err)
					os.Exit(1)
				}
				defer p.Close()
				st = p
				log.Info("durable store open", "dir", *dataDir)
			} else {
				log.Warn("embedded acceptor is in-memory: a restart wipes its promises, which is unsafe for consensus — set --data-dir for anything beyond a demo")
			}
			local = caspaxos.NewAcceptor(st)
			mux := http.NewServeMux()
			mux.Handle(transport.ConnectHandler(local))
			go func() {
				log.Info("embedded cask acceptor serving", "listen", *consLn)
				if err := http.ListenAndServe(*consLn, mux); err != nil {
					log.Error("consensus server stopped", "err", err)
					os.Exit(1)
				}
			}()
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

	srv := newAPIServer(*cluster, fs, log)
	ctx := context.Background()
	go srv.runReconciler(ctx, 2*time.Second)

	// The server has no import path and no readiness gate yet, so nothing
	// checks imported owners before it serves.
	//= docs/spec/fleet.md#7-migration
	//= type=exception
	//= reason=tracked in issue #40
	//# The APIService MUST NOT become available while any imported object that other objects reference by ownerReference is missing.
	log.Info("cask-apiserver serving", "group", apiGroup+"/"+apiVersion, "cluster", *cluster, "listen", *listen, "tls", *selfTLS)
	server := &http.Server{Addr: *listen, Handler: srv.routes()}
	var err error
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
