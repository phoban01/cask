package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

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
		peers   = flag.String("cask-peers", "", "comma-separated cask node consensus addresses; empty = embedded single-node cask (demo/dev)")
		self    = flag.Uint64("id", 0, "proposer id when connecting to a cask cluster (unique per apiserver)")
		selfTLS = flag.Bool("self-signed-tls", false, "serve HTTPS with an in-memory self-signed cert (required for k8s API aggregation; pair with insecureSkipTLSVerify on the APIService)")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *cluster == "" {
		log.Error("--cluster is required (e.g. --cluster eu-west-a)")
		os.Exit(1)
	}

	// The storage engine: an embedded single-node cask for dev/demo-lite, or
	// a proposer over an existing cask cluster's consensus transport. Every
	// apiserver in the fleet points at the SAME cask replicas — that shared
	// consensus is what makes one device lease globally exclusive.
	var prop mvcc.Proposer
	switch {
	case *peers == "":
		log.Warn("embedded single-node cask: state is process-local and non-durable (demo mode)")
		prop = caspaxos.NewProposer(1, []caspaxos.AcceptorClient{caspaxos.NewAcceptor(store.NewMem())})
	default:
		var clients []caspaxos.AcceptorClient
		for _, addr := range splitCSV(*peers) {
			clients = append(clients, transport.NewConnectClient("http://"+addr, nil))
		}
		id := *self
		if id == 0 {
			log.Error("--id required with --cask-peers (unique per apiserver)")
			os.Exit(1)
		}
		prop = caspaxos.NewProposer(id, clients)
	}

	clock := hlc.New(func() int64 { return time.Now().UnixNano() })
	kv := mvcc.New(prop, clock, uint64(os.Getpid()))
	sessions := lease.NewSessions(prop, func() int64 { return time.Now().UnixNano() })
	locks := lease.NewLocks(prop, sessions)
	fs := &fleetStore{kv: kv, sessions: sessions, locks: locks}

	srv := newAPIServer(*cluster, fs, log)
	ctx := context.Background()
	go srv.runReconciler(ctx, 2*time.Second)

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
