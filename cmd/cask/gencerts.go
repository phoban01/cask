package main

import (
	"flag"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/phoban01/cask/internal/transport/nebula"
)

// genCerts implements `cask gen-certs`: it mints a CA and per-node Nebula
// configs for a loopback demo cluster, writing one <name>.yml per node plus a
// run script. Node 1 is the lighthouse; the rest join through it. This makes the
// self-forming overlay runnable end-to-end without the external nebula-cert tool.
func genCerts(args []string) {
	fs := flag.NewFlagSet("gen-certs", flag.ExitOnError)
	var (
		n        = fs.Int("n", 3, "number of nodes")
		outDir   = fs.String("out", "clusterconf", "output directory for configs")
		baseIP   = fs.String("base-ip", "10.42.0", "overlay /24 prefix; node i gets <base-ip>.i")
		udpBase  = fs.Int("udp-base", 4242, "underlay UDP port for node 1; node i uses udp-base+i-1")
		caskPort = fs.Int("overlay-port", 8001, "consensus port nodes serve on the overlay")
		apiBase  = fs.Int("api-base", 8080, "local client API port for node 1; node i uses api-base+i-1")
	)
	_ = fs.Parse(args)

	zones := []string{"z1", "z2", "z3"}
	specs := make([]nebula.NodeSpec, *n)
	for i := 0; i < *n; i++ {
		ip, err := netip.ParseAddr(fmt.Sprintf("%s.%d", *baseIP, i+1))
		if err != nil {
			fmt.Fprintln(os.Stderr, "gen-certs: bad base-ip:", err)
			os.Exit(1)
		}
		specs[i] = nebula.NodeSpec{
			Name:       fmt.Sprintf("node%d", i+1),
			OverlayIP:  ip,
			Zone:       zones[i%len(zones)],
			UDP:        fmt.Sprintf("127.0.0.1:%d", *udpBase+i),
			Lighthouse: i == 0,
		}
	}

	configs, err := nebula.GenerateConfigs(specs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen-certs:", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "gen-certs:", err)
		os.Exit(1)
	}
	for name, yml := range configs {
		path := filepath.Join(*outDir, name+".yml")
		if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "gen-certs:", err)
			os.Exit(1)
		}
	}

	fmt.Printf("wrote %d node configs to %s/\n\n", *n, *outDir)
	fmt.Println("# start the self-forming cluster:")
	for i, s := range specs {
		fmt.Printf("bin/cask --nebula-config %s/%s.yml --overlay-port %d --listen 127.0.0.1:%d &\n",
			*outDir, s.Name, *caskPort, *apiBase+i)
	}
	fmt.Printf("\n# then, against any node's local API:\n")
	fmt.Printf("curl -XPUT localhost:%d/kv/greeting -d hello && curl localhost:%d/kv/greeting\n",
		*apiBase, *apiBase+(*n-1)%*n)
}
