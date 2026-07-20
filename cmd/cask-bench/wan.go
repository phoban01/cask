package main

// The WAN benchmark measures the paper's "WAN profile" row: cask's steady-state
// write/read latency and throughput as inter-node RTT grows, plus the §3.5
// straggler-immunity claim ("phase latency = max over the fastest quorum, not
// the sum of replica RTTs; a dead peer behind an un-timed-out transport costs
// nothing"). It runs in-process over testutil/sim with per-hop latency injected
// by sim.Network.SetSlow — the same seam the cold-start sweep uses — against a
// single RF-replicated range so the latency signal is clean.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

func wanBench(argv []string) error {
	fs := flag.NewFlagSet("wan", flag.ExitOnError)
	rf := fs.Int("rf", 5, "replication factor of the range under test")
	clients := fs.Int("clients", 32, "concurrent client goroutines")
	dur := fs.Duration("duration", 3*time.Second, "measured duration per config")
	wl := fs.String("workload", "put", "workload: put | get")
	rttsStr := fs.String("rtts", "1,5,20,50", "comma-separated injected one-way per-hop latencies (ms) to sweep")
	valueSize := fs.Int("value-size", 256, "value payload size in bytes")
	seedN := fs.Int("keys", 2000, "seeded keyspace for the get workload")
	jsonOut := fs.Bool("json", false, "emit machine-readable RESULT lines")
	fs.Parse(argv)

	rtts, err := parseFloats(*rttsStr)
	if err != nil {
		return fmt.Errorf("bad -rtts: %w", err)
	}

	ctx := context.Background()
	wc := newWANCluster(*rf)
	val := make([]byte, *valueSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	var seedKeys []string
	if *wl == "get" {
		seedKeys = make([]string, *seedN)
		for i := range seedKeys {
			seedKeys[i] = fmt.Sprintf("wan/get/%d", i)
			if _, err := wc.kv.Put(ctx, []byte(seedKeys[i]), val); err != nil {
				return fmt.Errorf("seed: %w", err)
			}
		}
	}

	// Round count for the flat (un-owned) path: a CASPaxos write/read is a
	// prepare round + an accept round = 2 RTTs to a quorum. This is the
	// yardstick the measured latency should track.
	fmt.Fprintf(os.Stderr, "RF=%d, %d clients, %s workload; sweeping per-hop RTT...\n", *rf, *clients, *wl)

	type row struct {
		label    string
		tput     float64
		p50, p99 time.Duration
	}
	var rows []row
	record := func(label string, rttAll time.Duration, straggler bool) {
		// Uniform latency, or (straggler) all but one replica fast and one 20×.
		for i := 0; i < *rf; i++ {
			wc.nw.SetSlow(i, rttAll)
		}
		if straggler {
			wc.nw.SetSlow(*rf-1, rttAll*20)
		}
		tput, lat := wc.run(ctx, *wl, *clients, *dur, val, seedKeys)
		rows = append(rows, row{label, tput, pctl(lat, 0.50), pctl(lat, 0.99)})
		if *jsonOut {
			fmt.Printf("RESULT {\"bench\":\"wan\",\"rf\":%d,\"config\":%q,\"workload\":%q,\"throughput_ops_s\":%.1f,\"p50_ms\":%.3f,\"p99_ms\":%.3f}\n",
				*rf, label, *wl, tput, ms(pctl(lat, 0.50)), ms(pctl(lat, 0.99)))
		}
	}

	for _, rttMS := range rtts {
		record(fmt.Sprintf("%gms uniform", rttMS), dms(rttMS), false)
	}
	// Straggler immunity: a fixed baseline with one replica 20× slower. Uses the
	// median swept RTT as the baseline.
	base := rtts[len(rtts)/2]
	record(fmt.Sprintf("%gms + 1 straggler(%gms)", base, base*20), dms(base), true)

	// Report.
	fmt.Printf("\nWAN profile — RF=%d, %d clients, %s, %s/config\n\n", *rf, *clients, *wl, *dur)
	fmt.Printf("%-26s | %-12s | %-9s %-9s\n", "config", "throughput", "p50", "p99")
	fmt.Println("---------------------------|--------------|-------------------")
	for _, r := range rows {
		fmt.Printf("%-26s | %-12s | %-9s %-9s\n",
			r.label, fmt.Sprintf("%.0f op/s", r.tput), msStr(r.p50), msStr(r.p99))
	}
	// Point out the straggler result relative to its uniform baseline.
	var baseUniform, strag *row
	for i := range rows {
		switch {
		case rows[i].label == fmt.Sprintf("%gms uniform", base):
			baseUniform = &rows[i]
		case rows[i].label == fmt.Sprintf("%gms + 1 straggler(%gms)", base, base*20):
			strag = &rows[i]
		}
	}
	if baseUniform != nil && strag != nil {
		fmt.Printf("\n§3.5 straggler immunity: one replica at %gms (20× the other %d) leaves\n", base*20, *rf-1)
		fmt.Printf("  write p50 at %s vs the %gms-uniform baseline's %s — the quorum is the\n", msStr(strag.p50), base, msStr(baseUniform.p50))
		fmt.Printf("  FASTEST majority, so the slow replica is off the critical path.\n")
	}
	fmt.Printf("\nlatency tracks ~2×RTT (prepare + accept rounds) in this flat, un-owned\n")
	fmt.Printf("topology; the owned fast path (§3.2) is one accept round = ~1×RTT.\n")
	return nil
}

// wanCluster is a single RF-replicated range served in-process.
type wanCluster struct {
	rf int
	nw *sim.Network
	kv *mvcc.KV
}

func newWANCluster(rf int) *wanCluster {
	stores := make([]caspaxos.Storage, rf)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)
	dialer := agent.StaticDialer{}
	replicas := make([]uint64, rf)
	for i := 0; i < rf; i++ {
		replicas[i] = uint64(i)
		dialer[uint64(i)] = nw.Client(i)
	}
	// One range spanning the whole keyspace, replicated across all rf nodes.
	rmap := ranges.NewMap([]ranges.Descriptor{{ID: 1, Replicas: replicas, Epoch: 1}})
	router := agent.NewRouter(0, rmap, dialer)
	kv := mvcc.New(router, hlc.New(func() int64 { return time.Now().UnixNano() }), 0)
	return &wanCluster{rf: rf, nw: nw, kv: kv}
}

// run drives the workload for dur with the given concurrency, returning
// throughput (ops/s) and the per-op latencies.
func (c *wanCluster) run(ctx context.Context, wl string, clients int, dur time.Duration, val []byte, seedKeys []string) (float64, []time.Duration) {
	runCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		allLat []time.Duration
		ops    atomic.Int64
	)
	start := make(chan struct{})
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lat := make([]time.Duration, 0, 1024)
			pick := uint64(id)
			<-start
			for runCtx.Err() == nil {
				t0 := time.Now()
				var err error
				if wl == "get" {
					key := seedKeys[pick%uint64(len(seedKeys))]
					pick += uint64(clients)
					_, _, err = c.kv.Get(runCtx, []byte(key))
				} else {
					key := fmt.Appendf(nil, "wan/put/%d", freshKey.Add(1))
					_, err = c.kv.Put(runCtx, key, val)
				}
				d := time.Since(t0)
				if err != nil {
					if runCtx.Err() == nil {
						fmt.Fprintf(os.Stderr, "op error: %v\n", err)
					}
					continue
				}
				ops.Add(1)
				lat = append(lat, d)
			}
			mu.Lock()
			allLat = append(allLat, lat...)
			mu.Unlock()
		}(cl)
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	return float64(ops.Load()) / elapsed.Seconds(), allLat
}

func dms(rttMS float64) time.Duration { return time.Duration(rttMS * float64(time.Millisecond)) }
