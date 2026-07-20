// Command cask-bench drives a fixed workload against either a running cask
// cluster (over its HTTP client API) or an etcd cluster (over clientv3) and
// reports throughput and latency percentiles. The two targets share the same
// load generator, the same key/value shapes, and the same measurement code, so
// their numbers are directly comparable — this is the harness behind the
// paper's "throughput/latency vs etcd, same hardware" row.
//
// Fairness notes (see bench/README.md for the full rationale):
//   - The write workload uses a FRESH key per op (monotonic counter). cask's
//     MVCC register appends an unbounded version chain per key, so hammering a
//     small keyspace would measure history growth, not the commit path. Fresh
//     keys keep every register one version deep on both sides.
//   - The read workload seeds a finite keyspace once, then reads it back with
//     linearizable reads on both sides (etcd default consistency, not
//     WithSerializable), so neither side is quietly served a stale local copy.
//   - Both clusters run as real 3-node processes on the same host, so both pay
//     real client RPC, real inter-node consensus RPC, and real fsync.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type workload string

const (
	wlPut  workload = "put"  // blind write, fresh key per op
	wlGet  workload = "get"  // linearizable read over a seeded keyspace
	wlCAS  workload = "cas"  // compare-and-set read-modify-write on a per-client key
	wlLock workload = "lock" // acquire+release a distributed lock (cask's differentiator)
)

type config struct {
	target    string
	endpoints string
	wl        workload
	clients   int
	duration  time.Duration
	warmup    time.Duration
	keyspace  int
	valueSize int
	jsonOut   bool
}

func main() {
	// `cask-bench table results.jsonl` renders the side-by-side comparison from
	// RESULT lines collected across runs — kept in Go so run.sh needs no gawk.
	if len(os.Args) > 1 && os.Args[1] == "table" {
		if err := renderTable(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "cask-bench table: %v\n", err)
			os.Exit(1)
		}
		return
	}
	// `cask-bench coldstart` measures a node joining a formed 100-node/50-range
	// cluster (paper §5 cold-start row); fully in-process, no etcd/docker.
	if len(os.Args) > 1 && os.Args[1] == "coldstart" {
		if err := coldStart(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "cask-bench coldstart: %v\n", err)
			os.Exit(1)
		}
		return
	}

	var (
		cfg     config
		wlStr   string
		targets string
	)
	flag.StringVar(&cfg.target, "target", "cask", "system under test: cask | etcd")
	flag.StringVar(&targets, "endpoints", "", "comma-separated endpoints (cask: http://host:port base URLs; etcd: host:port). Defaults per target.")
	flag.StringVar(&wlStr, "workload", "put", "workload: put | get | cas | lock")
	flag.IntVar(&cfg.clients, "clients", 64, "number of concurrent client goroutines")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "measured run duration")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "warmup duration (discarded)")
	flag.IntVar(&cfg.keyspace, "keys", 10000, "seeded keyspace size (get/cas workloads)")
	flag.IntVar(&cfg.valueSize, "value-size", 256, "value payload size in bytes")
	flag.BoolVar(&cfg.jsonOut, "json", false, "emit a machine-readable RESULT line")
	flag.Parse()

	cfg.wl = workload(wlStr)
	cfg.endpoints = targets

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "cask-bench: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	drv, err := newDriver(cfg)
	if err != nil {
		return err
	}
	defer drv.close()

	val := make([]byte, cfg.valueSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	// Seed the keyspace for read/cas workloads so reads hit live keys and
	// cas has a known prior value to compare against.
	var seedKeys []string
	switch cfg.wl {
	case wlGet:
		seedKeys = make([]string, cfg.keyspace)
		for i := range seedKeys {
			seedKeys[i] = fmt.Sprintf("bench/get/%d", i)
		}
	}
	if len(seedKeys) > 0 {
		fmt.Fprintf(os.Stderr, "seeding %d keys...\n", len(seedKeys))
		if err := drv.seed(ctx, seedKeys, val); err != nil {
			return fmt.Errorf("seed: %w", err)
		}
	}

	// Warmup, then the measured run. Warmup lets connection pools, JITless Go
	// runtime caches, and the owner-lease fast path settle before we measure.
	if cfg.warmup > 0 {
		fmt.Fprintf(os.Stderr, "warmup %s...\n", cfg.warmup)
		load(ctx, cfg, drv, val, seedKeys, cfg.warmup, false)
	}
	fmt.Fprintf(os.Stderr, "measuring %s (%s, %d clients)...\n", cfg.wl, cfg.duration, cfg.clients)
	res := load(ctx, cfg, drv, val, seedKeys, cfg.duration, true)

	res.report(cfg)
	return nil
}

// freshKey is a process-global monotonic counter for the put and cas
// workloads' fresh keys. Being global (not per-load) keeps warmup and the
// measured run from ever reusing a key number — otherwise the measured cas
// phase would re-create keys warmup already made and see spurious conflicts.
var freshKey atomic.Uint64

// result accumulates per-op outcomes across all client goroutines.
type result struct {
	lat       []time.Duration // one entry per successful op
	ok        int64
	conflicts int64
	errs      int64
	elapsed   time.Duration
}

// load runs cfg.clients goroutines against drv for dur, each executing the
// configured workload op in a tight loop. When record is false (warmup) no
// latencies are kept. All clients start together and stop at the deadline.
func load(ctx context.Context, cfg config, drv driver, val []byte, seedKeys []string, dur time.Duration, record bool) result {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		allLat    []time.Duration
		ok        atomic.Int64
		conflicts atomic.Int64
		errs      atomic.Int64
	)

	runCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()
	start := make(chan struct{})

	for c := 0; c < cfg.clients; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := drv.open(id)
			if err != nil {
				errs.Add(1)
				return
			}
			defer conn.close()

			var lat []time.Duration
			if record {
				lat = make([]time.Duration, 0, 4096)
			}
			// Per-client RNG-free key selection: a striped counter gives each
			// client a deterministic, collision-light walk over the keyspace.
			pick := uint64(id)

			<-start
			for runCtx.Err() == nil {
				var (
					opErr    error
					conflict bool
				)
				t0 := time.Now()
				switch cfg.wl {
				case wlPut:
					key := fmt.Sprintf("bench/put/%d", freshKey.Add(1))
					opErr = conn.write(runCtx, key, val)
				case wlGet:
					key := seedKeys[pick%uint64(len(seedKeys))]
					pick += uint64(cfg.clients)
					_, opErr = conn.read(runCtx, key)
				case wlCAS:
					// Conditional create on a FRESH key (expect-absent): this
					// exercises the precondition-inside-consensus path without
					// growing any key's MVCC version chain, so it measures the
					// CAS commit cost rather than hot-key history growth. A
					// fresh key is always absent, so applied is expected true;
					// a false here would be a real precondition surprise.
					key := fmt.Sprintf("bench/cas/%d", freshKey.Add(1))
					var applied bool
					applied, opErr = conn.cas(runCtx, key, nil, val)
					if opErr == nil && !applied {
						conflict = true
					}
				case wlLock:
					opErr = conn.lockCycle(runCtx, fmt.Sprintf("bench/lock/%d", id))
				}
				d := time.Since(t0)

				switch {
				case opErr != nil:
					// A deadline hit mid-op is the clean end of the run, not a
					// failure — don't count it.
					if runCtx.Err() == nil {
						errs.Add(1)
					}
				default:
					// A conflict is still a completed consensus round with real
					// latency — count its timing, tally it separately from ok.
					if record {
						lat = append(lat, d)
					}
					if conflict {
						conflicts.Add(1)
					} else {
						ok.Add(1)
					}
				}
			}
			if record && len(lat) > 0 {
				mu.Lock()
				allLat = append(allLat, lat...)
				mu.Unlock()
			}
		}(c)
	}

	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)

	return result{
		lat:       allLat,
		ok:        ok.Load(),
		conflicts: conflicts.Load(),
		errs:      errs.Load(),
		elapsed:   elapsed,
	}
}

func (r result) report(cfg config) {
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	total := r.ok + r.conflicts
	tput := float64(total) / r.elapsed.Seconds()

	p := func(q float64) time.Duration {
		if len(r.lat) == 0 {
			return 0
		}
		i := int(q * float64(len(r.lat)))
		if i >= len(r.lat) {
			i = len(r.lat) - 1
		}
		return r.lat[i]
	}
	var mean time.Duration
	if len(r.lat) > 0 {
		var sum time.Duration
		for _, d := range r.lat {
			sum += d
		}
		mean = sum / time.Duration(len(r.lat))
	}

	fmt.Printf("\n%-6s %-5s  clients=%d  dur=%s\n", cfg.target, cfg.wl, cfg.clients, r.elapsed.Round(time.Millisecond))
	fmt.Printf("  ops        %d ok, %d conflicts, %d errors\n", r.ok, r.conflicts, r.errs)
	fmt.Printf("  throughput %.0f ops/s\n", tput)
	fmt.Printf("  latency    mean=%s  p50=%s  p90=%s  p99=%s  p999=%s  max=%s\n",
		rd(mean), rd(p(0.50)), rd(p(0.90)), rd(p(0.99)), rd(p(0.999)), rd(p(1.0)))

	if cfg.jsonOut {
		fmt.Printf("RESULT {\"target\":%q,\"workload\":%q,\"clients\":%d,\"ops\":%d,\"conflicts\":%d,\"errors\":%d,\"throughput_ops_s\":%.1f,\"mean_us\":%d,\"p50_us\":%d,\"p90_us\":%d,\"p99_us\":%d,\"p999_us\":%d,\"max_us\":%d}\n",
			cfg.target, cfg.wl, cfg.clients, r.ok, r.conflicts, r.errs, tput,
			us(mean), us(p(0.50)), us(p(0.90)), us(p(0.99)), us(p(0.999)), us(p(1.0)))
	}
}

func rd(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

func us(d time.Duration) int64 { return d.Microseconds() }
