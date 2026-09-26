package main

// The cold-start benchmark measures the paper's "Cold start (100 nodes, 50
// ranges) < 1s" row (docs/etcd-little-sister.md "Done when"): a NEW node joins
// an already-formed cluster — it reads the roster register from the Core, reads
// every range descriptor from the Core, builds its local range map, and serves
// its first read. We time exactly that join, not whole-cluster formation.
//
// It runs fully in-process over testutil/sim (in-memory acceptors, direct
// method-call transport), so with no injected latency the number is the pure
// coordination + rmap-build CPU floor. The real cost in a deployment is the
// 1 + N register reads to the Core, so we sweep an injected per-hop latency
// (sim.Network.SetSlow) to show where the < 1s bar actually sits — which also
// exercises the same latency-injection seam the WAN row will use.

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/phoban01/cask/internal/agent"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/placement"
	"github.com/phoban01/cask/internal/ranges"
	"github.com/phoban01/cask/internal/roster"
	"github.com/phoban01/cask/internal/store"
	"github.com/phoban01/cask/testutil/sim"
)

func coldStart(argv []string) error {
	fs := flag.NewFlagSet("coldstart", flag.ExitOnError)
	nodes := fs.Int("nodes", 100, "cluster size (roster Members)")
	nranges := fs.Int("ranges", 50, "number of ranges (descriptors) placed on the Core")
	rf := fs.Int("rf", 3, "replication factor per range and Core size")
	trials := fs.Int("trials", 30, "joining-node cold-start trials per config")
	rttsStr := fs.String("rtts", "0,0.5,1,2,5", "comma-separated injected one-way per-hop latencies (ms) to sweep")
	jsonOut := fs.Bool("json", false, "emit machine-readable RESULT lines")
	fs.Parse(argv)

	rtts, err := parseFloats(*rttsStr)
	if err != nil {
		return fmt.Errorf("bad -rtts: %w", err)
	}

	ctx := context.Background()
	cl, err := formCluster(ctx, *nodes, *nranges, *rf)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "formed: %d nodes, %d ranges, Core=%d; measuring joins...\n", *nodes, *nranges, *rf)

	type row struct {
		rttMS                float64
		serialP50, serialP99 time.Duration
		fanP50, fanP99       time.Duration
	}
	var rows []row
	for _, rttMS := range rtts {
		d := time.Duration(rttMS * float64(time.Millisecond))
		for i := 0; i < cl.n; i++ {
			cl.nw.SetSlow(i, d)
		}
		ser := cl.measure(ctx, *trials, false)
		fan := cl.measure(ctx, *trials, true)
		rows = append(rows, row{rttMS, pctl(ser, 0.50), pctl(ser, 0.99), pctl(fan, 0.50), pctl(fan, 0.99)})
		if *jsonOut {
			fmt.Printf("RESULT {\"bench\":\"coldstart\",\"nodes\":%d,\"ranges\":%d,\"rf\":%d,\"rtt_ms\":%g,\"serial_p50_ms\":%.3f,\"serial_p99_ms\":%.3f,\"fanout_p50_ms\":%.3f,\"fanout_p99_ms\":%.3f}\n",
				*nodes, *nranges, *rf, rttMS,
				ms(pctl(ser, 0.50)), ms(pctl(ser, 0.99)), ms(pctl(fan, 0.50)), ms(pctl(fan, 0.99)))
		}
	}

	// Report.
	fmt.Printf("\ncold start — %d nodes, %d ranges, Core=%d, %d trials/config\n", *nodes, *nranges, *rf, *trials)
	fmt.Printf("(join = read roster + %d descriptors from the Core, build rmap, serve first read)\n\n", *nranges)
	fmt.Printf("%-10s | %-19s | %-19s\n", "per-hop", "serial fetch", "fanout fetch")
	fmt.Printf("%-10s | %-9s %-9s | %-9s %-9s\n", "RTT/2", "p50", "p99", "p50", "p99")
	fmt.Println(strings.Repeat("-", 46))
	target := time.Second
	worstFan := time.Duration(0)
	serialBreach := -1.0 // first per-hop RTT (ms) where serial fetch crosses 1s
	for _, r := range rows {
		fmt.Printf("%-10s | %-9s %-9s | %-9s %-9s\n",
			fmt.Sprintf("%gms", r.rttMS), msStr(r.serialP50), msStr(r.serialP99), msStr(r.fanP50), msStr(r.fanP99))
		if r.fanP99 > worstFan {
			worstFan = r.fanP99
		}
		if serialBreach < 0 && r.serialP99 >= target {
			serialBreach = r.rttMS
		}
	}
	// The production join fans out the N independent descriptor reads, so the
	// verdict is judged on the fanout path.
	verdict := "PASS"
	if worstFan >= target {
		verdict = "FAIL"
	}
	fmt.Printf("\ntarget: first served read < 1s — %s (fanout path; worst p99 across sweep: %s)\n", verdict, msStr(worstFan))
	fmt.Println("  fanout fetch is ~2 Core round-trips regardless of range count (all N")
	fmt.Println("  descriptor reads issued in parallel); serial fetch is 1+N round-trips.")
	if serialBreach >= 0 {
		fmt.Printf("  serial fetch of %d descriptors crosses 1s at ~%gms/hop — fan-out is\n  what keeps a %d-range cold start sub-second on a WAN.\n", *nranges, serialBreach, *nranges)
	}
	return nil
}

// cluster is a formed, in-process cask cluster ready to be joined.
type cluster struct {
	n           int
	rf          int
	nw          *sim.Network
	dialer      agent.StaticDialer
	coreIDs     []uint64
	coreClients []caspaxos.AcceptorClient
}

func formCluster(ctx context.Context, n, nranges, rf int) (*cluster, error) {
	stores := make([]caspaxos.Storage, n)
	for i := range stores {
		stores[i] = store.NewMem()
	}
	nw := sim.NewNetwork(stores)

	nodeIDs := make([]uint64, n)
	dialer := agent.StaticDialer{}
	for i := 0; i < n; i++ {
		nodeIDs[i] = uint64(i)
		dialer[uint64(i)] = nw.Client(i)
	}
	coreIDs := nodeIDs[:rf]
	coreClients := make([]caspaxos.AcceptorClient, rf)
	for g, id := range coreIDs {
		coreClients[g] = nw.Client(int(id))
	}

	// Roster: a bounded Core (rf nodes) holds the register; the other n-rf join
	// as Members only. This is the production shape — a small Core storing the
	// control-plane registers, full membership driving placement.
	mk := func(groups [][]uint64) roster.Proposer {
		acl := make([][]caspaxos.AcceptorClient, len(groups))
		for g, ids := range groups {
			for _, id := range ids {
				acl[g] = append(acl[g], nw.Client(int(id)))
			}
		}
		if len(acl) == 1 {
			return caspaxos.NewProposer(0, acl[0])
		}
		return caspaxos.NewJointProposer(0, acl)
	}
	r := roster.New(0, mk)
	member := func(id uint64) roster.Member {
		return roster.Member{NodeID: id, Addr: fmt.Sprintf("10.0.0.%d", id)}
	}
	coreMembers := make([]roster.Member, rf)
	for i, id := range coreIDs {
		coreMembers[i] = member(id)
	}
	if _, err := r.Genesis(ctx, coreMembers); err != nil {
		return nil, fmt.Errorf("genesis: %w", err)
	}
	for _, id := range nodeIDs[rf:] {
		if _, err := r.Add(ctx, member(id)); err != nil {
			return nil, fmt.Errorf("add %d: %w", id, err)
		}
	}

	// Place nranges descriptors as registers on the Core, each RF-replicated
	// across the full membership via HRW placement, with contiguous key bounds.
	rstore := ranges.NewStore(caspaxos.NewProposer(0, coreClients))
	for i := 1; i <= nranges; i++ {
		id := uint64(i)
		d := ranges.Descriptor{
			ID:       id,
			Start:    bound(i-1, nranges),
			End:      bound(i, nranges),
			Replicas: placement.Top(ranges.RangeKey(id), nodeIDs, rf),
			Epoch:    1,
		}
		if _, err := rstore.Publish(ctx, id, func(ranges.State, bool) (ranges.State, error) {
			return ranges.State{Descriptor: d}, nil
		}); err != nil {
			return nil, fmt.Errorf("seed range %d: %w", id, err)
		}
	}
	// Publish the range index into the roster so a joining node can enumerate
	// the descriptors to fetch.
	ids := make([]uint64, nranges)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	if _, err := r.UpdateRangeIDs(ctx, func([]uint64) []uint64 { return ids }); err != nil {
		return nil, fmt.Errorf("update range ids: %w", err)
	}

	return &cluster{n: n, rf: rf, nw: nw, dialer: dialer, coreIDs: coreIDs, coreClients: coreClients}, nil
}

// measure runs trials joins and returns the per-join durations.
func (c *cluster) measure(ctx context.Context, trials int, fanout bool) []time.Duration {
	out := make([]time.Duration, 0, trials)
	for t := 0; t < trials; t++ {
		d, err := c.joinOnce(ctx, fanout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "join failed: %v\n", err)
			continue
		}
		out = append(out, d)
	}
	return out
}

// joinOnce times one cold start: a fresh node reads the roster and every
// descriptor from the Core, builds its range map, and serves one read.
func (c *cluster) joinOnce(ctx context.Context, fanout bool) (time.Duration, error) {
	const clientID = 1 << 20 // a fresh id, not a cluster member

	// Precondition (not timed): the node has discovered a live Core member via
	// the overlay/DNS-seeds, so it knows which acceptor set holds the roster.
	// AdoptCore models exactly that hint; the timed join begins at the first
	// register read.
	jr := roster.NewWithProposer(clientID, caspaxos.NewProposer(clientID, c.coreClients))
	jr.AdoptCore(c.coreIDs)

	t0 := time.Now()

	// 1. Read the roster register from the Core → Members, RangeIDs.
	v, err := jr.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("read roster: %w", err)
	}

	// 2. Read every descriptor from the Core → build the local rmap.
	js := ranges.NewStore(caspaxos.NewProposer(clientID, c.coreClients))
	descs := make([]ranges.Descriptor, len(v.RangeIDs))
	if fanout {
		var wg sync.WaitGroup
		errs := make([]error, len(v.RangeIDs))
		for i, id := range v.RangeIDs {
			wg.Add(1)
			go func(i int, id uint64) {
				defer wg.Done()
				st, present, err := js.Get(ctx, id)
				switch {
				case err != nil:
					errs[i] = err
				case !present:
					errs[i] = fmt.Errorf("descriptor %d absent", id)
				default:
					descs[i] = st.Descriptor
				}
			}(i, id)
		}
		wg.Wait()
		for _, e := range errs {
			if e != nil {
				return 0, e
			}
		}
	} else {
		for i, id := range v.RangeIDs {
			st, present, err := js.Get(ctx, id)
			if err != nil {
				return 0, err
			}
			if !present {
				return 0, fmt.Errorf("descriptor %d absent", id)
			}
			descs[i] = st.Descriptor
		}
	}

	// 3. Build the range map + router + KV engine.
	rmap := ranges.NewMap(descs)
	router := agent.NewRouter(clientID, rmap, c.dialer)
	kv := mvcc.New(router, hlc.New(func() int64 { return time.Now().UnixNano() }), clientID)

	// 4. Serve the first read: route a key through the rmap to its range's
	// replicas and run a linearizable read (key absent is fine — the round
	// still executes, which is what "serves traffic" means).
	if _, _, err := kv.Get(ctx, []byte("bench/coldstart/probe")); err != nil {
		return 0, fmt.Errorf("first read: %w", err)
	}
	return time.Since(t0), nil
}

// bound returns the i-th of total contiguous key boundaries: nil at the ends
// (±inf) and an 8-byte big-endian split point in between, so the descriptors
// tile the keyspace without overlap.
func bound(i, total int) []byte {
	if i <= 0 || i >= total {
		return nil
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i)*(^uint64(0)/uint64(total)))
	return b
}

func parseFloats(s string) ([]float64, error) {
	var out []float64
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func pctl(ds []time.Duration, q float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(q * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func msStr(d time.Duration) string { return fmt.Sprintf("%.2fms", ms(d)) }
