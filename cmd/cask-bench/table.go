package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// resultLine mirrors the JSON object emitted after "RESULT " by report().
type resultLine struct {
	Target     string  `json:"target"`
	Workload   string  `json:"workload"`
	Throughput float64 `json:"throughput_ops_s"`
	P50        int64   `json:"p50_us"`
	P99        int64   `json:"p99_us"`
	Ops        int64   `json:"ops"`
	Conflicts  int64   `json:"conflicts"`
}

// renderTable reads RESULT/JSON lines from the given files (or stdin) and prints
// a cask-vs-etcd comparison grouped by workload, in workload-first-seen order.
func renderTable(files []string) error {
	byWL := map[string]map[string]resultLine{}
	var order []string
	seen := map[string]bool{}

	scan := func(r *bufio.Scanner) error {
		for r.Scan() {
			line := strings.TrimPrefix(strings.TrimSpace(r.Text()), "RESULT ")
			if line == "" || line[0] != '{' {
				continue
			}
			var rl resultLine
			if err := json.Unmarshal([]byte(line), &rl); err != nil {
				continue // tolerate interleaved non-JSON noise
			}
			if byWL[rl.Workload] == nil {
				byWL[rl.Workload] = map[string]resultLine{}
			}
			byWL[rl.Workload][rl.Target] = rl
			if !seen[rl.Workload] {
				seen[rl.Workload] = true
				order = append(order, rl.Workload)
			}
		}
		return r.Err()
	}

	if len(files) == 0 {
		if err := scan(bufio.NewScanner(os.Stdin)); err != nil {
			return err
		}
	}
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			return err
		}
		err = scan(bufio.NewScanner(fh))
		fh.Close()
		if err != nil {
			return err
		}
	}

	fmt.Printf("%-6s | %-21s | %-19s | %-19s\n", "wkld", "throughput (ops/s)", "p50 latency (us)", "p99 latency (us)")
	fmt.Printf("%-6s | %-10s %-10s | %-9s %-9s | %-9s %-9s\n", "", "cask", "etcd", "cask", "etcd", "cask", "etcd")
	fmt.Println(strings.Repeat("-", 74))
	for _, wl := range order {
		c, e := byWL[wl]["cask"], byWL[wl]["etcd"]
		fmt.Printf("%-6s | %-10.0f %-10.0f | %-9d %-9d | %-9d %-9d\n",
			wl, c.Throughput, e.Throughput, c.P50, e.P50, c.P99, e.P99)
	}
	return nil
}
