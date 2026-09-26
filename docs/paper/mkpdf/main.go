// Command mkpdf builds the paper PDF from the canonical markdown source. It has
// two modes, both driven by build.sh:
//
//	mkpdf figures <dir>       generate the evaluation figures (PDF) into <dir>
//	mkpdf prep <in> <out>     transform the paper markdown into a pandoc-ready
//	                          document: front matter, figure includes, and the
//	                          few Unicode glyphs XeTeX's default font lacks
//	                          rewritten as LaTeX math.
//
// Keeping the transforms here (rather than in sed/perl/python) means the build
// needs only Go plus pandoc+tectonic, and the canonical markdown stays clean.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "figures":
		if len(os.Args) != 3 {
			usage()
		}
		genFigures(os.Args[2])
	case "prep":
		if len(os.Args) != 4 {
			usage()
		}
		prep(os.Args[2], os.Args[3])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mkpdf figures <dir> | prep <in.md> <out.md>")
	os.Exit(2)
}

const frontMatter = `---
title: "Coordination Without a Log"
subtitle: "cask --- a coordination store with no log"
author: "Piaras Hoban"
date: "July 2026 --- working draft"
---
`

// Figure includes, inserted at their evaluation subsections. Captions use plain
// Unicode; texify (below) rewrites the math glyphs afterwards.
const (
	figThroughput = "![Throughput across the four workloads (64 clients, same host). cask is within an order of magnitude of etcd on writes and CAS; etcd's read-index dominates the un-owned linearizable read.](fig-throughput.pdf){width=80%}"
	figLatency    = "![p99 latency. cask carries a heavier write tail from full-jitter backoff between the leaderless proposers; the un-owned linearizable read is a full consensus round.](fig-latency.pdf){width=80%}"
	figColdstart  = "![Cold-start join latency vs injected per-hop RTT (log scale). Fanned-out descriptor fetch stays ~2 Core round-trips regardless of range count and well under the 1 s target; serial fetch is 1+N round-trips and crosses 1 s near 8 ms/hop.](fig-coldstart.pdf){width=82%}"
	figWan        = "![WAN write latency tracks ~2×RTT (a prepare round plus an accept round to a quorum). One replica at 400 ms --- 20× the other four --- leaves the median unchanged (the triangle sits on the uniform line), because a quorum is the fastest majority.](fig-wan.pdf){width=82%}"
)

func prep(in, out string) {
	raw, err := os.ReadFile(in)
	if err != nil {
		log.Fatal(err)
	}
	s := string(raw)

	// 1. Replace the title block + working-draft note with pandoc front matter;
	//    keep everything from the abstract onward.
	const absMark = "## Abstract (draft)"
	i := strings.Index(s, absMark)
	if i < 0 {
		log.Fatalf("prep: %q not found in %s", absMark, in)
	}
	s = frontMatter + "\n## Abstract\n" + s[i+len(absMark):]

	// 2. Insert the four figures at their subsections.
	s = mustReplace(s, in,
		"| lock | 3,850 | 6,271 | 15.5ms/38ms | 9.0ms/39ms |\n\nThe reading is honest",
		"| lock | 3,850 | 6,271 | 15.5ms/38ms | 9.0ms/39ms |\n\n"+figThroughput+"\n\n"+figLatency+"\n\nThe reading is honest")
	s = mustReplace(s, in,
		"gossip-wait.)\n\n### 5.3 WAN profile",
		"gossip-wait.)\n\n"+figColdstart+"\n\n### 5.3 WAN profile")
	s = mustReplace(s, in,
		"replica RTTs,\" measured. Full data and method: `bench/README.md`.\n\n## 6. Limitations (honest)",
		"replica RTTs,\" measured. Full data and method: `bench/README.md`.\n\n"+figWan+"\n\n## 6. Limitations (honest)")

	// 3. Rewrite Unicode XeTeX's default font can't represent. The "≈1×RTT"
	//    fragment must go first: a bare pass would leave two adjacent math
	//    spans with a digit sandwiched between them, which pandoc misparses.
	s = strings.ReplaceAll(s, "≈1×RTT", `$\approx 1\times$RTT`)
	s = strings.NewReplacer(
		"→", `$\rightarrow$`,
		"×", `$\times$`,
		"≥", `$\geq$`,
		"≤", `$\leq$`,
		"≈", `$\approx$`,
		"½", `$1/2$`,
		"2^24", `$2^{24}$`,
	).Replace(s)

	if err := os.WriteFile(out, []byte(s), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("prepared %s -> %s", in, out)
}

// mustReplace replaces exactly one occurrence of old, failing loudly if the
// anchor has drifted — so a paper edit that moves an anchor breaks the build
// visibly rather than silently dropping a figure.
func mustReplace(s, src, old, new string) string {
	if strings.Count(s, old) != 1 {
		log.Fatalf("prep: anchor not found exactly once in %s:\n  %q", src, old)
	}
	return strings.Replace(s, old, new, 1)
}
