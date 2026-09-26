package main

// Generates the paper's evaluation figures as vector PDFs from the measured
// benchmark data (see bench/README.md). Palette is Okabe-Ito (colorblind-safe);
// every series also carries a second encoding — marker shape and dash pattern
// on lines, distinct lightness on bars — so the figures survive grayscale
// printing.

import (
	"image/color"
	"log"
	"path/filepath"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

var (
	caskC = color.RGBA{R: 0x00, G: 0x72, B: 0xB2, A: 255} // Okabe-Ito blue
	etcdC = color.RGBA{R: 0xD5, G: 0x5E, B: 0x00, A: 255} // Okabe-Ito vermillion
	grayC = color.RGBA{R: 0x77, G: 0x77, B: 0x77, A: 255}
)

const (
	figW = 5.4 * vg.Inch
	figH = 3.3 * vg.Inch
)

var outDir = "."

func genFigures(dir string) {
	outDir = dir
	throughput()
	latency()
	coldstart()
	wan()
	log.Printf("figures written to %s", dir)
}

func newPlot(title, xlab, ylab string) *plot.Plot {
	p := plot.New()
	p.Title.Text = title
	p.X.Label.Text = xlab
	p.Y.Label.Text = ylab
	p.BackgroundColor = color.White
	p.Title.TextStyle.Font.Size = 13
	p.Legend.TextStyle.Font.Size = 10
	return p
}

func mustSave(p *plot.Plot, name string) {
	if err := p.Save(figW, figH, filepath.Join(outDir, name)); err != nil {
		log.Fatalf("%s: %v", name, err)
	}
}

func group(vals []float64, c color.Color, off vg.Length) *plotter.BarChart {
	b, err := plotter.NewBarChart(plotter.Values(vals), vg.Points(11))
	if err != nil {
		log.Fatal(err)
	}
	b.Color = c
	b.LineStyle.Width = 0
	b.Offset = off
	return b
}

// Figure 1 — throughput vs etcd (grouped bars).
func throughput() {
	cask := []float64{10626, 9586, 10582, 3850}
	etcd := []float64{13168, 65282, 12173, 6271}
	p := newPlot("Throughput vs etcd (64 clients, same host)", "", "throughput (ops/s)")
	cb, eb := group(cask, caskC, -vg.Points(6)), group(etcd, etcdC, vg.Points(6))
	p.Add(cb, eb)
	p.Legend.Add("cask", cb)
	p.Legend.Add("etcd", eb)
	p.Legend.Top = true
	p.NominalX("put", "get", "cas", "lock")
	p.Y.Min = 0
	mustSave(p, "fig-throughput.pdf")
}

// Figure 2 — p99 latency vs etcd (grouped bars).
func latency() {
	cask := []float64{19.1, 21.9, 19.0, 37.9}
	etcd := []float64{10.2, 3.4, 12.6, 38.8}
	p := newPlot("p99 latency vs etcd (64 clients)", "", "p99 latency (ms)")
	cb, eb := group(cask, caskC, -vg.Points(6)), group(etcd, etcdC, vg.Points(6))
	p.Add(cb, eb)
	p.Legend.Add("cask", cb)
	p.Legend.Add("etcd", eb)
	p.Legend.Top = true
	p.NominalX("put", "get", "cas", "lock")
	p.Y.Min = 0
	mustSave(p, "fig-latency.pdf")
}

func xys(x, y []float64) plotter.XYs {
	pts := make(plotter.XYs, len(x))
	for i := range x {
		pts[i].X, pts[i].Y = x[i], y[i]
	}
	return pts
}

func series(p *plot.Plot, x, y []float64, c color.Color, dashes []vg.Length, g draw.GlyphDrawer, name string) {
	ln, sc, err := plotter.NewLinePoints(xys(x, y))
	if err != nil {
		log.Fatal(err)
	}
	ln.LineStyle.Color = c
	ln.LineStyle.Width = vg.Points(1.8)
	ln.LineStyle.Dashes = dashes
	sc.GlyphStyle.Color = c
	sc.GlyphStyle.Radius = vg.Points(3)
	sc.GlyphStyle.Shape = g
	p.Add(ln, sc)
	if name != "" {
		p.Legend.Add(name, ln, sc)
	}
}

// Figure 3 — cold start: join latency vs per-hop RTT (log y), serial vs fanout.
func coldstart() {
	rtt := []float64{0, 1, 2, 5, 10, 20}
	serial := []float64{1.85, 241.99, 423.58, 703.99, 1226.23, 2429.57}
	fanout := []float64{2.72, 25.95, 37.16, 60.65, 105.04, 233.62}
	p := newPlot("Cold start: node join latency (100 nodes, 50 ranges)", "injected per-hop RTT (ms)", "p99 join latency (ms, log)")
	tgt, err := plotter.NewLine(xys([]float64{0, 20}, []float64{1000, 1000}))
	if err != nil {
		log.Fatal(err)
	}
	tgt.LineStyle.Color = grayC
	tgt.LineStyle.Width = vg.Points(1)
	tgt.LineStyle.Dashes = []vg.Length{vg.Points(2), vg.Points(2)}
	p.Add(tgt)
	p.Legend.Add("1 s target", tgt)
	series(p, rtt, serial, etcdC, nil, draw.CircleGlyph{}, "serial fetch (1+N round-trips)")
	series(p, rtt, fanout, caskC, []vg.Length{vg.Points(5), vg.Points(3)}, draw.BoxGlyph{}, "fanout fetch (~2 round-trips)")
	p.Legend.Top = true
	p.Legend.Left = true
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{}
	p.Y.Min = 1
	mustSave(p, "fig-coldstart.pdf")
}

// Figure 4 — WAN: write latency tracks ~2xRTT; one straggler is off the path.
func wan() {
	rtt := []float64{1, 5, 20, 50}
	p50 := []float64{4.00, 11.67, 44.40, 106.21}
	ref := []float64{2, 10, 40, 100} // 2xRTT reference
	p := newPlot("WAN: write latency vs per-hop RTT (RF=5)", "injected per-hop RTT (ms)", "p50 write latency (ms)")
	rl, err := plotter.NewLine(xys(rtt, ref))
	if err != nil {
		log.Fatal(err)
	}
	rl.LineStyle.Color = grayC
	rl.LineStyle.Width = vg.Points(1)
	rl.LineStyle.Dashes = []vg.Length{vg.Points(2), vg.Points(2)}
	p.Add(rl)
	p.Legend.Add("2x RTT reference", rl)
	series(p, rtt, p50, caskC, nil, draw.CircleGlyph{}, "cask uniform")
	strag, err := plotter.NewScatter(xys([]float64{20}, []float64{44.41}))
	if err != nil {
		log.Fatal(err)
	}
	strag.GlyphStyle.Color = etcdC
	strag.GlyphStyle.Radius = vg.Points(5)
	strag.GlyphStyle.Shape = draw.PyramidGlyph{}
	p.Add(strag)
	p.Legend.Add("20ms + 1 straggler @400ms", strag)
	p.Legend.Top = true
	p.Legend.Left = true
	p.Y.Min = 0
	mustSave(p, "fig-wan.pdf")
}
