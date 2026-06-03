package failure

// CutDetector aggregates per-subject suspicion reports from many observers and
// only decides a node is DOWN (or back UP) once at least Threshold distinct
// observers agree. This is the Rapid insight: requiring multiple independent
// witnesses before a membership change suppresses the false positives a single
// flaky link or overloaded monitor would otherwise cause, keeping the consensus
// roster stable.
type CutDetector struct {
	threshold int
	// reports[subject][observer] = observer currently suspects subject.
	reports map[Node]map[Node]bool
}

// NewCutDetector requires threshold agreeing observers to declare a change.
func NewCutDetector(threshold int) *CutDetector {
	if threshold < 1 {
		threshold = 1
	}
	return &CutDetector{threshold: threshold, reports: map[Node]map[Node]bool{}}
}

// Report records observer's current opinion (down=true means suspected) of
// subject. Re-reporting overwrites the observer's previous opinion.
func (c *CutDetector) Report(observer, subject Node, down bool) {
	m := c.reports[subject]
	if m == nil {
		m = map[Node]bool{}
		c.reports[subject] = m
	}
	if down {
		m[observer] = true
	} else {
		delete(m, observer)
	}
}

// DownVotes returns how many observers currently suspect subject.
func (c *CutDetector) DownVotes(subject Node) int { return len(c.reports[subject]) }

// IsDown reports whether subject has reached the agreement threshold.
func (c *CutDetector) IsDown(subject Node) bool { return c.DownVotes(subject) >= c.threshold }

// Down returns every subject that has reached the threshold — the set of nodes
// the roster should propose removing.
func (c *CutDetector) Down() []Node {
	var out []Node
	for subject := range c.reports {
		if c.IsDown(subject) {
			out = append(out, subject)
		}
	}
	return out
}

// Clear forgets all reports about subject (e.g. after the roster has acted on
// it, or it has recovered).
func (c *CutDetector) Clear(subject Node) { delete(c.reports, subject) }
