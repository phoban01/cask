package failure

import "testing"

func TestPhiLowWhileHeartbeating(t *testing.T) {
	d := New(100, 10)
	const interval = 1000
	now := int64(0)
	for i := 0; i < 50; i++ {
		now += interval
		d.Heartbeat("a", now)
	}
	// Checked right at the expected next-beat time, suspicion is low.
	if got := d.Phi("a", now+interval); got > 1.0 {
		t.Fatalf("phi while healthy = %.2f, want < 1", got)
	}
}

func TestPhiRisesWhenHeartbeatsStop(t *testing.T) {
	d := New(100, 10)
	const interval = 1000
	now := int64(0)
	for i := 0; i < 50; i++ {
		now += interval
		d.Heartbeat("a", now)
	}
	// Long after the last heartbeat, phi must cross a typical down threshold.
	silent := now + 10*interval
	if got := d.Phi("a", silent); got < 8.0 {
		t.Fatalf("phi after long silence = %.2f, want >= 8", got)
	}
	suspected := d.Suspected(silent, 8.0)
	if len(suspected) != 1 || suspected[0] != "a" {
		t.Fatalf("Suspected = %v, want [a]", suspected)
	}
}

func TestPhiZeroWithoutSamples(t *testing.T) {
	d := New(100, 10)
	if got := d.Phi("never-seen", 9999); got != 0 {
		t.Fatalf("phi for unknown peer = %.2f, want 0", got)
	}
	d.Heartbeat("a", 100) // a single beat: still no interval samples
	if got := d.Phi("a", 100000); got != 0 {
		t.Fatalf("phi with one beat = %.2f, want 0", got)
	}
}

func TestCutDetectorNeedsThreshold(t *testing.T) {
	c := NewCutDetector(3)

	c.Report("o1", "x", true)
	c.Report("o2", "x", true)
	if c.IsDown("x") {
		t.Fatal("declared down with only 2/3 observers")
	}
	c.Report("o3", "x", true)
	if !c.IsDown("x") {
		t.Fatal("not down with 3/3 observers")
	}
	if got := c.Down(); len(got) != 1 || got[0] != "x" {
		t.Fatalf("Down() = %v, want [x]", got)
	}

	// An observer retracting drops it back below threshold (recovery / flap).
	c.Report("o2", "x", false)
	if c.IsDown("x") {
		t.Fatal("still down after an observer retracted")
	}
}

func TestCutDetectorIgnoresDuplicateObserver(t *testing.T) {
	c := NewCutDetector(2)
	c.Report("o1", "y", true)
	c.Report("o1", "y", true) // same observer twice must not count as two votes
	if c.IsDown("y") {
		t.Fatal("one observer reporting twice should not reach threshold 2")
	}
	if got := c.DownVotes("y"); got != 1 {
		t.Fatalf("DownVotes = %d, want 1", got)
	}
}
