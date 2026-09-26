package backoff_test

import (
	"context"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/backoff"
)

// Delays stay within [0, min(max, base<<attempt)] and the exponent saturates
// instead of overflowing at large attempt numbers.
func TestSeededBounds(t *testing.T) {
	ctx := context.Background()
	f := backoff.Seeded(time.Millisecond, 8*time.Millisecond, 42)
	for attempt := range 70 { // past any shift-overflow boundary
		start := time.Now()
		if err := f(ctx, attempt); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if d := time.Since(start); d > 100*time.Millisecond {
			t.Fatalf("attempt %d slept %v, cap is 8ms", attempt, d)
		}
	}
}

// A cancelled context aborts the sleep with the context's error.
func TestCancelAbortsSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := backoff.FullJitter(time.Hour, time.Hour)
	start := time.Now()
	if err := f(ctx, 10); err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancelled backoff still slept")
	}
}
