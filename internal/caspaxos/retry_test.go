package caspaxos

import (
	"context"
	"errors"
	"testing"
)

func noPause(context.Context, int) error { return nil }

// RetryLost runs a lost round again, stops at any other error, and stops
// after a fixed number of attempts.
func TestRetryLost(t *testing.T) {
	ctx := context.Background()
	other := errors.New("other")
	for _, tc := range []struct {
		name      string
		errs      []error // the result of each call; nil after the list
		wantCalls int
		wantErr   error
	}{
		{"first call wins", nil, 1, nil},
		{"lost then won", []error{ErrPreempted, ErrUnknownOutcome}, 3, nil},
		{"other error stops", []error{ErrPreempted, other}, 2, other},
		{"every call lost", []error{ErrPreempted, ErrPreempted, ErrPreempted, ErrPreempted, ErrPreempted}, 4, ErrPreempted},
	} {
		calls := 0
		v, err := RetryLost(ctx, 4, noPause, func() (int, error) {
			calls++
			if calls <= len(tc.errs) {
				return 0, tc.errs[calls-1]
			}
			return calls, nil
		})
		if calls != tc.wantCalls || (err == nil) != (tc.wantErr == nil) || (err != nil && !errors.Is(err, tc.wantErr)) {
			t.Errorf("%s: %d calls, err %v; want %d calls, err %v", tc.name, calls, err, tc.wantCalls, tc.wantErr)
		}
		if err == nil && v != calls {
			t.Errorf("%s: value %d, want %d", tc.name, v, calls)
		}
	}

	// A failed pause ends the retries and names both errors.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := RetryLost(cctx, 4, func(ctx context.Context, _ int) error { return ctx.Err() }, func() (int, error) {
		return 0, ErrPreempted
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrPreempted) {
		t.Errorf("cancelled pause: err %v, want context.Canceled and ErrPreempted", err)
	}
}
