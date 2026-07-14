package caspaxos_test

import (
	"context"
	"errors"
	"testing"

	"github.com/phoban01/cask/internal/caspaxos"
)

// appendChange appends marker to a comma-joined list value. Values encode the
// full write history, so a lost update is visible as a missing marker.
func appendChange(marker string) caspaxos.ChangeFunc {
	return func(cur []byte) ([]byte, error) {
		if len(cur) == 0 {
			return []byte(marker), nil
		}
		out := append(append([]byte{}, cur...), ',')
		return append(out, marker...), nil
	}
}

// The W0 regression: a full proposer that preempts an owner must jump clear of
// the owner's epoch space. If it instead bumped the conflict counter by one, it
// would mint the exact counter of the owner's next sequence-bumped write, and
// whichever NodeID is higher would win the tiebreak — in the owner-higher
// order, the owner's stale-cache accept silently overwrites the full
// proposer's committed value (a lost update). Both NodeID orders must behave
// identically: the full proposer's write survives and the owner is fenced out.
func TestFullProposerPreemptsOwnerNoLostUpdate(t *testing.T) {
	cases := []struct {
		name          string
		ownerID, fullID uint64
	}{
		// The dangerous order: pre-fix, the owner wins the tiebreak and the
		// full proposer's committed "b" is silently lost.
		{"owner has higher node id", 9, 2},
		// The benign order: pre-fix this happened to be safe by luck of the
		// tiebreak; it must of course stay safe.
		{"owner has lower node id", 1, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			acc := ownedCluster(3)
			key := []byte("k")

			owner := caspaxos.NewOwnedProposer(tc.ownerID, acc)
			if err := owner.TakeOwnership(ctx, key, 1); err != nil {
				t.Fatalf("take ownership: %v", err)
			}
			if _, err := owner.Write(ctx, key, appendChange("a")); err != nil {
				t.Fatalf("owner write a: %v", err)
			}

			// A full proposer preempts the owner. It must commit on top of the
			// owner's value from a ballot the owner can never tie.
			full := caspaxos.NewProposer(tc.fullID, acc)
			got, err := full.Propose(ctx, key, appendChange("b"))
			if err != nil {
				t.Fatalf("full propose b: %v", err)
			}
			if string(got) != "a,b" {
				t.Fatalf("full propose = %q, want a,b", got)
			}

			// The owner, unaware, writes from its stale cache. It must be
			// fenced out and told which epoch the register has reached.
			_, err = owner.Write(ctx, key, appendChange("c"))
			if !errors.Is(err, caspaxos.ErrLostOwnership) {
				t.Fatalf("stale owner write = %v, want ErrLostOwnership", err)
			}
			var behind *caspaxos.EpochBehindError
			if !errors.As(err, &behind) {
				t.Fatalf("stale owner write = %v, want EpochBehindError", err)
			}
			if behind.Observed != 2 {
				t.Fatalf("observed epoch = %d, want 2 (the jump boundary)", behind.Observed)
			}

			// Re-acquiring above the observed epoch resumes the fast path with
			// the freshly carried-forward value.
			if err := owner.TakeOwnership(ctx, key, behind.Observed+1); err != nil {
				t.Fatalf("re-take ownership: %v", err)
			}
			if _, err := owner.Write(ctx, key, appendChange("c")); err != nil {
				t.Fatalf("owner write c after re-take: %v", err)
			}

			reader := caspaxos.NewProposer(5, acc)
			final, err := reader.Propose(ctx, key, caspaxos.Identity)
			if err != nil {
				t.Fatalf("final read: %v", err)
			}
			if string(final) != "a,b,c" {
				t.Fatalf("final value = %q, want a,b,c (no lost update)", final)
			}
		})
	}
}

// Repeated full-proposer preemption keeps the register progressing: every
// jump lands on a fresh epoch boundary and each committed write survives.
func TestRepeatedPreemptionAccumulates(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(3)
	key := []byte("k")

	owner := caspaxos.NewOwnedProposer(9, acc)
	full := caspaxos.NewProposer(1, acc)

	want := ""
	epoch := uint64(1)
	for i := range 3 {
		if err := owner.TakeOwnership(ctx, key, epoch); err != nil {
			t.Fatalf("round %d take ownership at %d: %v", i, epoch, err)
		}
		if _, err := owner.Write(ctx, key, appendChange("o")); err != nil {
			t.Fatalf("round %d owner write: %v", i, err)
		}
		if _, err := full.Propose(ctx, key, appendChange("f")); err != nil {
			t.Fatalf("round %d full propose: %v", i, err)
		}
		_, err := owner.Write(ctx, key, appendChange("x"))
		var behind *caspaxos.EpochBehindError
		if !errors.As(err, &behind) {
			t.Fatalf("round %d stale write = %v, want EpochBehindError", i, err)
		}
		epoch = behind.Observed + 1
		if want == "" {
			want = "o,f"
		} else {
			want += ",o,f"
		}
	}
	final, err := full.Propose(ctx, key, caspaxos.Identity)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if string(final) != want {
		t.Fatalf("final value = %q, want %q", final, want)
	}
}

func TestTakeOwnershipRejectsEpochZero(t *testing.T) {
	owner := caspaxos.NewOwnedProposer(1, ownedCluster(3))
	if err := owner.TakeOwnership(context.Background(), []byte("k"), 0); !errors.Is(err, caspaxos.ErrEpochReserved) {
		t.Fatalf("take ownership at epoch 0 = %v, want ErrEpochReserved", err)
	}
}

// TakeOwnership at a stale epoch reports the epoch the register has reached.
func TestTakeOwnershipReportsEpochBehind(t *testing.T) {
	ctx := context.Background()
	acc := ownedCluster(3)
	key := []byte("k")

	cur := caspaxos.NewOwnedProposer(1, acc)
	if err := cur.TakeOwnership(ctx, key, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := cur.Write(ctx, key, appendChange("v")); err != nil {
		t.Fatal(err)
	}

	stale := caspaxos.NewOwnedProposer(2, acc)
	err := stale.TakeOwnership(ctx, key, 3)
	var behind *caspaxos.EpochBehindError
	if !errors.As(err, &behind) {
		t.Fatalf("stale take ownership = %v, want EpochBehindError", err)
	}
	if behind.Observed != 5 {
		t.Fatalf("observed epoch = %d, want 5", behind.Observed)
	}
}
