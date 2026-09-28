package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/internal/caspaxos"
	"github.com/phoban01/cask/internal/hlc"
	"github.com/phoban01/cask/internal/mvcc"
	"github.com/phoban01/cask/internal/store"
	apistorage "k8s.io/apiserver/pkg/storage"
)

// errInjected aborts a round before its accept phase.
var errInjected = errors.New("injected fault")

// mvccUnknownRetries is how many unknown outcomes mvcc resolves by itself
// before it returns caspaxos.ErrUnknownOutcome (mvcc.unknownOutcomeRetries).
// A fault must last this many rounds to reach the Store.
const mvccUnknownRetries = 8

// lossyProposer answers writes to one key with caspaxos.ErrUnknownOutcome.
// Once armed, it fails the next mvccUnknownRetries rounds that write the
// key. With lands false, no failed write reaches an acceptor. With lands
// true, the first failed write commits, and the answer is lost. The
// retries that follow then run, and their answers are lost too.
type lossyProposer struct {
	next  caspaxos.Proposing
	key   []byte
	lands bool

	mu      sync.Mutex
	left    int  // rounds still to fail
	landed  bool // the first failed write committed
	injects int  // rounds failed so far
}

func (p *lossyProposer) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.left, p.landed, p.injects = mvccUnknownRetries, false, 0
}

func (p *lossyProposer) Propose(ctx context.Context, key []byte, change caspaxos.ChangeFunc) ([]byte, error) {
	p.mu.Lock()
	armed, landed := p.left > 0 && bytes.Equal(key, p.key), p.landed
	p.mu.Unlock()
	if !armed {
		return p.next.Propose(ctx, key, change)
	}
	if landed {
		// The earlier write committed. This round runs, and its answer
		// is lost as well.
		_, _ = p.next.Propose(ctx, key, change)
		p.fail(true)
		return nil, caspaxos.ErrUnknownOutcome
	}
	wrote := false
	raw, err := p.next.Propose(ctx, key, func(cur []byte) ([]byte, error) {
		next, err := change(cur)
		if err == nil && !bytes.Equal(next, cur) {
			wrote = true
			if !p.lands {
				return nil, errInjected
			}
		}
		return next, err
	})
	if !wrote {
		return raw, err
	}
	p.fail(p.lands)
	return nil, caspaxos.ErrUnknownOutcome
}

func (p *lossyProposer) fail(landed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.left--
	p.injects++
	p.landed = landed
}

// newLossyStore returns a Store whose proposer fails index writes of
// devices when armed.
func newLossyStore(t *testing.T, lands bool) (*Store, *lossyProposer) {
	t.Helper()
	acceptors := make([]caspaxos.AcceptorClient, 3)
	for i := range acceptors {
		acceptors[i] = caspaxos.NewAcceptor(store.NewMem())
	}
	var now atomic.Int64
	clock := hlc.New(func() int64 { return now.Add(1) })
	lossy := &lossyProposer{next: caspaxos.NewProposer(1, acceptors), key: IndexKey("devices"), lands: lands}
	return newStoreOn(t, mvcc.New(lossy, clock, 1)), lossy
}

// A mutation whose object write committed must not fail because its index
// write has an unknown outcome. The Store reads again and retries, and the
// response still carries the write's own version at its own index step.
func TestIndexWriteSurvivesUnknownOutcome(t *testing.T) {
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A mutation whose object write committed MUST retry its index write when that index write loses a round or has an unknown outcome.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A create or an update MUST return the object version that it wrote, with the index sequence at which the index register recorded that version.
	//= docs/spec/fleet.md#3-storage-model
	//= type=test
	//# A delete MUST return the index sequence at which the index register removed the name.
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		lands bool
	}{{"write lost", false}, {"write landed", true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, lossy := newLossyStore(t, tc.lands)
			check := func(op string, err error, out *v1alpha1.Device, wantModel string, wantRV uint64) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s: %v", op, err)
				}
				lossy.mu.Lock()
				injects := lossy.injects
				lossy.mu.Unlock()
				if injects != mvccUnknownRetries {
					t.Fatalf("%s: %d faults injected, want %d", op, injects, mvccUnknownRetries)
				}
				if out.Spec.Model != wantModel || rvOf(t, out) != wantRV {
					t.Fatalf("%s returned %s@%s, want %s@%d", op, out.Spec.Model, out.ResourceVersion, wantModel, wantRV)
				}
				if got := mustIndex(t, s.kv, "devices").Seq; got != wantRV {
					t.Fatalf("%s: index sequence = %d, want %d", op, got, wantRV)
				}
			}

			lossy.arm()
			created := &v1alpha1.Device{}
			err := s.Create(ctx, keyPrefix+"gpu-0", device("gpu-0", "a100"), created, 0)
			check("create", err, created, "a100", 1)

			lossy.arm()
			updated := &v1alpha1.Device{}
			err = s.GuaranteedUpdate(ctx, keyPrefix+"gpu-0", updated, false, nil,
				mutate(func(d *v1alpha1.Device) { d.Spec.Model = "h100" }), nil)
			check("update", err, updated, "h100", 2)

			lossy.arm()
			deleted := &v1alpha1.Device{}
			err = s.Delete(ctx, keyPrefix+"gpu-0", deleted, nil, nil, nil, apistorage.DeleteOptions{})
			check("delete", err, deleted, "h100", 3)
			if _, named := indexEntry(t, s, "gpu-0"); named {
				t.Fatal("the index still names gpu-0 after the delete")
			}
		})
	}
}
