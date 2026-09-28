package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phoban01/cask/internal/roster"
)

// postIDs sends body to path on the member at addr and returns the status,
// the Location header, and the body.
func postIDs(t *testing.T, c *http.Client, addr, path, body string) (int, string, []byte) {
	t.Helper()
	resp, err := c.Post("http://"+addr+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s to %s: %v", path, addr, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Location"), out
}

// noRedirects is a client that returns a redirect instead of following it.
var noRedirects = &http.Client{
	Timeout:       30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// follows is a client that follows a redirect to the driver.
var follows = &http.Client{Timeout: 30 * time.Second}

func decodeChange(t *testing.T, body []byte) voterChange {
	t.Helper()
	var vc voterChange
	if err := json.Unmarshal(body, &vc); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return vc
}

// recordJoints wraps m's carry so the test sees every joint core that m
// publishes during a core change.
func recordJoints(m *membership) func() []roster.Joint {
	var mu sync.Mutex
	var seen []roster.Joint
	m.rost.SetCarry(func(ctx context.Context, v roster.Value) error {
		if v.Joint != nil {
			mu.Lock()
			seen = append(seen, *v.Joint)
			mu.Unlock()
		}
		return m.carry(ctx, v)
	})
	return func() []roster.Joint {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(seen)
	}
}

// One promote call grows the core from one voter to three by joint
// consensus, and every data key written on the one-voter core survives the
// loss of the founder.
func TestPromoteTwoParticipantsGrowsCoreToThree(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter set MUST change only by joint-consensus reconfiguration of the roster.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A promote or demote request MUST apply its whole list of member ids in one core change.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST carry every data register forward to the new core before it releases the old core.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, stopFounder := startFleetOfThree(t, ctx)
	founder, second := ms[0], ms[1]
	joints := recordJoints(founder)
	before, _ := founder.current()

	want := writeKeys(t, ctx, 20, founder, second)

	code, _, body := postIDs(t, noRedirects, founder.cfg.Advertise, promotePath, "[2, 3]")
	if code != http.StatusOK {
		t.Fatalf("promote [2 3] = %d %s, want 200", code, body)
	}
	vc := decodeChange(t, body)
	if !slices.Equal(vc.Core, []uint64{1, 2, 3}) || vc.ConfigGen <= before.ConfigGen {
		t.Fatalf("promote answered core %v cfg_gen %d, want core [1 2 3] and cfg_gen above %d", vc.Core, vc.ConfigGen, before.ConfigGen)
	}
	// The change went through exactly one joint core, from [1] to
	// [1 2 3]. It never published a core of two voters.
	got := joints()
	if len(got) == 0 {
		t.Fatal("no joint core was published: the core did not change by joint consensus")
	}
	for _, j := range got {
		if !slices.Equal(j.Old, []uint64{1}) || !slices.Equal(j.New, []uint64{1, 2, 3}) {
			t.Fatalf("joint core %v -> %v, want [1] -> [1 2 3]", j.Old, j.New)
		}
	}
	for _, m := range ms[1:] {
		waitFor(t, 5*time.Second, "the new voters to see core [1 2 3]", func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}

	// Voters 2 and 3 are a quorum of the new core without the founder.
	stopFounder()
	checkKeys(t, ctx, second, want, "after the promote and the founder stopped")
}

// A promote that would give two voters is refused, and the core does not
// change.
func TestPromoteToTwoVotersIsRefused(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter count MUST be odd.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, _ := startFleetOfThree(t, ctx)
	founder := ms[0]
	before, _ := founder.current()

	code, _, body := postIDs(t, noRedirects, founder.cfg.Advertise, promotePath, "[2]")
	if code != http.StatusConflict || !strings.Contains(string(body), "must be odd") {
		t.Fatalf("promote [2] = %d %s, want 409 naming the odd voter count", code, body)
	}
	for _, bad := range []string{`2`, `{"ids":[2]}`, `not json`} {
		if code, _, body := postIDs(t, noRedirects, founder.cfg.Advertise, promotePath, bad); code != http.StatusBadRequest {
			t.Errorf("promote body %q = %d %s, want 400", bad, code, body)
		}
	}
	resp, err := noRedirects.Get("http://" + founder.cfg.Advertise + demotePath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET %s = %d, want 405", demotePath, resp.StatusCode)
	}

	after, err := founder.rost.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Core, []uint64{1}) || after.Joint != nil || after.ConfigGen != before.ConfigGen {
		t.Fatalf("after refusals the roster has core %v joint %v cfg_gen %d, want core [1] cfg_gen %d", after.Core, after.Joint, after.ConfigGen, before.ConfigGen)
	}
}

// A demote shrinks the core from three voters to one and keeps every data
// key. The request goes to the founder, which is not the driver of [1 2 3],
// and a client that follows the redirect reaches the driver.
func TestDemoteThreeToOneKeepsKeys(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A core change MUST carry every data register forward to the new core before it releases the old core.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A promote or demote request MUST apply its whole list of member ids in one core change.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, _ := startFleetOfThree(t, ctx)
	founder := ms[0]

	if code, _, body := postIDs(t, noRedirects, founder.cfg.Advertise, promotePath, "[2, 3]"); code != http.StatusOK {
		t.Fatalf("promote [2 3] = %d %s, want 200", code, body)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to see core [1 2 3]", func() bool { return hasCore(m, []uint64{1, 2, 3}) })
	}
	want := writeKeys(t, ctx, 20, ms...)

	code, _, body := postIDs(t, follows, founder.cfg.Advertise, demotePath, "[2, 3]")
	if code != http.StatusOK {
		t.Fatalf("demote [2 3] = %d %s, want 200", code, body)
	}
	if vc := decodeChange(t, body); !slices.Equal(vc.Core, []uint64{1}) {
		t.Fatalf("demote answered core %v, want [1]", vc.Core)
	}
	for _, m := range ms {
		waitFor(t, 5*time.Second, "every member to see core [1]", func() bool { return hasCore(m, []uint64{1}) })
	}

	// Members 2 and 3 stop answering. The founder alone is the core now,
	// so it must hold every key.
	hang(ms[1])
	hang(ms[2])
	checkKeys(t, ctx, founder, want, "after the demote with members 2 and 3 hung")
}

// A member that is not the driver does not run the change. It answers 307
// with the driver's address and does not forward the request.
func TestVoterChangeOnNonDriverRedirects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms, _ := startFleetOfThree(t, ctx)
	founder, second := ms[0], ms[1]

	code, loc, body := postIDs(t, noRedirects, second.cfg.Advertise, promotePath, "[2, 3]")
	if code != http.StatusTemporaryRedirect {
		t.Fatalf("promote on a participant = %d %s, want 307", code, body)
	}
	if want := "http://" + founder.cfg.Advertise + promotePath; loc != want {
		t.Fatalf("Location = %q, want %q", loc, want)
	}
	var red struct {
		Driver string `json:"driver"`
	}
	if err := json.Unmarshal(body, &red); err != nil || red.Driver != founder.cfg.Advertise {
		t.Fatalf("redirect body %s names driver %q, want %q (err %v)", body, red.Driver, founder.cfg.Advertise, err)
	}
	if v, _ := second.current(); !slices.Equal(v.Core, []uint64{1}) {
		t.Fatalf("a participant changed the core to %v", v.Core)
	}

	// A client that follows the redirect sends the same body to the driver,
	// and the driver answers.
	code, _, body = postIDs(t, follows, second.cfg.Advertise, promotePath, "[2]")
	if code != http.StatusConflict {
		t.Fatalf("followed promote [2] = %d %s, want the driver's 409", code, body)
	}
}

// planVoters refuses every request that breaks a voter rule.
func TestPlanVotersRules(t *testing.T) {
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter count MUST be odd.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# The voter count MUST NOT exceed five.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A promote request MUST name only participants.
	//= docs/spec/fleet.md#6-membership
	//= type=test
	//# A demote request MUST name only voters.
	value := func(core ...uint64) roster.Value {
		v := roster.Value{Core: core}
		for id := uint64(1); id <= 7; id++ {
			v.Members = append(v.Members, roster.Member{NodeID: id})
		}
		return v
	}
	joint := value(1)
	joint.Joint = &roster.Joint{Old: []uint64{1}, New: []uint64{1, 2, 3}}
	for _, tc := range []struct {
		name    string
		cur     roster.Value
		ids     []uint64
		promote bool
		want    []uint64 // nil: refused
		refusal string   // text in the refusal
	}{
		{"one to three", value(1), []uint64{2, 3}, true, []uint64{1, 2, 3}, ""},
		{"three to five", value(1, 2, 3), []uint64{4, 5}, true, []uint64{1, 2, 3, 4, 5}, ""},
		{"one to five", value(1), []uint64{2, 3, 4, 5}, true, []uint64{1, 2, 3, 4, 5}, ""},
		{"duplicate ids", value(1), []uint64{3, 2, 3}, true, []uint64{1, 2, 3}, ""},
		{"one to two", value(1), []uint64{2}, true, nil, "must be odd"},
		{"three to four", value(1, 2, 3), []uint64{4}, true, nil, "must be odd"},
		{"one to seven", value(1), []uint64{2, 3, 4, 5, 6, 7}, true, nil, "must not exceed 5"},
		{"promote non-member", value(1), []uint64{2, 9}, true, nil, "not a roster member"},
		{"promote voter", value(1), []uint64{1, 2}, true, nil, "already a voter"},
		{"promote nothing", value(1), nil, true, nil, "at least one"},
		{"three to one", value(1, 2, 3), []uint64{2, 3}, false, []uint64{1}, ""},
		{"five to three", value(1, 2, 3, 4, 5), []uint64{1, 5}, false, []uint64{2, 3, 4}, ""},
		{"three to two", value(1, 2, 3), []uint64{3}, false, nil, "must be odd"},
		{"demote all", value(1, 2, 3), []uint64{1, 2, 3}, false, nil, "no voters"},
		{"demote participant", value(1, 2, 3), []uint64{4, 5}, false, nil, "not a voter"},
		{"demote non-member", value(1, 2, 3), []uint64{9}, false, nil, "not a voter"},
		{"demote nothing", value(1, 2, 3), []uint64{}, false, nil, "at least one"},
	} {
		got, err := planVoters(tc.cur, tc.ids, tc.promote)
		if tc.want != nil {
			if err != nil || !slices.Equal(got, tc.want) {
				t.Errorf("%s: plan = %v, %v; want %v", tc.name, got, err, tc.want)
			}
			continue
		}
		var refused *refusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.msg, tc.refusal) {
			t.Errorf("%s: plan = %v, %v; want a refusal naming %q", tc.name, got, err, tc.refusal)
		}
	}
	if _, err := planVoters(joint, []uint64{4, 5}, true); !errors.Is(err, errCoreChangeInFlight) {
		t.Errorf("plan during a joint core = %v, want errCoreChangeInFlight", err)
	}
}
