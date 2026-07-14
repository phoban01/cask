package buggify_test

import (
	"testing"

	"github.com/phoban01/cask/internal/buggify"
)

// With no hook installed (the production case), a site never fires regardless
// of its probability hint.
func TestMaybeFalseWithoutHook(t *testing.T) {
	buggify.Reset()
	if buggify.Maybe("x", 1.0) {
		t.Fatal("Maybe fired with no hook installed; production must take a single nil-check and return false")
	}
}

// An installed hook is consulted, and its decision is what Maybe returns.
func TestMaybeConsultsHook(t *testing.T) {
	t.Cleanup(buggify.Reset)

	var gotName string
	var gotProb float64
	buggify.Hook = func(name string, prob float64) bool {
		gotName, gotProb = name, prob
		return true
	}
	if !buggify.Maybe("site_a", 0.25) {
		t.Fatal("Maybe should return the hook's true decision")
	}
	if gotName != "site_a" || gotProb != 0.25 {
		t.Fatalf("hook saw (%q, %v), want (site_a, 0.25)", gotName, gotProb)
	}

	buggify.Hook = func(string, float64) bool { return false }
	if buggify.Maybe("site_a", 1.0) {
		t.Fatal("Maybe should return the hook's false decision")
	}
}

// Register populates the hook-independent declared set, which Declared exposes.
// This must work with no Hook installed, since Register runs at package init.
func TestRegisterPopulatesDeclared(t *testing.T) {
	buggify.Reset()
	buggify.Register("site_decl", "a description", 0.05)

	var found *buggify.SiteInfo
	for _, s := range buggify.Declared() {
		if s.Name == "site_decl" {
			s := s
			found = &s
			break
		}
	}
	if found == nil {
		t.Fatal("Declared did not include the registered site")
	}
	if found.Description != "a description" || found.DefaultProb != 0.05 {
		t.Fatalf("declared site = %+v, want desc 'a description' prob 0.05", *found)
	}
}
