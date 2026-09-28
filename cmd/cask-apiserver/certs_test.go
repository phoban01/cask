package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/phoban01/cask/internal/mtls"
)

// The demo's certificate command writes files that the consensus flags
// accept, one identity per member, all from one CA.
func TestGenConsensusCerts(t *testing.T) {
	dir := t.TempDir()
	if err := genConsensusCerts([]string{"--out", dir, "east", "west"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"east", "west"} {
		_, err := mtls.Load(mtls.Files{
			Cert: filepath.Join(dir, name+".crt"),
			Key:  filepath.Join(dir, name+".key"),
			CA:   filepath.Join(dir, "ca.crt"),
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if err := genConsensusCerts([]string{"--out", dir}); err == nil {
		t.Fatal("no member names: want an error")
	}
}

// A member name cannot write outside --out or over the CA certificate.
func TestGenConsensusCertsRejectsPathNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"../east", "a/b", `a\b`, "..", ".", "x..y", "ca"} {
		if err := genConsensusCerts([]string{"--out", dir, name}); err == nil {
			t.Errorf("name %q: want an error", name)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("rejected names wrote %d files", len(entries))
	}
}

// A key file is 0600 even when a looser file was there before.
func TestGenConsensusCertsKeyMode(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "east.key")
	if err := os.WriteFile(key, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := genConsensusCerts([]string{"--out", dir, "east"}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("key mode = %v, want 0600", st.Mode().Perm())
	}
}
