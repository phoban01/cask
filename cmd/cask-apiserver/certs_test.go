package main

import (
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
