package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/phoban01/cask/internal/mtls"
)

// genConsensusCerts implements `cask-apiserver gen-consensus-certs`. It
// makes a fleet CA and one member certificate per name, and writes ca.crt,
// <name>.crt, and <name>.key to --out. It does not write the CA key, so a
// later member needs a new CA. The kind demo uses it; a real fleet uses its
// own CA.
func genConsensusCerts(args []string) error {
	fs := flag.NewFlagSet("gen-consensus-certs", flag.ContinueOnError)
	out := fs.String("out", ".", "directory to write ca.crt, <name>.crt, and <name>.key to")
	ttl := fs.Duration("ttl", 365*24*time.Hour, "validity of the CA and the member certificates")
	if err := fs.Parse(args); err != nil {
		return err
	}
	names := fs.Args()
	if len(names) == 0 {
		return fmt.Errorf("usage: cask-apiserver gen-consensus-certs [--out dir] name...")
	}
	ca, err := mtls.NewCA("cask-consensus-ca", *ttl)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "ca.crt"), ca.CertPEM(), 0o644); err != nil {
		return err
	}
	for _, name := range names {
		cert, key, err := ca.Issue(name, name)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, name+".crt"), cert, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, name+".key"), key, 0o600); err != nil {
			return err
		}
	}
	return nil
}
