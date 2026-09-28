package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/phoban01/cask/internal/mtls"
)

// genConsensusCerts implements `cask-apiserver gen-consensus-certs`. It
// makes a fleet CA and one member certificate per name, and writes ca.crt,
// <name>.crt, and <name>.key to --out. It does not write the CA key, so a
// later member needs a new CA. The kind demo uses it; a real fleet uses its
// own CA.
func genConsensusCerts(args []string) error {
	flags := flag.NewFlagSet("gen-consensus-certs", flag.ContinueOnError)
	out := flags.String("out", ".", "directory to write ca.crt, <name>.crt, and <name>.key to")
	ttl := flags.Duration("ttl", 365*24*time.Hour, "validity of the CA and the member certificates")
	if err := flags.Parse(args); err != nil {
		return err
	}
	names := flags.Args()
	if len(names) == 0 {
		return fmt.Errorf("usage: cask-apiserver gen-consensus-certs [--out dir] name...")
	}
	for _, name := range names {
		// A name becomes a file name in --out. It must not reach outside
		// --out, and "ca" would overwrite the CA certificate.
		if name == "." || name == ".." || name == "ca" || strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") {
			return fmt.Errorf("bad member name %q: use a plain name such as east", name)
		}
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
		if err := writeKey(filepath.Join(*out, name+".key"), key); err != nil {
			return err
		}
	}
	return nil
}

// writeKey writes a private key readable by its owner only. os.WriteFile
// keeps the mode of a file that exists, so writeKey removes it first and
// creates the new file exclusively.
func writeKey(path string, key []byte) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(key); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
