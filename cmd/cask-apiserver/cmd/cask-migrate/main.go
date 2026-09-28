// Command cask-migrate moves fleet objects from a CRD into cask.
//
//	cask-migrate export --output FILE [--group fleet.cask.dev] [--kubeconfig PATH] [--context NAME]
//
// export reads every object of the group through the Kubernetes API and
// writes an export file. It never writes to the cluster. It prints the
// source etcd revision. See package migrate for the file format.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/phoban01/cask/cmd/cask-apiserver/apis/fleet/v1alpha1"
	"github.com/phoban01/cask/cmd/cask-apiserver/migrate"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

const usage = `usage: cask-migrate <command> [flags]

commands:
  export   write every object of an API group to an export file
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var err error
	switch os.Args[1] {
	case "export":
		err = runExport(ctx, os.Args[2:], os.Stdout, os.Stderr)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "cask-migrate:", err)
		os.Exit(1)
	}
}

func runExport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		kubeconfig = fs.String("kubeconfig", "", "path to the kubeconfig; empty uses the default rules, then in-cluster config")
		kubeCtx    = fs.String("context", "", "kubeconfig context; empty uses the current context")
		group      = fs.String("group", v1alpha1.GroupName, "API group to export")
		output     = fs.String("output", "", "export file to write; - writes to stdout; required")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return fmt.Errorf("--output is required")
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = *kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: *kubeCtx}).ClientConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	cfg.UserAgent = "cask-migrate"
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	resources, err := migrate.Discover(disc, *group)
	if err != nil {
		return err
	}

	report := stdout
	var h migrate.Header
	if *output == "-" {
		report = stderr
		h, err = migrate.Export(ctx, dyn, *group, resources, stdout)
	} else {
		h, err = exportFile(ctx, dyn, *group, resources, *output)
	}
	if err != nil {
		return err
	}
	n := 0
	for _, r := range h.Resources {
		n += r.Count
	}
	fmt.Fprintf(report, "exported %d objects in %d resources of %s at source etcd revision %d\n",
		n, len(h.Resources), h.Group, h.Revision)
	return nil
}

// exportFile writes the export to a temporary file and renames it to
// path. A reader never sees a partial file.
func exportFile(ctx context.Context, dyn dynamic.Interface, group string, resources []migrate.Resource, path string) (migrate.Header, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return migrate.Header{}, err
	}
	defer os.Remove(f.Name())
	h, err := migrate.Export(ctx, dyn, group, resources, f)
	if err != nil {
		f.Close()
		return migrate.Header{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return migrate.Header{}, err
	}
	if err := f.Close(); err != nil {
		return migrate.Header{}, err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return migrate.Header{}, err
	}
	return h, nil
}
