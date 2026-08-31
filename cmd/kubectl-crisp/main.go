// Command kubectl-crisp is a kubectl plugin for working with
// CustomResourceProjections from outside the server.
//
// Invoked as `kubectl crisp ...` when it is on PATH under this name, and as
// `kubectl-crisp ...` directly.
//
// A second binary rather than more subcommands on the API server, because the
// audience differs. Whoever grants a colleague access to a projected kind has
// kubectl and does not have the server binary — which this repository tells
// people to build themselves, since a database/sql driver has to be linked in.
// Nothing here links one: generating RBAC needs the projection and nothing else.
package main

import (
	"os"

	"github.com/spf13/cobra"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/mrueg/kube-crisp/pkg/cmd/plugin"
	"github.com/mrueg/kube-crisp/pkg/version"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()

	root := &cobra.Command{
		Use:   "kubectl-crisp",
		Short: "Work with kube-crisp projections",
		Long: "A kubectl plugin for kube-crisp, the aggregated API server that serves SQL\n" +
			"query results as Kubernetes custom resources.\n\n" +
			"Nothing here talks to a database or needs the server binary. Checking a\n" +
			"projection does need it: `kube-crisp-apiserver validate` consults the driver\n" +
			"registry, whose contents depend on which drivers that build linked in.",
		SilenceUsage: true,
		Version:      version.Version,
	}
	root.AddCommand(plugin.NewCommandRBAC(os.Stdout, os.Stderr))
	root.AddCommand(plugin.NewCommandCanI(os.Stdout, os.Stderr))
	root.AddCommand(plugin.NewCommandPrune(os.Stdout, os.Stderr))
	root.AddCommand(plugin.NewCommandSchema(os.Stdout, os.Stderr))

	root.SetContext(ctx)
	os.Exit(cli.Run(root))
}
