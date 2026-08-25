// Command kube-crisp-apiserver serves SQL query results as Kubernetes custom
// resources through the aggregation layer.
package main

import (
	"os"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/mrueg/kube-crisp/pkg/cmd/server"
	"github.com/mrueg/kube-crisp/pkg/version"
)

func main() {
	ctx := genericapiserver.SetupSignalContext()

	options := server.NewCrispServerOptions(os.Stdout, os.Stderr)
	cmd := server.NewCommandStartCrispServer(ctx, options)
	cmd.Use = "kube-crisp-apiserver"
	cmd.Version = version.Version

	// Checking a projection needs neither a cluster nor a database, so it does
	// not need the server either.
	cmd.AddCommand(server.NewCommandValidate(os.Stdout, os.Stderr))

	os.Exit(cli.Run(cmd))
}
