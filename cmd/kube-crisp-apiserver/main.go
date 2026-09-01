// Command kube-crisp-apiserver serves SQL query results as Kubernetes custom
// resources through the aggregation layer.
package main

import (
	"fmt"
	"os"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/mrueg/kube-crisp/pkg/cmd/server"
	"github.com/mrueg/kube-crisp/pkg/credentials/rdsiam"
	"github.com/mrueg/kube-crisp/pkg/credentials/tokencommand"
	"github.com/mrueg/kube-crisp/pkg/credentials/tokenfile"
	"github.com/mrueg/kube-crisp/pkg/version"
)

func main() {
	// Before the server starts, so that a projection naming a provider is
	// compiled against a registry that already has it — and so that `validate`,
	// which needs no cluster and no database, still checks a projection against
	// the providers the build serving it will have. Registration fails only on
	// a name collision with another provider this binary registered, which is a
	// mistake here rather than in a cluster, hence a refusal to start.
	//
	// Adding a provider is one more line here. What a provider is allowed to
	// reach is not settled here either way: tokenfile reads only where an
	// operator's flag permits, parsed after this returns.
	for _, register := range []struct {
		name string
		fn   func() error
	}{
		{rdsiam.ProviderName, rdsiam.Register},
		{tokenfile.ProviderName, tokenfile.Register},
		// Registered, and refusing everything until --credential-command-dir
		// names a directory of commands. Registered rather than left out so
		// that a projection naming it is told the operator has not enabled it,
		// which is something to act on, instead of being told this build has
		// never heard of it, which would send somebody to rebuild the binary
		// they already have.
		{tokencommand.ProviderName, tokencommand.Register},
	} {
		if err := register.fn(); err != nil {
			fmt.Fprintf(os.Stderr, "registering the %s credential provider: %v\n", register.name, err)
			os.Exit(1)
		}
	}

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
