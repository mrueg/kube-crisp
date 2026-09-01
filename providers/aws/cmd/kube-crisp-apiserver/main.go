// Command kube-crisp-apiserver is kube-crisp with the AWS RDS IAM credential
// provider linked in.
//
// It is the stock server plus one registration, and that is the whole point:
// the set of credential providers is open, every provider is a cloud SDK, and
// kube-crisp links no dependency a given build does not need — so the binary
// published from the kube-crisp repository has none of them and this one has
// exactly the one it is named for.
//
// Copy this file to add a second provider, a database/sql driver, or both. It is
// short on purpose: everything the server does lives in the library, so a custom
// build is a main function and a go.mod, not a fork.
package main

import (
	"fmt"
	"os"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/mrueg/kube-crisp/pkg/cmd/server"
	"github.com/mrueg/kube-crisp/pkg/version"
	rdsiam "github.com/mrueg/kube-crisp/providers/aws"
)

func main() {
	// Before the server starts, so that a projection naming the provider is
	// compiled against a registry that already has it. Registration cannot fail
	// for anything but a name collision with another provider this build
	// registered, which is a mistake in this file rather than in a cluster —
	// hence a refusal to start rather than a log line.
	if err := rdsiam.Register(); err != nil {
		fmt.Fprintf(os.Stderr, "registering the AWS RDS IAM credential provider: %v\n", err)
		os.Exit(1)
	}

	ctx := genericapiserver.SetupSignalContext()

	options := server.NewCrispServerOptions(os.Stdout, os.Stderr)
	cmd := server.NewCommandStartCrispServer(ctx, options)
	cmd.Use = "kube-crisp-apiserver"
	cmd.Version = version.Version

	// Checking a projection needs neither a cluster nor a database, so it does
	// not need the server either. It does need the provider registered, which
	// is why the registration above is not inside the serve path: a projection
	// using dataSource.auth has to validate against the build that will serve
	// it.
	cmd.AddCommand(server.NewCommandValidate(os.Stdout, os.Stderr))

	os.Exit(cli.Run(cmd))
}
