// Package plugin holds the commands of kubectl-crisp, the kubectl plugin.
//
// Separate from the server binary on purpose. `kube-crisp-apiserver validate`
// has to ship with the server because its answer is build-specific — it
// consults the driver registry, so a projection naming a driver this build did
// not link in is rejected here and accepted by a build that did. Nothing in
// this package depends on the registry, on a database, or on cgo, and the
// person who needs it is granting a colleague access to a projected kind. They
// have kubectl. They do not have the API server binary.
package plugin

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/rbac"
)

type rbacOptions struct {
	filenames  []string
	aggregate  bool
	output     string
	namePrefix string

	client clientFlags
}

// NewCommandRBAC builds the `rbac` subcommand.
func NewCommandRBAC(out, errOut io.Writer) *cobra.Command {
	o := &rbacOptions{}

	cmd := &cobra.Command{
		Use:   "rbac [NAME...]",
		Short: "Print the ClusterRoles that grant access to projected kinds",
		Long: "Generates the RBAC a projected API group needs to be reachable by anyone other\n" +
			"than cluster-admin.\n\n" +
			"kube-crisp serves projected kinds through the aggregation layer, which delegates\n" +
			"authorization to the kube-apiserver: a projection that is compiled, registered\n" +
			"and serving is still Forbidden until a ClusterRole grants its group. This writes\n" +
			"that role, granting exactly the verbs the projection can serve — a projection\n" +
			"with no create query refuses create whatever a role says, so the role does not\n" +
			"claim otherwise.\n\n" +
			"With no arguments it reads the projections in the cluster. Named arguments select\n" +
			"projections by name; -f reads manifests instead and needs no cluster at all.\n\n" +
			"Cluster-scoped projected kinds are granted with a ClusterRoleBinding. Namespaced\n" +
			"ones — a projection mapping a tenant column onto metadata.namespace — are granted\n" +
			"per tenant with a RoleBinding in that namespace, which is what makes ordinary\n" +
			"namespace RBAC scope rows.",
		Args:         cobra.ArbitraryArgs,
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			return o.run(c.Context(), args, out, errOut)
		},
	}

	f := cmd.Flags()
	f.StringSliceVarP(&o.filenames, "filename", "f", nil,
		"Manifest file or directory to read projections from, instead of the cluster. Repeatable.")
	f.BoolVar(&o.aggregate, "aggregate", false,
		"Label the roles so the built-in view, edit and admin roles absorb them. Off by default: "+
			"aggregating grants every existing holder of those roles access to the rows behind the "+
			"projection the moment it is applied.")
	f.StringVarP(&o.output, "output", "o", "yaml", "Output format: yaml or json.")
	f.StringVar(&o.namePrefix, "name-prefix", rbac.DefaultNamePrefix,
		"Prefix for the generated role names.")
	cmd.ValidArgsFunction = completeProjectionNames(o.client.projections, &o.filenames)
	_ = cmd.MarkFlagFilename("filename", "yaml", "yml")
	_ = cmd.RegisterFlagCompletionFunc("output", fixed("yaml", "json"))
	// A prefix is whatever the operator chose; filenames are the wrong guess.
	_ = cmd.RegisterFlagCompletionFunc("name-prefix", cobra.NoFileCompletions)
	o.client.bind(cmd)

	return cmd
}

func (o *rbacOptions) run(ctx context.Context, names []string, out, errOut io.Writer) error {
	if len(o.filenames) > 0 && len(names) > 0 {
		return fmt.Errorf("cannot combine -f with projection names: -f reads manifests, names read the cluster")
	}
	switch o.output {
	case "yaml", "json":
	default:
		return fmt.Errorf("unsupported output format %q: use yaml or json", o.output)
	}

	var (
		projections []crispv1alpha1.CustomResourceProjection
		err         error
	)
	if len(o.filenames) > 0 {
		projections, err = loadFiles(o.filenames)
	} else {
		projections, err = projectionsFromCluster(ctx, &o.client, names)
	}
	if err != nil {
		return err
	}
	if len(projections) == 0 {
		// Said on stderr and not treated as a failure. A cluster with no
		// projections yet is ordinary, and a command that exits non-zero on it
		// is one nobody can put in a pipeline. A path holding none is caught
		// earlier, where it means a mistyped argument rather than an empty
		// cluster.
		_, _ = fmt.Fprintln(errOut, "no projections found")
		return nil
	}

	roles, err := rbac.ClusterRoles(projections, rbac.Options{
		NamePrefix: o.namePrefix,
		Aggregate:  o.aggregate,
	})
	if err != nil {
		return err
	}

	return write(out, roles, o.output)
}

// write emits the roles as one YAML stream or one JSON list.
//
// A stream rather than a List for YAML, because that is what kubectl apply -f -
// reads and what a file in a kustomize base looks like. JSON has no document
// separator, so there it is a List — which is also what -o json means
// everywhere else in kubectl.
func write(out io.Writer, roles []rbacv1.ClusterRole, format string) error {
	if format == "json" {
		list := rbacv1.ClusterRoleList{
			TypeMeta: metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "ClusterRoleList"},
			Items:    roles,
		}
		data, err := yaml.Marshal(list)
		if err != nil {
			return err
		}
		converted, err := yaml.YAMLToJSON(data)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "%s\n", strings.TrimSpace(string(converted)))
		return err
	}

	for i := range roles {
		data, err := yaml.Marshal(roles[i])
		if err != nil {
			return err
		}
		if i > 0 {
			if _, err := fmt.Fprintln(out, "---"); err != nil {
				return err
			}
		}
		if _, err := out.Write(data); err != nil {
			return err
		}
	}
	return nil
}
