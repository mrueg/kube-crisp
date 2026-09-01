package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
	"github.com/mrueg/kube-crisp/pkg/rbac"
)

// allVerbs is every verb a projected resource could be granted, in the order
// the API server lists them.
//
// All of them, not only the ones a projection serves, because the answer worth
// having is in both directions: a verb granted on a projection that cannot
// serve it is authorized and still fails, and nothing except asking both
// questions finds it.
var allVerbs = []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}

type caniOptions struct {
	client    clientFlags
	filenames []string
	namespace string
	asUser    string
	asGroups  []string
	output    string
}

// verdict is one cell: whether the projection serves the verb, and whether the
// subject is allowed it.
type verdict struct {
	Served  bool `json:"served"`
	Allowed bool `json:"allowed"`
}

// resourceVerdict is one row.
type resourceVerdict struct {
	Group    string             `json:"group"`
	Resource string             `json:"resource"`
	Scope    string             `json:"scope"`
	Verbs    map[string]verdict `json:"verbs"`
}

// NewCommandCanI builds the `can-i` subcommand.
func NewCommandCanI(out, errOut io.Writer) *cobra.Command {
	o := &caniOptions{}

	cmd := &cobra.Command{
		Use:   "can-i [NAME...]",
		Short: "Show who may do what to projected kinds",
		Long: "Asks the cluster, for every kind a projection serves and every verb, whether the\n" +
			"subject is allowed it — and crosses that with what the projection can actually\n" +
			"serve.\n\n" +
			"`kubectl auth can-i get films` already answers one cell of this. What it cannot\n" +
			"answer is the disagreement: RBAC and the projection are two independent gates,\n" +
			"and a caller granted a verb the projection has no query for is authorized and\n" +
			"still gets 405 Method Not Allowed. The grant says yes, the server says no, and\n" +
			"neither side shows it.\n\n" +
			"With no arguments it reads the projections in the cluster. Named arguments select\n" +
			"projections by name; -f reads manifests instead — though the cluster is still\n" +
			"asked, since it is the one that decides.\n\n" +
			"Checks the current user unless --as is given.",
		Args:         cobra.ArbitraryArgs,
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			return o.run(c.Context(), args, out, errOut)
		},
	}

	f := cmd.Flags()
	f.StringSliceVarP(&o.filenames, "filename", "f", nil,
		"Manifest file or directory to read projections from, instead of the cluster. Repeatable.")
	f.StringVarP(&o.namespace, "namespace", "n", "",
		"Namespace to ask about, for namespaced projected kinds. Empty asks whether the subject "+
			"may act in every namespace. Ignored for cluster-scoped kinds, which have no namespace to ask about.")
	f.StringVar(&o.asUser, "as", "", "Username to check instead of the current user.")
	f.StringSliceVar(&o.asGroups, "as-group", nil, "Group to check. Repeatable.")
	f.StringVarP(&o.output, "output", "o", "table", "Output format: table or json.")
	cmd.ValidArgsFunction = completeProjectionNames(o.client.projections, &o.filenames)
	_ = cmd.MarkFlagFilename("filename", "yaml", "yml")
	_ = cmd.RegisterFlagCompletionFunc("output", fixed("table", "json"))
	_ = cmd.RegisterFlagCompletionFunc("namespace",
		func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return completeFrom(cmd, o.client.namespaces, nil, toComplete)
		})
	// A username and a group name are not enumerable -- the cluster holds no
	// list of them -- and offering files instead would be a worse guess than
	// offering nothing.
	_ = cmd.RegisterFlagCompletionFunc("as", cobra.NoFileCompletions)
	_ = cmd.RegisterFlagCompletionFunc("as-group", cobra.NoFileCompletions)
	o.client.bind(cmd)

	return cmd
}

func (o *caniOptions) run(ctx context.Context, names []string, out, errOut io.Writer) error {
	if len(o.filenames) > 0 && len(names) > 0 {
		return fmt.Errorf("cannot combine -f with projection names: -f reads manifests, names read the cluster")
	}
	switch o.output {
	case "table", "json":
	default:
		return fmt.Errorf("unsupported output format %q: use table or json", o.output)
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
		_, _ = fmt.Fprintln(errOut, "no projections found")
		return nil
	}

	kube, err := o.client.kube()
	if err != nil {
		return err
	}

	rows, err := o.review(ctx, kube, projections)
	if err != nil {
		return err
	}

	if o.output == "json" {
		return json.NewEncoder(out).Encode(rows)
	}
	return o.table(out, errOut, rows)
}

// review asks the cluster about every verb of every projected resource.
func (o *caniOptions) review(
	ctx context.Context,
	kube kubernetes.Interface,
	projections []crispv1alpha1.CustomResourceProjection,
) ([]resourceVerdict, error) {
	var rows []resourceVerdict

	for i := range projections {
		spec := projections[i].Spec
		namespaced := spec.Resource.Scope == crispv1alpha1.NamespaceScoped

		// A cluster-scoped kind has no namespace to be asked about, and a
		// review carrying one would be answered against Roles in it — which
		// cannot grant a cluster-scoped resource, so the answer would be no for
		// a reason that has nothing to do with the caller.
		namespace := ""
		if namespaced {
			namespace = o.namespace
		}

		served := map[string]sets.Set[string]{
			spec.Resource.Plural: sets.New(projection.ServedVerbs(spec)...),
		}
		for _, sub := range []string{"status", "scale"} {
			if verbs := projection.SubresourceVerbs(spec, sub); verbs != nil {
				served[spec.Resource.Plural+"/"+sub] = sets.New(verbs...)
			}
		}

		resources := make([]string, 0, len(served))
		for resource := range served {
			resources = append(resources, resource)
		}
		sort.Strings(resources)

		for _, resource := range resources {
			row := resourceVerdict{
				Group:    spec.Resource.Group,
				Resource: resource,
				Scope:    string(spec.Resource.Scope),
				Verbs:    map[string]verdict{},
			}

			name, subresource, _ := strings.Cut(resource, "/")
			for _, verb := range allVerbs {
				allowed, err := o.allowed(ctx, kube, namespace, spec.Resource.Group, name, subresource, verb)
				if err != nil {
					return nil, err
				}
				row.Verbs[verb] = verdict{Served: served[resource].Has(verb), Allowed: allowed}
			}
			rows = append(rows, row)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Group != rows[j].Group {
			return rows[i].Group < rows[j].Group
		}
		return rows[i].Resource < rows[j].Resource
	})
	return rows, nil
}

// allowed runs one access review.
//
// A self review when the subject is the caller, because that needs no
// permission beyond being able to ask about oneself. Checking somebody else is
// a SubjectAccessReview, which is a privileged question and answered as one.
func (o *caniOptions) allowed(
	ctx context.Context,
	kube kubernetes.Interface,
	namespace, group, resource, subresource, verb string,
) (bool, error) {
	attributes := &authorizationv1.ResourceAttributes{
		Namespace:   namespace,
		Group:       group,
		Resource:    resource,
		Subresource: subresource,
		Verb:        verb,
	}

	if o.asUser == "" && len(o.asGroups) == 0 {
		review := &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: attributes},
		}
		result, err := kube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			return false, fmt.Errorf("checking %s on %s: %w", verb, resource, err)
		}
		return result.Status.Allowed, nil
	}

	review := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			ResourceAttributes: attributes,
			User:               o.asUser,
			Groups:             o.asGroups,
		},
	}
	result, err := kube.AuthorizationV1().SubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		return false, fmt.Errorf("checking %s on %s for %s: %w", verb, resource, o.subject(), err)
	}
	return result.Status.Allowed, nil
}

// subject names who was asked about, for the header and for errors.
func (o *caniOptions) subject() string {
	switch {
	case o.asUser != "" && len(o.asGroups) > 0:
		return fmt.Sprintf("%s (groups %s)", o.asUser, strings.Join(o.asGroups, ", "))
	case o.asUser != "":
		return o.asUser
	case len(o.asGroups) > 0:
		return "groups " + strings.Join(o.asGroups, ", ")
	default:
		return "the current user"
	}
}

// table prints the matrix, and then the two things it found that a single
// can-i could not.
func (o *caniOptions) table(out, errOut io.Writer, rows []resourceVerdict) error {
	_, _ = fmt.Fprintf(errOut, "Checking %s.\n\n", o.subject())

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	header := "RESOURCE"
	for _, verb := range allVerbs {
		header += "\t" + strings.ToUpper(verb)
	}
	if _, err := fmt.Fprintln(w, header); err != nil {
		return err
	}

	var (
		unservable []string
		// Denials grouped by the role that would grant them, so the answer to
		// "why is this forbidden" is a role name rather than a list of verbs.
		deniedByRole = map[string][]string{}
		roles        []string
	)
	for _, row := range rows {
		line := row.Resource
		for _, verb := range allVerbs {
			v := row.Verbs[verb]
			switch {
			case v.Served && v.Allowed:
				line += "\tyes"
			case v.Served && !v.Allowed:
				line += "\tno"
				role := roleFor(row.Group, verb)
				if _, seen := deniedByRole[role]; !seen {
					roles = append(roles, role)
				}
				deniedByRole[role] = append(deniedByRole[role], fmt.Sprintf("%s %s", verb, row.Resource))
			case !v.Served && v.Allowed:
				// The finding. Authorized, and the server has no query for it.
				line += "\tyes!"
				unservable = append(unservable, fmt.Sprintf("%s %s", verb, row.Resource))
			default:
				line += "\t-"
			}
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(errOut, "\n-    the projection has no query for this verb, so no grant can make it work")
	if len(roles) > 0 {
		sort.Strings(roles)
		_, _ = fmt.Fprintln(errOut, "no   refused by RBAC. `kubectl crisp rbac` writes the role that grants each:")
		for _, role := range roles {
			_, _ = fmt.Fprintf(errOut, "       %s: %s\n", role, strings.Join(deniedByRole[role], ", "))
		}
	}
	if len(unservable) > 0 {
		_, _ = fmt.Fprintf(errOut, "yes! granted but not served: %s\n", strings.Join(unservable, ", "))
		_, _ = fmt.Fprintln(errOut,
			"     the request is authorized and returns 405 Method Not Allowed. Something granted a\n"+
				"     verb this projection cannot serve — a hand-written role, or one left from a\n"+
				"     projection that used to have the query")
	}
	return nil
}

// roleFor names the role kubectl crisp rbac would generate for a verb, so a
// denial can say what would fix it.
func roleFor(group, verb string) string {
	tier := "edit"
	if sets.New("get", "list", "watch").Has(verb) {
		tier = "view"
	}
	return fmt.Sprintf("%s:%s:%s", rbac.DefaultNamePrefix, group, tier)
}
