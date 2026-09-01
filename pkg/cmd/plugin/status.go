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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// apiServiceGVR is the aggregation layer's own resource.
//
// Spelled here rather than imported from pkg/controller/projection, which is
// where the objects are written: importing it would pull the compiler, the
// router and pkg/sql into a binary whose whole point is that it links no
// database driver. One GroupVersionResource is a cheaper duplicate than that.
var apiServiceGVR = schema.GroupVersionResource{
	Group:    "apiregistration.k8s.io",
	Version:  "v1",
	Resource: "apiservices",
}

// conditionReport is one condition, flattened for printing.
type conditionReport struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// registrationReport is what the aggregation layer says about one group
// version, which is a different question from what the projection says about
// itself.
type registrationReport struct {
	GroupVersion string `json:"groupVersion"`
	// Available is nil when the APIService could not be read at all, which is
	// not the same as it reporting unavailable.
	Available *bool  `json:"available,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Message   string `json:"message,omitempty"`
	Error     string `json:"error,omitempty"`
}

// projectionReport is everything known about one projection, from the two
// objects that each hold half of it.
type projectionReport struct {
	Name       string   `json:"name"`
	Resources  []string `json:"resources"`
	Generation int64    `json:"generation"`
	// ObservedGeneration is the spec generation the server last reconciled.
	ObservedGeneration int64                `json:"observedGeneration"`
	Conditions         []conditionReport    `json:"conditions,omitempty"`
	ServedPaths        []string             `json:"servedPaths,omitempty"`
	Registrations      []registrationReport `json:"registrations,omitempty"`
	// Findings are the sentences a person would otherwise have to derive by
	// reading the four fields above together.
	Findings []string `json:"findings,omitempty"`
	// Healthy is the summary the exit status and the one-line output use.
	Healthy bool `json:"healthy"`
}

type statusOptions struct {
	output  string
	verbose bool

	client clientFlags
}

// NewCommandStatus builds the `status` subcommand.
func NewCommandStatus(out, errOut io.Writer) *cobra.Command {
	o := &statusOptions{}

	cmd := &cobra.Command{
		Use:   "status [NAME...]",
		Short: "Say why a projection is not answering",
		Long: "Joins what a projection says about itself to what the aggregation layer says about\n" +
			"it, which are two objects that each hold half the answer.\n\n" +
			"`kubectl get customresourceprojections` prints Ready and stops there. The next\n" +
			"question is always somewhere else: the APIService carries the aggregator's own\n" +
			"message about why a group is unreachable, and metadata.generation against\n" +
			"status.observedGeneration is the only thing that distinguishes a projection that\n" +
			"is serving from one that is serving the spec it had before you edited it.\n\n" +
			"Three failures are invisible without the join. A projection with no status at all\n" +
			"has not been seen by any server — the conditions are written by kube-crisp, so an\n" +
			"empty status means the thing that would have told you is the thing that is down.\n" +
			"A projection that is Ready and unregistered compiled here and is reachable from\n" +
			"nowhere. And a projection whose observedGeneration trails its generation answers\n" +
			"requests correctly from the wrong spec, which looks healthy from every angle\n" +
			"except this one.\n\n" +
			"Reads the cluster and nothing else: no database is opened and no Secret is read.\n" +
			"Whether the data source is reachable is a question the server has already\n" +
			"answered in the DataSourceConnected condition.",
		Args:         cobra.ArbitraryArgs,
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			return o.run(c.Context(), args, out, errOut)
		},
	}

	f := cmd.Flags()
	f.StringVarP(&o.output, "output", "o", "text", "Output format: text or json.")
	f.BoolVar(&o.verbose, "verbose", false,
		"Print every condition, including the ones that are fine.")
	o.client.bind(cmd)

	cmd.ValidArgsFunction = completeProjectionNames(o.client.projections, nil)
	_ = cmd.RegisterFlagCompletionFunc("output", fixed("text", "json"))

	return cmd
}

func (o *statusOptions) run(ctx context.Context, names []string, out, errOut io.Writer) error {
	switch o.output {
	case "text", "json":
	default:
		return fmt.Errorf("unsupported output format %q: use text or json", o.output)
	}

	projections, err := projectionsFromCluster(ctx, &o.client, names)
	if err != nil {
		return err
	}
	if len(projections) == 0 {
		// Said on stderr and not a failure, for the reason the other commands
		// give: a cluster with no projections yet is ordinary.
		_, _ = fmt.Fprintln(errOut, "no projections found")
		return nil
	}

	// The APIServices are read once for the whole run rather than per
	// projection, since several projections commonly share a group version and
	// each read is a request.
	registrations := readRegistrations(ctx, &o.client, projections)

	reports := make([]projectionReport, 0, len(projections))
	for i := range projections {
		reports = append(reports, report(&projections[i], registrations))
	}

	if o.output == "json" {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(reports)
	}
	return o.text(out, errOut, reports)
}

// readRegistrations reads the APIService behind every group version the given
// projections declare.
//
// Failures are kept rather than returned. Whoever is diagnosing a projection
// often cannot read cluster-scoped objects, and a status command that refuses
// to say anything because one of its two halves was forbidden is a status
// command nobody can use.
func readRegistrations(
	ctx context.Context,
	flags *clientFlags,
	projections []crispv1alpha1.CustomResourceProjection,
) map[string]registrationReport {
	wanted := map[string]bool{}
	for i := range projections {
		for _, gv := range groupVersions(&projections[i]) {
			wanted[gv] = true
		}
	}

	out := make(map[string]registrationReport, len(wanted))
	client, err := flags.dynamic()
	if err != nil {
		for gv := range wanted {
			out[gv] = registrationReport{GroupVersion: gv, Error: err.Error()}
		}
		return out
	}

	for gv := range wanted {
		out[gv] = readRegistration(ctx, client, gv)
	}
	return out
}

func readRegistration(ctx context.Context, client dynamic.Interface, gv string) registrationReport {
	report := registrationReport{GroupVersion: gv}

	parts := strings.SplitN(gv, "/", 2)
	if len(parts) != 2 {
		report.Error = fmt.Sprintf("cannot name an APIService for %q", gv)
		return report
	}
	// An APIService is named version.group, the other way round from the path.
	name := parts[1] + "." + parts[0]

	object, err := client.Resource(apiServiceGVR).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		available := false
		report.Available = &available
		report.Reason = "NotRegistered"
		report.Message = "no APIService " + name + " exists, so nothing routes this group here"
		return report
	case err != nil:
		report.Error = err.Error()
		return report
	}

	available, reason, message := apiServiceAvailability(object)
	report.Available = available
	report.Reason = reason
	report.Message = message
	return report
}

// apiServiceAvailability digs the Available condition out of an unstructured
// APIService.
func apiServiceAvailability(object *unstructured.Unstructured) (*bool, string, string) {
	conditions, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil || !found {
		// Written by the aggregator shortly after the object appears, so an
		// APIService without it is one nothing has judged yet.
		return nil, "NotYetJudged", "the aggregation layer has not reported on this APIService yet"
	}

	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if condition["type"] != "Available" {
			continue
		}
		status, _ := condition["status"].(string)
		reason, _ := condition["reason"].(string)
		message, _ := condition["message"].(string)
		value := status == string(metav1.ConditionTrue)
		return &value, reason, message
	}
	return nil, "NotYetJudged", "the APIService carries no Available condition"
}

// groupVersions lists the group versions a projection declares, which is what
// its APIServices are named after.
func groupVersions(p *crispv1alpha1.CustomResourceProjection) []string {
	group := p.Spec.Resource.Group
	seen := map[string]bool{}
	var out []string

	add := func(version string) {
		if version == "" {
			return
		}
		gv := group + "/" + version
		if !seen[gv] {
			seen[gv] = true
			out = append(out, gv)
		}
	}

	add(p.Spec.Resource.Version)
	for _, version := range p.Spec.Resource.Versions {
		// A version that is not served has no APIService and is not missing
		// one.
		if version.Served != nil && !*version.Served {
			continue
		}
		add(version.Name)
	}
	sort.Strings(out)
	return out
}

// report joins one projection's own account of itself to the aggregator's.
func report(
	p *crispv1alpha1.CustomResourceProjection,
	registrations map[string]registrationReport,
) projectionReport {
	out := projectionReport{
		Name:               p.Name,
		Generation:         p.Generation,
		ObservedGeneration: p.Status.ObservedGeneration,
		ServedPaths:        p.Status.ServedPaths,
		Healthy:            true,
	}

	for _, gv := range groupVersions(p) {
		out.Resources = append(out.Resources, p.Spec.Resource.Plural+"."+gv)
		if registration, ok := registrations[gv]; ok {
			out.Registrations = append(out.Registrations, registration)
		}
	}

	for _, condition := range p.Status.Conditions {
		out.Conditions = append(out.Conditions, conditionReport{
			Type:    condition.Type,
			Status:  string(condition.Status),
			Reason:  condition.Reason,
			Message: condition.Message,
		})
	}

	out.Findings = findings(p, out)
	out.Healthy = len(out.Findings) == 0
	return out
}

// findings says, in sentences, what the fields mean together.
//
// This is the whole command: every one of these is derivable from what
// `kubectl get -o yaml` already prints, and every one of them is derivable only
// by knowing which two fields to hold next to each other.
func findings(p *crispv1alpha1.CustomResourceProjection, r projectionReport) []string {
	var out []string

	// The conditions are written by kube-crisp and by nothing else, so their
	// absence is a statement about the server rather than about the projection.
	if len(p.Status.Conditions) == 0 {
		return []string{
			"no status at all: no kube-crisp server has reconciled this projection. " +
				"Either none is running, or the one running cannot read " +
				"CustomResourceProjections — check the deployment and its RBAC.",
		}
	}

	for _, condition := range p.Status.Conditions {
		if condition.Status == metav1.ConditionTrue {
			continue
		}
		out = append(out, explain(condition))
	}

	// Requests succeed and the spec answering them is not the spec that was
	// applied. Nothing else reports this: Ready is true, the resource resolves,
	// and the rows come back.
	if p.Status.ObservedGeneration != 0 && p.Status.ObservedGeneration < p.Generation {
		out = append(out, fmt.Sprintf(
			"serving generation %d while %d is applied: the spec answering requests is not "+
				"the spec in the cluster. Usually the newer one failed to compile — see Ready.",
			p.Status.ObservedGeneration, p.Generation))
	}

	for _, registration := range r.Registrations {
		switch {
		case registration.Error != "":
			out = append(out, fmt.Sprintf(
				"could not read the APIService for %s (%s): whether this group is reachable "+
					"from outside is unchecked.", registration.GroupVersion, registration.Error))
		case registration.Available != nil && !*registration.Available:
			out = append(out, fmt.Sprintf(
				"%s is not reachable through the aggregation layer: %s. The message is the "+
					"aggregator's own and is about the Service, its endpoints, or the CA bundle "+
					"— not about the projection.", registration.GroupVersion, registration.Message))
		}
	}

	// A version the spec declares as served that is not in servedPaths did not
	// compile, and the projection can be Ready on the strength of the others.
	if len(p.Status.ServedPaths) > 0 {
		for _, gv := range groupVersions(p) {
			if !servedPathCovers(p.Status.ServedPaths, gv) {
				out = append(out, fmt.Sprintf(
					"%s is declared and not served: the other versions compiled and this one "+
						"did not, so a client asking for it is told the resource does not exist.",
					gv))
			}
		}
	}

	return out
}

// servedPathCovers reports whether any served path belongs to a group version.
func servedPathCovers(paths []string, groupVersion string) bool {
	for _, path := range paths {
		if strings.Contains(path, groupVersion) {
			return true
		}
	}
	return false
}

// explain turns one false condition into the sentence a person would write
// after reading it.
func explain(condition metav1.Condition) string {
	base := fmt.Sprintf("%s is %s (%s): %s",
		condition.Type, condition.Status, condition.Reason, condition.Message)

	switch condition.Type {
	case crispv1alpha1.ConditionDataSourceConnected:
		return base + " — the projection is installed and answering with 503 until the " +
			"database is reachable; the Secret and the network are where to look."
	case crispv1alpha1.ConditionSchemaResolved:
		return base + " — the borrowed CRD is missing or has no such version, so there is " +
			"no schema to serve this kind with."
	case crispv1alpha1.ConditionRegistered:
		return base + " — compiled here and not routed to; the message comes from the " +
			"aggregation layer."
	}
	return base
}

// text prints one block per projection, and by default only the parts that are
// not fine.
//
// A status command that prints everything every time is one whose output has to
// be read in full before it can be dismissed, which is the opposite of what it
// is for.
func (o *statusOptions) text(out, errOut io.Writer, reports []projectionReport) error {
	healthy := 0
	for _, r := range reports {
		if r.Healthy {
			healthy++
		}
		if r.Healthy && !o.verbose {
			continue
		}

		if _, err := fmt.Fprintf(out, "%s\t%s\n", r.Name, strings.Join(r.Resources, ", ")); err != nil {
			return err
		}
		if o.verbose {
			w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			for _, condition := range r.Conditions {
				line := "  " + condition.Type + "\t" + condition.Status
				if condition.Reason != "" {
					line += "\t" + condition.Reason
				}
				if _, err := fmt.Fprintln(w, line); err != nil {
					return err
				}
			}
			for _, registration := range r.Registrations {
				available := "unknown"
				if registration.Available != nil {
					available = fmt.Sprintf("%t", *registration.Available)
				}
				if _, err := fmt.Fprintf(w, "  APIService %s\tavailable=%s\n",
					registration.GroupVersion, available); err != nil {
					return err
				}
			}
			if err := w.Flush(); err != nil {
				return err
			}
		}
		for _, finding := range r.Findings {
			if _, err := fmt.Fprintf(out, "  %s\n", finding); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}

	// On stderr, so that piping the report somewhere does not carry the count
	// into it.
	_, _ = fmt.Fprintf(errOut, "%d projection(s), %d serving, %d with something to report\n",
		len(reports), healthy, len(reports)-healthy)
	return nil
}
