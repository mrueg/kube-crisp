package plugin

import (
	"context"
	"fmt"
	"io"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
)

// The label the server puts on the registrations it owns. Anything without it
// was written by somebody else, and this command never considers it — the same
// rule the server applies before touching one.
const (
	managedByLabel    = "app.kubernetes.io/managed-by"
	managedByKubeCris = "kube-crisp"
)

// strandedAPIService is a registration whose projection is gone and whose
// server is not answering for it.
type strandedAPIService struct {
	name         string
	groupVersion string
	message      string
}

// apiServiceSurvey is what one pass over the registrations found: the ones to
// remove, and the ones deliberately left, which have to be reported or their
// absence from the list looks like the command missing them.
type apiServiceSurvey struct {
	stranded []strandedAPIService
	// serving are labelled, unclaimed, and answered by a running server. That
	// is what a projection loaded from --projection-dir looks like from the
	// cluster: there is no object to find, and the registration is in use.
	serving []string
	// unjudged are labelled, unclaimed, and carry no verdict from the
	// aggregation layer yet.
	unjudged []string
}

// surveyAPIServices sorts the registrations this server owns into the three
// cases.
//
// Takes its clients rather than building them, the reason prune does: what a
// --delete does with them is the half worth testing.
func surveyAPIServices(
	ctx context.Context,
	crisp crispclient.Interface,
	dyn dynamic.Interface,
) (apiServiceSurvey, error) {
	var survey apiServiceSurvey

	projections, err := crisp.CrispV1alpha1().CustomResourceProjections().
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return survey, fmt.Errorf("listing projections: %w", err)
	}

	claimed := map[string]string{}
	for i := range projections.Items {
		for _, gv := range groupVersions(&projections.Items[i]) {
			claimed[gv] = projections.Items[i].Name
		}
	}

	list, err := dyn.Resource(apiServiceGVR).List(ctx, metav1.ListOptions{
		LabelSelector: managedByLabel + "=" + managedByKubeCris,
	})
	if err != nil {
		return survey, fmt.Errorf("listing APIServices: %w", err)
	}

	for i := range list.Items {
		object := &list.Items[i]
		groupVersion, ok := apiServiceGroupVersion(object)
		if !ok {
			// An APIService with no group or version in its spec is not one
			// this server wrote, whatever its label says.
			continue
		}
		if _, stillClaimed := claimed[groupVersion]; stillClaimed {
			continue
		}

		available, _, message := apiServiceAvailability(object)
		switch {
		case available == nil:
			survey.unjudged = append(survey.unjudged, object.GetName())
		case *available:
			survey.serving = append(survey.serving, object.GetName())
		default:
			survey.stranded = append(survey.stranded, strandedAPIService{
				name:         object.GetName(),
				groupVersion: groupVersion,
				message:      message,
			})
		}
	}

	sort.Slice(survey.stranded, func(i, j int) bool {
		return survey.stranded[i].name < survey.stranded[j].name
	})
	sort.Strings(survey.serving)
	sort.Strings(survey.unjudged)
	return survey, nil
}

// apiServiceGroupVersion reads the group version an APIService routes.
//
// From the spec rather than by splitting the name: the name is version.group by
// convention and the spec is what the aggregation layer actually reads.
func apiServiceGroupVersion(object *unstructured.Unstructured) (string, bool) {
	group, foundGroup, err := unstructured.NestedString(object.Object, "spec", "group")
	if err != nil || !foundGroup || group == "" {
		return "", false
	}
	version, foundVersion, err := unstructured.NestedString(object.Object, "spec", "version")
	if err != nil || !foundVersion || version == "" {
		return "", false
	}
	return group + "/" + version, true
}

// pruneAPIServices reports, and with --delete removes, the registrations that
// outlived what they registered.
func (o *pruneOptions) pruneAPIServices(
	ctx context.Context,
	crisp crispclient.Interface,
	dyn dynamic.Interface,
	out, errOut io.Writer,
) error {
	survey, err := surveyAPIServices(ctx, crisp, dyn)
	if err != nil {
		return err
	}

	// Said whether or not anything was found. A registration left alone
	// because a server is answering it is indistinguishable, from the outside,
	// from one this command failed to notice.
	for _, name := range survey.serving {
		_, _ = fmt.Fprintf(errOut,
			"%s is available and left alone: something is serving that group, which is what a "+
				"projection loaded from --projection-dir looks like from the cluster.\n", name)
	}
	for _, name := range survey.unjudged {
		_, _ = fmt.Fprintf(errOut,
			"%s carries no verdict from the aggregation layer yet and is left alone.\n", name)
	}

	if len(survey.stranded) == 0 {
		_, _ = fmt.Fprintln(errOut, "no stranded APIServices")
		return nil
	}

	if !o.delete {
		for _, stranded := range survey.stranded {
			_, _ = fmt.Fprintf(out, "%s\t(%s: no projection serves this group, and it is unavailable)\n",
				stranded.name, stranded.groupVersion)
			if stranded.message != "" {
				_, _ = fmt.Fprintf(out, "  %s\n", stranded.message)
			}
		}
		_, _ = fmt.Fprintf(errOut,
			"\n%d stranded APIService(s). Pass --delete to remove them.\n", len(survey.stranded))
		return nil
	}

	client := dyn.Resource(apiServiceGVR)
	for _, stranded := range survey.stranded {
		err := client.Delete(ctx, stranded.name, metav1.DeleteOptions{})
		switch {
		case apierrors.IsNotFound(err):
			// Removed between the survey and now, which is the outcome asked
			// for.
		case err != nil:
			return fmt.Errorf("deleting %s: %w", stranded.name, err)
		default:
			_, _ = fmt.Fprintf(out, "apiservice.apiregistration.k8s.io/%s deleted\n", stranded.name)
		}
	}
	return nil
}
