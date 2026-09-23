package plugin

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
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
	// served are the API groups something answers for: every group a
	// projection in the cluster declares, and the group of every registration
	// this server owns, whatever the aggregation layer currently says about
	// it. The second is how a group served from --projection-dir shows up at
	// all, and it is what the RBAC half reads before it calls a role
	// orphaned.
	//
	// The registration's availability is deliberately not consulted. While
	// the server restarts or is upgraded the aggregation layer reports every
	// registration it owns unavailable for a while, and a survey that read
	// the verdict would, in that window, find every file-backed group
	// unserved and hand the RBAC half every one of their roles to delete.
	// A registration that is stranded rather than restarting is the
	// --apiservices half's to remove, and once it is gone the group stops
	// counting.
	served sets.Set[string]
}

// surveyAPIServices sorts the registrations this server owns into the three
// cases, and records which groups are served along the way.
//
// Both halves of prune start from the same question, which groups something
// still answers for, and this is the one place that answers it. Takes its
// clients rather than building them, the reason prune does: what a --delete
// does with them is the half worth testing.
func surveyAPIServices(
	ctx context.Context,
	crisp crispclient.Interface,
	dyn dynamic.Interface,
) (apiServiceSurvey, error) {
	survey := apiServiceSurvey{served: sets.New[string]()}

	projections, err := crisp.CrispV1alpha1().CustomResourceProjections().
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return survey, fmt.Errorf("listing projections: %w", err)
	}

	claimed := map[string]string{}
	for i := range projections.Items {
		survey.served.Insert(projections.Items[i].Spec.Resource.Group)
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

		// A registration counts its group as served by existing, not by
		// being available. An unavailable one is either stranded, which
		// removing it settles, or answered by a server that is restarting,
		// and the roles for a restarting server's groups are the ones a
		// prune must not touch.
		group, _, _ := strings.Cut(groupVersion, "/")
		survey.served.Insert(group)

		if _, stillClaimed := claimed[groupVersion]; stillClaimed {
			continue
		}

		// Here the verdict does decide, since what is being sorted is the
		// registration itself. One the aggregation layer has not judged yet
		// has not failed, and a group about to come up is not one to
		// unregister.
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
