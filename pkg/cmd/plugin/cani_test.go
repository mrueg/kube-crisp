package plugin

import (
	"bytes"
	"context"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// allowVerbs answers every access review, allowing the verbs named and
// recording the attributes each review carried.
func allowVerbs(kube *kubefake.Clientset, allowed ...string) *[]authorizationv1.ResourceAttributes {
	var asked []authorizationv1.ResourceAttributes
	permitted := map[string]bool{}
	for _, verb := range allowed {
		permitted[verb] = true
	}

	record := func(attrs *authorizationv1.ResourceAttributes) bool {
		asked = append(asked, *attrs)
		return permitted[attrs.Verb]
	}

	kube.PrependReactor("create", "selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
			review.Status.Allowed = record(review.Spec.ResourceAttributes)
			return true, review, nil
		})
	kube.PrependReactor("create", "subjectaccessreviews",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
			review.Status.Allowed = record(review.Spec.ResourceAttributes)
			return true, review, nil
		})

	return &asked
}

// readOnlyFilms is a projection with a list query and nothing else, so it
// serves get, list and watch and refuses everything else.
func readOnlyFilms(scope crispv1alpha1.ResourceScope) crispv1alpha1.CustomResourceProjection {
	p := crispv1alpha1.CustomResourceProjection{}
	p.Name = "pagila-films"
	p.Spec.Resource.Group = "pagila.example.com"
	p.Spec.Resource.Plural = "films"
	p.Spec.Resource.Scope = scope
	return p
}

func rowFor(t *testing.T, rows []resourceVerdict, resource string) resourceVerdict {
	t.Helper()
	for _, row := range rows {
		if row.Resource == resource {
			return row
		}
	}
	t.Fatalf("no row for %s in %+v", resource, rows)
	return resourceVerdict{}
}

// TestGrantedButNotServedIsTheFinding. RBAC and the projection are two
// independent gates and nothing else compares them: a caller granted create on
// a projection with no create query is authorized and gets 405.
func TestGrantedButNotServedIsTheFinding(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	allowVerbs(kube, "get", "list", "watch", "create")

	o := &caniOptions{}
	rows, err := o.review(context.Background(), kube,
		[]crispv1alpha1.CustomResourceProjection{readOnlyFilms(crispv1alpha1.ClusterScoped)})
	if err != nil {
		t.Fatal(err)
	}

	films := rowFor(t, rows, "films")
	if got := films.Verbs["create"]; !got.Allowed || got.Served {
		t.Fatalf("create = %+v, want allowed and not served", got)
	}
	if got := films.Verbs["get"]; !got.Allowed || !got.Served {
		t.Fatalf("get = %+v, want allowed and served", got)
	}
	if got := films.Verbs["delete"]; got.Allowed || got.Served {
		t.Fatalf("delete = %+v, want neither", got)
	}
}

// TestEveryVerbIsAsked, including the ones the projection cannot serve —
// without those the granted-but-not-served case cannot be found at all.
func TestEveryVerbIsAsked(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	asked := allowVerbs(kube)

	o := &caniOptions{}
	if _, err := o.review(context.Background(), kube,
		[]crispv1alpha1.CustomResourceProjection{readOnlyFilms(crispv1alpha1.ClusterScoped)}); err != nil {
		t.Fatal(err)
	}

	if len(*asked) != len(allVerbs) {
		t.Fatalf("asked %d question(s), want one per verb (%d)", len(*asked), len(allVerbs))
	}
}

// TestClusterScopedKindIsAskedWithoutANamespace. A review carrying one would be
// answered against Roles in that namespace, which cannot grant a cluster-scoped
// resource — so the answer would be no for a reason unrelated to the caller.
func TestClusterScopedKindIsAskedWithoutANamespace(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	asked := allowVerbs(kube)

	o := &caniOptions{namespace: "store-1"}
	if _, err := o.review(context.Background(), kube,
		[]crispv1alpha1.CustomResourceProjection{readOnlyFilms(crispv1alpha1.ClusterScoped)}); err != nil {
		t.Fatal(err)
	}

	for _, attrs := range *asked {
		if attrs.Namespace != "" {
			t.Fatalf("cluster-scoped kind asked about namespace %q", attrs.Namespace)
		}
	}
}

// TestNamespacedKindCarriesTheNamespace, which is what makes a tenant-column
// projection's per-namespace grants checkable.
func TestNamespacedKindCarriesTheNamespace(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	asked := allowVerbs(kube)

	o := &caniOptions{namespace: "store-1"}
	if _, err := o.review(context.Background(), kube,
		[]crispv1alpha1.CustomResourceProjection{readOnlyFilms(crispv1alpha1.NamespaceScoped)}); err != nil {
		t.Fatal(err)
	}

	for _, attrs := range *asked {
		if attrs.Namespace != "store-1" {
			t.Fatalf("namespaced kind asked about namespace %q, want store-1", attrs.Namespace)
		}
	}
}

// TestSubresourcesAreAskedAboutSeparately: RBAC treats films/status as its own
// resource, so a role granting films says nothing about it.
func TestSubresourcesAreAskedAboutSeparately(t *testing.T) {
	p := readOnlyFilms(crispv1alpha1.ClusterScoped)
	p.Spec.Queries.Update = &crispv1alpha1.Query{}
	p.Spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Status: &crispv1alpha1.ProjectedStatusSubresource{},
	}

	kube := kubefake.NewSimpleClientset()
	asked := allowVerbs(kube, "update")

	o := &caniOptions{}
	rows, err := o.review(context.Background(), kube, []crispv1alpha1.CustomResourceProjection{p})
	if err != nil {
		t.Fatal(err)
	}

	status := rowFor(t, rows, "films/status")
	if got := status.Verbs["update"]; !got.Served || !got.Allowed {
		t.Fatalf("films/status update = %+v, want served and allowed", got)
	}
	if got := status.Verbs["list"]; got.Served {
		t.Fatalf("films/status list = %+v, want not served: a subresource is one object", got)
	}

	var sawSubresource bool
	for _, attrs := range *asked {
		if attrs.Subresource == "status" {
			sawSubresource = true
			if attrs.Resource != "films" {
				t.Fatalf("subresource review named resource %q, want films", attrs.Resource)
			}
		}
	}
	if !sawSubresource {
		t.Fatal("no review carried Subresource=status")
	}
}

// TestSubjectReviewOnlyWhenAskingAboutSomebodyElse. A self review needs no
// permission; checking another user is privileged, so it is only asked when
// asked for.
func TestSubjectReviewOnlyWhenAskingAboutSomebodyElse(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    caniOptions
		verb string
	}{
		{"current user", caniOptions{}, "selfsubjectaccessreviews"},
		{"named user", caniOptions{asUser: "alice"}, "subjectaccessreviews"},
		{"named group", caniOptions{asGroups: []string{"ops"}}, "subjectaccessreviews"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kube := kubefake.NewSimpleClientset()
			allowVerbs(kube)

			o := tc.o
			if _, err := o.review(context.Background(), kube,
				[]crispv1alpha1.CustomResourceProjection{readOnlyFilms(crispv1alpha1.ClusterScoped)}); err != nil {
				t.Fatal(err)
			}

			var used string
			for _, action := range kube.Actions() {
				if action.GetVerb() == "create" {
					used = action.GetResource().Resource
					break
				}
			}
			if used != tc.verb {
				t.Fatalf("used %s, want %s", used, tc.verb)
			}
		})
	}
}

// TestTableExplainsEachSymbol. The table is three-valued, and a matrix nobody
// can read is worse than the eight can-i calls it replaces.
func TestTableExplainsEachSymbol(t *testing.T) {
	rows := []resourceVerdict{{
		Group:    "pagila.example.com",
		Resource: "films",
		Scope:    "Cluster",
		Verbs: map[string]verdict{
			"get":              {Served: true, Allowed: true},
			"list":             {Served: true, Allowed: false},
			"watch":            {Served: true, Allowed: true},
			"create":           {Served: false, Allowed: true},
			"update":           {Served: false, Allowed: false},
			"patch":            {Served: false, Allowed: false},
			"delete":           {Served: false, Allowed: false},
			"deletecollection": {Served: false, Allowed: false},
		},
	}}

	var out, errOut bytes.Buffer
	o := &caniOptions{}
	if err := o.table(&out, &errOut, rows); err != nil {
		t.Fatal(err)
	}

	table, notes := out.String(), errOut.String()
	if !strings.Contains(table, "yes!") {
		t.Errorf("granted-but-not-served is not marked in the table:\n%s", table)
	}
	if !strings.Contains(notes, "405") {
		t.Errorf("notes do not say what yes! costs:\n%s", notes)
	}
	// The denial names the role that would fix it, which is the question
	// somebody has when they see "no".
	if !strings.Contains(notes, "kube-crisp:pagila.example.com:view") {
		t.Errorf("denial does not name the role that grants list:\n%s", notes)
	}
	// The table goes to stdout and the legend to stderr, so piping the table
	// somewhere does not carry the prose with it.
	if strings.Contains(table, "refused by RBAC") {
		t.Errorf("legend leaked into stdout:\n%s", table)
	}
}
