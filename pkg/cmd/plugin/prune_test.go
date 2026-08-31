package plugin

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"
	"github.com/mrueg/kube-crisp/pkg/rbac"
)

func generatedRole(name, group string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{rbac.GroupLabel: group},
		},
	}
}

func projectionInGroup(name, group string) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = group
	p.Spec.Resource.Plural = "films"
	return p
}

// TestOrphanedRolesFindsRolesWithNoProjection is the whole point: deleting a
// projection takes its API group away and leaves the role that granted it.
func TestOrphanedRolesFindsRolesWithNoProjection(t *testing.T) {
	crisp := crispfake.NewSimpleClientset(projectionInGroup("films", "pagila.example.com"))
	kube := kubefake.NewSimpleClientset(
		generatedRole("kube-crisp:pagila.example.com:view", "pagila.example.com"),
		generatedRole("kube-crisp:gone.example.com:view", "gone.example.com"),
		generatedRole("kube-crisp:gone.example.com:edit", "gone.example.com"),
	)

	orphans, err := orphanedRoles(context.Background(), crisp, kube)
	if err != nil {
		t.Fatal(err)
	}

	if len(orphans) != 2 {
		t.Fatalf("got %d orphan(s), want the two from gone.example.com: %+v", len(orphans), orphans)
	}
	// Sorted, so the report reads the same twice.
	if orphans[0].Name != "kube-crisp:gone.example.com:edit" || orphans[1].Name != "kube-crisp:gone.example.com:view" {
		t.Fatalf("orphans are not sorted by name: %s, %s", orphans[0].Name, orphans[1].Name)
	}
}

// TestOrphanedRolesIgnoresRolesItDidNotGenerate: a hand-written role granting a
// projected group is somebody's, and a command that deleted it would be
// deleting work it has no claim on.
func TestOrphanedRolesIgnoresRolesItDidNotGenerate(t *testing.T) {
	handWritten := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "ops-can-read-films"}}
	emptyLabel := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "edited-by-somebody",
			Labels: map[string]string{rbac.GroupLabel: ""},
		},
	}

	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset(handWritten, emptyLabel)

	orphans, err := orphanedRoles(context.Background(), crisp, kube)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("would have pruned a role it did not generate: %+v", orphans)
	}
}

// TestOrphanedRolesRefusesAPartialProjectionList is the safety property the
// whole command rests on. An error listing projections leaves every group
// looking unserved and every generated role looking orphaned — so nothing may
// be reported, let alone deleted, on the strength of a list that failed.
func TestOrphanedRolesRefusesAPartialProjectionList(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	crisp.PrependReactor("list", "customresourceprojections",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("the aggregation layer is unavailable")
		})

	kube := kubefake.NewSimpleClientset(
		generatedRole("kube-crisp:pagila.example.com:view", "pagila.example.com"),
	)

	orphans, err := orphanedRoles(context.Background(), crisp, kube)
	if err == nil {
		t.Fatalf("a failed projection list returned %d orphan(s) instead of an error", len(orphans))
	}
	if orphans != nil {
		t.Fatalf("orphans returned alongside an error: %+v", orphans)
	}
}

// TestOrphanedRolesSelectsOnTheLabel checks the request rather than the result:
// the list has to be filtered server-side, or a cluster with thousands of
// ClusterRoles pulls all of them to find a handful.
func TestOrphanedRolesSelectsOnTheLabel(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset()

	var selector string
	kube.PrependReactor("list", "clusterroles", func(action k8stesting.Action) (bool, runtime.Object, error) {
		selector = action.(k8stesting.ListAction).GetListRestrictions().Labels.String()
		return false, nil, nil
	})

	if _, err := orphanedRoles(context.Background(), crisp, kube); err != nil {
		t.Fatal(err)
	}
	if selector != rbac.GroupLabel {
		t.Fatalf("list selector = %q, want %q", selector, rbac.GroupLabel)
	}
}

// clusterBinding and roleBinding build bindings pointing at a ClusterRole.
func clusterBinding(name, role string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role},
	}
}

func roleBinding(name, namespace, role string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role},
	}
}

// TestBindingsToFindsBothKinds. A cluster-scoped projected kind is granted with
// a ClusterRoleBinding and a namespaced one with a RoleBinding per tenant
// referencing the same ClusterRole — which is what scopes a tenant-column
// projection. Finding only the first would miss the case the documentation
// pushes people towards.
func TestBindingsToFindsBothKinds(t *testing.T) {
	kube := kubefake.NewSimpleClientset(
		clusterBinding("pagila-view", "kube-crisp:gone.example.com:view"),
		roleBinding("pagila-view", "store-1", "kube-crisp:gone.example.com:view"),
		roleBinding("pagila-view", "store-2", "kube-crisp:gone.example.com:view"),
	)

	found, err := bindingsTo(context.Background(), kube,
		sets.New("kube-crisp:gone.example.com:view"))
	if err != nil {
		t.Fatal(err)
	}

	if len(found) != 3 {
		t.Fatalf("got %d binding(s), want the ClusterRoleBinding and both RoleBindings: %+v", len(found), found)
	}
	// Sorted with the cluster-scoped one first, its namespace being empty, then
	// by namespace — so two runs report in the same order.
	if found[0].namespace != "" || found[1].namespace != "store-1" || found[2].namespace != "store-2" {
		t.Fatalf("bindings are not sorted by namespace: %+v", found)
	}
}

// TestBindingsToIgnoresOtherReferences is the safety rule. A binding is a
// candidate because it points at a role that is going away, and for no other
// reason: not its name, not a label, and not a same-named Role in a namespace,
// which is a different object entirely.
func TestBindingsToIgnoresOtherReferences(t *testing.T) {
	sameNamedRole := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "points-at-a-role", Namespace: "store-1"},
		// Kind Role, not ClusterRole: a namespaced role that happens to share
		// the name is not the object being pruned.
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "kube-crisp:gone.example.com:view"},
	}

	kube := kubefake.NewSimpleClientset(
		clusterBinding("points-elsewhere", "some-other-role"),
		roleBinding("points-elsewhere-too", "store-1", "cluster-admin"),
		sameNamedRole,
	)

	found, err := bindingsTo(context.Background(), kube,
		sets.New("kube-crisp:gone.example.com:view"))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("would have removed a binding that does not reference the orphaned role: %+v", found)
	}
}

// TestPruneDeletesBindingsBeforeRoles. Either order leaves something behind if
// the second half fails, and a role with no binding grants nothing and is
// removed by a re-run — where a binding whose roleRef has gone is the dangling
// reference this exists to clean up.
func TestPruneDeletesBindingsBeforeRoles(t *testing.T) {
	const orphan = "kube-crisp:gone.example.com:view"

	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset(
		generatedRole(orphan, "gone.example.com"),
		clusterBinding("pagila-view", orphan),
	)

	var order []string
	kube.PrependReactor("delete", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		order = append(order, action.GetResource().Resource)
		return false, nil, nil
	})

	var out, errOut bytes.Buffer
	o := &pruneOptions{delete: true}
	if err := o.prune(context.Background(), crisp, kube, &out, &errOut); err != nil {
		t.Fatal(err)
	}

	if len(order) != 2 || order[0] != "clusterrolebindings" || order[1] != "clusterroles" {
		t.Fatalf("delete order was %v, want the binding before the role", order)
	}
	if printed := out.String(); !strings.Contains(printed, "clusterrolebinding.rbac.authorization.k8s.io/pagila-view deleted") {
		t.Fatalf("the deleted binding was not reported:\n%s", printed)
	}
}

// TestPruneReportsBindingsWithoutDeleting: reporting is not removing, for the
// bindings as much as for the roles.
func TestPruneReportsBindingsWithoutDeleting(t *testing.T) {
	const orphan = "kube-crisp:gone.example.com:view"

	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset(
		generatedRole(orphan, "gone.example.com"),
		roleBinding("pagila-view", "store-1", orphan),
	)

	var out, errOut bytes.Buffer
	o := &pruneOptions{}
	if err := o.prune(context.Background(), crisp, kube, &out, &errOut); err != nil {
		t.Fatal(err)
	}

	printed := out.String()
	if !strings.Contains(printed, orphan) {
		t.Errorf("the orphaned role was not reported:\n%s", printed)
	}
	if !strings.Contains(printed, "rolebinding.rbac.authorization.k8s.io/pagila-view -n store-1") {
		t.Errorf("the binding was not reported under its role:\n%s", printed)
	}
	if !strings.Contains(errOut.String(), "1 orphaned role(s) and 1 binding(s)") {
		t.Errorf("the summary does not count the bindings:\n%s", errOut.String())
	}

	for _, action := range kube.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatalf("reporting deleted %s", action.GetResource().Resource)
		}
	}
}
