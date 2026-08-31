package plugin

import (
	"context"
	"errors"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
