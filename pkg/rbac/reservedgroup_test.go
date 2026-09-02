package rbac_test

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/rbac"
)

func claiming(group, plural string) crispv1alpha1.CustomResourceProjection {
	p := crispv1alpha1.CustomResourceProjection{}
	p.Name = "innocuous-report"
	p.Spec.Resource.Group = group
	p.Spec.Resource.Version = "v1"
	p.Spec.Resource.Plural = plural
	p.Spec.Queries.List = crispv1alpha1.Query{SQL: "SELECT id FROM t"}
	p.Spec.Queries.Update = &crispv1alpha1.Query{SQL: "UPDATE t SET x = :x"}
	return p
}

// A generated role names its group verbatim, and the documented way to use one
// is to pipe it into kubectl apply. So a projection naming a group Kubernetes
// owns would have produced a role granting access to the cluster's own
// resources — and with --aggregate it lands in the built-in view, edit and
// admin roles with no binding step at all.
//
// Whoever can write a projection is not necessarily whoever can grant
// cluster-admin. This is the check that keeps those two apart.
func TestARoleIsNeverGeneratedForAGroupKubernetesOwns(t *testing.T) {
	for _, group := range []string{
		"rbac.authorization.k8s.io",
		"apps",
		"batch",
		"policy",
		"autoscaling",
		"extensions",
		"networking.k8s.io",
		"apiextensions.k8s.io",
		"admissionregistration.k8s.io",
		"storage.k8s.io",
		"k8s.io",
		"kubernetes.io",
		"anything.kubernetes.io",
	} {
		t.Run(group, func(t *testing.T) {
			_, err := rbac.ClusterRoles(
				[]crispv1alpha1.CustomResourceProjection{claiming(group, "clusterroles")},
				rbac.Options{})
			if err == nil {
				t.Fatalf("a role was generated granting %s", group)
			}
			if !strings.Contains(err.Error(), group) {
				t.Errorf("the refusal does not name the group: %v", err)
			}
		})
	}
}

// --aggregate is the sharper path, since the rules reach the built-in roles
// without anybody writing a binding. It must refuse too.
func TestAggregatingDoesNotBypassTheCheck(t *testing.T) {
	_, err := rbac.ClusterRoles(
		[]crispv1alpha1.CustomResourceProjection{claiming("rbac.authorization.k8s.io", "clusterroles")},
		rbac.Options{Aggregate: true})
	if err == nil {
		t.Fatal("aggregating generated a role for a group Kubernetes owns")
	}
}

// And an ordinary projected group still works, or the check has eaten the
// feature.
func TestAProjectedGroupStillGeneratesRoles(t *testing.T) {
	roles, err := rbac.ClusterRoles(
		[]crispv1alpha1.CustomResourceProjection{claiming("store.example.com", "orders")},
		rbac.Options{})
	if err != nil {
		t.Fatalf("ClusterRoles() refused an ordinary projected group: %v", err)
	}
	if len(roles) == 0 {
		t.Error("no roles were generated for a projected group")
	}
	// A group that merely contains a reserved domain as a label is not that
	// domain: it is somebody's own.
	if _, err := rbac.ClusterRoles(
		[]crispv1alpha1.CustomResourceProjection{claiming("k8s.io.example.com", "orders")},
		rbac.Options{}); err != nil {
		t.Errorf("a group ending in example.com was refused: %v", err)
	}
}
