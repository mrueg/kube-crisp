package projection

import (
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// Namespace RBAC is the whole reason mapping.namespace exists, and a projection
// whose query forgets the filter hands a namespaced request rows from every
// other namespace.
func TestANamespacedReadOnlyReturnsItsOwnNamespace(t *testing.T) {
	spec := testSpec()
	// The filter left out — a rename, a refactor, a copy from a cluster-wide
	// query. Nothing about the projection says it is wrong.
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at FROM orders ORDER BY id`,
	}
	spec.Queries.Get = &crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at FROM orders WHERE id = :name`,
	}

	store := newStorage(t, spec).(*REST)

	obj, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range obj.(*unstructured.UnstructuredList).Items {
		if item.GetNamespace() != "acme" {
			t.Errorf("a list in acme returned %s from namespace %q", item.GetName(), item.GetNamespace())
		}
	}

	// order-1003 lives in globex.
	got, err := store.Get(namespacedContext("acme"), "order-1003", &metav1.GetOptions{})
	if err == nil {
		t.Errorf("a get in acme returned %s from namespace %q",
			got.(*unstructured.Unstructured).GetName(), got.(*unstructured.Unstructured).GetNamespace())
	}
}

// A cluster-wide read is scoped by RBAC rather than by a namespace, and must
// still see every namespace.
func TestAClusterWideReadStillSeesEveryNamespace(t *testing.T) {
	spec := testSpec()
	// The shape the reference recommends, so one query serves both a
	// namespaced read and the cluster-wide one.
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders WHERE (:namespace IS NULL OR tenant = :namespace) ORDER BY id`,
	}

	store := newStorage(t, spec).(*REST)

	obj, err := store.List(namespacedContext(""), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	namespaces := map[string]bool{}
	for _, item := range obj.(*unstructured.UnstructuredList).Items {
		namespaces[item.GetNamespace()] = true
	}
	for _, want := range []string{"acme", "globex"} {
		if !namespaces[want] {
			t.Errorf("a cluster-wide list did not return namespace %q", want)
		}
	}
}
