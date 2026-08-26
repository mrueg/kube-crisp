package projection

import (
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// callerScopedSpec is a projection that shows each caller only their own rows,
// and caches. The rows are scoped by a bound parameter rather than by row-level
// security, which is the combination the cache key has to account for.
func callerScopedSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}
	spec.Queries.Get = nil
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders WHERE tenant = :namespace AND customer = :caller`,
		Parameters: []crispv1alpha1.QueryParameter{
			{Name: "caller", From: crispv1alpha1.ParameterSourceRequestUser},
		},
	}
	return spec
}

// TestReadCacheDoesNotCrossTenants covers a cache that answers one caller from
// another's rows.
//
// A projection may scope rows by binding the authenticated user into the query.
// The read cache keys on namespace, selectors, limit, continue and the session
// variables — so two callers whose requests differ only in who they are share a
// key, and the second is served the first's rows without their binding ever
// reaching the database.
//
// The in-flight coalescing path already guards against exactly this: flightKey
// hashes every bound value, and its comment names "a projection binding the
// authenticated user" as the case it exists to prevent. The cache has to make
// the same distinction.
func TestReadCacheDoesNotCrossTenants(t *testing.T) {
	store, ok := newStorage(t, callerScopedSpec()).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	customers := func(who string) []string {
		t.Helper()

		obj, err := store.List(userContext("acme", who), &metainternalversion.ListOptions{})
		if err != nil {
			t.Fatalf("List() as %s returned error: %v", who, err)
		}
		list, isList := obj.(*unstructured.UnstructuredList)
		if !isList {
			t.Fatalf("List() returned %T", obj)
		}
		var out []string
		for _, item := range list.Items {
			customer, _, _ := unstructured.NestedString(item.Object, "spec", "customer")
			out = append(out, customer)
		}
		return out
	}

	// The fixture holds rows for several customers; each caller must see only
	// the rows bound to their own name.
	ada := customers("ada")
	if len(ada) == 0 {
		t.Fatal("ada sees no rows of her own, so this test cannot detect her rows leaking")
	}
	for _, customer := range ada {
		if customer != "ada" {
			t.Fatalf("ada's own list contains %q, so the fixture does not scope by caller", customer)
		}
	}

	// Warm, then a different caller with an otherwise identical request.
	grace := customers("grace")
	for _, customer := range grace {
		if customer == "ada" {
			t.Fatalf("grace was served ada's rows from the read cache: %v", grace)
		}
	}
}

// TestReadCacheStillCachesWithinACaller, so the fix does not simply disable the
// cache it was protecting: the same caller asking twice must still be a hit.
func TestReadCacheStillCachesWithinACaller(t *testing.T) {
	store, ok := newStorage(t, callerScopedSpec()).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	ctx := userContext("acme", "ada")
	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("first List() returned error: %v", err)
	}

	before := store.cache.Len()
	if before == 0 {
		t.Fatal("nothing was cached at all, so this proves nothing about reuse")
	}
	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("second List() returned error: %v", err)
	}
	if after := store.cache.Len(); after != before {
		t.Errorf("the cache grew from %d to %d for the same caller asking twice; the key is "+
			"varying on something it should not", before, after)
	}
}

// TestReadCacheKeyIsUnchangedWithoutCallerParameters. A projection that does
// not scope by identity must key exactly as before, or every existing
// deployment quietly loses its cache.
func TestReadCacheKeyIsUnchangedWithoutCallerParameters(t *testing.T) {
	spec := testSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}

	store, ok := newStorage(t, spec).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	if got := store.callerKey(userContext("acme", "ada"), store.list); got != "" {
		t.Errorf("callerKey = %q for a projection with no caller-derived parameter, want empty "+
			"so the key is what it always was", got)
	}

	// And two different callers share the entry, which is the whole point of
	// the cache when rows do not depend on who is asking.
	if _, err := store.List(userContext("acme", "ada"), &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() as ada: %v", err)
	}
	before := store.cache.Len()
	if _, err := store.List(userContext("acme", "grace"), &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() as grace: %v", err)
	}
	if after := store.cache.Len(); after != before {
		t.Errorf("the cache grew from %d to %d for a projection whose rows do not depend on the "+
			"caller; identity is being keyed on when it need not be", before, after)
	}
}

// builtinCallerSpec scopes rows to the caller by binding :user straight into
// the statement, without declaring a parameter.
//
// This is the documented way to do it — the reference calls the caller bindings
// "always available" and the shipped example uses them — which is what made
// keying the cache off declared parameters alone a hole rather than a corner.
func builtinCallerSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}
	spec.Queries.Get = nil
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders WHERE tenant = :namespace AND customer = :user`,
	}
	return spec
}

// TestReadCacheDoesNotCrossTenantsForBuiltinBindings is the same leak as above,
// through the binding a projection gets without asking for it.
//
// callerKey used to walk the declared parameters, so a projection binding :user
// directly produced one key for every caller and the second was served the
// first one's rows for the whole cacheTTL. Reproduced before the fix: ada
// listed her own row, then grace listed and was handed ada's.
func TestReadCacheDoesNotCrossTenantsForBuiltinBindings(t *testing.T) {
	store, ok := newStorage(t, builtinCallerSpec()).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	customers := func(who string) []string {
		t.Helper()

		obj, err := store.List(userContext("acme", who), &metainternalversion.ListOptions{})
		if err != nil {
			t.Fatalf("List() as %s returned error: %v", who, err)
		}
		list, isList := obj.(*unstructured.UnstructuredList)
		if !isList {
			t.Fatalf("List() returned %T", obj)
		}
		out := make([]string, 0, len(list.Items))
		for i := range list.Items {
			customer, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "customer")
			out = append(out, customer)
		}
		return out
	}

	if got := customers("ada"); len(got) != 1 || got[0] != "ada" {
		t.Fatalf("ada was served %v, want her own row", got)
	}
	for _, customer := range customers("grace") {
		if customer != "grace" {
			t.Errorf("grace was served %q from the read cache", customer)
		}
	}

	// The key has to distinguish them, and it is what will be bound that
	// decides — not what was declared, which here is nothing.
	ada := store.callerKey(userContext("acme", "ada"), store.list)
	grace := store.callerKey(userContext("acme", "grace"), store.list)
	if ada == "" {
		t.Error("callerKey is empty for a query that binds :user")
	}
	if ada == grace {
		t.Errorf("callerKey is %q for both callers", ada)
	}
}
