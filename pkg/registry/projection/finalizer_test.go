package projection

import (
	"database/sql"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// finalizerSpec projects a table that can hold finalizers and owner
// references, and that marks a row terminating rather than removing it while
// any finalizer remains.
func finalizerSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()

	read := `SELECT id, tenant, customer, status, total_cents, line_items, updated_at,
	                deleted_at, finalizers, owners
	         FROM orders WHERE tenant = :namespace`
	spec.Queries.List.SQL = read + " ORDER BY id"
	spec.Queries.Get.SQL = read + " AND id = :name"
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at, finalizers, owners)
		      VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items,
		              CAST((SELECT COALESCE(MAX(CAST(updated_at AS INTEGER)), 0) + 1 FROM orders) AS TEXT),
		              :finalizers, :owners)`,
	}
	spec.Queries.Update = &crispv1alpha1.Query{
		SQL: `UPDATE orders
		      SET customer = :customer, status = :status, total_cents = :total_cents,
		          finalizers = :finalizers, owners = :owners,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name`,
	}
	spec.Queries.MarkDeleted = &crispv1alpha1.Query{
		SQL: `UPDATE orders
		      SET deleted_at = '2026-08-21T10:00:00Z',
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name AND deleted_at IS NULL`,
	}

	spec.Mapping.DeletionTimestamp = "deleted_at"
	spec.Mapping.Finalizers = "finalizers"
	spec.Mapping.OwnerReferences = "owners"
	return spec
}

func newFinalizerStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) (*WritableREST, string) {
	t.Helper()

	path := newTestDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	for _, stmt := range []string{
		`ALTER TABLE orders ADD COLUMN deleted_at TEXT`,
		`ALTER TABLE orders ADD COLUMN finalizers TEXT`,
		`ALTER TABLE orders ADD COLUMN owners TEXT`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("extending the table: %v", err)
		}
	}
	_ = db.Close()

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable, path
}

// createFinalized makes an object that something is holding onto.
func createFinalized(t *testing.T, store *WritableREST, name string) *unstructured.Unstructured {
	t.Helper()

	obj := newOrder(name, "ada", 10)
	obj.SetFinalizers([]string{"example.com/drain"})

	created, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	return created.(*unstructured.Unstructured)
}

func TestFinalizersRoundTrip(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	created := createFinalized(t, store, "order-fin-1")

	if got := created.GetFinalizers(); len(got) != 1 || got[0] != "example.com/drain" {
		t.Fatalf("finalizers = %v, want the one that was set", got)
	}

	read, err := store.Get(namespacedContext("acme"), "order-fin-1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := read.(*unstructured.Unstructured).GetFinalizers(); len(got) != 1 {
		t.Errorf("finalizers read back as %v, want one", got)
	}
}

// TestDeleteWithAFinalizerMarksRatherThanRemoves is the whole point: a delete
// is accepted, and the object stays until whatever holds it lets go.
func TestDeleteWithAFinalizerMarksRatherThanRemoves(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")
	createFinalized(t, store, "order-fin-2")

	returned, deleted, err := store.Delete(ctx, "order-fin-2", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	if deleted {
		t.Error("Delete() reported the object as gone while a finalizer remained")
	}
	if returned.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Error("the returned object is not marked as terminating")
	}

	// It is still there, and still says it is going away.
	still, err := store.Get(ctx, "order-fin-2", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after the delete returned error: %v", err)
	}
	if still.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Error("the stored object carries no deletionTimestamp")
	}
	if got := still.(*unstructured.Unstructured).GetFinalizers(); len(got) != 1 {
		t.Errorf("finalizers = %v, want the delete to have left them alone", got)
	}
}

// TestClearingTheLastFinalizerRemovesTheRow is the other half of the contract.
func TestClearingTheLastFinalizerRemovesTheRow(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")
	createFinalized(t, store, "order-fin-3")

	if _, _, err := store.Delete(ctx, "order-fin-3", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	terminating, err := store.Get(ctx, "order-fin-3", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	released := terminating.(*unstructured.Unstructured).DeepCopy()
	released.SetFinalizers(nil)

	if _, _, err := store.Update(ctx, "order-fin-3",
		rest.DefaultUpdatedObjectInfo(released), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() clearing the finalizer returned error: %v", err)
	}

	if _, err := store.Get(ctx, "order-fin-3", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound once the last finalizer was cleared", err)
	}
}

// TestClearingOneOfTwoFinalizersKeepsTheObject: an object goes away when
// nothing is left holding it, not when the first holder lets go.
func TestClearingOneOfTwoFinalizersKeepsTheObject(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")

	obj := newOrder("order-fin-4", "ada", 10)
	obj.SetFinalizers([]string{"example.com/drain", "example.com/audit"})
	if _, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if _, _, err := store.Delete(ctx, "order-fin-4", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	terminating, err := store.Get(ctx, "order-fin-4", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	partial := terminating.(*unstructured.Unstructured).DeepCopy()
	partial.SetFinalizers([]string{"example.com/audit"})

	if _, _, err := store.Update(ctx, "order-fin-4",
		rest.DefaultUpdatedObjectInfo(partial), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	still, err := store.Get(ctx, "order-fin-4", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the object went away while a finalizer remained: %v", err)
	}
	if got := still.(*unstructured.Unstructured).GetFinalizers(); len(got) != 1 || got[0] != "example.com/audit" {
		t.Errorf("finalizers = %v, want the remaining one", got)
	}
}

// TestAddingAFinalizerToATerminatingObjectIsRefused stops a client holding an
// object open forever after its deletion was accepted.
func TestAddingAFinalizerToATerminatingObjectIsRefused(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")
	createFinalized(t, store, "order-fin-5")

	if _, _, err := store.Delete(ctx, "order-fin-5", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	terminating, err := store.Get(ctx, "order-fin-5", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	greedy := terminating.(*unstructured.Unstructured).DeepCopy()
	greedy.SetFinalizers([]string{"example.com/drain", "example.com/forever"})

	_, _, err = store.Update(ctx, "order-fin-5",
		rest.DefaultUpdatedObjectInfo(greedy), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsForbidden(err) {
		t.Fatalf("Update() error = %v, want Forbidden", err)
	}
	if !strings.Contains(err.Error(), "example.com/forever") {
		t.Errorf("error %q does not name the finalizer that was refused", err)
	}
}

// TestDeleteWithoutFinalizersStillRemoves keeps the ordinary path ordinary.
func TestDeleteWithoutFinalizersStillRemoves(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")

	if _, err := store.Create(ctx, newOrder("order-fin-6", "ada", 10), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if _, _, err := store.Delete(ctx, "order-fin-6", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	if _, err := store.Get(ctx, "order-fin-6", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound", err)
	}
}

func TestOwnerReferencesRoundTrip(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")

	obj := newOrder("order-owned-1", "ada", 10)
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "checkout",
		UID:        "6f1c2b7e-0f1a-4f5a-9b2e-1d3c4b5a6e7f",
	}})

	if _, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	read, err := store.Get(ctx, "order-owned-1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	owners := read.(*unstructured.Unstructured).GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("ownerReferences = %v, want one", owners)
	}
	if owners[0].Kind != "Deployment" || owners[0].Name != "checkout" {
		t.Errorf("owner = %+v, want the Deployment it was given", owners[0])
	}
	if owners[0].UID == "" {
		t.Error("the owner's uid was lost; the garbage collector could not resolve it")
	}
}

// TestOwnerReferenceWithoutAUIDIsRefused: a reference the collector cannot
// resolve is how objects get collected by surprise.
func TestOwnerReferenceWithoutAUIDIsRefused(t *testing.T) {
	store, path := newFinalizerStorage(t, finalizerSpec())

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE orders SET owners = '[{"apiVersion":"apps/v1","kind":"Deployment","name":"checkout"}]' WHERE id = 'order-1001'`,
	); err != nil {
		t.Fatalf("writing the row: %v", err)
	}
	_ = db.Close()

	_, err = store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{})
	if err == nil || !strings.Contains(err.Error(), "uid") {
		t.Fatalf("Get() error = %v, want a complaint about the missing uid", err)
	}
}

func TestFinalizersNeedTheirSupportingQueries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*crispv1alpha1.CustomResourceProjectionSpec)
		want   string
	}{
		{"no deletion timestamp", func(s *crispv1alpha1.CustomResourceProjectionSpec) {
			s.Mapping.DeletionTimestamp = ""
		}, "mapping.deletionTimestamp"},
		{"no markDeleted", func(s *crispv1alpha1.CustomResourceProjectionSpec) {
			s.Queries.MarkDeleted = nil
		}, "queries.markDeleted"},
		{"no update", func(s *crispv1alpha1.CustomResourceProjectionSpec) {
			s.Queries.Update = nil
		}, "queries.update"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := finalizerSpec()
			tc.mutate(&spec)

			pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: newTestDB(t)})
			if err != nil {
				t.Fatalf("opening pool: %v", err)
			}
			t.Cleanup(func() { _ = pool.Close() })

			if _, err := New("orders", spec, pool, nil, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New() error = %v, want it to demand %s", err, tc.want)
			}
		})
	}
}

// TestDeleteCollectionRespectsFinalizers covers the shortcut that would
// otherwise walk past them: a bulk statement cannot tell which rows are still
// held, so a projection with finalizers deletes one object at a time.
func TestDeleteCollectionRespectsFinalizers(t *testing.T) {
	spec := finalizerSpec()
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace`,
	}

	store, _ := newFinalizerStorage(t, spec)
	ctx := namespacedContext("acme")

	held := newOrder("order-coll-1", "ada", 10)
	held.SetFinalizers([]string{"example.com/drain"})
	if _, err := store.Create(ctx, held, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	free := newOrder("order-coll-2", "grace", 20)
	if _, err := store.Create(ctx, free, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	if _, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{}, nil); err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}

	// The held object is still there, and terminating.
	terminating, err := store.Get(ctx, "order-coll-1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the finalized object was removed by a collection delete: %v", err)
	}
	if terminating.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Error("the finalized object was not marked as terminating")
	}

	// The one nothing was holding is gone.
	if _, err := store.Get(ctx, "order-coll-2", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Errorf("Get() error = %v, want the unheld object to be gone", err)
	}
}

// TestOwnerReferenceRulesAreEnforcedOnWrite covers what the garbage collector
// depends on being true of anything stored here.
func TestOwnerReferenceRulesAreEnforcedOnWrite(t *testing.T) {
	store, _ := newFinalizerStorage(t, finalizerSpec())
	ctx := namespacedContext("acme")

	controller := true
	for _, tc := range []struct {
		name   string
		owners []metav1.OwnerReference
		want   string
	}{
		{"no uid", []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "Deployment", Name: "checkout"},
		}, "uid"},
		{"no name", []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "Deployment", UID: "a"},
		}, "name"},
		{"two controllers", []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "Deployment", Name: "one", UID: "a", Controller: &controller},
			{APIVersion: "apps/v1", Kind: "Deployment", Name: "two", UID: "b", Controller: &controller},
		}, "controller"},
		{"the same owner twice", []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "Deployment", Name: "checkout", UID: "a"},
			{APIVersion: "apps/v1", Kind: "Deployment", Name: "checkout", UID: "a"},
		}, "Duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := newOrder("order-owner-"+strings.ReplaceAll(tc.name, " ", "-"), "ada", 10)
			obj.SetOwnerReferences(tc.owners)

			_, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
			if !errors.IsInvalid(err) {
				t.Fatalf("Create() error = %v, want Invalid", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// One controller among several owners is allowed.
	obj := newOrder("order-owner-ok", "ada", 10)
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{APIVersion: "apps/v1", Kind: "Deployment", Name: "checkout", UID: "a", Controller: &controller},
		{APIVersion: "v1", Kind: "ConfigMap", Name: "settings", UID: "b"},
	})
	if _, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() with one controller returned error: %v", err)
	}
}

// TestPropagationPolicyIsRefusedWhenItCannotBeHonoured is the fix for a silent
// divergence: Foreground and Orphan were accepted and then ignored, so a client
// that asked for its dependents to be waited on or orphaned got neither, and
// nothing said so.
func TestPropagationPolicyIsRefusedWhenItCannotBeHonoured(t *testing.T) {
	// writableSpec maps no finalizers and no ownerReferences.
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	for _, policy := range []metav1.DeletionPropagation{
		metav1.DeletePropagationForeground,
		metav1.DeletePropagationOrphan,
	} {
		t.Run(string(policy), func(t *testing.T) {
			_, _, err := store.Delete(ctx, "order-1001", nil,
				&metav1.DeleteOptions{PropagationPolicy: &policy})
			if err == nil {
				t.Fatalf("propagationPolicy=%s was accepted by a projection that cannot honour it", policy)
			}
			if !errors.IsBadRequest(err) {
				t.Errorf("error = %v, want a BadRequest", err)
			}
			if !strings.Contains(err.Error(), string(policy)) {
				t.Errorf("error %q does not name the policy", err)
			}
		})
	}

	// Background is what storage owes nothing for, so it still works.
	background := metav1.DeletePropagationBackground
	if _, _, err := store.Delete(ctx, "order-1001", nil,
		&metav1.DeleteOptions{PropagationPolicy: &background}); err != nil {
		t.Errorf("propagationPolicy=Background returned error: %v", err)
	}
}

// TestForegroundDeletionHoldsTheObject: with somewhere to record it, the policy
// is expressed the way Kubernetes expresses it — a finalizer on a terminating
// object, which the garbage collector clears once the dependents are gone.
func TestForegroundDeletionHoldsTheObject(t *testing.T) {
	spec := finalizerSpec()
	store, _ := newFinalizerStorage(t, spec)
	ctx := namespacedContext("acme")

	foreground := metav1.DeletePropagationForeground
	deleted, _, err := store.Delete(ctx, "order-1001", nil,
		&metav1.DeleteOptions{PropagationPolicy: &foreground})
	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	obj, ok := deleted.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("Delete() returned %T", deleted)
	}
	if obj.GetDeletionTimestamp().IsZero() {
		t.Error("the object was not marked terminating")
	}
	if !hasFinalizer(obj, metav1.FinalizerDeleteDependents) {
		t.Errorf("finalizers = %v, want %s", obj.GetFinalizers(), metav1.FinalizerDeleteDependents)
	}

	// And the row is still there, holding, rather than gone.
	read, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the object was removed rather than held: %v", err)
	}
	if !hasFinalizer(read.(*unstructured.Unstructured), metav1.FinalizerDeleteDependents) {
		t.Error("the finalizer did not reach the database")
	}
}

// TestPropagationPolicyDefaultsToBackground covers the deprecated boolean too,
// since clients that predate the policy still send it and the kube-apiserver
// still reads it.
func TestPropagationPolicyDefaultsToBackground(t *testing.T) {
	orphan, background := true, false
	foreground := metav1.DeletePropagationForeground

	for _, tc := range []struct {
		name    string
		options *metav1.DeleteOptions
		want    metav1.DeletionPropagation
	}{
		{"no options", nil, metav1.DeletePropagationBackground},
		{"empty options", &metav1.DeleteOptions{}, metav1.DeletePropagationBackground},
		//nolint:staticcheck // SA1019: the deprecated field is exactly what is under test
		{"orphanDependents true", &metav1.DeleteOptions{OrphanDependents: &orphan}, metav1.DeletePropagationOrphan},
		//nolint:staticcheck // SA1019: as above
		{"orphanDependents false", &metav1.DeleteOptions{OrphanDependents: &background}, metav1.DeletePropagationBackground},
		{"the policy wins over the boolean",
			//nolint:staticcheck // SA1019: the point is that the policy wins over it
			&metav1.DeleteOptions{PropagationPolicy: &foreground, OrphanDependents: &orphan},
			metav1.DeletePropagationForeground},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := propagationPolicyOf(tc.options); got != tc.want {
				t.Errorf("propagationPolicyOf() = %q, want %q", got, tc.want)
			}
		})
	}
}
