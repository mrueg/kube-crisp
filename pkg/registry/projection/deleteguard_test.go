package projection

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// guardedDeleteSpec is writableSpec with the delete statement written the way
// the update is: conditional on the version this server read.
func guardedDeleteSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Queries.Delete = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace AND id = :name
		      AND (:resourceVersion IS NULL OR updated_at = :resourceVersion)`,
	}
	return spec
}

// openBehindTheServer opens the projection's database directly, for a write
// the server does not see.
func openBehindTheServer(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestADeleteGuardedOnResourceVersionLosesARaceWithConflict is a regression
// test.
//
// A delete's preconditions were only ever compared against the object read
// before the statement ran, and :resourceVersion was bound NULL in the delete
// itself — so a statement written to make that check atomic, the way the
// update's is, never applied it. A write that committed between the read and
// the delete was lost: the row was removed on the strength of a version it no
// longer had, and the client that asserted that version was told 200.
//
// The write behind the server's back lands in deleteValidation, which runs
// after the read and before the statement, so the window is hit every time.
func TestADeleteGuardedOnResourceVersionLosesARaceWithConflict(t *testing.T) {
	storage, path := newStorageWithDB(t, guardedDeleteSpec())
	store := storage.(*WritableREST)
	db := openBehindTheServer(t, path)

	read := "1"
	options := &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: &read}}

	_, _, err := store.Delete(namespacedContext("acme"), "order-1001", writeOnceBehind(db), options)
	if !errors.IsConflict(err) {
		t.Fatalf("Delete() after a concurrent write: error = %v, want Conflict", err)
	}

	var customer string
	if err := db.QueryRow(`SELECT customer FROM orders WHERE id = 'order-1001'`).Scan(&customer); err != nil {
		t.Fatalf("the row the delete lost the race for is gone: %v", err)
	}
	if customer != "mallory" {
		t.Errorf("customer = %q, want the concurrent write's %q", customer, "mallory")
	}
}

// TestAnUnconditionalDeleteThatLosesARaceIsDecidedAgain covers the client that
// asserted nothing. The statement is still bound to the version that was read,
// so the copy the delete was admitted and answered on is the one it removed;
// losing the race to a write means deciding again on the row as it now is, as
// the etcd store does, rather than a 409 the client never asked to risk.
func TestAnUnconditionalDeleteThatLosesARaceIsDecidedAgain(t *testing.T) {
	storage, path := newStorageWithDB(t, guardedDeleteSpec())
	store := storage.(*WritableREST)
	db := openBehindTheServer(t, path)

	removed, _, err := store.Delete(namespacedContext("acme"), "order-1001", writeOnceBehind(db), &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete() after a concurrent write returned error: %v", err)
	}

	obj := removed.(*unstructured.Unstructured)
	if customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer"); customer != "mallory" {
		t.Errorf("Delete() answered with spec.customer = %q, want %q: the response is not the row that was removed",
			customer, "mallory")
	}
	if got, want := obj.GetResourceVersion(), "99"; got != want {
		t.Errorf("Delete() answered with resourceVersion %q, want %q", got, want)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM orders WHERE id = 'order-1001'`).Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 0 {
		t.Errorf("the row is still there after a delete that answered success")
	}
}

// writeOnceBehind is a deleteValidation that changes order-1001 behind the
// server's back the first time it runs, and does nothing on a retry.
func writeOnceBehind(db *sql.DB) rest.ValidateObjectFunc {
	var once sync.Once
	return func(context.Context, runtime.Object) error {
		var err error
		once.Do(func() {
			_, err = db.Exec(`UPDATE orders SET customer = 'mallory', updated_at = '99'
			                  WHERE tenant = 'acme' AND id = 'order-1001'`)
		})
		return err
	}
}

// TestAnUnguardedDeleteStillAnswersNotFoundForAMissingRow keeps the conflict
// to statements that asked for it. A delete that does not bind
// :resourceVersion and matches nothing found nothing to delete.
func TestAnUnguardedDeleteStillAnswersNotFoundForAMissingRow(t *testing.T) {
	storage, path := newStorageWithDB(t, writableSpec())
	store := storage.(*WritableREST)
	db := openBehindTheServer(t, path)

	removed := func(context.Context, runtime.Object) error {
		_, err := db.Exec(`DELETE FROM orders WHERE id = 'order-1001'`)
		return err
	}
	_, _, err := store.Delete(namespacedContext("acme"), "order-1001", removed, &metav1.DeleteOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("Delete() of a row removed meanwhile: error = %v, want NotFound", err)
	}
}
