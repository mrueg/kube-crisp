package projection

import (
	"database/sql"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// newCachingREST is the read fixture with a cache long enough that nothing in
// a test expires, and watch off so the poller does not read the table from
// under the assertions.
func newCachingREST(t *testing.T) (*REST, func(customer, version string)) {
	t.Helper()

	spec := testSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}

	store, path := newStorageWithDB(t, spec)

	// Moves the row without going through the server, so the cache has no way
	// to know it happened — which is the situation a resourceVersion on the
	// request exists to describe.
	bump := func(customer, version string) {
		t.Helper()

		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatalf("opening sqlite: %v", err)
		}
		defer db.Close()

		if _, err := db.Exec(`UPDATE orders SET customer = ?, updated_at = ? WHERE id = 'order-1001'`,
			customer, version); err != nil {
			t.Fatalf("updating the row: %v", err)
		}
	}
	return store.(*REST), bump
}

// TestGetRefusesACachedObjectOlderThanTheRequestedVersion is the asymmetry this
// fixes. A resourceVersion on a read says the client has already seen that
// version and must not be handed anything older; List turns a cached page away
// for exactly that reason, and Get discarded its options entirely, so the same
// client asking for the same row was served the stale copy.
func TestGetRefusesACachedObjectOlderThanTheRequestedVersion(t *testing.T) {
	store, bump := newCachingREST(t)
	ctx := namespacedContext("acme")

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	bump("carol", "100")

	obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{ResourceVersion: "100"})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	got := obj.(*unstructured.Unstructured)
	if version := got.GetResourceVersion(); version != "100" {
		t.Fatalf("Get(resourceVersion=100) answered at version %q, from the cache", version)
	}
	customer, _, err := unstructured.NestedString(got.Object, "spec", "customer")
	if err != nil {
		t.Fatalf("reading spec.customer: %v", err)
	}
	if customer != "carol" {
		t.Errorf("spec.customer = %q, want the value the row now holds", customer)
	}
}

// TestGetStillAnswersFromTheCache is the other half: the version is a floor,
// not an order to re-read. A client asserting nothing, and the zero every
// client-go cache read sends, must still be served without a round trip, or
// cacheTTL would stop meaning anything for single-object reads.
func TestGetStillAnswersFromTheCache(t *testing.T) {
	for _, requested := range []string{"", "0"} {
		t.Run("resourceVersion "+requested, func(t *testing.T) {
			store, bump := newCachingREST(t)
			ctx := namespacedContext("acme")

			if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
				t.Fatalf("Get() returned error: %v", err)
			}
			bump("carol", "100")

			obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{ResourceVersion: requested})
			if err != nil {
				t.Fatalf("Get() returned error: %v", err)
			}
			if version := obj.(*unstructured.Unstructured).GetResourceVersion(); version != "1" {
				t.Errorf("Get() answered at version %q, want the cached %q", version, "1")
			}
		})
	}
}

// TestGetAtAVersionItAlreadyHasStaysCached, so the check is a comparison and
// not a way of switching caching off by naming any version at all.
func TestGetAtAVersionItAlreadyHasStaysCached(t *testing.T) {
	store, bump := newCachingREST(t)
	ctx := namespacedContext("acme")

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	bump("carol", "100")

	obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{ResourceVersion: "1"})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if version := obj.(*unstructured.Unstructured).GetResourceVersion(); version != "1" {
		t.Errorf("Get(resourceVersion=1) answered at %q; the cached copy already satisfied it", version)
	}
}

// TestFreshEnoughVersion pins what the object and the collection now share. A
// client that gets one answer from Get and another from List over the same
// rows has no way to tell which of them to believe.
func TestFreshEnoughVersion(t *testing.T) {
	for _, tc := range []struct {
		name string
		have string
		want string
		ok   bool
	}{
		{"nothing asserted", "7", "", true},
		{"the zero a cache read sends", "7", "0", true},
		{"the version it already holds", "7", "7", true},
		{"newer than asked for", "9", "7", true},
		{"older than asked for", "5", "7", false},
		{"no version to show", "", "7", false},
		{"no version and none asked for", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := freshEnoughVersion(tc.have, tc.want); got != tc.ok {
				t.Errorf("freshEnoughVersion(%q, %q) = %v, want %v", tc.have, tc.want, got, tc.ok)
			}
		})
	}
}
