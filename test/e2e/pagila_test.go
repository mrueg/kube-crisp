//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// The Pagila tutorial's projections, run against the database it vendors. A
// tutorial nobody runs is a tutorial that rots — the SQLite one was quoting
// PostgreSQL buffer counts for months.
var (
	pagilaGroup     = "pagila.example.com"
	filmsGVR        = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "films"}
	pagilaActorsGVR = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "actors"}
	pagilaStaffGVR  = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "staffmembers"}
	pagilaCustGVR   = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "customers"}
	rentalsGVR      = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "rentals"}
	stockGVR        = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "stock"}
	storeSalesGVR   = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "storesales"}
	paymentsGVR     = schema.GroupVersionResource{Group: pagilaGroup, Version: "v1alpha1", Resource: "payments"}

	storeOne = "store-1"
)

// A film is named by a slug built in SQL, and its status comes from columns
// PostgreSQL generates rather than from anything a client could set.
func TestPagilaFilmIsSluggedAndCarriesGeneratedStatus(t *testing.T) {
	ctx := context.Background()

	film, err := dynamicClient.Resource(filmsGVR).Get(ctx, "academy-dinosaur", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(academy-dinosaur) returned error: %v", err)
	}

	if title, _, _ := unstructured.NestedString(film.Object, "spec", "title"); title != "ACADEMY DINOSAUR" {
		t.Errorf("spec.title = %q, want %q", title, "ACADEMY DINOSAUR")
	}
	if breakEven, found, _ := unstructured.NestedInt64(film.Object, "status", "rentalsToBreakEven"); !found || breakEven <= 0 {
		t.Errorf("status.rentalsToBreakEven = %d (found=%v); it is a generated column and should be positive",
			breakEven, found)
	}

	// Two arrays gathered from the join tables.
	actors, found, _ := unstructured.NestedStringSlice(film.Object, "spec", "actors")
	if !found || len(actors) == 0 {
		t.Error("spec.actors is empty; the film_actor join produced nothing")
	}
	categories, found, _ := unstructured.NestedStringSlice(film.Object, "spec", "categories")
	if !found || len(categories) == 0 {
		t.Error("spec.categories is empty; the film_category join produced nothing")
	}
}

// A label selector over a mapped column, and a field selector the projection
// pushes into the statement.
func TestPagilaFilmSelectors(t *testing.T) {
	ctx := context.Background()
	films := dynamicClient.Resource(filmsGVR)

	labelled, err := films.List(ctx, metav1.ListOptions{LabelSelector: pagilaGroup + "/rating=PG-13"})
	if err != nil {
		t.Fatalf("List by label returned error: %v", err)
	}
	if len(labelled.Items) == 0 {
		t.Fatal("no films are labelled PG-13")
	}

	selected, err := films.List(ctx, metav1.ListOptions{FieldSelector: "spec.rating=PG-13"})
	if err != nil {
		t.Fatalf("List by field returned error: %v", err)
	}
	// The same question asked two ways: one filtered here, one in the database.
	if len(selected.Items) != len(labelled.Items) {
		t.Errorf("field selector returned %d films, label selector %d; they describe the same set",
			len(selected.Items), len(labelled.Items))
	}
}

// Pagila has two actors called Susan Davis. The name carries the id because of
// it, and both have to be reachable.
func TestPagilaBothSusanDavisesExist(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(pagilaActorsGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(actors) returned error: %v", err)
	}

	var susans []string
	for _, actor := range list.Items {
		if strings.HasPrefix(actor.GetName(), "susan-davis-") {
			susans = append(susans, actor.GetName())
		}
	}
	if len(susans) != 2 {
		t.Fatalf("found %v, want two actors named Susan Davis with distinct object names", susans)
	}
	for _, name := range susans {
		if _, err := dynamicClient.Resource(pagilaActorsGVR).Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("Get(%s) returned error: %v", name, err)
		}
	}
}

// The staff table has a password and a picture. The projection's SELECT list is
// the allow-list, and nothing reaches a column it does not name.
func TestPagilaStaffNeverExposesTheirPassword(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(pagilaStaffGVR).Namespace(storeOne).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(staffmembers) returned error: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatal("no staff in store-1, so this test proves nothing")
	}

	for _, member := range list.Items {
		// Served at all, or the absence below would prove nothing.
		if username, found, _ := unstructured.NestedString(member.Object, "spec", "username"); !found || username == "" {
			t.Errorf("%s has no spec.username, so this projection is not serving staff", member.GetName())
		}
		for _, forbidden := range []string{"password", "picture"} {
			if _, found, _ := unstructured.NestedFieldNoCopy(member.Object, "spec", forbidden); found {
				t.Errorf("%s exposes spec.%s", member.GetName(), forbidden)
			}
			if _, found, _ := unstructured.NestedFieldNoCopy(member.Object, "status", forbidden); found {
				t.Errorf("%s exposes status.%s", member.GetName(), forbidden)
			}
		}
	}
}

// Namespaces are stores, so a read in one cannot see the other's rows.
func TestPagilaCustomersAreScopedToTheirStore(t *testing.T) {
	ctx := context.Background()

	one, err := dynamicClient.Resource(pagilaCustGVR).Namespace(storeOne).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(customers) in store-1 returned error: %v", err)
	}
	two, err := dynamicClient.Resource(pagilaCustGVR).Namespace("store-2").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(customers) in store-2 returned error: %v", err)
	}
	if len(one.Items) == 0 || len(two.Items) == 0 {
		t.Fatal("one of the stores has no customers, so crossing between them cannot be detected")
	}

	inStoreOne := map[string]bool{}
	for _, customer := range one.Items {
		if got := customer.GetNamespace(); got != storeOne {
			t.Errorf("a customer listed in store-1 reports namespace %q", got)
		}
		inStoreOne[customer.GetName()] = true
	}
	for _, customer := range two.Items {
		if inStoreOne[customer.GetName()] {
			t.Errorf("%s appears in both stores", customer.GetName())
		}
	}
}

// Renting opens the tsrange and returning closes it, so returning a film is a
// status write — and returning it twice is a 404 rather than a quiet success.
func TestPagilaRentAndReturn(t *testing.T) {
	ctx := context.Background()
	rentals := dynamicClient.Resource(rentalsGVR).Namespace(storeOne)

	rental := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": pagilaGroup + "/v1alpha1",
		"kind":       "Rental",
		"metadata":   map[string]any{"generateName": "rental-", "namespace": storeOne},
		"spec":       map[string]any{"film": "academy-dinosaur", "customer": "MARY SMITH"},
	}}

	created, err := rentals.Create(ctx, rental, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("renting academy-dinosaur returned error: %v", err)
	}
	name := created.GetName()
	t.Cleanup(func() { _ = rentals.Delete(context.Background(), name, metav1.DeleteOptions{}) })

	if phase, _, _ := unstructured.NestedString(created.Object, "status", "phase"); phase != "Out" {
		t.Errorf("a fresh rental has status.phase = %q, want Out", phase)
	}

	patch := []byte(`{"status":{"phase":"Returned"}}`)
	returned, err := rentals.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		t.Fatalf("returning %s returned error: %v", name, err)
	}
	if phase, _, _ := unstructured.NestedString(returned.Object, "status", "phase"); phase != "Returned" {
		t.Errorf("after the return status.phase = %q, want Returned", phase)
	}
	if at, found, _ := unstructured.NestedString(returned.Object, "status", "returnedAt"); !found || at == "" {
		t.Error("status.returnedAt is empty after a return")
	}

	// The statement requires an open range, so the second one matches nothing.
	_, err = rentals.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if !apierrors.IsNotFound(err) {
		t.Errorf("returning an already-returned rental gave %v, want NotFound", err)
	}
}

// Stock has no table of its own: it is a count over inventory, and kubectl
// scale drives it.
func TestPagilaStockScales(t *testing.T) {
	ctx := context.Background()
	stock := dynamicClient.Resource(stockGVR).Namespace(storeOne)

	before, err := stock.Get(ctx, "academy-dinosaur", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(stock) returned error: %v", err)
	}
	original, _, _ := unstructured.NestedInt64(before.Object, "status", "replicas")
	if original == 0 {
		t.Fatal("store-1 holds no copies of academy-dinosaur")
	}
	t.Cleanup(func() {
		current, err := stock.Get(context.Background(), "academy-dinosaur", metav1.GetOptions{})
		if err != nil {
			return
		}
		_ = unstructured.SetNestedField(current.Object, original, "spec", "replicas")
		_, _ = stock.Update(context.Background(), current, metav1.UpdateOptions{})
	})

	grown := before.DeepCopy()
	if err := unstructured.SetNestedField(grown.Object, original+3, "spec", "replicas"); err != nil {
		t.Fatalf("setting spec.replicas: %v", err)
	}
	scaled, err := stock.Update(ctx, grown, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("scaling up returned error: %v", err)
	}
	if got, _, _ := unstructured.NestedInt64(scaled.Object, "status", "replicas"); got != original+3 {
		t.Fatalf("after scaling up status.replicas = %d, want %d", got, original+3)
	}
	// The answer the write gives has to be internally consistent, not merely
	// consistent once something re-reads it. A data-modifying CTE cannot see its
	// own inserts, so counting the new copies off the table reported "7 copies,
	// 4 available" — a status that was never true of any moment.
	onLoan, _, _ := unstructured.NestedInt64(scaled.Object, "status", "onLoan")
	available, _, _ := unstructured.NestedInt64(scaled.Object, "status", "available")
	if onLoan+available != original+3 {
		t.Errorf("the write answered replicas=%d but onLoan=%d + available=%d = %d",
			original+3, onLoan, available, onLoan+available)
	}

	// The copies just added have never been rented, so they can go again.
	shrunk := scaled.DeepCopy()
	if err := unstructured.SetNestedField(shrunk.Object, original, "spec", "replicas"); err != nil {
		t.Fatalf("setting spec.replicas: %v", err)
	}
	back, err := stock.Update(ctx, shrunk, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("scaling down returned error: %v", err)
	}
	if got, _, _ := unstructured.NestedInt64(back.Object, "status", "replicas"); got != original {
		t.Errorf("after scaling back status.replicas = %d, want %d", got, original)
	}
}

// A projection over a view. Everything on it is status, because a view has no
// spec and nothing can write to it.
func TestPagilaStoreSalesProjectsAView(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(storeSalesGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(storesales) returned error: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("listed %d storesales, want one per store", len(list.Items))
	}
	for _, row := range list.Items {
		if sales, found, _ := unstructured.NestedString(row.Object, "status", "totalSales"); !found || sales == "" {
			t.Errorf("%s has no status.totalSales", row.GetName())
		}
	}
}

// Keyset paging has to return every row exactly once. The first page binds a
// NULL :after and the rest bind an integer, which is the shape that hid a
// mismatched cast: page one worked and every page after it failed.
func TestPagilaPaymentsPageThroughEverything(t *testing.T) {
	ctx := context.Background()
	payments := dynamicClient.Resource(paymentsGVR).Namespace(storeOne)

	whole, err := payments.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List(payments) returned error: %v", err)
	}

	seen := map[string]bool{}
	pages, cont := 0, ""
	for {
		page, err := payments.List(ctx, metav1.ListOptions{Limit: 500, Continue: cont})
		if err != nil {
			t.Fatalf("page %d returned error: %v", pages+1, err)
		}
		pages++
		for _, payment := range page.Items {
			if seen[payment.GetName()] {
				t.Fatalf("%s was returned on more than one page", payment.GetName())
			}
			seen[payment.GetName()] = true
		}
		if cont = page.GetContinue(); cont == "" {
			break
		}
		if pages > 100 {
			t.Fatal("paging did not terminate")
		}
	}

	if pages < 2 {
		t.Fatalf("the whole collection arrived in %d page(s); paging is not being exercised", pages)
	}
	// The invariant that matters: paging and not paging see the same
	// collection. Reading it in pieces must not lose rows or invent them.
	if len(seen) != len(whole.Items) {
		t.Errorf("paging over %d pages returned %d payments; a single read returned %d",
			pages, len(seen), len(whole.Items))
	}
	for _, payment := range whole.Items {
		if !seen[payment.GetName()] {
			t.Errorf("%s was returned by a single read but never by a page", payment.GetName())
		}
	}
}

// A rental something else references cannot be deleted, and the database saying
// so should reach the client as a conflict rather than an internal error.
//
// Which payment is picked matters. Pagila puts the rental_id foreign key on six
// of payment's eight partitions — payment_p0000_default and
// payment_p2007_07_max carry none — so a rental paid for outside 2007-01..06 is
// not protected at all, and deleting it succeeds and orphans the payment. The
// API inherits the constraints the schema has and no others, which is the whole
// claim; asserting it against an unconstrained partition would be asserting
// something Pagila never promised.
func TestPagilaDeletingAPaidRentalConflicts(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(paymentsGVR).Namespace(storeOne).
		List(ctx, metav1.ListOptions{Limit: 500})
	if err != nil {
		t.Fatalf("List(payments) returned error: %v", err)
	}

	var payment *unstructured.Unstructured
	for i := range list.Items {
		paidAt, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "paidAt")
		if len(paidAt) >= 7 && paidAt[:7] >= "2007-01" && paidAt[:7] <= "2007-06" {
			payment = &list.Items[i]
			break
		}
	}
	if payment == nil {
		t.Fatal("no payment from a partition that carries the foreign key")
	}

	paidRental, _, _ := unstructured.NestedString(payment.Object, "spec", "rental")
	if paidRental == "" {
		t.Fatal("the payment names no rental")
	}
	// A payment and the rental it names belong to the same store, or the
	// reference points out of its own namespace. Half of Pagila's payments were
	// taken by staff at the other store, so attributing a payment to the staff
	// member rather than to the rented copy broke exactly this.
	if _, err := dynamicClient.Resource(rentalsGVR).Namespace(storeOne).
		Get(ctx, paidRental, metav1.GetOptions{}); err != nil {
		t.Fatalf("payment %s in store-1 names rental %s, which store-1 does not hold: %v",
			payment.GetName(), paidRental, err)
	}

	err = dynamicClient.Resource(rentalsGVR).Namespace(storeOne).Delete(ctx, paidRental, metav1.DeleteOptions{})
	if err == nil {
		t.Fatalf("deleted %s, which payment %s references", paidRental, payment.GetName())
	}
	if !apierrors.IsConflict(err) {
		t.Errorf("deleting %s gave %v, want Conflict", paidRental, err)
	}
}
