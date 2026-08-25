//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestDeletePropagationPolicy covers what a delete's propagationPolicy means
// for a projected object.
//
// Kubernetes expresses Foreground and Orphan as finalizers: storage marks the
// object terminating and holds it while the garbage collector deals with the
// dependents, and only then is it really gone. Background asks storage for
// nothing. A projection can only take part in that if it maps a column to keep
// finalizers in — so the interesting cases are the one that can and the one
// that cannot, and the second must be refused rather than quietly downgraded.
// A client told its dependents will be handled, when nothing will handle them,
// is worse off than one told this projection cannot do that.
func TestDeletePropagationPolicy(t *testing.T) {
	ctx := context.Background()
	audited := dynamicClient.Resource(auditedOrdersGVR).Namespace(acmeNamespace)

	// Both policies that need a finalizer, and the name each is recorded under.
	for _, tc := range []struct {
		policy    metav1.DeletionPropagation
		finalizer string
	}{
		{metav1.DeletePropagationForeground, metav1.FinalizerDeleteDependents},
		{metav1.DeletePropagationOrphan, metav1.FinalizerOrphanDependents},
	} {
		t.Run(string(tc.policy)+" holds the object", func(t *testing.T) {
			name := fmt.Sprintf("order-propagation-%s", strings.ToLower(string(tc.policy)))
			t.Cleanup(func() {
				execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
				execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
			})

			created, err := audited.Create(ctx, newAuditedOrder(name, "ada", 700), metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("creating: %v", err)
			}
			if got := created.GetFinalizers(); len(got) != 0 {
				t.Fatalf("finalizers = %v on a fresh object; the policy under test must be "+
					"what puts one there, or this proves nothing", got)
			}

			policy := tc.policy
			if err := audited.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
				t.Fatalf("deleting with propagationPolicy=%s: %v", tc.policy, err)
			}

			// Held, not gone: the garbage collector has not run yet, and this
			// is the window it needs.
			held, err := audited.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("reading an object held by propagationPolicy=%s: %v", tc.policy, err)
			}
			if held.GetDeletionTimestamp() == nil {
				t.Error("the object carries no deletionTimestamp, so nothing marks it as terminating")
			}
			if got := held.GetFinalizers(); !slices.Contains(got, tc.finalizer) {
				t.Errorf("finalizers = %v, want %q — without it the garbage collector has "+
					"no record that this delete asked for %s", got, tc.finalizer, tc.policy)
			}

			// And the row is still there, which is the half a status field
			// could fake but a table cannot.
			if got := querySQL(t, fmt.Sprintf("SELECT count(*) FROM orders WHERE id = '%s'", name)); got != "1" {
				t.Errorf("the row is already gone (%s rows), so the object was only held in appearance", got)
			}

			// Clearing it is what lets go.
			released := held.DeepCopy()
			released.SetFinalizers(nil)
			if _, err := audited.Update(ctx, released, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("clearing the finalizer: %v", err)
			}
			if _, err := audited.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("reading after the finalizer was cleared returned %v, want NotFound", err)
			}
		})
	}

	t.Run("Background needs no finalizer", func(t *testing.T) {
		name := "order-propagation-background"
		t.Cleanup(func() {
			execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
			execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
		})

		if _, err := audited.Create(ctx, newAuditedOrder(name, "grace", 100), metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating: %v", err)
		}

		policy := metav1.DeletePropagationBackground
		if err := audited.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
			t.Fatalf("deleting with propagationPolicy=Background: %v", err)
		}
		if _, err := audited.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("get after a Background delete returned %v, want NotFound — the collector "+
				"cleans up afterwards, so storage has nothing to hold the object for", err)
		}
	})

	// The boolean that predates propagationPolicy. The kube-apiserver still
	// reads it, so a client that believes it asked to orphan must not be
	// silently given a Background delete instead.
	t.Run("the deprecated orphanDependents boolean still means Orphan", func(t *testing.T) {
		name := "order-propagation-legacy"
		t.Cleanup(func() {
			execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
			execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
		})

		if _, err := audited.Create(ctx, newAuditedOrder(name, "edsger", 200), metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating: %v", err)
		}

		orphan := true
		if err := audited.Delete(ctx, name, metav1.DeleteOptions{OrphanDependents: &orphan}); err != nil { //nolint:staticcheck // SA1019: the point of the test
			t.Fatalf("deleting with orphanDependents=true: %v", err)
		}

		held, err := audited.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading an object deleted with orphanDependents=true: %v", err)
		}
		if got := held.GetFinalizers(); !slices.Contains(got, metav1.FinalizerOrphanDependents) {
			t.Errorf("finalizers = %v, want %q — the deprecated boolean was ignored, so a client "+
				"asking to orphan its dependents had them collected instead",
				got, metav1.FinalizerOrphanDependents)
		}

		released := held.DeepCopy()
		released.SetFinalizers(nil)
		if _, err := audited.Update(ctx, released, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("clearing the finalizer: %v", err)
		}
	})

	// A projection with nowhere to record a finalizer cannot hold anything, and
	// says so.
	t.Run("a projection that cannot hold refuses rather than downgrading", func(t *testing.T) {
		orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

		name := "order-propagation-unheld"
		t.Cleanup(func() {
			execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
		})

		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "store.example.com/v1alpha1",
			"kind":       "Order",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"customer": "linus", "totalCents": int64(300)},
			"status":     map[string]any{"phase": "pending"},
		}}
		if _, err := orders.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating: %v", err)
		}

		for _, policy := range []metav1.DeletionPropagation{
			metav1.DeletePropagationForeground,
			metav1.DeletePropagationOrphan,
		} {
			policy := policy
			err := orders.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
			if !apierrors.IsBadRequest(err) {
				t.Errorf("deleting with propagationPolicy=%s returned %v, want BadRequest", policy, err)
				continue
			}
			// The message has to name the way out, or the only way to find it
			// is to read this server's source.
			if msg := err.Error(); !strings.Contains(msg, "mapping.finalizers") {
				t.Errorf("the refusal for propagationPolicy=%s does not mention mapping.finalizers: %s",
					policy, msg)
			}
		}

		// And the object is untouched by the attempts, rather than half-deleted.
		still, err := orders.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading after two refused deletes: %v", err)
		}
		if still.GetDeletionTimestamp() != nil {
			t.Error("a refused delete still marked the object as terminating")
		}

		// Background works, so the resource is deletable and the refusals above
		// are about the policy rather than about this projection being read-only.
		if err := orders.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("deleting with the default policy: %v", err)
		}
	})
}
