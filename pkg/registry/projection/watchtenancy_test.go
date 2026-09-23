package projection

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// loadProjection builds the storages without failing the test, so a projection
// that is meant to be refused can be checked for the refusal.
func loadProjection(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) error {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                newTestDB(t),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	_, err = New("orders", spec, pool, nil, nil)
	return err
}

// A watch is one poll shared by every watcher, so a projection whose rows
// depend on who is asking cannot have one. Left to itself the poll runs with
// whatever context the first watcher brought, and every watcher after that is
// served that caller's rows for as long as the stream stays open.
//
// Session variables that depend on the request are already refused alongside
// watch; this is the same rule for the other way of scoping rows.
func TestCallerScopedQueriesAreRefusedAlongsideWatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec func() crispv1alpha1.CustomResourceProjectionSpec
	}{
		{
			// The identity declared as a parameter.
			name: "a declared caller parameter",
			spec: func() crispv1alpha1.CustomResourceProjectionSpec {
				spec := callerScopedSpec()
				spec.CacheTTL = nil
				spec.Watch = nil
				return spec
			},
		},
		{
			// The identity written straight into the SQL, which is the form
			// the reference and the shipped example use.
			name: "a query naming :user directly",
			spec: func() crispv1alpha1.CustomResourceProjectionSpec {
				spec := testSpec()
				spec.Queries.Get = nil
				spec.Queries.List = crispv1alpha1.Query{
					SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
					      FROM orders WHERE tenant = :namespace AND customer = :user`,
				}
				return spec
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := loadProjection(t, tc.spec())
			if err == nil {
				t.Fatal("the projection loaded with watch enabled, so every watcher after the " +
					"first is served the first caller's rows")
			}
			if !strings.Contains(err.Error(), "watch.disabled") {
				t.Fatalf("New() error = %v, want it to say how to resolve this", err)
			}

			// Saying so is the whole fix, so the projection has to work once
			// it does.
			spec := tc.spec()
			spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}
			if err := loadProjection(t, spec); err != nil {
				t.Fatalf("New() with watch disabled returned error: %v", err)
			}
		})
	}
}

// The rule must not take watch away from the projections it is not about.
func TestOrdinaryProjectionsStillLoadWithWatch(t *testing.T) {
	if err := loadProjection(t, testSpec()); err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if err := loadProjection(t, writableSpec()); err != nil {
		t.Fatalf("New() on a writable projection returned error: %v", err)
	}
}

// scopedEventsFrom is eventsFrom with the namespace, since which namespace an
// event's object came from is the whole question here.
func scopedEventsFrom(t *testing.T, w watch.Interface) []string {
	t.Helper()

	var seen []string
	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("an event carried %T, want an object", event.Object)
			}
			seen = append(seen, string(event.Type)+"("+obj.GetNamespace()+"/"+obj.GetName()+")")
		case <-time.After(500 * time.Millisecond):
			return seen
		case <-deadline:
			return seen
		}
	}
}

// A watcher resuming past the history ring is answered out of the database,
// which holds every namespace's rows as they are now and nothing of what they
// were. With no previous object to compare, a changed row the watcher's
// selector rejected used to be taken for one that had left the selector and
// reported as a deletion carrying the row — and a row in another namespace is
// rejected by the namespace, so a watcher on acme was handed a DELETED
// carrying every changed row in other. That is the row its scope exists to
// keep from it, over a watch, to any client allowed to watch one namespace.
//
// The namespace is a hard filter on the row in hand whatever is known about
// the previous one: a row in another namespace was never this watcher's and
// cannot have left it. Within the namespace the selector halves keep the
// cautious answer, a deletion for a row the watcher may have held, since that
// discloses nothing the watcher was not entitled to and cures a stale entry.
func TestADatabaseReplayNeverCrossesNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name      string
		namespace string
		selector  labels.Selector
		fields    fields.Selector
		want      []string
	}{
		{
			name:      "a namespaced watcher",
			namespace: "acme",
			want: []string{
				"MODIFIED(acme/order-1)", "MODIFIED(acme/order-2)", "DELETED(acme/order-gone)",
			},
		},
		{
			// The reported case: order-2 has left the label selector, and
			// the replay cannot know whether the watcher held it, so it is
			// told. Nothing about other, whose rows the selector rejects
			// just the same.
			name:      "a namespaced watcher with a label selector",
			namespace: "acme",
			selector:  goldSelector(),
			want: []string{
				"MODIFIED(acme/order-1)", "DELETED(acme/order-2)", "DELETED(acme/order-gone)",
			},
		},
		{
			name:      "a namespaced watcher with a field selector",
			namespace: "acme",
			fields:    fields.OneTermEqualSelector("metadata.name", "order-1"),
			want: []string{
				"MODIFIED(acme/order-1)", "DELETED(acme/order-2)",
			},
		},
		{
			// Every namespace is its own, so it is told about all of them.
			name: "a cluster-wide watcher",
			want: []string{
				"MODIFIED(acme/order-1)", "MODIFIED(acme/order-2)",
				"MODIFIED(other/order-1)", "MODIFIED(other/order-3)",
				"DELETED(acme/order-gone)", "DELETED(other/order-gone)",
			},
		},
		{
			name:     "a cluster-wide watcher with a label selector",
			selector: goldSelector(),
			want: []string{
				"MODIFIED(acme/order-1)", "DELETED(acme/order-2)",
				"MODIFIED(other/order-1)", "DELETED(other/order-3)",
				"DELETED(acme/order-gone)", "DELETED(other/order-gone)",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gold := map[string]string{"tier": "gold"}
			silver := map[string]string{"tier": "silver"}
			rows := []unstructured.Unstructured{
				labelledItem("acme", "order-1", "2", gold),
				labelledItem("acme", "order-2", "3", silver),
				labelledItem("other", "order-1", "4", gold),
				labelledItem("other", "order-3", "5", silver),
			}
			// Tombstones that describe their rows, so a label selector can
			// be applied to them as it is to a live deletion.
			acmeGone := labelledItem("acme", "order-gone", "6", gold)
			otherGone := labelledItem("other", "order-gone", "7", gold)
			removed := []cacheIdentity{
				{namespace: "acme", name: "order-gone", object: &acmeGone},
				{namespace: "other", name: "order-gone", object: &otherGone},
			}
			cache := replayCache(t, rows, removed)
			cache.matchFields = func(obj *unstructured.Unstructured, selector fields.Selector) bool {
				return selector == nil || selector.Matches(fields.Set{"metadata.name": obj.GetName()})
			}

			// Resuming from before every change, past anything the ring
			// can cover.
			w, err := cache.Watch(context.Background(), tc.namespace, tc.selector, tc.fields, "1",
				false, false, deletedTestGVK)
			if err != nil {
				t.Fatalf("resuming from the database returned %v", err)
			}
			defer w.Stop()

			seen := scopedEventsFrom(t, w)
			if tc.namespace != "" {
				for _, event := range seen {
					if !strings.Contains(event, "("+tc.namespace+"/") {
						t.Errorf("a watcher on %s was handed %s: a row from another namespace, "+
							"over a watch, to a client allowed to see one", tc.namespace, event)
					}
				}
			}
			if !slices.Equal(seen, tc.want) {
				t.Errorf("the resumed watch received %v, want %v", seen, tc.want)
			}
		})
	}
}
