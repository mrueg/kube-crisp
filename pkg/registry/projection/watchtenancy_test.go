package projection

import (
	"strings"
	"testing"

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
