package dynamic

import (
	"context"
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// The watch queries are SQL like any other, and they were the only SQL a
// projection could declare that was never put to the database. One whose schema
// had moved compiled, reported Ready, and failed every poll behind a watch
// nobody was told had stopped working.
func TestWatchQueriesAreCheckedAgainstTheDatabase(t *testing.T) {
	for _, tc := range []struct {
		name  string
		watch *crispv1alpha1.WatchSpec
		want  string
	}{
		{
			name: "watch.query",
			watch: &crispv1alpha1.WatchSpec{
				Query: &crispv1alpha1.Query{
					SQL: "SELECT id, tenant, changed_at FROM orders WHERE changed_at > :since",
				},
			},
			want: "watch.query",
		},
		{
			name: "watch.deletedQuery",
			watch: &crispv1alpha1.WatchSpec{
				Query: &crispv1alpha1.Query{
					SQL: "SELECT id, tenant, updated_at FROM orders WHERE updated_at > :since",
				},
				DeletedQuery: &crispv1alpha1.Query{
					SQL: "SELECT id, tenant, removed_at FROM tombstones WHERE removed_at > :since",
				},
			},
			want: "watch.deletedQuery",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := newTestCompiler(t)

			p := testProjection()
			p.Spec.Mapping.ResourceVersion = "updated_at"
			p.Spec.Queries.List = crispv1alpha1.Query{
				SQL: "SELECT id, tenant, updated_at FROM orders WHERE tenant = :namespace",
			}
			p.Spec.Watch = tc.watch

			_, err := compiler.Compile(context.Background(), p)
			if err == nil {
				t.Fatal("the projection compiled with SQL the database cannot run")
			}
			// Named, and named as a statement the database refused —
			// not as a spec that does not hang together, which is a
			// different check that would pass this test for free.
			if !strings.Contains(err.Error(), tc.want+": the database cannot run this statement") {
				t.Fatalf("Compile() error = %v, want %s rejected by the database", err, tc.want)
			}
		})
	}
}

// The queries that were already checked still name themselves the same way, so
// an operator reading a CompilationFailed message sees no change.
func TestQueryFailuresStillNameTheField(t *testing.T) {
	compiler := newTestCompiler(t)

	p := testProjection()
	p.Spec.Queries.List = crispv1alpha1.Query{SQL: "SELECT id, missing FROM orders"}

	_, err := compiler.Compile(context.Background(), p)
	if err == nil {
		t.Fatal("the projection compiled with SQL the database cannot run")
	}
	if !strings.Contains(err.Error(), "queries.list:") {
		t.Fatalf("Compile() error = %v, want it to name queries.list", err)
	}
}
