package projection

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// everything is the set an apply might try to take ownership of, so that each
// storage can be asked what survives its filter.
func everything() *fieldpath.Set {
	return fieldpath.NewSet(
		fieldpath.MakePathOrDie("spec"),
		fieldpath.MakePathOrDie("status"),
		fieldpath.MakePathOrDie("metadata"),
	)
}

// ownable applies the filter the installer builds from a storage's reset
// fields, and reports what an apply through it could come to own.
func ownable(t *testing.T, storage any, groupVersion string) []string {
	t.Helper()

	strategy, ok := storage.(rest.ResetFieldsStrategy)
	if !ok {
		t.Fatalf("%T does not report its reset fields, so the field manager assumes it writes everything", storage)
	}
	filters := fieldpath.NewExcludeFilterSetMap(strategy.GetResetFields())

	filter, ok := filters[fieldpath.APIVersion(groupVersion)]
	if !ok {
		// No filter for this version means nothing is excluded.
		return []string{"spec", "status", "metadata"}
	}

	var out []string
	filter.Filter(everything()).Iterate(func(p fieldpath.Path) {
		// Path.String() renders a leading separator; the names are what the
		// assertions are about.
		out = append(out, strings.TrimPrefix(p.String(), "."))
	})
	return out
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// The storage the installer sees has to answer, and it is not *REST.
//
// verbs.go composes the served storage out of small types holding a *REST as a
// field rather than embedding it, deliberately, so that nothing on REST reaches
// a projection by accident. That means a method added to REST alone is not on
// the object the installer type-asserts, and the field manager would go on
// assuming this storage writes every field.
func TestTheServedStorageReportsItsResetFields(t *testing.T) {
	// Storages.Resource, not the implementation behind it: the composed type is
	// what Rebuild installs and what the installer type-asserts. The test
	// helpers deliberately hand back the implementation instead, which is the
	// one object whose answer does not matter here.
	if _, ok := servedStorage(t, statusSpec()).(rest.ResetFieldsStrategy); !ok {
		t.Fatalf("the served storage (%T) does not implement rest.ResetFieldsStrategy",
			servedStorage(t, statusSpec()))
	}
}

// servedStorage returns the storage the router installs for a projection.
func servedStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) rest.Storage {
	t.Helper()

	storages, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.Resource
}

// An apply to the main resource must not come to own status.
//
// The subresource exists so a controller can own status while a user owns the
// spec. Recording the user as an owner of status makes the controller's next
// apply either report a conflict that is not one, or take the field away from
// it — depending only on which apply arrived first.
func TestAnApplyToTheResourceCannotOwnStatus(t *testing.T) {
	got := ownable(t, servedStorage(t, statusSpec()), "store.example.com/v1alpha1")
	if contains(got, "status") {
		t.Errorf("an apply to the resource could own %v, which includes status", got)
	}
	for _, want := range []string{"spec", "metadata"} {
		if !contains(got, want) {
			t.Errorf("an apply to the resource could not own %s; it owns everything but status", want)
		}
	}
}

// And an apply to /status must own status and nothing else.
func TestAnApplyToStatusOwnsOnlyStatus(t *testing.T) {
	storages, err := New("orders", statusSpec(), newTestPoolFor(t, statusSpec()), nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if storages.Status == nil {
		t.Fatal("the status subresource was not installed")
	}

	got := ownable(t, storages.Status, "store.example.com/v1alpha1")
	if !contains(got, "status") {
		t.Errorf("an apply to /status could own %v, which does not include status", got)
	}
	for _, unwanted := range []string{"spec", "metadata"} {
		if contains(got, unwanted) {
			t.Errorf("an apply to /status could own %s, which it cannot write", unwanted)
		}
	}
}

// Without the subresource there is no boundary, and claiming one would hide a
// field the applier really does own.
//
// A projection with no status subresource writes status like any other field,
// so an apply through the resource owns it like any other field.
func TestWithoutTheSubresourceNothingIsReset(t *testing.T) {
	storage := servedStorage(t, writableSpec())

	strategy, ok := storage.(rest.ResetFieldsStrategy)
	if !ok {
		t.Fatalf("the served storage (%T) does not implement rest.ResetFieldsStrategy", storage)
	}
	if fields := strategy.GetResetFields(); len(fields) != 0 {
		t.Errorf("GetResetFields() = %v, want nothing reset without the subresource", fields)
	}

	if got := ownable(t, storage, "store.example.com/v1alpha1"); !contains(got, "status") {
		t.Errorf("an apply could own %v, which excludes status on a projection that writes it", got)
	}
}

// The two sides have to be keyed by the version being served, or the field
// manager finds no filter for the request's version and falls back to
// excluding nothing — the bug, silently.
func TestResetFieldsAreKeyedByTheServedVersion(t *testing.T) {
	storages, err := New("orders", statusSpec(), newTestPoolFor(t, statusSpec()), nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	const want = fieldpath.APIVersion("store.example.com/v1alpha1")
	for name, storage := range map[string]any{
		"resource": storages.Resource,
		"status":   storages.Status,
	} {
		fields := storage.(rest.ResetFieldsStrategy).GetResetFields()
		if _, ok := fields[want]; !ok {
			t.Errorf("%s storage keyed its reset fields by %v, want %q", name, keysOf(fields), want)
		}
	}
}

func keysOf(m map[fieldpath.APIVersion]*fieldpath.Set) []fieldpath.APIVersion {
	out := make([]fieldpath.APIVersion, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Every combination has to answer, not just the one a fixture happens to build.
//
// The served storage is assembled from the verbs a projection declares — there
// are twenty-four of them — and each holds readable as a field rather than
// embedding *REST. A combination that lost the forwarding would go on serving
// perfectly while the field manager quietly recorded ownership across the
// spec/status line for that projection alone.
func TestEveryVerbCombinationReportsItsResetFields(t *testing.T) {
	base := statusSpec()

	for _, canWatch := range []bool{false, true} {
		for _, canCreate := range []bool{false, true} {
			for _, canUpdate := range []bool{false, true} {
				for _, canDelete := range []bool{false, true} {
					spec := base
					spec.Queries.Create = nil
					spec.Queries.Delete = nil
					spec.Watch = &crispv1alpha1.WatchSpec{Disabled: !canWatch}
					if canCreate {
						spec.Queries.Create = base.Queries.Create
					}
					if canDelete {
						spec.Queries.Delete = base.Queries.Delete
					}
					if !canUpdate {
						continue // the fixture's status subresource needs a writable storage
					}

					name := fmt.Sprintf("watch=%v/create=%v/delete=%v", canWatch, canCreate, canDelete)
					t.Run(name, func(t *testing.T) {
						storage := servedStorage(t, spec)
						if _, ok := storage.(rest.ResetFieldsStrategy); !ok {
							t.Errorf("%T does not report its reset fields", storage)
						}
					})
				}
			}
		}
	}
}
