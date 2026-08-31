package projection

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// advertised names the verbs the endpoint installer would derive from a
// storage, by asking the same questions it asks: which interfaces does this
// satisfy?
//
// Not an approximation of the installer — these are the exact type assertions
// from k8s.io/apiserver/pkg/endpoints/installer.go.
func advertised(storage rest.Storage) []string {
	var verbs []string
	add := func(ok bool, verb string) {
		if ok {
			verbs = append(verbs, verb)
		}
	}
	_, getter := storage.(rest.Getter)
	_, updater := storage.(rest.Updater)

	add(getter, "get")
	_, lister := storage.(rest.Lister)
	add(lister, "list")
	_, watcher := storage.(rest.Watcher)
	add(watcher, "watch")
	_, creater := storage.(rest.Creater)
	add(creater, "create")
	add(updater, "update")
	_, patcher := storage.(rest.Patcher)
	add(patcher, "patch")
	_, deleter := storage.(rest.GracefulDeleter)
	add(deleter, "delete")
	_, collectionDeleter := storage.(rest.CollectionDeleter)
	add(collectionDeleter, "deletecollection")

	sort.Strings(verbs)
	return verbs
}

// TestAdvertisedVerbsMatchDeclaredQueries walks every combination of declared
// queries and checks that discovery would offer exactly the verbs the
// projection can actually serve.
//
// This is the whole point of the type matrix. A projection that declares only
// an update statement used to advertise create, delete, deletecollection and
// watch as well, and refuse all four at request time with a 405 — which the
// garbage collector retries, and kubectl delete --all reports as an error.
func TestAdvertisedVerbsMatchDeclaredQueries(t *testing.T) {
	for _, canWatch := range []bool{false, true} {
		for _, canCreate := range []bool{false, true} {
			for _, canUpdate := range []bool{false, true} {
				for _, del := range []string{"none", "collection", "row"} {
					name := fmt.Sprintf("watch=%v/create=%v/update=%v/delete=%s",
						canWatch, canCreate, canUpdate, del)
					t.Run(name, func(t *testing.T) {
						r := &REST{}
						if canWatch {
							r.watch = &watchCache{}
						}
						w := &WritableREST{REST: r}
						if canCreate {
							w.create = &compiledQuery{}
						}
						if canUpdate {
							w.update = &compiledQuery{}
						}
						switch del {
						case "row":
							w.delete = &compiledQuery{}
						case "collection":
							w.deleteCollection = &compiledQuery{}
						}

						want := []string{"get", "list"}
						if canWatch {
							want = append(want, "watch")
						}
						if canCreate {
							want = append(want, "create")
						}
						if canUpdate {
							want = append(want, "update", "patch")
						}
						if del == "row" {
							want = append(want, "delete")
						}
						if del != "none" {
							want = append(want, "deletecollection")
						}
						sort.Strings(want)

						got := advertised(newProjectionStorage(r, w))
						if strings.Join(got, ",") != strings.Join(want, ",") {
							t.Fatalf("advertised verbs\n got: %v\nwant: %v", got, want)
						}
					})
				}
			}
		}
	}
}

// TestReadOnlyProjectionAdvertisesNoWrites covers the path taken when a
// projection declares no write query at all, which builds the storage with a
// nil WritableREST rather than an empty one.
func TestReadOnlyProjectionAdvertisesNoWrites(t *testing.T) {
	for _, canWatch := range []bool{false, true} {
		r := &REST{}
		want := []string{"get", "list"}
		if canWatch {
			r.watch = &watchCache{}
			want = append(want, "watch")
		}
		sort.Strings(want)

		got := advertised(newProjectionStorage(r, nil))
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("watch=%v: advertised verbs\n got: %v\nwant: %v", canWatch, got, want)
		}
	}
}

// TestServedVerbsMatchAdvertisedVerbs is the lockstep check between the two
// places that decide what a projection can do.
//
// The storage matrix above answers it in Go interfaces, from compiled queries,
// and is what discovery reports. projection.ServedVerbs answers it from the
// spec, and is what generated RBAC grants. They are reached by different code
// and used by different callers, so nothing except this test stops them from
// drifting — and drift is close to invisible: a verb added to the matrix but
// not to ServedVerbs shows up as a 403 on a verb discovery advertises, which
// reads like a misconfigured binding rather than a bug here.
func TestServedVerbsMatchAdvertisedVerbs(t *testing.T) {
	for _, watchDisabled := range []bool{false, true} {
		for _, canCreate := range []bool{false, true} {
			for _, canUpdate := range []bool{false, true} {
				for _, del := range []string{"none", "collection", "row"} {
					name := fmt.Sprintf("watchDisabled=%v/create=%v/update=%v/delete=%s",
						watchDisabled, canCreate, canUpdate, del)
					t.Run(name, func(t *testing.T) {
						spec := crispv1alpha1.CustomResourceProjectionSpec{
							Watch: &crispv1alpha1.WatchSpec{Disabled: watchDisabled},
						}

						r := &REST{}
						if !watchDisabled {
							r.watch = &watchCache{}
						}
						w := &WritableREST{REST: r}
						if canCreate {
							spec.Queries.Create = &crispv1alpha1.Query{}
							w.create = &compiledQuery{}
						}
						if canUpdate {
							spec.Queries.Update = &crispv1alpha1.Query{}
							w.update = &compiledQuery{}
						}
						switch del {
						case "row":
							spec.Queries.Delete = &crispv1alpha1.Query{}
							w.delete = &compiledQuery{}
						case "collection":
							spec.Queries.DeleteCollection = &crispv1alpha1.Query{}
							w.deleteCollection = &compiledQuery{}
						}

						// A projection with no write query at all builds its
						// storage with a nil WritableREST rather than an empty
						// one, so the comparison has to take the same path the
						// server does.
						if !canCreate && !canUpdate && del == "none" {
							w = nil
						}

						got := projection.ServedVerbs(spec)
						sort.Strings(got)
						want := advertised(newProjectionStorage(r, w))

						if strings.Join(got, ",") != strings.Join(want, ",") {
							t.Fatalf("ServedVerbs disagrees with discovery\n ServedVerbs: %v\n  advertised: %v", got, want)
						}
					})
				}
			}
		}
	}
}

// TestSubresourceVerbsMatchSubresourceStorage checks the other half: which
// subresources are served, and with which verbs.
//
// Both subresource storages hang off the writable storage, so a projection
// declaring subresources.status with no write query serves no status
// subresource at all — a grant for films/status there would name a path that
// 404s. That is exactly the kind of detail a hand-written role gets wrong.
func TestSubresourceVerbsMatchSubresourceStorage(t *testing.T) {
	for _, wantStatus := range []bool{false, true} {
		for _, wantScale := range []bool{false, true} {
			for _, writable := range []bool{false, true} {
				name := fmt.Sprintf("status=%v/scale=%v/writable=%v", wantStatus, wantScale, writable)
				t.Run(name, func(t *testing.T) {
					spec := crispv1alpha1.CustomResourceProjectionSpec{}
					sub := &crispv1alpha1.ProjectedSubresources{}
					if wantStatus {
						sub.Status = &crispv1alpha1.ProjectedStatusSubresource{}
					}
					if wantScale {
						sub.Scale = &crispv1alpha1.ProjectedScaleSubresource{SpecReplicasPath: ".spec.replicas"}
					}
					spec.Resource.Subresources = sub
					if writable {
						spec.Queries.Update = &crispv1alpha1.Query{}
					}

					status := projection.SubresourceVerbs(spec, "status")
					scale := projection.SubresourceVerbs(spec, "scale")

					if served := wantStatus && writable; served != (status != nil) {
						t.Errorf("status subresource served=%v, SubresourceVerbs returned %v", served, status)
					}
					if served := wantScale && writable; served != (scale != nil) {
						t.Errorf("scale subresource served=%v, SubresourceVerbs returned %v", served, scale)
					}

					// The storages themselves read and write one object and do
					// nothing else, which is what the verbs claim.
					for _, verbs := range [][]string{status, scale} {
						if verbs == nil {
							continue
						}
						if strings.Join(verbs, ",") != "get,update,patch" {
							t.Errorf("subresource verbs = %v, want get,update,patch", verbs)
						}
					}
				})
			}
		}
	}
}

// TestSubresourceVerbsRejectsUnknownSubresource keeps the switch closed: a name
// nobody serves must not come back with verbs.
func TestSubresourceVerbsRejectsUnknownSubresource(t *testing.T) {
	spec := crispv1alpha1.CustomResourceProjectionSpec{}
	spec.Queries.Update = &crispv1alpha1.Query{}
	spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Status: &crispv1alpha1.ProjectedStatusSubresource{},
	}

	if verbs := projection.SubresourceVerbs(spec, "logs"); verbs != nil {
		t.Fatalf("SubresourceVerbs(logs) = %v, want nil", verbs)
	}
}
