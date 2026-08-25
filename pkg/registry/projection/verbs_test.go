package projection

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/registry/rest"
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
