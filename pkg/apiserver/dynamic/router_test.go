package dynamic

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	crispapiserver "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

var testGVK = schema.GroupVersionKind{Group: "store.example.com", Version: "v1alpha1", Kind: "Order"}

// fakeStorage serves a fixed object, standing in for SQL-backed storage.
type fakeStorage struct {
	rest.TableConvertor
}

func (f *fakeStorage) New() runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(testGVK)
	return obj
}

func (f *fakeStorage) NewList() runtime.Object {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(testGVK.GroupVersion().WithKind("OrderList"))
	return list
}

func (f *fakeStorage) Destroy() {}

func (f *fakeStorage) NamespaceScoped() bool { return true }

func (f *fakeStorage) GetSingularName() string { return "order" }

func (f *fakeStorage) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind { return testGVK }

func (f *fakeStorage) Get(_ context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(testGVK)
	obj.SetName(name)
	obj.SetNamespace("acme")
	return obj, nil
}

func (f *fakeStorage) List(_ context.Context, _ *metainternalversion.ListOptions) (runtime.Object, error) {
	item := &unstructured.Unstructured{Object: map[string]any{}}
	item.SetGroupVersionKind(testGVK)
	item.SetName("order-1001")
	item.SetNamespace("acme")

	list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*item}}
	list.SetGroupVersionKind(testGVK.GroupVersion().WithKind("OrderList"))
	return list, nil
}

func testResource() Resource {
	return Resource{
		Group:          testGVK.Group,
		Version:        testGVK.Version,
		Plural:         "orders",
		Kind:           testGVK.Kind,
		Namespaced:     true,
		Storage:        &fakeStorage{TableConvertor: rest.NewDefaultTableConvertor(schema.GroupResource{Group: testGVK.Group, Resource: "orders"})},
		ProjectionName: "orders",
	}
}

// newTestRouter returns a router wrapped in the request-info filter the
// generic apiserver would normally supply.
func newTestRouter(t *testing.T) (*Router, http.Handler) {
	t.Helper()

	// Every rebuild builds its own scheme; the test kind is taught to each one
	// the same way the router teaches it the resources it is installing.
	newScheme := func() (*runtime.Scheme, serializer.CodecFactory) {
		scheme, codecs := crispapiserver.New()
		scheme.AddKnownTypeWithName(testGVK, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(testGVK.GroupVersion().WithKind("OrderList"), &unstructured.UnstructuredList{})
		metav1.AddToGroupVersion(scheme, testGVK.GroupVersion())
		return scheme, codecs
	}

	router := NewRouter(Options{NewScheme: newScheme})

	resolver := &genericapirequest.RequestInfoFactory{
		APIPrefixes:          sets.NewString("apis"),
		GrouplessAPIPrefixes: sets.NewString(),
	}
	return router, genericapifilters.WithRequestInfo(router, resolver)
}

func TestRouterInstallsAndRemovesGroups(t *testing.T) {
	router, handler := newTestRouter(t)

	const listPath = "/apis/store.example.com/v1alpha1/namespaces/acme/orders"

	// Nothing is installed yet.
	if code := do(handler, listPath); code != http.StatusNotFound {
		t.Fatalf("before install: status = %d, want 404", code)
	}

	if err := router.Rebuild([]Resource{testResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	if code := do(handler, listPath); code != http.StatusOK {
		t.Fatalf("after install: status = %d, want 200", code)
	}

	// Removing the projection must take the API path with it.
	if err := router.Rebuild(nil); err != nil {
		t.Fatalf("Rebuild(nil) returned error: %v", err)
	}
	if code := do(handler, listPath); code != http.StatusNotFound {
		t.Fatalf("after removal: status = %d, want 404", code)
	}
}

func TestRouterServesDiscovery(t *testing.T) {
	router, handler := newTestRouter(t)
	if err := router.Rebuild([]Resource{testResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	t.Run("group", func(t *testing.T) {
		body, code := doBody(handler, "/apis/store.example.com")
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200", code)
		}
		var group metav1.APIGroup
		if err := json.Unmarshal(body, &group); err != nil {
			t.Fatalf("decoding APIGroup: %v", err)
		}
		if group.Name != "store.example.com" {
			t.Errorf("group name = %q, want %q", group.Name, "store.example.com")
		}
		if got, want := group.PreferredVersion.Version, "v1alpha1"; got != want {
			t.Errorf("preferred version = %q, want %q", got, want)
		}
	})

	t.Run("resources", func(t *testing.T) {
		body, code := doBody(handler, "/apis/store.example.com/v1alpha1")
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200", code)
		}
		var list metav1.APIResourceList
		if err := json.Unmarshal(body, &list); err != nil {
			t.Fatalf("decoding APIResourceList: %v", err)
		}

		var found *metav1.APIResource
		for i := range list.APIResources {
			if list.APIResources[i].Name == "orders" {
				found = &list.APIResources[i]
			}
		}
		if found == nil {
			t.Fatalf("orders not advertised; got %+v", list.APIResources)
		}
		if found.Kind != "Order" || !found.Namespaced {
			t.Errorf("orders = %+v, want kind Order and namespaced", found)
		}
	})
}

func TestRouterRejectsDuplicateResources(t *testing.T) {
	router, _ := newTestRouter(t)

	duplicate := testResource()
	duplicate.ProjectionName = "orders-copy"

	if err := router.Rebuild([]Resource{testResource(), duplicate}); err == nil {
		t.Fatal("expected an error when two projections claim the same resource")
	}
}

func TestRouterKeepsServingWhenRebuildFails(t *testing.T) {
	router, handler := newTestRouter(t)
	if err := router.Rebuild([]Resource{testResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	duplicate := testResource()
	if err := router.Rebuild([]Resource{testResource(), duplicate}); err == nil {
		t.Fatal("expected the duplicate rebuild to fail")
	}

	// The previous generation must still be serving.
	if code := do(handler, "/apis/store.example.com/v1alpha1/namespaces/acme/orders"); code != http.StatusOK {
		t.Fatalf("after failed rebuild: status = %d, want 200", code)
	}
}

func do(handler http.Handler, path string) int {
	_, code := doBody(handler, path)
	return code
}

func doBody(handler http.Handler, path string) ([]byte, int) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Body.Bytes(), rec.Code
}

// TestRouterRebuildsWhileServing is a race regression test. A projected kind
// has to be taught to a scheme before it can be served, and runtime.Scheme has
// no locking of its own — so registering kinds on the scheme that is already
// answering requests raced every read the serializer makes. Each rebuild now
// gets a scheme of its own, published with the handler that uses it.
//
// Run under -race, which is what makes this test mean anything.
func TestRouterRebuildsWhileServing(t *testing.T) {
	router, handler := newTestRouter(t)
	if err := router.Rebuild([]Resource{testResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				do(handler, "/apis/store.example.com/v1alpha1/namespaces/acme/orders")
			}
		}()
	}

	for i := 0; i < 20; i++ {
		if err := router.Rebuild([]Resource{testResource()}); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Rebuild() returned error: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	if code := do(handler, "/apis/store.example.com/v1alpha1/namespaces/acme/orders"); code != http.StatusOK {
		t.Fatalf("after concurrent rebuilds: status = %d, want 200", code)
	}
}

// kindedStorage serves one projected kind, so a group version can be given
// several and the responses told apart.
type kindedStorage struct {
	rest.TableConvertor
	gvk schema.GroupVersionKind
}

func (s *kindedStorage) New() runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(s.gvk)
	return obj
}

func (s *kindedStorage) NewList() runtime.Object {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(s.gvk.GroupVersion().WithKind(s.gvk.Kind + "List"))
	return list
}

func (s *kindedStorage) Destroy()                {}
func (s *kindedStorage) NamespaceScoped() bool   { return true }
func (s *kindedStorage) GetSingularName() string { return strings.ToLower(s.gvk.Kind) }

func (s *kindedStorage) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind { return s.gvk }

func (s *kindedStorage) Get(_ context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(s.gvk)
	obj.SetName(name)
	obj.SetNamespace("acme")
	return obj, nil
}

func (s *kindedStorage) List(_ context.Context, _ *metainternalversion.ListOptions) (runtime.Object, error) {
	item := &unstructured.Unstructured{Object: map[string]any{}}
	item.SetGroupVersionKind(s.gvk)
	item.SetName("thing-1")
	item.SetNamespace("acme")

	list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*item}}
	list.SetGroupVersionKind(s.gvk.GroupVersion().WithKind(s.gvk.Kind + "List"))
	return list, nil
}

func kindedResource(kind, plural string) Resource {
	gvk := testGVK.GroupVersion().WithKind(kind)
	return Resource{
		Group:      gvk.Group,
		Version:    gvk.Version,
		Plural:     plural,
		Kind:       kind,
		Singular:   strings.ToLower(kind),
		ListKind:   kind + "List",
		Namespaced: true,
		Storage: &kindedStorage{
			gvk:            gvk,
			TableConvertor: rest.NewDefaultTableConvertor(schema.GroupResource{Group: gvk.Group, Resource: plural}),
		},
		ProjectionName: plural,
	}
}

// TestEachKindKeepsItsOwnKind is a regression test for the whole class.
//
// Every projected kind is registered in the scheme against one Go type,
// *unstructured.Unstructured, so the scheme's reverse lookup from that type
// returns every kind in the group version and conversion took the first. A list
// of orders came back stamped ScalableOrderList, and server-side apply typed an
// Order against another kind's schema and refused the request. Only a group
// version serving more than one kind shows it, which is why a single-kind test
// never would.
func TestEachKindKeepsItsOwnKind(t *testing.T) {
	newScheme := func() (*runtime.Scheme, serializer.CodecFactory) {
		scheme, codecs := crispapiserver.New()
		for _, kind := range []string{"Order", "ScalableOrder", "Widget"} {
			gvk := testGVK.GroupVersion().WithKind(kind)
			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind(kind+"List"), &unstructured.UnstructuredList{})
		}
		metav1.AddToGroupVersion(scheme, testGVK.GroupVersion())
		return scheme, codecs
	}

	router := NewRouter(Options{NewScheme: newScheme})
	resolver := &genericapirequest.RequestInfoFactory{
		APIPrefixes:          sets.NewString("apis"),
		GrouplessAPIPrefixes: sets.NewString(),
	}
	handler := genericapifilters.WithRequestInfo(router, resolver)

	if err := router.Rebuild([]Resource{
		kindedResource("Order", "orders"),
		kindedResource("ScalableOrder", "scalableorders"),
		kindedResource("Widget", "widgets"),
	}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	for _, tc := range []struct{ plural, kind string }{
		{"orders", "Order"},
		{"scalableorders", "ScalableOrder"},
		{"widgets", "Widget"},
	} {
		t.Run(tc.plural, func(t *testing.T) {
			body, code := doBody(handler, "/apis/store.example.com/v1alpha1/namespaces/acme/"+tc.plural)
			if code != http.StatusOK {
				t.Fatalf("list status = %d, want 200 (%s)", code, body)
			}
			var list map[string]any
			if err := json.Unmarshal(body, &list); err != nil {
				t.Fatalf("decoding the list: %v", err)
			}
			if got, want := list["kind"], tc.kind+"List"; got != want {
				t.Errorf("list kind = %v, want %v", got, want)
			}

			body, code = doBody(handler, "/apis/store.example.com/v1alpha1/namespaces/acme/"+tc.plural+"/thing-1")
			if code != http.StatusOK {
				t.Fatalf("get status = %d, want 200 (%s)", code, body)
			}
			var obj map[string]any
			if err := json.Unmarshal(body, &obj); err != nil {
				t.Fatalf("decoding the object: %v", err)
			}
			if got := obj["kind"]; got != tc.kind {
				t.Errorf("object kind = %v, want %v", got, tc.kind)
			}
		})
	}
}
