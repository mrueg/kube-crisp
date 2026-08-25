package dynamic

import (
	"fmt"
	"testing"

	"context"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"

	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

// keyedStorage answers for whichever kind it was built for, which fakeStorage —
// fixed to the one test kind — cannot.
type keyedStorage struct {
	rest.TableConvertor
	gvk schema.GroupVersionKind
}

func (s *keyedStorage) New() runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(s.gvk)
	return obj
}

func (s *keyedStorage) NewList() runtime.Object {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(s.gvk.GroupVersion().WithKind(s.gvk.Kind + "List"))
	return list
}

func (s *keyedStorage) Destroy()                {}
func (s *keyedStorage) NamespaceScoped() bool   { return true }
func (s *keyedStorage) GetSingularName() string { return "order" }

func (s *keyedStorage) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind { return s.gvk }

func (s *keyedStorage) Get(_ context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(s.gvk)
	obj.SetName(name)
	return obj, nil
}

func (s *keyedStorage) List(_ context.Context, _ *metainternalversion.ListOptions) (runtime.Object, error) {
	return s.NewList(), nil
}

// projections builds n resources, each in a group of its own.
func projections(n int) []Resource {
	out := make([]Resource, 0, n)
	for i := range n {
		res := testResource()
		res.Group = fmt.Sprintf("group%d.example.com", i)
		res.Kind = fmt.Sprintf("Order%d", i)
		res.Plural = fmt.Sprintf("orders%d", i)
		res.Schema = &apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"spec": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"customer": {Type: "string"},
				}},
			},
		}
		res.Storage = &keyedStorage{
			TableConvertor: rest.NewDefaultTableConvertor(
				schema.GroupResource{Group: res.Group, Resource: res.Plural}),
			gvk: schema.GroupVersionKind{Group: res.Group, Version: res.Version, Kind: res.Kind},
		}
		out = append(out, res)
	}
	return out
}

// TestOpenAPIDocumentsAreReusedWhenNothingChanged is the property the cache
// exists for: a rebuild is triggered by any projection changing, by any data
// source Secret changing, and by the resync — so the common rebuild is one where
// almost nothing is different.
func TestOpenAPIDocumentsAreReusedWhenNothingChanged(t *testing.T) {
	router := NewRouter(Options{NewScheme: crispscheme.New})
	resources := projections(3)

	if err := router.Rebuild(resources); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}
	first := router.current.Load().documents
	if len(first) != 3 {
		t.Fatalf("%d documents were built, want 3", len(first))
	}

	if err := router.Rebuild(resources); err != nil {
		t.Fatalf("second Rebuild() returned error: %v", err)
	}
	second := router.current.Load().documents

	for gv, document := range first {
		if second[gv] != document {
			t.Errorf("%s was rebuilt though nothing about it changed", gv)
		}
	}
}

// TestAChangedSchemaRebuildsItsDocument is the other half: reuse has to be
// exactly as narrow as "would have produced the same document", or a projection
// whose schema changed would keep publishing the old one and kubectl explain
// would describe a shape that is no longer served.
func TestAChangedSchemaRebuildsItsDocument(t *testing.T) {
	router := NewRouter(Options{NewScheme: crispscheme.New})
	resources := projections(2)

	if err := router.Rebuild(resources); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}
	before := router.current.Load().documents

	changed := projections(2)
	changed[0].Schema.Properties["spec"] = apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"customer": {Type: "string"},
			"currency": {Type: "string"},
		},
	}

	if err := router.Rebuild(changed); err != nil {
		t.Fatalf("second Rebuild() returned error: %v", err)
	}
	after := router.current.Load().documents

	edited := changed[0].GroupVersion()
	if after[edited] == before[edited] {
		t.Error("the edited projection kept its old document")
	}
	untouched := changed[1].GroupVersion()
	if after[untouched] != before[untouched] {
		t.Error("the untouched projection was rebuilt anyway")
	}
}

// TestTheCacheForgetsWhatIsNoLongerServed, or a server whose projections come
// and go accumulates the documents of every group version it ever had.
func TestTheCacheForgetsWhatIsNoLongerServed(t *testing.T) {
	router := NewRouter(Options{NewScheme: crispscheme.New})

	if err := router.Rebuild(projections(5)); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}
	router.openAPIMu.Lock()
	full := len(router.openAPI)
	router.openAPIMu.Unlock()
	if full != 5 {
		t.Fatalf("the cache holds %d entries, want 5", full)
	}

	if err := router.Rebuild(projections(1)); err != nil {
		t.Fatalf("second Rebuild() returned error: %v", err)
	}
	router.openAPIMu.Lock()
	remaining := len(router.openAPI)
	router.openAPIMu.Unlock()
	if remaining != 1 {
		t.Errorf("the cache holds %d entries after four groups went away, want 1", remaining)
	}
}

// TestOpenAPIKeyCoversTheWholeInput: the key is the synthetic CRD itself, so a
// field added to it is covered without anybody remembering to extend a list.
func TestOpenAPIKeyCoversTheWholeInput(t *testing.T) {
	base := projections(1)
	key, ok := openAPIKey(base[0].GroupVersion(), base)
	if !ok {
		t.Fatal("openAPIKey() could not key a plain resource")
	}

	for _, tc := range []struct {
		name   string
		change func(res *Resource)
	}{
		{"the schema", func(res *Resource) {
			res.Schema = &apiextensionsv1.JSONSchemaProps{Type: "object"}
		}},
		{"a printer column", func(res *Resource) {
			res.PrinterColumns = []apiextensionsv1.CustomResourceColumnDefinition{
				{Name: "Customer", Type: "string", JSONPath: ".spec.customer"},
			}
		}},
		{"the singular name", func(res *Resource) { res.Singular = "purchase" }},
		{"short names", func(res *Resource) { res.ShortNames = []string{"ord"} }},
		{"categories", func(res *Resource) { res.Categories = []string{"store"} }},
		{"the list kind", func(res *Resource) { res.ListKind = "OrderCollection" }},
		{"scope", func(res *Resource) { res.Namespaced = !res.Namespaced }},
		{"the status subresource", func(res *Resource) { res.StatusStorage = res.Storage }},
		{"the scale subresource", func(res *Resource) {
			res.ScaleStorage = res.Storage
			res.ScaleSubresource = &apiextensionsv1.CustomResourceSubresourceScale{
				SpecReplicasPath: ".spec.replicas", StatusReplicasPath: ".status.replicas",
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := projections(1)
			tc.change(&altered[0])

			changed, ok := openAPIKey(altered[0].GroupVersion(), altered)
			if !ok {
				t.Fatal("openAPIKey() could not key the altered resource")
			}
			if changed == key {
				t.Error("the key did not move, so the document would be reused for a different shape")
			}
		})
	}
}

// BenchmarkRebuild is what the cache is measured by: the first install of a
// surface against the rebuilds that follow every unrelated change.
func BenchmarkRebuild(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		resources := projections(count)

		b.Run(fmt.Sprintf("cold/projections=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// A router of its own each time, so nothing is reused.
				router := NewRouter(Options{NewScheme: crispscheme.New})
				if err := router.Rebuild(resources); err != nil {
					b.Fatalf("Rebuild() returned error: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("warm/projections=%d", count), func(b *testing.B) {
			router := NewRouter(Options{NewScheme: crispscheme.New})
			if err := router.Rebuild(resources); err != nil {
				b.Fatalf("Rebuild() returned error: %v", err)
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := router.Rebuild(resources); err != nil {
					b.Fatalf("Rebuild() returned error: %v", err)
				}
			}
		})
	}
}
