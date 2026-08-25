package dynamic

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

var convertGVK = schema.GroupVersionKind{
	Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
}

func orderList(n int) *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(convertGVK.GroupVersion().WithKind("OrderList"))
	list.SetResourceVersion("42")
	for i := 0; i < n; i++ {
		item := unstructured.Unstructured{Object: map[string]any{}}
		item.SetGroupVersionKind(convertGVK)
		item.SetName("order-1")
		item.SetNamespace("acme")
		list.Items = append(list.Items, item)
	}
	return list
}

func safeConvertor(t *testing.T) projectedConvertor {
	t.Helper()

	scheme, _ := crispscheme.New()
	return projectedConvertor{ObjectConvertor: scheme, copy: true}
}

// TestListsAreConvertedAsViews: restating a collection's kind used to deep-copy
// every object in it, which for a large list costs about as much as the query
// that produced it. The items are shared instead, and treated as immutable —
// the same contract the read cache keeps.
func TestListsAreConvertedAsViews(t *testing.T) {
	in := orderList(3)

	out, err := safeConvertor(t).ConvertToVersion(in, convertGVK.GroupVersion())
	if err != nil {
		t.Fatalf("ConvertToVersion() returned error: %v", err)
	}

	list, ok := out.(*unstructured.UnstructuredList)
	if !ok {
		t.Fatalf("got %T, want an UnstructuredList", out)
	}
	if len(list.Items) != len(in.Items) {
		t.Fatalf("converted list holds %d items, want %d", len(list.Items), len(in.Items))
	}

	// Shared: the item maps are the same maps, not copies of them.
	for i := range list.Items {
		if &list.Items[i].Object == &in.Items[i].Object {
			continue
		}
		list.Items[i].Object["probe"] = "written"
		if in.Items[i].Object["probe"] != "written" {
			t.Errorf("item %d was copied; a large list pays for that on every response", i)
		}
		delete(list.Items[i].Object, "probe")
	}
}

// TestConvertedListMetadataIsSafeToStamp is the other half of the contract: the
// caller may write the list's own metadata without that reaching the original.
func TestConvertedListMetadataIsSafeToStamp(t *testing.T) {
	in := orderList(2)

	out, err := safeConvertor(t).ConvertToVersion(in, convertGVK.GroupVersion())
	if err != nil {
		t.Fatalf("ConvertToVersion() returned error: %v", err)
	}

	list := out.(*unstructured.UnstructuredList)
	list.SetResourceVersion("99")
	list.SetContinue("next-page")

	if got := in.GetResourceVersion(); got != "42" {
		t.Errorf("stamping the converted list changed the original's resourceVersion to %q", got)
	}
	if got := in.GetContinue(); got != "" {
		t.Errorf("stamping the converted list set the original's continue token to %q", got)
	}

	// Appending must not reach into the caller's backing array either.
	list.Items = append(list.Items, orderList(1).Items[0])
	if len(in.Items) != 2 {
		t.Errorf("appending to the converted list changed the original to %d items", len(in.Items))
	}
}

// TestListItemsAreCopiedWhenTheVersionChanges: sharing is only safe while
// nothing is written to the items. A conversion that really does move them to
// another group version has to leave the caller's copies alone.
func TestListItemsAreCopiedWhenTheVersionChanges(t *testing.T) {
	in := orderList(2)
	other := schema.GroupVersion{Group: convertGVK.Group, Version: "v1beta1"}

	out, err := safeConvertor(t).ConvertToVersion(in, other)
	if err != nil {
		t.Fatalf("ConvertToVersion() returned error: %v", err)
	}

	list := out.(*unstructured.UnstructuredList)
	if got := list.GetAPIVersion(); got != other.String() {
		t.Errorf("list apiVersion = %q, want %q", got, other.String())
	}
	for i := range list.Items {
		if got := list.Items[i].GetAPIVersion(); got != other.String() {
			t.Errorf("item %d apiVersion = %q, want %q", i, got, other.String())
		}
		if got := in.Items[i].GetAPIVersion(); got != convertGVK.GroupVersion().String() {
			t.Errorf("item %d of the original was rewritten to %q", i, got)
		}
		if list.Items[i].GetKind() != "Order" {
			t.Errorf("item %d lost its kind: %q", i, list.Items[i].GetKind())
		}
	}
}

// TestDecoderToVersionReadsProjectedObjects covers the decode half, which was
// added for symmetry with the encoder and is what a request body goes through.
func TestDecoderToVersionReadsProjectedObjects(t *testing.T) {
	scheme, codecs := crispscheme.New()
	scheme.AddKnownTypeWithName(convertGVK, &unstructured.Unstructured{})
	metav1.AddToGroupVersion(scheme, convertGVK.GroupVersion())

	serializer := projectedSerializer{
		NegotiatedSerializer: codecs,
		scheme:               scheme,
		convertor:            projectedConvertor{ObjectConvertor: scheme, copy: true},
	}

	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), "application/json")
	if !ok {
		t.Fatal("no JSON serializer")
	}

	decoder := serializer.DecoderToVersion(info.Serializer, convertGVK.GroupVersion())
	body := []byte(`{"apiVersion":"store.example.com/v1alpha1","kind":"Order",` +
		`"metadata":{"name":"order-1","namespace":"acme"},"spec":{"customer":"ada"}}`)

	obj, gvk, err := decoder.Decode(body, nil, &unstructured.Unstructured{})
	if err != nil {
		t.Fatalf("Decode() returned error: %v", err)
	}
	if gvk.Kind != "Order" {
		t.Errorf("decoded kind = %q, want Order", gvk.Kind)
	}

	decoded, ok := obj.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("decoded %T, want an Unstructured", obj)
	}
	if decoded.GetName() != "order-1" {
		t.Errorf("decoded name = %q, want order-1", decoded.GetName())
	}
	if customer, _, _ := unstructured.NestedString(decoded.Object, "spec", "customer"); customer != "ada" {
		t.Errorf("decoded spec.customer = %q, want ada", customer)
	}
}

// TestConvertRejectsObjectsWithNoKind: the kind is taken from the object, so an
// object that does not say what it is cannot be converted — saying so beats
// guessing, which is the bug this convertor exists to prevent.
func TestConvertRejectsObjectsWithNoKind(t *testing.T) {
	convertor := safeConvertor(t)

	if _, err := convertor.ConvertToVersion(
		&unstructured.Unstructured{Object: map[string]any{}}, convertGVK.GroupVersion(),
	); err == nil {
		t.Error("an object with no kind was converted")
	}

	noVersion := &unstructured.Unstructured{Object: map[string]any{}}
	noVersion.SetKind("Order")
	if _, err := convertor.ConvertToVersion(noVersion, convertGVK.GroupVersion()); err == nil {
		t.Error("an object with no apiVersion was converted")
	}
}

// TestTypedObjectsStillGoToTheScheme: the ambiguity this convertor works around
// only affects the projected kinds, which all share one Go type. A Status or a
// Scale has a type of its own, and the scheme converts those correctly.
func TestTypedObjectsStillGoToTheScheme(t *testing.T) {
	status := &metav1.Status{Status: "Failure", Message: "nope", Code: 500}

	out, err := safeConvertor(t).ConvertToVersion(status, metav1.SchemeGroupVersion)
	if err != nil {
		t.Fatalf("ConvertToVersion() returned error: %v", err)
	}
	converted, ok := out.(*metav1.Status)
	if !ok {
		t.Fatalf("converted a Status into %T", out)
	}
	if converted.Message != "nope" {
		t.Errorf("message = %q, want nope", converted.Message)
	}
}

// TestConvertReportsAnUnreachableTarget: a target that cannot accept the
// object's kind is an error rather than a silent pass-through, because passing
// it through is how an object ends up served under a version nobody asked for.
func TestConvertReportsAnUnreachableTarget(t *testing.T) {
	order := &unstructured.Unstructured{Object: map[string]any{}}
	order.SetGroupVersionKind(convertGVK)

	// Another group entirely: a GroupVersion accepts a kind from its own group
	// at another version, but never one from somewhere else.
	elsewhere := schema.GroupVersion{Group: "other.example.com", Version: "v1"}
	if _, err := safeConvertor(t).ConvertToVersion(order, elsewhere); err == nil {
		t.Error("converting to a target in another group succeeded")
	}
}
