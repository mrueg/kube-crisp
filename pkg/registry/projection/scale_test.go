package projection

import (
	"context"
	"math"
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// scalableSpec projects the order total as a replica count, so the fixture can
// exercise scale without a second table: what the number means matters less
// than that it round-trips through the same column a write goes to.
func scalableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()

	read := `SELECT id, tenant, customer, status, total_cents, total_cents AS observed, line_items, updated_at
	         FROM orders WHERE tenant = :namespace`
	spec.Queries.List.SQL = read + " ORDER BY id"
	spec.Queries.Get.SQL = read + " AND id = :name"

	for i := range spec.Mapping.Fields {
		if spec.Mapping.Fields[i].Column == "total_cents" {
			spec.Mapping.Fields[i].Path = "spec.replicas"
		}
	}
	spec.Mapping.Fields = append(spec.Mapping.Fields, crispv1alpha1.FieldMapping{
		Column: "observed", Path: "status.replicas", Type: crispv1alpha1.FieldTypeInteger,
	})

	spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Scale: &crispv1alpha1.ProjectedScaleSubresource{
			SpecReplicasPath:   ".spec.replicas",
			StatusReplicasPath: ".status.replicas",
		},
	}
	return spec
}

func newScaleREST(t *testing.T) (*ScaleREST, *WritableREST) {
	t.Helper()

	pool := newTestPoolFor(t, scalableSpec())
	storages, err := New("orders", scalableSpec(), pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if storages.Scale == nil {
		t.Fatal("a projection with subresources.scale produced no scale storage")
	}
	return storages.Scale.(*ScaleREST), storages.writable
}

func TestScaleNotEnabledByDefault(t *testing.T) {
	pool := newTestPoolFor(t, writableSpec())
	storages, err := New("orders", writableSpec(), pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if storages.Scale != nil {
		t.Error("scale storage was installed for a projection that did not ask for it")
	}
}

func TestScaleRequiresSpecReplicasPath(t *testing.T) {
	spec := scalableSpec()
	spec.Resource.Subresources.Scale.SpecReplicasPath = ""

	if _, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil); err == nil {
		t.Fatal("New() accepted a scale subresource with no specReplicasPath")
	}
}

func TestScaleGetReportsReplicas(t *testing.T) {
	scaleREST, _ := newScaleREST(t)
	ctx := namespacedContext("acme")

	obj, err := scaleREST.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	scale, ok := obj.(*autoscalingv1.Scale)
	if !ok {
		t.Fatalf("Get() returned %T, want *autoscalingv1.Scale", obj)
	}
	if got, want := scale.Spec.Replicas, int32(4999); got != want {
		t.Errorf("spec.replicas = %d, want %d", got, want)
	}
	if got, want := scale.Status.Replicas, int32(4999); got != want {
		t.Errorf("status.replicas = %d, want %d", got, want)
	}
	if got, want := scale.Name, "order-1001"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := scale.Namespace, "acme"; got != want {
		t.Errorf("namespace = %q, want %q", got, want)
	}
	if scale.ResourceVersion == "" {
		t.Error("the scale carries no resourceVersion, so a client cannot write it back safely")
	}
}

// TestScaleServesAutoscalingKind matters because `kubectl scale` and the
// horizontal pod autoscaler only understand autoscaling/v1 Scale.
func TestScaleServesAutoscalingKind(t *testing.T) {
	scaleREST, _ := newScaleREST(t)

	gvk := scaleREST.GroupVersionKind(schema.GroupVersion{Group: "store.example.com", Version: "v1alpha1"})
	if want := autoscalingv1.SchemeGroupVersion.WithKind("Scale"); gvk != want {
		t.Errorf("GroupVersionKind() = %v, want %v", gvk, want)
	}
	if _, ok := scaleREST.New().(*autoscalingv1.Scale); !ok {
		t.Errorf("New() returned %T, want *autoscalingv1.Scale", scaleREST.New())
	}
	if !scaleREST.NamespaceScoped() {
		t.Error("scale reports cluster scope for a namespaced kind")
	}
}

func TestScaleUpdateWritesOnlyTheReplicaCount(t *testing.T) {
	scaleREST, store := newScaleREST(t)
	ctx := namespacedContext("acme")

	current, err := scaleREST.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	desired := current.(*autoscalingv1.Scale).DeepCopy()
	desired.Spec.Replicas = 7

	updated, _, err := scaleREST.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(desired), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	if got := updated.(*autoscalingv1.Scale).Spec.Replicas; got != 7 {
		t.Errorf("spec.replicas after the write = %d, want 7", got)
	}

	// The row really changed, and nothing else about it did.
	obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the object back returned error: %v", err)
	}
	order := obj.(*unstructured.Unstructured)
	replicas, _, err := unstructured.NestedInt64(order.Object, "spec", "replicas")
	if err != nil || replicas != 7 {
		t.Errorf("spec.replicas in the row = %d (err %v), want 7", replicas, err)
	}
	if customer, _, _ := unstructured.NestedString(order.Object, "spec", "customer"); customer != "ada" {
		t.Errorf("spec.customer = %q, want it untouched at %q", customer, "ada")
	}
}

func TestScaleUpdateRejectsNegativeReplicas(t *testing.T) {
	scaleREST, _ := newScaleREST(t)
	ctx := namespacedContext("acme")

	current, err := scaleREST.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	desired := current.(*autoscalingv1.Scale).DeepCopy()
	desired.Spec.Replicas = -1

	_, _, err = scaleREST.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(desired), nil, nil, false, &metav1.UpdateOptions{})
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("Update() error = %v, want a rejection of the negative count", err)
	}
}

func TestScaleUpdateHonoursResourceVersion(t *testing.T) {
	scaleREST, _ := newScaleREST(t)
	ctx := namespacedContext("acme")

	current, err := scaleREST.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	stale := current.(*autoscalingv1.Scale).DeepCopy()
	stale.ResourceVersion = "does-not-match"
	stale.Spec.Replicas = 3

	if _, _, err := scaleREST.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(stale), nil, nil, false, &metav1.UpdateOptions{}); err == nil {
		t.Fatal("Update() accepted a stale resourceVersion")
	}
}

// TestScaleAdmissionSeesAScale checks that a webhook registered for the scale
// subresource is handed the object it asked about, not the row behind it.
func TestScaleAdmissionSeesAScale(t *testing.T) {
	scaleREST, _ := newScaleREST(t)
	ctx := namespacedContext("acme")

	current, err := scaleREST.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	desired := current.(*autoscalingv1.Scale).DeepCopy()
	desired.Spec.Replicas = 2

	var seen []runtime.Object
	_, _, err = scaleREST.Update(ctx, "order-1001", rest.DefaultUpdatedObjectInfo(desired), nil,
		func(_ context.Context, obj, old runtime.Object) error {
			seen = append(seen, obj, old)
			return nil
		}, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	for _, obj := range seen {
		if _, ok := obj.(*autoscalingv1.Scale); !ok {
			t.Errorf("admission saw %T, want *autoscalingv1.Scale", obj)
		}
	}
	if len(seen) != 2 {
		t.Errorf("admission was called with %d objects, want both sides of the update", len(seen))
	}
}

// TestScaleRejectsOutOfRangeReplicas: a Scale carries its counts as int32.
// Narrowing an int64 column to one silently wraps, so a projection pointing
// scale at the wrong column would report an arbitrary — possibly negative —
// replica count, and an autoscaler would act on it.
func TestScaleRejectsOutOfRangeReplicas(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	if err := unstructured.SetNestedField(obj.Object, int64(math.MaxInt32)+1, "spec", "replicas"); err != nil {
		t.Fatalf("building the object: %v", err)
	}

	if _, err := readReplicas(obj, ".spec.replicas"); err == nil {
		t.Error("a replica count past int32 was accepted, so it wrapped silently")
	}

	// The boundary itself is a legitimate value.
	if err := unstructured.SetNestedField(obj.Object, int64(math.MaxInt32), "spec", "replicas"); err != nil {
		t.Fatalf("building the object: %v", err)
	}
	got, err := readReplicas(obj, ".spec.replicas")
	if err != nil {
		t.Fatalf("readReplicas() returned error at the boundary: %v", err)
	}
	if got != math.MaxInt32 {
		t.Errorf("readReplicas() = %d, want %d", got, int32(math.MaxInt32))
	}
}
