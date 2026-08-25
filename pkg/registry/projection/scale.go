package projection

import (
	"context"
	"fmt"
	"math"
	"strings"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// ScaleREST serves <resource>/scale.
//
// It is the same row seen through a much smaller window: a replica count in,
// a replica count out. That is enough for `kubectl scale` and for the
// horizontal pod autoscaler, neither of which knows anything about the object
// beyond those numbers.
type ScaleREST struct {
	writable *WritableREST
	spec     crispv1alpha1.ProjectedScaleSubresource
}

var (
	_ rest.Storage                  = &ScaleREST{}
	_ rest.Getter                   = &ScaleREST{}
	_ rest.Updater                  = &ScaleREST{}
	_ rest.Patcher                  = &ScaleREST{}
	_ rest.Scoper                   = &ScaleREST{}
	_ rest.GroupVersionKindProvider = &ScaleREST{}
)

// New returns an empty Scale.
func (s *ScaleREST) New() runtime.Object { return &autoscalingv1.Scale{} }

// Destroy releases resources owned by this storage. The main storage owns them.
func (s *ScaleREST) Destroy() {}

// NamespaceScoped reports whether the projected kind lives in namespaces.
func (s *ScaleREST) NamespaceScoped() bool { return s.writable.NamespaceScoped() }

// GetSingularName returns the singular resource name.
func (s *ScaleREST) GetSingularName() string { return s.writable.GetSingularName() }

// GroupVersionKind reports autoscaling/v1 Scale, the same kind a Deployment's
// scale subresource answers with. Clients like `kubectl scale` and the
// horizontal pod autoscaler only know that one.
func (s *ScaleREST) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind {
	return autoscalingv1.SchemeGroupVersion.WithKind("Scale")
}

// Get reads the object and reports the counts it carries.
func (s *ScaleREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj, err := s.writable.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}

	return s.scaleFor(obj)
}

// Update writes a new desired count and nothing else.
func (s *ScaleREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	_ rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	_ bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if s.writable.update == nil {
		return nil, false, errors.NewMethodNotSupported(s.writable.groupResource(), "update")
	}

	// The client sends a Scale, so the object it is applied to has to be
	// presented as one, and the result translated back. Admission is told about
	// the Scale too, since that is the resource the request named.
	scaled, _, err := s.writable.applyUpdate(ctx, name, scaleUpdate{rest: s, info: objInfo},
		s.scaleValidation(updateValidation), options, s.writable.update, nil)
	if err != nil {
		return nil, false, err
	}

	result, err := s.scaleFor(scaled)
	if err != nil {
		return nil, false, err
	}
	return result, false, nil
}

// scaleFor projects an object down to a Scale.
func (s *ScaleREST) scaleFor(object runtime.Object) (*autoscalingv1.Scale, error) {
	obj, ok := object.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.NewInternalError(fmt.Errorf("expected an unstructured object, got %T", object))
	}

	replicas, err := readReplicas(obj, s.spec.SpecReplicasPath)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}

	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:              obj.GetName(),
			Namespace:         obj.GetNamespace(),
			UID:               obj.GetUID(),
			ResourceVersion:   obj.GetResourceVersion(),
			CreationTimestamp: obj.GetCreationTimestamp(),
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: replicas},
	}

	if s.spec.StatusReplicasPath != "" {
		observed, err := readReplicas(obj, s.spec.StatusReplicasPath)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		scale.Status.Replicas = observed
	}
	if s.spec.LabelSelectorPath != "" {
		selector, _, err := unstructured.NestedString(obj.Object, splitPath(s.spec.LabelSelectorPath)...)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		scale.Status.Selector = selector
	}

	return scale, nil
}

// scaleValidation presents both sides of the update as Scales, so an admission
// webhook registered for the scale subresource sees the object it asked for
// rather than the row behind it.
func (s *ScaleREST) scaleValidation(validate rest.ValidateObjectUpdateFunc) rest.ValidateObjectUpdateFunc {
	if validate == nil {
		return nil
	}

	return func(ctx context.Context, obj, old runtime.Object) error {
		newScale, err := s.scaleFor(obj)
		if err != nil {
			return err
		}
		oldScale, err := s.scaleFor(old)
		if err != nil {
			return err
		}
		return validate(ctx, newScale, oldScale)
	}
}

// scaleUpdate turns the Scale a client sent into the change it implies on the
// projected object.
type scaleUpdate struct {
	rest *ScaleREST
	info rest.UpdatedObjectInfo
}

// Preconditions passes through whatever the client attached.
func (u scaleUpdate) Preconditions() *metav1.Preconditions { return u.info.Preconditions() }

// UpdatedObject applies the requested replica count to the stored object.
func (u scaleUpdate) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	current, ok := oldObj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expected an unstructured object, got %T", oldObj)
	}

	oldScale, err := u.rest.scaleFor(current)
	if err != nil {
		return nil, err
	}

	updated, err := u.info.UpdatedObject(ctx, oldScale)
	if err != nil {
		return nil, err
	}

	scale, ok := updated.(*autoscalingv1.Scale)
	if !ok {
		return nil, fmt.Errorf("expected a Scale, got %T", updated)
	}
	if scale.Spec.Replicas < 0 {
		return nil, fmt.Errorf("spec.replicas must not be negative")
	}

	// Only the desired count crosses over; everything else stays as stored.
	result := current.DeepCopy()
	if err := unstructured.SetNestedField(
		result.Object, int64(scale.Spec.Replicas), splitPath(u.rest.spec.SpecReplicasPath)...,
	); err != nil {
		return nil, fmt.Errorf("setting %s: %w", u.rest.spec.SpecReplicasPath, err)
	}
	if scale.ResourceVersion != "" {
		result.SetResourceVersion(scale.ResourceVersion)
	}
	return result, nil
}

// readReplicas reads a count from a dotted path, treating an absent value as
// zero the way a CustomResourceDefinition does.
//
// A Scale carries its counts as int32. A column holding something larger is a
// projection pointing scale at the wrong column, and silently wrapping it would
// report a replica count that is not merely wrong but arbitrary — possibly
// negative, which an autoscaler would act on.
func readReplicas(obj *unstructured.Unstructured, path string) (int32, error) {
	value, found, err := unstructured.NestedInt64(obj.Object, splitPath(path)...)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", path, err)
	}
	if !found {
		return 0, nil
	}
	if value > math.MaxInt32 || value < math.MinInt32 {
		return 0, fmt.Errorf("%s holds %d, which is not a replica count", path, value)
	}
	return int32(value), nil
}

// splitPath turns ".spec.replicas" into the field path the unstructured
// helpers take.
func splitPath(path string) []string {
	return strings.Split(strings.TrimPrefix(path, "."), ".")
}
