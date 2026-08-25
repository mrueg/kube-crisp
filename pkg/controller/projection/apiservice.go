package projection

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
)

// APIServiceGVR is the resource the aggregation layer routes with.
var APIServiceGVR = schema.GroupVersionResource{
	Group:    "apiregistration.k8s.io",
	Version:  "v1",
	Resource: "apiservices",
}

// managedByLabel marks the APIServices this controller owns. Anything without
// it was created by someone else and is left alone.
const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "kube-crisp"
)

// APIServiceOptions describes how to reach this server, which is what an
// APIService needs in order to route to it.
type APIServiceOptions struct {
	// Enabled turns the reconciler on.
	Enabled bool

	// ServiceName and ServiceNamespace locate the Service in front of this
	// server; Port is the Service port, not the container port.
	ServiceName      string
	ServiceNamespace string
	Port             int32

	// CABundle verifies this server's serving certificate. When empty, the
	// APIService is created with insecureSkipTLSVerify, which matches the
	// self-signed certificates the server generates by default.
	CABundle []byte

	// GroupPriorityMinimum and VersionPriority order this group against others.
	GroupPriorityMinimum int32
	VersionPriority      int32
}

// DefaultAPIServiceOptions returns options pointing at the conventional
// in-cluster deployment.
func DefaultAPIServiceOptions() APIServiceOptions {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "kube-crisp"
	}

	return APIServiceOptions{
		Enabled:              true,
		ServiceName:          "kube-crisp-apiserver",
		ServiceNamespace:     namespace,
		Port:                 443,
		GroupPriorityMinimum: 1000,
		VersionPriority:      15,
	}
}

// apiServiceManager creates, updates, and removes the APIService objects that
// delegate projected groups to this server.
//
// Without this, installing a projection in a new API group would still need a
// manual step, and deleting the last projection in a group would leave an
// APIService pointing at an API the server no longer serves.
type apiServiceManager struct {
	client  dynamic.Interface
	options APIServiceOptions

	// indexer is the APIService informer's cache. Reconciling reads one object
	// per served group version plus a full list on every sync, and those are
	// requests to the kube-apiserver for objects this server already watches.
	// Nil falls back to reading through the client, which is what the tests and
	// any caller without an informer do.
	indexer cache.Indexer
}

func newAPIServiceManager(client dynamic.Interface, options APIServiceOptions, indexer cache.Indexer) *apiServiceManager {
	return &apiServiceManager{client: client, options: options, indexer: indexer}
}

// lookup returns the named APIService, preferring the informer cache.
//
// A miss is reported as "not found", which is also what the client would say.
// The cache can be behind by a write this server just made, and the recovery is
// the same either way: creating something that exists reports AlreadyExists and
// is left alone, and updating against a stale version conflicts and is retried
// on the next sync.
func (m *apiServiceManager) lookup(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	if m.indexer == nil {
		return m.client.Resource(APIServiceGVR).Get(ctx, name, metav1.GetOptions{})
	}

	item, exists, err := m.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(APIServiceGVR.GroupResource(), name)
	}
	existing, ok := item.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("APIService cache holds %T", item)
	}
	return existing, nil
}

// managed lists the APIServices this server owns, preferring the cache.
func (m *apiServiceManager) managed(ctx context.Context) ([]*unstructured.Unstructured, error) {
	if m.indexer == nil {
		list, err := m.client.Resource(APIServiceGVR).List(ctx, metav1.ListOptions{
			LabelSelector: managedByLabel + "=" + managedByValue,
		})
		if err != nil {
			return nil, err
		}
		out := make([]*unstructured.Unstructured, 0, len(list.Items))
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
		return out, nil
	}

	// The informer is not label-filtered, because ensure has to be able to see
	// an APIService someone else owns in order to leave it alone.
	var out []*unstructured.Unstructured
	for _, item := range m.indexer.List() {
		existing, ok := item.(*unstructured.Unstructured)
		if !ok || existing.GetLabels()[managedByLabel] != managedByValue {
			continue
		}
		out = append(out, existing)
	}
	return out, nil
}

// reconcile makes the set of managed APIServices match the group versions this
// server is currently serving.
func (m *apiServiceManager) reconcile(
	ctx context.Context,
	resources []apidynamic.Resource,
	owners map[schema.GroupVersion][]metav1.OwnerReference,
) (map[schema.GroupVersion]error, error) {
	if !m.options.Enabled {
		return nil, nil
	}

	wanted := map[string]schema.GroupVersion{}
	for _, res := range resources {
		gv := res.GroupVersion()
		wanted[apiServiceName(gv)] = gv
	}

	// One group version failing does not stop the others being registered, and
	// the failures come back per group version so that the projections behind
	// them can say so in their own status. Registration used to abort on the
	// first error and report it only to the log, which left every projection
	// claiming Ready while requests for them went nowhere.
	unregistered := map[schema.GroupVersion]error{}
	for name, gv := range wanted {
		if err := m.ensure(ctx, name, gv, owners[gv]); err != nil {
			unregistered[gv] = fmt.Errorf("ensuring APIService %s: %w", name, err)
			continue
		}
		if err := m.routable(ctx, name); err != nil {
			unregistered[gv] = err
		}
	}

	return unregistered, m.prune(ctx, wanted)
}

// errRegistrationPending marks a registration that has not been confirmed yet
// rather than one that has failed.
//
// The distinction matters for what the projection reports. An APIService the
// aggregator has explicitly marked unavailable is a projection that is not
// serving, and Ready has to say so. One it has not dialled yet — because it was
// created a moment ago, or because there is no aggregation layer, which is how
// the unit tests run — is not a failure, and reporting NotReady for it would
// make every newly created projection flap.
var errRegistrationPending = errors.New("registration pending")

// routable reports whether the aggregation layer is actually sending requests
// for this group version here.
//
// The APIService existing is not the same as it working. The aggregator dials
// the Service and sets Available itself, so a stale CA bundle, a Service with
// no endpoints, or a certificate that does not name the Service all leave a
// registration that looks correct and routes nothing. Available=False is the
// only place that shows up.
func (m *apiServiceManager) routable(ctx context.Context, name string) error {
	existing, err := m.lookup(ctx, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("APIService %s does not exist", name)
		}
		return fmt.Errorf("reading APIService %s: %w", name, err)
	}

	conditions, found, err := unstructured.NestedSlice(existing.Object, "status", "conditions")
	if err != nil || !found {
		return fmt.Errorf("%w: nothing has reported on APIService %s yet", errRegistrationPending, name)
	}

	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		conditionType, _, _ := unstructured.NestedString(condition, "type")
		if conditionType != "Available" {
			continue
		}
		status, _, _ := unstructured.NestedString(condition, "status")
		if status == "True" {
			return nil
		}
		reason, _, _ := unstructured.NestedString(condition, "reason")
		message, _, _ := unstructured.NestedString(condition, "message")
		if message == "" {
			message = "no detail given"
		}
		return fmt.Errorf("the aggregation layer reports APIService %s unavailable (%s): %s", name, reason, message)
	}

	return fmt.Errorf("%w: APIService %s has no Available condition", errRegistrationPending, name)
}

// ensure creates or corrects one APIService.
func (m *apiServiceManager) ensure(ctx context.Context, name string, gv schema.GroupVersion, owners []metav1.OwnerReference) error {
	client := m.client.Resource(APIServiceGVR)

	existing, err := m.lookup(ctx, name)
	switch {
	case apierrors.IsNotFound(err):
		if _, err := client.Create(ctx, m.desired(name, gv, nil, owners), metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return nil
			}
			return err
		}
		klog.InfoS("registered the projected API group with the aggregation layer",
			"apiService", name, "groupVersion", gv.String())
		return nil
	case err != nil:
		return err
	}

	// An APIService someone else manages is never adopted: taking it over could
	// redirect an unrelated API to this server.
	if existing.GetLabels()[managedByLabel] != managedByValue {
		klog.V(2).InfoS("leaving an APIService alone because it is not managed by kube-crisp",
			"apiService", name)
		return nil
	}

	desired := m.desired(name, gv, existing, owners)
	if equalAPIServiceSpec(existing, desired) && equalOwnerReferences(existing, desired) {
		return nil
	}

	if _, err := client.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsConflict(err) {
			// The cache was behind the object. The next sync sees the newer one
			// and either agrees with it or corrects it then.
			klog.V(2).InfoS("APIService changed under us; correcting it on the next sync", "apiService", name)
			return nil
		}
		return err
	}
	klog.InfoS("corrected the APIService registration", "apiService", name)
	return nil
}

// prune removes managed APIServices for group versions that are no longer
// served, so a deleted projection does not leave a dangling registration.
func (m *apiServiceManager) prune(ctx context.Context, wanted map[string]schema.GroupVersion) error {
	client := m.client.Resource(APIServiceGVR)

	managed, err := m.managed(ctx)
	if err != nil {
		return fmt.Errorf("listing managed APIServices: %w", err)
	}

	for _, item := range managed {
		name := item.GetName()
		if _, still := wanted[name]; still {
			continue
		}
		if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting APIService %s: %w", name, err)
		}
		klog.InfoS("removed the registration of a group that is no longer served", "apiService", name)
	}
	return nil
}

// desired builds the APIService this server wants for a group version,
// preserving the resource version of an existing object so it can be updated.
func (m *apiServiceManager) desired(name string, gv schema.GroupVersion, existing *unstructured.Unstructured, owners []metav1.OwnerReference) *unstructured.Unstructured {
	spec := map[string]any{
		"group":                gv.Group,
		"version":              gv.Version,
		"groupPriorityMinimum": int64(m.options.GroupPriorityMinimum),
		"versionPriority":      int64(m.options.VersionPriority),
		"service": map[string]any{
			"name":      m.options.ServiceName,
			"namespace": m.options.ServiceNamespace,
			"port":      int64(m.options.Port),
		},
	}

	if len(m.options.CABundle) > 0 {
		// Unstructured JSON encodes []byte fields as base64 strings.
		spec["caBundle"] = base64.StdEncoding.EncodeToString(m.options.CABundle)
	} else {
		// Matches the self-signed certificate the server generates when no
		// serving certificate is supplied.
		spec["insecureSkipTLSVerify"] = true
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name":   name,
			"labels": map[string]any{managedByLabel: managedByValue},
		},
		"spec": spec,
	}}

	if len(owners) > 0 {
		obj.SetOwnerReferences(owners)
	}

	if existing != nil {
		obj.SetResourceVersion(existing.GetResourceVersion())
	}
	return obj
}

// equalOwnerReferences reports whether the live registration is already owned
// by exactly the projections it should be.
func equalOwnerReferences(existing, desired *unstructured.Unstructured) bool {
	return apiequality.Semantic.DeepEqual(existing.GetOwnerReferences(), desired.GetOwnerReferences())
}

// equalAPIServiceSpec reports whether the live object already says what this
// server wants it to say.
//
// Semantic equality rather than a rendered comparison: both sides are decoded
// JSON, so the values are the same handful of types and comparing them is what
// the question actually is.
func equalAPIServiceSpec(existing, desired *unstructured.Unstructured) bool {
	current, _, _ := unstructured.NestedMap(existing.Object, "spec")
	wanted, _, _ := unstructured.NestedMap(desired.Object, "spec")
	return apiequality.Semantic.DeepEqual(current, wanted)
}

func apiServiceName(gv schema.GroupVersion) string {
	return gv.Version + "." + gv.Group
}
