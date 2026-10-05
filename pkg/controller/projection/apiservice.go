package projection

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

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

// managedByLabel marks the APIServices a kube-crisp server writes. Anything
// without it was created by someone else and is left alone. Having it is not
// enough to be this server's, though; see owns.
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

	// AllowedGroupSuffixes bounds which API groups a projection may claim. A
	// group is allowed when it equals one of these or ends in "." followed by
	// one. Empty allows any group, which is the default and what every existing
	// deployment had.
	//
	// Registering a group is not a local act. An APIService is cluster-scoped
	// and routes a whole group/version to this server, so a projection naming
	// "cert-manager.io/v1" -- a group whose operator is not installed yet --
	// takes it, and takes it for good: the kube-apiserver's own controllers
	// only manage APIServices carrying the automanaged label, so nothing hands
	// it back when the real operator arrives.
	//
	// Whoever may write a projection is not necessarily whoever decides which
	// API groups a cluster serves. This is how an operator keeps those apart,
	// by naming the suffixes their projections live under.
	AllowedGroupSuffixes []string
}

// groupAllowed reports whether a projection may claim this API group.
func (o APIServiceOptions) groupAllowed(group string) bool {
	if len(o.AllowedGroupSuffixes) == 0 {
		return true
	}
	for _, suffix := range o.AllowedGroupSuffixes {
		if suffix == "" {
			continue
		}
		if group == suffix || strings.HasSuffix(group, "."+suffix) {
			return true
		}
	}
	return false
}

// groupRefusal says why a projection may not claim this API group, or nil when
// it may.
func (o APIServiceOptions) groupRefusal(group string) error {
	if o.groupAllowed(group) {
		return nil
	}
	return fmt.Errorf(
		"%w: %q is not under any of the suffixes --projection-group-suffixes allows (%s). "+
			"An APIService routes the whole group to this server and is not given back, so the "+
			"group a projection claims is an operator's decision rather than the projection's",
		errGroupNotAllowed, group, strings.Join(o.AllowedGroupSuffixes, ", "))
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
			if m.owns(&list.Items[i]) {
				out = append(out, &list.Items[i])
			}
		}
		return out, nil
	}

	// The informer is not label-filtered, because ensure has to be able to see
	// an APIService someone else owns in order to leave it alone.
	var out []*unstructured.Unstructured
	for _, item := range m.indexer.List() {
		existing, ok := item.(*unstructured.Unstructured)
		if !ok || !m.owns(existing) {
			continue
		}
		out = append(out, existing)
	}
	return out, nil
}

// owns reports whether an APIService is this server's to correct and to prune:
// it carries the kube-crisp label and routes to this server's Service.
//
// The label alone said which program wrote it, not which installation. Two
// kube-crisp installations in one cluster -- which the webhook and the lease
// names are there to allow -- each read the other's registrations as their
// own: each pruned every group the other served, and for a group version both
// wanted, each pointed it back at itself on every sync. The Service is what
// tells installations apart, and every registration an installation has
// written names its own, so nothing it wrote before stops being its own.
//
// What does is a registration whose Service has changed under it: one an
// installation wrote before its --apiservice-service-name or namespace changed,
// or one somebody edited to point elsewhere. That is reported as the group
// being served elsewhere, as any other server's registration is, rather than
// taken back -- telling it apart from another installation's is exactly what
// the label could not do.
func (m *apiServiceManager) owns(existing *unstructured.Unstructured) bool {
	return existing.GetLabels()[managedByLabel] == managedByValue && m.routesHere(existing)
}

// reconcile makes the set of managed APIServices match the group versions this
// server is currently serving.
//
// declared names the group versions live projections declare, served here or
// not. Nothing is registered for them that is not also served -- an APIService
// for a group this server does not serve would be marked unavailable -- but
// nothing registered for them is pruned either.
func (m *apiServiceManager) reconcile(
	ctx context.Context,
	resources []apidynamic.Resource,
	owners map[schema.GroupVersion][]metav1.OwnerReference,
	declared map[schema.GroupVersion]struct{},
) (map[schema.GroupVersion]error, error) {
	if !m.options.Enabled {
		return nil, nil
	}

	// One group version failing does not stop the others being registered, and
	// the failures come back per group version so that the projections behind
	// them can say so in their own status. Registration used to abort on the
	// first error and report it only to the log, which left every projection
	// claiming Ready while requests for them went nowhere.
	unregistered := map[schema.GroupVersion]error{}

	// A group outside the permitted suffixes is neither wanted nor kept, so a
	// registration this server wrote for it before the suffixes were set is
	// pruned. Wanted, it was kept: ensure refused to touch it and prune
	// skipped it, and the group went on routing here for good -- the claim
	// the suffixes exist to stop. The controller does not install such a
	// projection at all; this is where the registration follows.
	wanted := map[string]schema.GroupVersion{}
	for _, res := range resources {
		gv := res.GroupVersion()
		if err := m.options.groupRefusal(gv.Group); err != nil {
			unregistered[gv] = err
			continue
		}
		wanted[apiServiceName(gv)] = gv
	}

	for name, gv := range wanted {
		if err := m.ensure(ctx, name, gv, owners[gv]); err != nil {
			// A conflict already names the APIService; wrapping it would name
			// it twice.
			if !errors.Is(err, errGroupServedElsewhere) {
				err = fmt.Errorf("ensuring APIService %s: %w", name, err)
			}
			unregistered[gv] = err
			continue
		}
		if err := m.routable(ctx, name); err != nil {
			unregistered[gv] = err
		}
	}

	keep := make(map[string]struct{}, len(wanted)+len(declared))
	for name := range wanted {
		keep[name] = struct{}{}
	}
	for gv := range declared {
		if m.options.groupAllowed(gv.Group) {
			keep[apiServiceName(gv)] = struct{}{}
		}
	}
	return unregistered, m.prune(ctx, keep)
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

// errGroupServedElsewhere marks a group version that cannot be registered
// because an APIService for it already exists and sends requests somewhere
// other than here.
//
// It is not pending and it is not the aggregator's verdict: that APIService is
// usually Available, because whatever it routes to is answering. The common
// case is a CustomResourceDefinition in the same group, whose APIService the
// kube-apiserver manages itself and which is available for as long as the
// kube-apiserver is. Reading that condition as this projection's would report
// a healthy registration for an API no request ever reaches.
var errGroupServedElsewhere = errors.New("the group version is already served elsewhere")

// errGroupNotAllowed marks a group version this server has been told not to
// register, because the group is outside every suffix the operator allows.
//
// Like a group served elsewhere, and unlike a registration waiting on the
// aggregator, it is settled: nothing that happens in the cluster changes the
// answer, only the projection or the flags do.
var errGroupNotAllowed = errors.New("the API group is not one this server may register")

// routesHere reports whether an existing APIService sends the group version to
// this server.
//
// One this controller wrote does by construction. One somebody else wrote
// against the same Service does too: that is what examples/apiservice.yaml
// produces, and a registration made by hand before management was turned on
// is still a registration. Anything else routes the group version away from
// here, whatever its Available condition says -- including one carrying the
// kube-crisp label, which another installation's registrations carry as well.
func (m *apiServiceManager) routesHere(existing *unstructured.Unstructured) bool {
	name, _, _ := unstructured.NestedString(existing.Object, "spec", "service", "name")
	namespace, _, _ := unstructured.NestedString(existing.Object, "spec", "service", "namespace")
	return name != "" && name == m.options.ServiceName && namespace == m.options.ServiceNamespace
}

// servedElsewhere describes an APIService for a wanted group version that this
// server neither manages nor is routed by, and says what to do about it.
func servedElsewhere(name string, existing *unstructured.Unstructured) error {
	target := "the kube-apiserver itself, which is what a CustomResourceDefinition in the group looks like"
	if service, _, _ := unstructured.NestedString(existing.Object, "spec", "service", "name"); service != "" {
		namespace, _, _ := unstructured.NestedString(existing.Object, "spec", "service", "namespace")
		target = fmt.Sprintf("Service %s/%s", namespace, service)
	}
	owner := "is not managed by kube-crisp"
	switch by := existing.GetLabels()[managedByLabel]; by {
	case "":
	case managedByValue:
		owner = "belongs to another kube-crisp installation"
	default:
		owner = fmt.Sprintf("is managed by %q rather than by kube-crisp", by)
	}
	return fmt.Errorf(
		"%w: APIService %s %s and routes the group version to %s, so no request for the projection "+
			"arrives here. Give the projection another group, or remove that APIService (and the "+
			"CustomResourceDefinition behind it, if that is what serves the group) so that kube-crisp "+
			"can register it",
		errGroupServedElsewhere, name, owner, target)
}

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

	// Available says that whatever the APIService points at is answering. That
	// is only this server's registration if the APIService points here.
	if !m.routesHere(existing) {
		return servedElsewhere(name, existing)
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

	// Before anything is created, because what would be created is the claim.
	if err := m.options.groupRefusal(gv.Group); err != nil {
		return err
	}

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
	// redirect an unrelated API to this server. Left alone is not the same as
	// registered, though. Unless it happens to route here anyway, the group
	// version belongs to whatever it does route to, and the projection has to
	// say so rather than report the other API's health as its own.
	//
	// Another kube-crisp installation's is someone else's too, for all that it
	// carries the same label. Rewriting it took the group version from that installation, which
	// took it straight back on its next sync.
	if !m.owns(existing) {
		if !m.routesHere(existing) {
			return servedElsewhere(name, existing)
		}
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

// prune removes managed APIServices for group versions no projection declares
// any more, so a deleted projection does not leave a dangling registration.
//
// Not "no longer served": a projection that exists and failed to compile is
// not a deleted one, and a replica with nothing of it to keep serving is no
// reason to withdraw it from every replica.
func (m *apiServiceManager) prune(ctx context.Context, keep map[string]struct{}) error {
	client := m.client.Resource(APIServiceGVR)

	managed, err := m.managed(ctx)
	if err != nil {
		return fmt.Errorf("listing managed APIServices: %w", err)
	}

	for _, item := range managed {
		name := item.GetName()
		if _, still := keep[name]; still {
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
