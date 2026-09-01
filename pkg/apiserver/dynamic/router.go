// Package dynamic serves projected API groups from a set of projections that
// can change while the server is running.
//
// The generic apiserver installs API groups once, at startup. Projections come
// and go, so instead of installing groups directly this package keeps a
// go-restful container built from the current set of projections and swaps it
// atomically whenever that set changes. Requests are routed through whatever
// container is current when they arrive, which is the same approach
// apiextensions-apiserver takes for CustomResourceDefinitions.
package dynamic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	restful "github.com/emicklei/go-restful/v3"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapi "k8s.io/apiserver/pkg/endpoints"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/spec3"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

// Resource is one projected resource ready to be served.
type Resource struct {
	// Group, Version, and Plural identify the API path the resource is served at.
	Group   string
	Version string
	Plural  string

	// Kind is the projected kind, used for discovery.
	Kind string

	// Singular and ListKind complete the resource's naming for OpenAPI.
	Singular string
	ListKind string

	// Schema is the projected schema, published as OpenAPI. It is nil when the
	// projection borrows its schema from a CRD.
	Schema *apiextensionsv1.JSONSchemaProps

	// PrinterColumns are advertised in the published schema.
	PrinterColumns []apiextensionsv1.CustomResourceColumnDefinition

	// Namespaced reports whether the resource lives in namespaces.
	Namespaced bool

	// ShortNames and Categories are advertised through discovery.
	ShortNames []string
	Categories []string

	// SelectableFields are the field selectors this resource accepts. They are
	// registered with the scheme, which is what validates a selector before the
	// request ever reaches storage.
	SelectableFields []crispv1alpha1.SelectableField

	// Storage answers requests for the resource.
	Storage rest.Storage

	// StatusStorage answers requests for <resource>/status, and is nil unless
	// the projection enables the subresource.
	StatusStorage rest.Storage

	// ScaleStorage answers requests for <resource>/scale, and is nil unless the
	// projection enables the subresource. ScaleSubresource describes it for the
	// published schema.
	ScaleStorage     rest.Storage
	ScaleSubresource *apiextensionsv1.CustomResourceSubresourceScale

	// ProjectionName is the CustomResourceProjection this came from, used for
	// logging and status reporting.
	ProjectionName string

	// PoolKey identifies the connection pool this resource reads through, so
	// pools nobody references any more can be released. ReadPoolKey is the
	// replica's, and is empty unless the projection names one.
	PoolKey     string
	ReadPoolKey string

	// DataSourceReady records whether the database answered at compile time,
	// and DataSourceError why it did not. The resource is served either way.
	DataSourceReady bool
	DataSourceError error
}

// GroupVersion returns the resource's group version.
func (r Resource) GroupVersion() schema.GroupVersion {
	return schema.GroupVersion{Group: r.Group, Version: r.Version}
}

// Path returns the API path prefix the resource is served at.
func (r Resource) Path() string {
	return fmt.Sprintf("/apis/%s/%s/%s", r.Group, r.Version, r.Plural)
}

// Destroy releases what this resource's storage owns — its watch cache, and
// with it the poll that feeds it. Connection pools are not touched: they belong
// to the pool cache and are shared with every other projection reaching the
// same database.
//
// It has to be called on resources a rebuild drops, or a projection that was
// replaced or deleted keeps polling its table forever with nobody reading the
// result.
func (r Resource) Destroy() {
	for _, storage := range []rest.Storage{r.Storage, r.StatusStorage, r.ScaleStorage} {
		if storage != nil {
			storage.Destroy()
		}
	}
}

// DestroyAll releases every resource in the slice.
func DestroyAll(resources []Resource) {
	for _, res := range resources {
		res.Destroy()
	}
}

// Options configures a Router. The values mirror what the generic apiserver
// passes when it installs an API group statically.
type Options struct {
	// NewScheme returns a fresh scheme and codec factory holding the meta
	// types every apiserver needs, ready for projected kinds to be added.
	//
	// It is a constructor rather than a value because a projected kind has to
	// be taught to a scheme before it can be served, and runtime.Scheme has no
	// locking: mutating one that is already answering requests races every
	// read the serializer makes. Each rebuild therefore gets a scheme of its
	// own, published with the handler that uses it.
	NewScheme func() (*runtime.Scheme, serializer.CodecFactory)

	// Authorizer is the narrower of the two interfaces upstream offers, because
	// it is the narrower one this needs: the router hands it straight to
	// APIGroupVersion, whose own field is an UnconditionalAuthorizer. Nothing
	// here evaluates conditional decisions, so asking for an Authorizer would
	// be asking callers for methods that are never called — and since
	// GenericAPIServer.Authorizer is itself an UnconditionalAuthorizer, asking
	// for the wider one is not something a caller can satisfy anyway.
	Authorizer                 authorizer.UnconditionalAuthorizer
	Admit                      admission.Interface
	EquivalentResourceRegistry runtime.EquivalentResourceRegistry
	MinRequestTimeout          time.Duration

	// DiscoveryManager receives the aggregated (v2) discovery document on every
	// rebuild. Modern kubectl reads only the aggregated document, so a group
	// missing from it is invisible even when legacy discovery lists it.
	DiscoveryManager discoveryendpoint.ResourceManager

	// OpenAPIV3Service resolves the endpoint that serves projected schemas,
	// which is what lets kubectl explain them.
	//
	// It is a getter rather than a value because the generic apiserver only
	// creates that endpoint in PrepareRun, long after the router is built.
	OpenAPIV3Service func() *handler3.OpenAPIService
}

// Router serves whichever projected resources are currently installed.
type Router struct {
	opts    Options
	current atomic.Pointer[snapshot]

	// openAPIMu guards openAPI, the cache of built schema documents.
	openAPIMu sync.Mutex
	openAPI   map[string]openAPICacheEntry

	// publishedMu guards publishedOpenAPI, which records which group versions
	// currently have a document so the ones that go away can be withdrawn.
	//
	// Rebuild publishes from the controller's goroutine and PublishOpenAPI from
	// the post-start hook, and the two run concurrently while the server is
	// coming up.
	publishedMu      sync.Mutex
	publishedOpenAPI map[string]struct{}
}

// snapshot is one immutable generation of the served API surface.
type snapshot struct {
	handler http.Handler
	paths   []string

	// scheme is what this generation's handlers decode and convert with. It is
	// never modified after the snapshot is published, so requests routed here
	// read a scheme nothing is writing to.
	scheme *runtime.Scheme
	codecs serializer.CodecFactory

	// documents are kept so the schemas can be published again once the
	// OpenAPI endpoint exists.
	documents map[schema.GroupVersion]*spec3.OpenAPI
}

// NewRouter returns a router serving nothing until the first Rebuild.
func NewRouter(opts Options) *Router {
	if opts.EquivalentResourceRegistry == nil {
		opts.EquivalentResourceRegistry = runtime.NewEquivalentResourceRegistry()
	}
	if opts.Admit == nil {
		opts.Admit = admission.NewChainHandler()
	}

	r := &Router{
		opts:             opts,
		openAPI:          map[string]openAPICacheEntry{},
		publishedOpenAPI: map[string]struct{}{},
	}
	r.current.Store(&snapshot{handler: http.NotFoundHandler()})
	return r
}

// ServeHTTP dispatches to the current generation.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.current.Load().handler.ServeHTTP(w, req)
}

// PublishOpenAPI publishes the current schemas again. The server calls this
// once it is running, because the endpoint that serves them does not exist
// while the API surface is first being installed.
func (r *Router) PublishOpenAPI() {
	r.publishOpenAPI(r.current.Load().documents)
}

// ServedPaths reports the API paths currently installed, sorted.
func (r *Router) ServedPaths() []string {
	return r.current.Load().paths
}

// Rebuild installs exactly the given resources, replacing whatever was served
// before. Resources that fail to install cause the whole rebuild to fail, so a
// bad projection never partially replaces a working API surface.
func (r *Router) Rebuild(resources []Resource) error {
	container := restful.NewContainer()
	container.ServeMux = http.NewServeMux()
	container.Router(restful.CurlyRouter{})

	// A scheme of this generation's own, taught about every kind it serves
	// before any of it is reachable. Nothing writes to it after the snapshot is
	// published, which is what keeps the serializer's reads race-free while a
	// later rebuild teaches a different scheme about a different set of kinds.
	newScheme := r.opts.NewScheme
	if newScheme == nil {
		newScheme = crispscheme.New
	}
	apiScheme, codecs := newScheme()
	registerKinds(apiScheme, resources)

	// group -> version -> plural -> storage
	byGroup := map[string]map[string]map[string]rest.Storage{}
	byGroupVersion := map[schema.GroupVersion][]Resource{}

	for _, res := range resources {
		byGroupVersion[res.GroupVersion()] = append(byGroupVersion[res.GroupVersion()], res)
		if byGroup[res.Group] == nil {
			byGroup[res.Group] = map[string]map[string]rest.Storage{}
		}
		if byGroup[res.Group][res.Version] == nil {
			byGroup[res.Group][res.Version] = map[string]rest.Storage{}
		}
		if _, exists := byGroup[res.Group][res.Version][res.Plural]; exists {
			return fmt.Errorf("resource %s.%s/%s is claimed by more than one projection",
				res.Plural, res.Group, res.Version)
		}
		byGroup[res.Group][res.Version][res.Plural] = res.Storage
		if res.StatusStorage != nil {
			byGroup[res.Group][res.Version][res.Plural+"/status"] = res.StatusStorage
		}
		if res.ScaleStorage != nil {
			byGroup[res.Group][res.Version][res.Plural+"/scale"] = res.ScaleStorage
		}
	}

	// The schemas are built before the surface is installed, because field
	// management needs them: they are what tells server-side apply how lists
	// and maps merge.
	documents, converters := r.openAPIFor(byGroupVersion)

	var (
		paths            []string
		aggregatedGroups []apidiscoveryv2.APIGroupDiscovery
	)

	for group, versions := range byGroup {
		var (
			discoveryVersions  []metav1.GroupVersionForDiscovery
			aggregatedVersions []apidiscoveryv2.APIVersionDiscovery
		)

		for version, storages := range versions {
			gv := schema.GroupVersion{Group: group, Version: version}
			apiGroupVersion := r.newAPIGroupVersion(gv, storages, converters[gv], apiScheme, codecs)

			aggregatedResources, _, err := apiGroupVersion.InstallREST(container)
			if err != nil {
				return fmt.Errorf("installing %s: %w", gv.String(), err)
			}

			discoveryVersions = append(discoveryVersions, metav1.GroupVersionForDiscovery{
				GroupVersion: gv.String(),
				Version:      gv.Version,
			})
			aggregatedVersions = append(aggregatedVersions, apidiscoveryv2.APIVersionDiscovery{
				Version:   version,
				Resources: aggregatedResources,
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			})
		}

		sort.Slice(aggregatedVersions, func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(
				aggregatedVersions[i].Version, aggregatedVersions[j].Version) > 0
		})
		aggregatedGroups = append(aggregatedGroups, apidiscoveryv2.APIGroupDiscovery{
			ObjectMeta: metav1.ObjectMeta{Name: group},
			Versions:   aggregatedVersions,
		})

		// Ordered the way Kubernetes ranks versions, so the preferred version
		// is the most stable one rather than whichever sorts first
		// alphabetically: v1 outranks v1beta1, which outranks v1alpha1.
		sort.Slice(discoveryVersions, func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(
				discoveryVersions[i].Version, discoveryVersions[j].Version) > 0
		})

		apiGroup := metav1.APIGroup{
			Name:             group,
			Versions:         discoveryVersions,
			PreferredVersion: discoveryVersions[0],
		}
		container.Add(discovery.NewAPIGroupHandler(codecs, apiGroup).WebService())
	}

	// The routes are published before anything can reach them, so this is the
	// last chance to correct what they claim. See narrowPatchTypes: the generic
	// installer advertises strategic merge patch, which no projected kind can
	// serve, and go-restful is what turns a Content-Type nothing consumes into
	// a 415 rather than letting the request through to fail deeper.
	accepted := narrowPatchTypes(container)
	container.ServiceErrorHandler(serviceErrorHandler(codecs, accepted))

	for _, res := range resources {
		paths = append(paths, res.Path())
	}
	sort.Strings(paths)

	if r.opts.DiscoveryManager != nil {
		sort.Slice(aggregatedGroups, func(i, j int) bool {
			return aggregatedGroups[i].Name < aggregatedGroups[j].Name
		})
		r.opts.DiscoveryManager.SetGroups(aggregatedGroups)
	}

	r.current.Store(&snapshot{
		handler:   container,
		paths:     paths,
		documents: documents,
		scheme:    apiScheme,
		codecs:    codecs,
	})

	// Schemas are published after the surface is built, so a failed rebuild
	// never leaves clients explaining a kind that is not served.
	r.publishOpenAPI(documents)
	klog.V(2).InfoS("rebuilt projected API surface", "resources", len(resources), "groups", len(byGroup))
	return nil
}

// openAPICacheEntry is a built document and the field-management view derived
// from it. They are cached together because they are built from the same input
// and thrown away together.
type openAPICacheEntry struct {
	document  *spec3.OpenAPI
	converter managedfields.TypeConverter
}

// openAPIFor returns a schema document and a type converter per group version,
// building only the ones that have changed.
//
// Building them is the bulk of a rebuild — around six sevenths of it — and a
// rebuild happens whenever any projection changes, any data source Secret
// changes, or the resync comes round. Without a cache, changing one projection
// re-derives the schemas of every other one: at a hundred projections that was
// 120ms and 132MB of garbage to install a surface that was almost entirely the
// same as the one before it.
//
// The key is the input the builder is given, so a document is reused exactly
// when it would have been rebuilt identically. Group versions that are no
// longer served fall out, because the cache is replaced by what this rebuild
// looked up rather than added to.
func (r *Router) openAPIFor(byGroupVersion map[schema.GroupVersion][]Resource) (
	map[schema.GroupVersion]*spec3.OpenAPI,
	map[schema.GroupVersion]managedfields.TypeConverter,
) {
	r.openAPIMu.Lock()
	defer r.openAPIMu.Unlock()

	var (
		documents  = make(map[schema.GroupVersion]*spec3.OpenAPI, len(byGroupVersion))
		converters = make(map[schema.GroupVersion]managedfields.TypeConverter, len(byGroupVersion))
		retained   = make(map[string]openAPICacheEntry, len(byGroupVersion))
		reused     int
	)

	for gv, resources := range byGroupVersion {
		key, keyed := openAPIKey(gv, resources)
		if keyed {
			if cached, hit := r.openAPI[key]; hit {
				documents[gv] = cached.document
				converters[gv] = cached.converter
				retained[key] = cached
				reused++
				continue
			}
		}

		document, err := buildOpenAPIV3(gv, resources)
		if err != nil {
			klog.ErrorS(err, "could not build the OpenAPI document", "groupVersion", gv.String())
			converters[gv] = managedfields.NewDeducedTypeConverter()
			continue
		}

		entry := openAPICacheEntry{document: document, converter: typeConverterFor(document)}
		documents[gv] = entry.document
		converters[gv] = entry.converter
		if keyed {
			retained[key] = entry
		}
	}

	r.openAPI = retained
	if reused > 0 {
		klog.V(4).InfoS("reused unchanged OpenAPI documents",
			"reused", reused, "groupVersions", len(byGroupVersion))
	}
	return documents, converters
}

// openAPIKey identifies what a group version's document would be built from.
//
// It is the synthetic CRDs themselves, in the order the builder reads them —
// which is the whole input, so two rebuilds with the same key produce the same
// document by construction rather than by a list of fields somebody has to
// remember to extend. A resource that cannot be rendered is simply not cached.
func openAPIKey(gv schema.GroupVersion, resources []Resource) (string, bool) {
	sorted := append([]Resource(nil), resources...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Plural < sorted[j].Plural })

	// A hash never fails a write, so the errors below are the ones that cannot
	// happen rather than the ones being ignored.
	digest := sha256.New()
	digest.Write([]byte(gv.String()))
	digest.Write([]byte{0})

	for _, res := range sorted {
		encoded, err := json.Marshal(syntheticCRD(res))
		if err != nil {
			return "", false
		}
		digest.Write(encoded)
		digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil)), true
}

// unstructuredCreator stamps the kind onto the empty objects the endpoint
// machinery asks for.
//
// A scheme returns a zero value, which for an unstructured type is an object
// with no apiVersion or kind at all. Field management needs one: on a create it
// compares the submitted object against an empty "live" object, and an object
// without a kind cannot be converted, so managed fields are silently dropped.
type unstructuredCreator struct {
	runtime.ObjectCreater
}

// New returns an empty object of the requested kind, with that kind set.
func (c unstructuredCreator) New(gvk schema.GroupVersionKind) (runtime.Object, error) {
	obj, err := c.ObjectCreater.New(gvk)
	if err != nil {
		return nil, err
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		u.SetGroupVersionKind(gvk)
	}
	return obj, nil
}

// newAPIGroupVersion mirrors the configuration the generic apiserver builds for
// a statically installed group.
func (r *Router) newAPIGroupVersion(
	gv schema.GroupVersion,
	storages map[string]rest.Storage,
	typeConverter managedfields.TypeConverter,
	apiScheme *runtime.Scheme,
	codecs serializer.CodecFactory,
) *genericapi.APIGroupVersion {
	if typeConverter == nil {
		typeConverter = managedfields.NewDeducedTypeConverter()
	}

	metaGroupVersion := metav1.SchemeGroupVersion
	optionsExternalVersion := schema.GroupVersion{Version: "v1"}

	return &genericapi.APIGroupVersion{
		Storage:      storages,
		Root:         "/apis",
		GroupVersion: gv,

		OptionsExternalVersion: &optionsExternalVersion,
		MetaGroupVersion:       &metaGroupVersion,

		// Not the codec factory directly: its encoders convert through the
		// scheme, and it is the encoder that stamps a response's kind.
		Serializer: projectedSerializer{
			NegotiatedSerializer: codecs,
			scheme:               apiScheme,
			convertor:            projectedConvertor{ObjectConvertor: apiScheme, copy: true},
		},
		ParameterCodec: runtime.NewParameterCodec(apiScheme),

		Typer:   apiScheme,
		Creater: unstructuredCreator{apiScheme},
		// Not the scheme: every projected kind shares one Go type there, so it
		// converts an object into whichever kind was registered first. See
		// projectedConvertor.
		Convertor:             projectedConvertor{ObjectConvertor: apiScheme, copy: true},
		ConvertabilityChecker: apiScheme,
		Defaulter:             apiScheme,
		Namer:                 runtime.Namer(meta.NewAccessor()),
		UnsafeConvertor:       projectedConvertor{ObjectConvertor: runtime.UnsafeObjectConvertor(apiScheme)},

		// Built from the projection's own schema when it has one, so applies
		// merge lists and maps the way the schema says they should.
		TypeConverter: typeConverter,

		EquivalentResourceRegistry: r.opts.EquivalentResourceRegistry,

		Admit:             r.opts.Admit,
		Authorizer:        r.opts.Authorizer,
		MinRequestTimeout: r.opts.MinRequestTimeout,
	}
}
