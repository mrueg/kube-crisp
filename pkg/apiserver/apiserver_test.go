package apiserver

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"

	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/client-go/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// offlineConfig builds a server configuration that needs no cluster and no
// listener, which is enough to exercise the wiring that installs projections.
func offlineConfig(t *testing.T, projections ...crispv1alpha1.CustomResourceProjection) *Config {
	t.Helper()

	generic := genericapiserver.NewRecommendedConfig(Codecs)
	generic.EffectiveVersion = compatibility.DefaultBuildEffectiveVersion()
	generic.ExternalAddress = "kube-crisp.test:443"
	// The generic server insists on one, and nothing in these tests calls it.
	generic.LoopbackClientConfig = &rest.Config{Host: "https://kube-crisp.test:443"}

	pools := crispsql.NewPoolCache()
	t.Cleanup(pools.Close)

	// A database with the table the projections below select from. Compiling a
	// projection asks the database whether it could run each statement, so a
	// fixture pointing at an empty file would be rejected — correctly, since a
	// projection over a table that does not exist cannot serve anything.
	path := filepath.Join(t.TempDir(), "test.db")
	seedOrders(t, path)

	return &Config{
		GenericConfig: generic,
		ExtraConfig: ExtraConfig{
			StaticProjections: projections,
			DSNResolver:       fileResolver{path: path},
			Pools:             pools,
		},
	}
}

// seedOrders creates the table testProjection reads.
func seedOrders(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the test database: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE orders (id TEXT PRIMARY KEY, tenant TEXT)"); err != nil {
		t.Fatalf("creating the orders table: %v", err)
	}
}

// fileResolver points every data source at one SQLite file.
type fileResolver struct{ path string }

func (r fileResolver) Resolve(context.Context, crispv1alpha1.DataSource) (string, error) {
	return r.path, nil
}

func testProjection() crispv1alpha1.CustomResourceProjection {
	return crispv1alpha1.CustomResourceProjection{
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
			Resource: crispv1alpha1.ProjectedResource{
				Group:   "store.example.com",
				Version: "v1alpha1",
				Kind:    "Order",
				Plural:  "orders",
				Scope:   crispv1alpha1.NamespaceScoped,
				Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
			},
			Queries: crispv1alpha1.Queries{
				List: crispv1alpha1.Query{SQL: "SELECT id, tenant FROM orders WHERE tenant = :namespace"},
			},
			Mapping: crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"},
		},
	}
}

// TestNewInstallsStaticProjections covers the wiring end to end: a projection
// in the configuration becomes a served path, without a cluster behind it.
func TestNewInstallsStaticProjections(t *testing.T) {
	config := offlineConfig(t, testProjection())

	server, err := config.Complete().New()
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	paths := server.router.ServedPaths()
	if len(paths) != 1 {
		t.Fatalf("served paths = %v, want exactly one", paths)
	}
	if got, want := paths[0], "/apis/store.example.com/v1alpha1/orders"; got != want {
		t.Errorf("served path = %q, want %q", got, want)
	}
}

// TestNewRejectsABrokenProjection: without a cluster to watch, a projection
// that cannot compile is fatal rather than merely reported, because there is
// nothing that would ever retry it.
func TestNewRejectsABrokenProjection(t *testing.T) {
	broken := testProjection()
	broken.Name = "broken"
	broken.Spec.Mapping.Namespace = ""

	_, err := offlineConfig(t, broken).Complete().New()
	if err == nil {
		t.Fatal("a projection that cannot be compiled was accepted")
	}
	if !strings.Contains(err.Error(), "broken") {
		t.Errorf("error %q does not name the projection", err)
	}
}

func TestSchemeKnowsTheProjectionAPI(t *testing.T) {
	if !Scheme.Recognizes(crispv1alpha1.SchemeGroupVersion.WithKind("CustomResourceProjection")) {
		t.Error("the scheme does not know CustomResourceProjection")
	}
	// List options are decoded through the internal meta version on every
	// request, so a scheme without it fails at request time rather than here.
	internalListOptions := metainternalversion.SchemeGroupVersion.WithKind("ListOptions")
	if !Scheme.Recognizes(internalListOptions) {
		t.Error("the scheme does not know internal ListOptions")
	}
}

var _ = projection.DSNResolver(fileResolver{})

// TestDegradedProjectionDoesNotFailLiveness is the one that matters most in
// this file: a projection that cannot be served must not take the server down.
//
// AddHealthChecks registers into livez as well as healthz, so a check written
// for observability would have the kubelet restart the process — and every
// healthy projection with it — because one projection has a typo in its query.
func TestDegradedProjectionDoesNotFailLiveness(t *testing.T) {
	config := offlineConfig(t, testProjection())
	config.ExtraConfig.CrispClient = crispfake.NewSimpleClientset()

	server, err := config.Complete().New()
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	for _, path := range []string{"/livez", "/readyz"} {
		if checkNamed(t, server, path, "projections-degraded") {
			t.Errorf("%s includes the degradation check; one broken projection would take the server out", path)
		}
	}
	if !checkNamed(t, server, "/readyz", "projections-synced") {
		t.Error("readyz does not wait for projections to be installed")
	}
}

// TestRequireAllProjectionsGatesReadinessOnly covers the opt-in: an operator can
// ask for a degraded server to stop taking traffic, and that is a readiness
// gate — the server leaves the endpoints, and nothing restarts it.
func TestRequireAllProjectionsGatesReadinessOnly(t *testing.T) {
	config := offlineConfig(t, testProjection())
	config.ExtraConfig.CrispClient = crispfake.NewSimpleClientset()
	config.ExtraConfig.RequireAllProjections = true

	server, err := config.Complete().New()
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	if !checkNamed(t, server, "/readyz", "projections-degraded") {
		t.Error("--require-all-projections did not gate readiness")
	}
	if checkNamed(t, server, "/livez", "projections-degraded") {
		t.Error("--require-all-projections reached liveness; a degraded server would be restarted rather than drained")
	}
}

// checkNamed reports whether an endpoint lists a check, which it does in the
// body of a verbose request.
//
// The endpoints are installed by PrepareRun, not by New, so the test has to go
// that far before asking. It stops there: nothing is started.
func checkNamed(t *testing.T, server *CrispServer, path, name string) bool {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, path+"?verbose&exclude=etcd", nil)
	recorder := httptest.NewRecorder()
	server.GenericAPIServer.PrepareRun().Handler.ServeHTTP(recorder, request)

	body := recorder.Body.String()
	if !strings.Contains(body, "check passed") && !strings.Contains(body, "[+]") {
		t.Fatalf("%s listed no checks at all, so this test proves nothing:\n%s", path, body)
	}
	return strings.Contains(body, name)
}

// TestSecretCacheAnswersFromTheInformer covers the credential path that turns
// resolving a data source from a request per projection per sync into nothing.
//
// A miss has to be reported as a miss rather than as an empty Secret, because
// the caller falls back to the API server on one — which is also what keeps an
// unlabelled Secret, one the informers deliberately do not select, behaving as
// it always did.
func TestSecretCacheAnswersFromTheInformer(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders-db",
			Namespace: "kube-crisp",
			Labels:    map[string]string{projection.OptInLabel: projection.OptInValue},
		},
		Data: map[string][]byte{"dsn": []byte("postgres://user:pass@db:5432/store")},
	}

	client := k8sfake.NewSimpleClientset(secret)
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace("kube-crisp"))
	secrets := factory.Core().V1().Secrets()
	// Registered before the factory starts: the shared informer is created on
	// first use, so starting a factory nobody has asked anything of starts
	// nothing.
	lister := secrets.Lister().Secrets("kube-crisp")

	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	cache := &secretCache{listers: map[string]corelisters.SecretNamespaceLister{
		"kube-crisp": lister,
	}}

	got, ok := cache.Secret("kube-crisp", "orders-db")
	if !ok {
		t.Fatal("a watched Secret was not answered from the cache")
	}
	if string(got.Data["dsn"]) != "postgres://user:pass@db:5432/store" {
		t.Errorf("the cached Secret holds %q", got.Data["dsn"])
	}

	if _, ok := cache.Secret("kube-crisp", "nonesuch"); ok {
		t.Error("a Secret that is not in the cache was reported as found")
	}
	if _, ok := cache.Secret("elsewhere", "orders-db"); ok {
		t.Error("a namespace with no informer was reported as cached")
	}
}
