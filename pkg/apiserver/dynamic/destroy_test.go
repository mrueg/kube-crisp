package dynamic

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kube-openapi/pkg/handler3"

	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

// countingStorage records how often it was torn down.
type countingStorage struct{ destroyed int }

func (c *countingStorage) New() runtime.Object { return &unstructured.Unstructured{} }
func (c *countingStorage) Destroy()            { c.destroyed++ }

// TestDestroyAllReleasesEveryStorage covers the teardown a rebuild depends on.
//
// A projection that was replaced or deleted keeps its watch cache — and the poll
// feeding it — until its storage is destroyed. Missing one of the subresources
// here would leave a projection querying its table forever with nobody reading
// the result, which nothing else in the system would report.
func TestDestroyAllReleasesEveryStorage(t *testing.T) {
	main, status, scale := &countingStorage{}, &countingStorage{}, &countingStorage{}

	full := testResource()
	full.Storage = main
	full.StatusStorage = status
	full.ScaleStorage = scale

	// Subresources are nil unless a projection enables them, which is the
	// common case: a teardown that assumed otherwise would panic on it.
	plainStorage := &countingStorage{}
	plain := testResource()
	plain.Plural = "invoices"
	plain.Storage = plainStorage

	DestroyAll([]Resource{full, plain})

	for name, storage := range map[string]*countingStorage{
		"resource": main, "status": status, "scale": scale, "subresource-less": plainStorage,
	} {
		if storage.destroyed != 1 {
			t.Errorf("%s storage was destroyed %d times, want exactly 1", name, storage.destroyed)
		}
	}
}

// TestDestroyAllToleratesAnEmptySet: a rebuild that retires nothing is the
// ordinary case, and it must not be a special one.
func TestDestroyAllToleratesAnEmptySet(t *testing.T) {
	DestroyAll(nil)
	DestroyAll([]Resource{})
}

// TestPublishOpenAPIRepublishesOnceTheEndpointExists covers why the method is
// separate from Rebuild.
//
// The API surface is installed before the generic apiserver creates the endpoint
// that serves schemas, so the first publish has nowhere to go. Without the
// second one, from the post-start hook, `kubectl explain` would describe nothing
// until some later projection change happened to rebuild.
func TestPublishOpenAPIRepublishesOnceTheEndpointExists(t *testing.T) {
	var service *handler3.OpenAPIService

	router := NewRouter(Options{
		NewScheme:        crispscheme.New,
		OpenAPIV3Service: func() *handler3.OpenAPIService { return service },
	})

	res := testResource()
	res.Schema = &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {Type: "object"},
		},
	}

	// Installed while the endpoint does not exist yet, exactly as at startup.
	if err := router.Rebuild([]Resource{res}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}
	if got := router.ServedPaths(); len(got) != 1 {
		t.Fatalf("served paths = %v, want one", got)
	}

	// And now it does, as it does once the server is prepared.
	service = handler3.NewOpenAPIService()
	router.PublishOpenAPI()

	recorder := httptest.NewRecorder()
	service.HandleDiscovery(recorder, httptest.NewRequest(http.MethodGet, "/openapi/v3", nil))

	if want := "apis/store.example.com/v1alpha1"; !strings.Contains(recorder.Body.String(), want) {
		t.Errorf("the schema endpoint does not list %s: %s", want, recorder.Body.String())
	}
}
