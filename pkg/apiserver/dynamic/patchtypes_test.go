package dynamic

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	restful "github.com/emicklei/go-restful/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/rest"
)

// patchableStorage is fakeStorage that also accepts an update, which is what
// makes the generic installer register a PATCH route for it: the installer
// looks for a rest.Patcher, and rest.Patcher is a Getter plus an Updater.
//
// Update stores nothing — the object is handed straight back — because every
// assertion here is about what happens before storage is reached: whether the
// request was routed at all, and whether the patch could be applied to an
// *unstructured.Unstructured.
type patchableStorage struct {
	fakeStorage
}

// Get returns the object with a UID on it, because the patch endpoint reads the
// UID to tell "patch an object that already exists" from "create the object
// this patch describes". An object without one looks absent, and the request
// comes back 404 before any patch is applied — which would make every
// assertion below pass or fail for the wrong reason.
func (s *patchableStorage) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	obj, err := s.fakeStorage.Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	obj.(*unstructured.Unstructured).SetUID(types.UID(name + "-uid"))
	return obj, nil
}

func (s *patchableStorage) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	_ rest.ValidateObjectFunc,
	_ rest.ValidateObjectUpdateFunc,
	_ bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	old, err := s.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}
	updated, err := objInfo.UpdatedObject(ctx, old)
	if err != nil {
		return nil, false, err
	}
	return updated, false, nil
}

func patchableResource() Resource {
	res := testResource()
	res.Storage = &patchableStorage{fakeStorage: *res.Storage.(*fakeStorage)}
	return res
}

const patchPath = "/apis/store.example.com/v1alpha1/namespaces/acme/orders/order-1001"

func doPatch(handler http.Handler, path string, patchType types.PatchType, body string) ([]byte, int) {
	req := httptest.NewRequest(http.MethodPatch, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", string(patchType))
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Body.Bytes(), rec.Code
}

// TestStrategicMergePatchIsRejectedAsAnUnsupportedMediaType is the regression
// test for a 500 that `kubectl patch <projected-kind> <name> -p '{...}'`
// produced whenever the user left off `--type`.
//
// kubectl's default patch type is strategic merge, and strategic merge works by
// reading `patchStrategy` and `patchMergeKey` struct tags off the Go type the
// object decodes into. Every projected kind decodes into an
// *unstructured.Unstructured, which has no such tags and no `spec` field at
// all, so the merge blew up with `unable to find api field in struct
// Unstructured for the json field "spec"` — a 500 describing kube-crisp's
// internals, for a request the server should never have accepted.
//
// apiextensions-apiserver has the same constraint and answers it the same way:
// a CustomResourceDefinition advertises JSON Patch, merge patch and apply, and
// nothing else. This asserts a projected resource now behaves the same.
func TestStrategicMergePatchIsRejectedAsAnUnsupportedMediaType(t *testing.T) {
	router, handler := newTestRouter(t)
	if err := router.Rebuild([]Resource{patchableResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	body, code := doPatch(handler, patchPath, types.StrategicMergePatchType,
		`{"spec":{"quantity":3}}`)
	if code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415 (%s)", code, body)
	}

	var status metav1.Status
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("decoding the response as a Status: %v (%s)", err, body)
	}
	if status.Kind != "Status" || status.Status != metav1.StatusFailure {
		t.Errorf("response = %+v, want a failed Status", status)
	}

	// The message has to name what the server does accept, or the client is
	// told no without being told what to send instead.
	for _, want := range []string{
		string(types.JSONPatchType),
		string(types.MergePatchType),
		string(types.ApplyYAMLPatchType),
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("message %q does not mention %q", status.Message, want)
		}
	}
	if bytes.Contains(body, []byte(types.StrategicMergePatchType)) {
		t.Errorf("message %q offers strategic merge patch, which cannot work", status.Message)
	}
}

// TestTheSupportedPatchTypesAreTheOnesACustomResourceAdvertises checks the
// routes themselves rather than one request through them, because the route is
// what discovery, generated clients and any schema built off the container
// read. A route that consumes a media type the handler cannot honour is a lie
// told to every client that inspects it, not only to the one that sends it.
func TestTheSupportedPatchTypesAreTheOnesACustomResourceAdvertises(t *testing.T) {
	router, _ := newTestRouter(t)
	if err := router.Rebuild([]Resource{patchableResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	served, ok := router.current.Load().handler.(*restful.Container)
	if !ok {
		t.Fatalf("the router serves a %T, not a go-restful container", router.current.Load().handler)
	}

	want := []string{
		string(types.ApplyYAMLPatchType),
		string(types.JSONPatchType),
		string(types.MergePatchType),
	}
	sort.Strings(want)

	var found int
	for _, ws := range served.RegisteredWebServices() {
		for _, route := range ws.Routes() {
			if route.Method != http.MethodPatch {
				continue
			}
			found++

			got := append([]string(nil), route.Consumes...)
			sort.Strings(got)
			if len(got) != len(want) {
				t.Errorf("%s consumes %v, want %v", route.Path, got, want)
				continue
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("%s consumes %v, want %v", route.Path, got, want)
					break
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("no PATCH route was installed, so nothing was asserted")
	}
}

// TestMergePatchStillReachesStorage is the other half of the narrowing: cutting
// the advertised set down must not cut it down to nothing. A merge patch is
// what every example in the documentation sends, and it has to keep working.
func TestMergePatchStillReachesStorage(t *testing.T) {
	router, handler := newTestRouter(t)
	if err := router.Rebuild([]Resource{patchableResource()}); err != nil {
		t.Fatalf("Rebuild() returned error: %v", err)
	}

	body, code := doPatch(handler, patchPath, types.MergePatchType,
		`{"metadata":{"labels":{"store.example.com/reviewed":"yes"}}}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, body)
	}

	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("decoding the patched object: %v (%s)", err, body)
	}
	objectMeta, _ := obj["metadata"].(map[string]any)
	labels, _ := objectMeta["labels"].(map[string]any)
	if labels["store.example.com/reviewed"] != "yes" {
		t.Errorf("labels = %v, want the patched label", labels)
	}
}
