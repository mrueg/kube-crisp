package dynamic

import (
	"net/http"

	restful "github.com/emicklei/go-restful/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// narrowPatchTypes takes strategic merge patch back off every PATCH route the
// generic installer has just registered, and reports the media types that are
// left.
//
// The installer advertises the four patch types a built-in resource accepts —
// JSON Patch, merge patch, strategic merge patch and apply — because that is
// the right answer for a resource whose objects decode into a generated Go
// struct. A projected object decodes into an *unstructured.Unstructured, and
// strategic merge is the one of the four that cannot survive that: it decides
// how to merge each field by reading the `patchStrategy` and `patchMergeKey`
// struct tags off the destination type, and an Unstructured is a map with no
// fields and no tags. apimachinery fails the merge outright rather than guess
// (strategicpatch.StrategicMergeMapPatch has an explicit "we can't easily do a
// strategic merge for custom resources" branch), so the request came back 400
// "strategic merge patch format is not supported" — after the server had
// already told the client, in the route it published, that this was a media
// type it consumed.
//
// That mattered in practice because strategic merge is what `kubectl patch`
// sends when no `--type` is given, so the plainest possible patch command was
// also the one that failed. apiextensions-apiserver has exactly the same
// constraint for custom resources and answers it by never offering the type in
// the first place; this brings projected resources in line with that.
//
// Filtering the installer's list rather than replacing it is deliberate: what
// upstream advertises varies with feature gates (CBOR apply arrives that way),
// and only one entry of it is ever wrong here. Removing that one entry keeps
// kube-crisp in step with whatever else upstream decides to serve.
func narrowPatchTypes(container *restful.Container) []string {
	var (
		accepted []string
		seen     = map[string]struct{}{}
	)

	for _, ws := range container.RegisteredWebServices() {
		// Routes() hands back the web service's own slice, so writing through
		// the index is what actually changes the installed route. Should that
		// ever become a copy, the routes keep advertising strategic merge and
		// TestTheSupportedPatchTypesAreTheOnesACustomResourceAdvertises fails —
		// which is the point of asserting on the route rather than on a
		// request that happens to be rejected for some other reason.
		routes := ws.Routes()
		for i := range routes {
			if routes[i].Method != http.MethodPatch {
				continue
			}

			kept := make([]string, 0, len(routes[i].Consumes))
			for _, mediaType := range routes[i].Consumes {
				if mediaType == string(types.StrategicMergePatchType) {
					continue
				}
				kept = append(kept, mediaType)
				if _, ok := seen[mediaType]; !ok {
					seen[mediaType] = struct{}{}
					accepted = append(accepted, mediaType)
				}
			}
			routes[i].Consumes = kept
		}
	}

	return accepted
}

// serviceErrorHandler renders the errors go-restful raises before any handler
// runs — 404 for a path it has no route for, 405 for a method, 415 for a
// Content-Type no route consumes — as a metav1.Status.
//
// go-restful's own handler writes them as plain text, which a Kubernetes client
// cannot read: kubectl reports "an error on the server" and nothing else. The
// generic apiserver installs the same kind of shim over the container it builds
// for statically installed groups; this container is built here, so it needs
// its own.
//
// The 415 raised on a patch gets the message a CustomResourceDefinition would
// give, naming the patch types that are accepted, because "unsupported media
// type" on its own leaves the user with no way to find out that `--type=merge`
// is what they wanted.
func serviceErrorHandler(codecs runtime.NegotiatedSerializer, patchTypes []string) restful.ServiceErrorHandleFunction {
	return func(serviceErr restful.ServiceError, req *restful.Request, resp *restful.Response) {
		var err error = apierrors.NewGenericServerResponse(
			serviceErr.Code, "", schema.GroupResource{}, "", serviceErr.Message, 0, false)
		if serviceErr.Code == http.StatusUnsupportedMediaType && req.Request.Method == http.MethodPatch {
			err = negotiation.NewUnsupportedMediaTypeError(patchTypes)
		}
		responsewriters.ErrorNegotiated(err, codecs, schema.GroupVersion{}, resp, req.Request)
	}
}
