package dynamic

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

// TestProjectedSerializerDoesNotOfferProtobuf is what keeps a namespace able to
// be deleted.
//
// Projected objects are unstructured and unstructured cannot be encoded to
// protobuf. Offering it anyway means negotiating a format the server then fails
// to produce — a 406 after the fact rather than a clean refusal up front.
//
// The namespace controller's metadata client accepts protobuf, so it negotiated
// protobuf for every deletecollection it issues while emptying a namespace and
// got "object *unstructured.UnstructuredList does not implement the protobuf
// marshalling interface" every time. It retries forever and the namespace never
// leaves Terminating — every namespace in the cluster, not only ones holding
// projected objects, because the controller sweeps every resource advertising
// deletecollection whether or not that namespace has any.
func TestProjectedSerializerDoesNotOfferProtobuf(t *testing.T) {
	apiScheme, codecs := scheme.New()
	serializer := projectedSerializer{
		NegotiatedSerializer: codecs,
		scheme:               apiScheme,
		convertor:            projectedConvertor{ObjectConvertor: apiScheme, copy: true},
	}

	var offeredProtobuf bool
	var media []string
	for _, info := range serializer.SupportedMediaTypes() {
		media = append(media, info.MediaType)
		if info.MediaType == protobufMediaType {
			offeredProtobuf = true
		}
	}

	if offeredProtobuf {
		t.Errorf("protobuf is offered for projected objects, which cannot be encoded to it: %v", media)
	}

	// Still usable: dropping one format must not drop them all, or nothing can
	// talk to the server at all.
	if len(media) == 0 {
		t.Fatal("no media types are offered at all")
	}
	var offeredJSON bool
	for _, m := range media {
		if m == "application/json" {
			offeredJSON = true
		}
	}
	if !offeredJSON {
		t.Errorf("JSON is not offered: %v", media)
	}

	// And the factory underneath does offer protobuf, so this is a narrowing
	// rather than a property the codecs happened to have.
	var factoryOffersProtobuf bool
	for _, info := range codecs.SupportedMediaTypes() {
		if info.MediaType == protobufMediaType {
			factoryOffersProtobuf = true
		}
	}
	if !factoryOffersProtobuf {
		t.Error("the codec factory does not offer protobuf either, so this test proves nothing")
	}
}

var _ runtime.NegotiatedSerializer = projectedSerializer{}
