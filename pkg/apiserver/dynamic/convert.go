package dynamic

import (
	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/versioning"
)

// projectedConvertor answers conversion for the objects the router serves.
//
// Every projected kind is registered in the scheme against one Go type,
// *unstructured.Unstructured, because kube-crisp never generates Go types for
// user schemas. runtime.Scheme converts by going from the object to its Go type
// and back to a kind — and that reverse step returns every projected kind in
// the group version, of which it takes the first.
//
// So in a group version serving more than one kind, every object converts into
// whichever kind happens to be first. A list of orders comes back stamped
// ScalableOrderList, and field management types an Order against ScalableOrder's
// schema and rejects the apply with "field not declared in schema". Neither is
// visible to a client reading only the items, which is why it survived so long.
//
// An unstructured object already carries the only answer worth having, so the
// kind is taken from the object and only the group version is decided by the
// target. That is what apiextensions-apiserver does for custom resources, for
// the same reason. Typed objects — a Scale, a Status, the options types — have
// a Go type of their own and still go to the scheme.
type projectedConvertor struct {
	runtime.ObjectConvertor

	// copy follows the scheme's own split: the safe convertor hands back
	// something the caller may modify, the unsafe one may hand back its input.
	copy bool
}

var _ runtime.ObjectConvertor = projectedConvertor{}

// ConvertToVersion restates an object's group version, keeping its kind.
func (c projectedConvertor) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (runtime.Object, error) {
	if _, ok := in.(runtime.Unstructured); !ok {
		return c.ObjectConvertor.ConvertToVersion(in, target)
	}

	gvk := in.GetObjectKind().GroupVersionKind()
	if len(gvk.Kind) == 0 {
		return nil, runtime.NewMissingKindErr("unstructured object has no kind")
	}
	if len(gvk.Version) == 0 {
		return nil, runtime.NewMissingVersionErr("unstructured object has no version")
	}

	// Only this object's own kind is offered, so whatever comes back keeps it.
	desired, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{gvk})
	if !ok {
		return nil, runtime.NewNotRegisteredErrForTarget(schemeName, reflect.TypeOf(in), target)
	}

	// A list's items carry their own kind — the item's, not the list's — so
	// only their group version would move, and it almost never does: a
	// projection serves each version from the same rows, so the version asked
	// for is the version the objects already carry.
	if list, ok := in.(*unstructured.UnstructuredList); ok {
		return c.convertList(list, gvk, desired), nil
	}

	out := in
	if c.copy {
		out = in.DeepCopyObject()
	}
	out.GetObjectKind().SetGroupVersionKind(desired)
	return out, nil
}

// convertList restates a collection's group version.
//
// When the items are already in the version asked for — which is the ordinary
// case, since every read produces objects of the version it was made for — the
// result is a view: the list's own metadata is copied so the caller can stamp
// it, and the items are shared and treated as immutable. That is the same
// contract the read cache and the kube-apiserver's watch cache keep, and for
// the same reason. Copying ten thousand objects to restate a kind costs about
// as much as the query that produced them.
//
// A version that really does differ is the one case that has to touch each
// item, and there the items are copied before they are stamped.
func (c projectedConvertor) convertList(
	list *unstructured.UnstructuredList,
	gvk, desired schema.GroupVersionKind,
) runtime.Object {
	out := &unstructured.UnstructuredList{Items: list.Items}
	if c.copy {
		out.Object = runtime.DeepCopyJSON(list.Object)
	} else {
		out.Object = list.Object
	}

	if desired.GroupVersion() != gvk.GroupVersion() {
		items := make([]unstructured.Unstructured, len(list.Items))
		for i := range list.Items {
			items[i] = *list.Items[i].DeepCopy()
			if kind := items[i].GroupVersionKind().Kind; kind != "" {
				items[i].SetGroupVersionKind(desired.GroupVersion().WithKind(kind))
			}
		}
		out.Items = items
	} else if c.copy {
		// The slice is copied even when the items are not, so appending to the
		// result cannot reach into the caller's backing array.
		out.Items = append(make([]unstructured.Unstructured, 0, len(list.Items)), list.Items...)
	}

	out.SetGroupVersionKind(desired)
	return out
}

// schemeName labels the errors this convertor reports, matching what the scheme
// it stands in for would have said.
const schemeName = "kube-crisp"

// projectedSerializer supplies the codecs the router encodes and decodes with.
//
// Substituting the convertor on the APIGroupVersion is not enough on its own:
// the codec factory builds encoders that convert through the scheme, and it is
// the encoder that stamps a response's kind. A list therefore came back named
// after whichever kind the scheme happened to return first, however the
// endpoint itself was configured.
//
// The media types are narrowed too. Projected objects are unstructured, and
// unstructured cannot be encoded to protobuf — so offering it means the server
// advertises a format it will then fail to produce, with a 406 at encode time
// rather than a clean refusal during negotiation.
//
// That is not a cosmetic difference. The namespace controller's metadata client
// lists protobuf among the formats it accepts, so it negotiated protobuf for
// every deletecollection it issues while emptying a namespace, and got back
// "object *unstructured.UnstructuredList does not implement the protobuf
// marshalling interface". The controller retries forever and the namespace
// never leaves Terminating — for every namespace in the cluster, not only ones
// holding projected objects, because the controller sweeps every resource that
// advertises deletecollection regardless of whether that namespace has any.
//
// apiextensions does the same thing for the same reason, with the comment
// "CRDs explicitly do not support protobuf".
type projectedSerializer struct {
	runtime.NegotiatedSerializer

	scheme    *runtime.Scheme
	convertor runtime.ObjectConvertor
}

var _ runtime.NegotiatedSerializer = projectedSerializer{}

// protobufMediaType is the one a projected object cannot be encoded to.
const protobufMediaType = "application/vnd.kubernetes.protobuf"

// SupportedMediaTypes offers only the formats a projected object can actually
// be encoded to, so a client that cannot use them is told during negotiation
// instead of being handed a 406 after the work is done.
func (s projectedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	all := s.NegotiatedSerializer.SupportedMediaTypes()
	supported := make([]runtime.SerializerInfo, 0, len(all))
	for _, info := range all {
		if info.MediaType == protobufMediaType {
			continue
		}
		supported = append(supported, info)
	}
	return supported
}

// EncoderForVersion returns an encoder that restates a projected object's group
// version without losing its kind.
func (s projectedSerializer) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return versioning.NewCodec(
		encoder, nil, s.convertor,
		s.scheme, s.scheme, s.scheme,
		gv, nil, schemeName,
	)
}

// DecoderToVersion returns a decoder that reads into the requested version.
func (s projectedSerializer) DecoderToVersion(decoder runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return versioning.NewCodec(
		nil, decoder, s.convertor,
		s.scheme, s.scheme, s.scheme,
		nil, gv, schemeName,
	)
}
