package v1alpha1

// The shapes the names of a projected resource are held to.
//
// Each of these is also written on its field as a kubebuilder marker, and the
// marker is what reaches a cluster: the CRD it generates refuses the projection
// at apply time. A marker is a comment, though, and a comment cannot be read by
// the code that loads a projection from --projection-dir or checks one for
// `kube-crisp-apiserver validate`, neither of which ever meets the CRD. Those
// paths are held to the same rule by projection.Validate reading the constants
// here, and a test compares each constant with the pattern the served CRD
// carries so that the two cannot drift.
const (
	// GroupNamePattern is a DNS subdomain: what an API group is, and what the
	// APIService registering it is named after, as "<version>.<group>".
	GroupNamePattern = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

	// GroupNameMaxLength is the DNS subdomain limit.
	GroupNameMaxLength = 253

	// VersionNamePattern is the shape a CustomResourceDefinition requires of a
	// version, and the other half of the APIService's name. It applies to
	// spec.resource.version and to every spec.resource.versions[].name alike:
	// each served version registers an APIService of its own, and one named
	// after a version outside this shape is refused by the kube-apiserver on
	// every reconcile, so the projection is never routable.
	VersionNamePattern = `^v[0-9]+((alpha|beta)[0-9]+)?$`

	// KindNamePattern is a Go-style type name, for a kind and for a list kind.
	KindNamePattern = `^[A-Z][A-Za-z0-9]*$`

	// KindNameMaxLength bounds a kind the way a DNS label is bounded.
	KindNameMaxLength = 63

	// ResourceNamePattern is the shape of a plural, a singular and a short
	// name: lowercase letters and digits, starting with a letter. A resource
	// name is a path segment, so a slash in one -- "orders/status" -- is not a
	// resource but a subresource the endpoint installer has no parent for, and
	// it refuses the whole API group rather than the one resource.
	ResourceNamePattern = `^[a-z][a-z0-9]*$`

	// ResourceNameMaxLength bounds a resource name the way a DNS label is
	// bounded.
	ResourceNameMaxLength = 63
)
