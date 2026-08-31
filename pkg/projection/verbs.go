package projection

import (
	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// Which verbs a projection serves is decided twice: once in
// pkg/registry/projection, where the storage type is assembled out of the
// interfaces the declared queries can back, and once here, from the spec alone.
//
// Twice, because the two answers are needed in different places. The server
// needs it as a set of Go interfaces, at compile time, so that discovery offers
// nothing the projection would refuse. Anything reasoning about a projection it
// is not serving — generating the RBAC that makes it reachable, most of all —
// needs it as data, from a file, with no database and no compiled storage
// anywhere.
//
// Two answers that must agree, so TestServedVerbsMatchAdvertisedVerbs walks
// every combination of declared queries and checks that they do. Without it a
// verb added to the storage matrix would quietly stop appearing in generated
// roles, and the failure would surface as a 403 on a verb discovery advertises.

// ServedVerbs reports the verbs a projection serves for its main resource, in
// the order the API server lists them.
//
// get and list are unconditional: every projection has a list query, and a get
// with no get query is answered by filtering the collection.
func ServedVerbs(spec crispv1alpha1.CustomResourceProjectionSpec) []string {
	verbs := []string{"get", "list"}

	if WatchEnabled(spec) {
		verbs = append(verbs, "watch")
	}
	if spec.Queries.Create != nil {
		verbs = append(verbs, "create")
	}
	if spec.Queries.Update != nil {
		// An update query brings patch with it: a patch is read, apply, update.
		verbs = append(verbs, "update", "patch")
	}
	if spec.Queries.Delete != nil {
		verbs = append(verbs, "delete")
	}
	// A projection with only a collection statement still serves
	// deletecollection; one with a row statement serves both, deleting a row at
	// a time when a single statement cannot express the request.
	if spec.Queries.Delete != nil || spec.Queries.DeleteCollection != nil {
		verbs = append(verbs, "deletecollection")
	}

	return verbs
}

// SubresourceVerbs reports the verbs served for one of the projection's
// subresources, and nil for a subresource it does not serve.
//
// Both subresources are backed by the writable storage, so a projection that
// declares subresources.status but has no write query at all serves no status
// subresource — the storage it would hang off is never built. A role granting
// films/status there would be granting a path that returns 404.
func SubresourceVerbs(spec crispv1alpha1.CustomResourceProjectionSpec, subresource string) []string {
	if !writable(spec) {
		return nil
	}
	sub := spec.Resource.Subresources
	if sub == nil {
		return nil
	}

	switch subresource {
	case "status":
		if sub.Status == nil {
			return nil
		}
	case "scale":
		if sub.Scale == nil {
			return nil
		}
	default:
		return nil
	}

	// Both subresource storages read and write and do nothing else: they are
	// views onto one object, so there is no list, no watch, and no delete.
	return []string{"get", "update", "patch"}
}

// WatchEnabled reports whether the projection serves watch.
func WatchEnabled(spec crispv1alpha1.CustomResourceProjectionSpec) bool {
	return spec.Watch == nil || !spec.Watch.Disabled
}

// writable reports whether the projection declares any write query, which is
// what decides whether writable storage is built at all.
func writable(spec crispv1alpha1.CustomResourceProjectionSpec) bool {
	return spec.Queries.Create != nil ||
		spec.Queries.Update != nil ||
		spec.Queries.Delete != nil ||
		spec.Queries.DeleteCollection != nil
}
