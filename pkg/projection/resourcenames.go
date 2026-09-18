package projection

import (
	"fmt"
	"regexp"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// The CRD's patterns, compiled once. See the constants for why they exist
// twice.
var (
	groupName    = regexp.MustCompile(crispv1alpha1.GroupNamePattern)
	versionName  = regexp.MustCompile(crispv1alpha1.VersionNamePattern)
	kindName     = regexp.MustCompile(crispv1alpha1.KindNamePattern)
	resourceName = regexp.MustCompile(crispv1alpha1.ResourceNamePattern)
)

// checkResourceNames holds the names a projection serves under to the shapes
// the CRD demands of them.
//
// A projection applied to a cluster never gets here with a bad name: the CRD
// refuses it. One loaded from --projection-dir bypasses the CRD entirely, and
// Validate used to check only that the plural was lowercase. What got through
// was not a projection that served badly but one that could not be served at
// all, and in the worst way: a plural such as "bins/status" is a subresource to
// the endpoint installer, which refuses the whole API surface rather than the
// one resource, and a version named "V2" produces an APIService the
// kube-apiserver rejects on every reconcile, so the group is never routable and
// nothing says so but a condition.
//
// The rules are the ones Kubernetes applies to a CustomResourceDefinition's own
// names, which is what a projected resource is standing in for.
func checkResourceNames(projection string, res crispv1alpha1.ProjectedResource) error {
	if !groupName.MatchString(res.Group) || len(res.Group) > crispv1alpha1.GroupNameMaxLength {
		return fmt.Errorf(
			"projection %s: spec.resource.group is %q, which is not a DNS subdomain; an APIService is named "+
				"\"<version>.<group>\", and the kube-apiserver refuses one named after anything else",
			projection, res.Group)
	}

	if !versionName.MatchString(res.Version) {
		return fmt.Errorf("projection %s: spec.resource.version is %q; %s", projection, res.Version, versionShape)
	}
	for i, version := range res.Versions {
		if !versionName.MatchString(version.Name) {
			return fmt.Errorf("projection %s: spec.resource.versions[%d].name is %q; %s",
				projection, i, version.Name, versionShape)
		}
	}

	if !kindName.MatchString(res.Kind) || len(res.Kind) > crispv1alpha1.KindNameMaxLength {
		return fmt.Errorf("projection %s: spec.resource.kind is %q; %s", projection, res.Kind, kindShape)
	}
	if res.ListKind != "" && (!kindName.MatchString(res.ListKind) || len(res.ListKind) > crispv1alpha1.KindNameMaxLength) {
		return fmt.Errorf("projection %s: spec.resource.listKind is %q; %s", projection, res.ListKind, kindShape)
	}

	if !resourceName.MatchString(res.Plural) || len(res.Plural) > crispv1alpha1.ResourceNameMaxLength {
		return fmt.Errorf("projection %s: spec.resource.plural is %q; %s", projection, res.Plural, resourceShape)
	}
	if res.Singular != "" && (!resourceName.MatchString(res.Singular) || len(res.Singular) > crispv1alpha1.ResourceNameMaxLength) {
		return fmt.Errorf("projection %s: spec.resource.singular is %q; %s", projection, res.Singular, resourceShape)
	}
	for i, short := range res.ShortNames {
		if !resourceName.MatchString(short) || len(short) > crispv1alpha1.ResourceNameMaxLength {
			return fmt.Errorf("projection %s: spec.resource.shortNames[%d] is %q; %s",
				projection, i, short, resourceShape)
		}
	}
	return nil
}

// What each name has to look like, said the same way wherever it is said.
const (
	versionShape = "a version is written like v1, v2beta1 or v1alpha1, " +
		"which is what the APIService registering it is named after"
	kindShape     = "a kind starts with an upper-case letter and continues with letters and digits"
	resourceShape = "a resource name is lowercase letters and digits, starting with a letter, " +
		"since it is a path segment"
)
