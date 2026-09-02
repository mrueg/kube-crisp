package projection

import (
	"fmt"
	"strings"
)

// The API groups a projection may not claim.
//
// A generated role names its group verbatim in APIGroups, which is what makes
// it useful and what makes this necessary: nothing about `kubectl crisp rbac`
// checks that the group belongs to kube-crisp, and the documented way to use it
// is to pipe it into `kubectl apply`. A projection declaring
// rbac.authorization.k8s.io/clusterroles therefore produced a ClusterRole
// granting write access to the cluster's real ClusterRoles, and with
// --aggregate it landed in the built-in view, edit and admin roles without even
// a binding step. Whoever can write a projection is not necessarily whoever can
// grant cluster-admin, and this closed the gap between them.
//
// The list is of groups Kubernetes owns rather than of groups that happen to be
// installed, because the check has to give the same answer from a file with no
// cluster in reach as it does against a live one. It is deliberately not a
// guess at what else might be registered: a projection can still claim an
// unserved third-party group, which is a different problem with a different
// answer, and one the server rather than the role generator has to police.
var reservedGroups = map[string]bool{
	// The core group, which a projection cannot name anyway, listed so that the
	// empty string is refused here for a reason rather than by accident.
	"":            true,
	"apps":        true,
	"batch":       true,
	"autoscaling": true,
	"policy":      true,
	"extensions":  true,
}

// reservedSuffixes are the domains Kubernetes reserves for its own groups.
//
// Every built-in group beyond the handful above lives under one of them --
// rbac.authorization.k8s.io, networking.k8s.io, storage.k8s.io,
// apiextensions.k8s.io, admissionregistration.k8s.io, and the rest -- so
// matching the domain covers the ones that exist today and the ones added
// after this was written.
var reservedSuffixes = []string{"k8s.io", "kubernetes.io"}

// CheckAPIGroup refuses a group a projection must not claim.
//
// Called both where a projection is validated, so one naming a reserved group
// cannot be created, and where roles are generated, because that path also runs
// over files the server never saw.
func CheckAPIGroup(projection, group string) error {
	if reservedGroups[group] {
		return fmt.Errorf(
			"%s: spec.resource.group is %q, which Kubernetes owns; a generated role for it would grant "+
				"access to the cluster's own resources rather than to projected ones",
			projection, group)
	}

	for _, suffix := range reservedSuffixes {
		if group == suffix || strings.HasSuffix(group, "."+suffix) {
			return fmt.Errorf(
				"%s: spec.resource.group is %q, and %s is reserved for Kubernetes; a generated role for it "+
					"would grant access to the cluster's own resources rather than to projected ones",
				projection, group, suffix)
		}
	}
	return nil
}
