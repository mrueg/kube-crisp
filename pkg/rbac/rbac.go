// Package rbac generates the ClusterRoles that make a projected API group
// reachable.
//
// kube-crisp serves projected kinds through the aggregation layer, and the
// aggregation layer delegates authorization to the kube-apiserver. So a
// projection that is compiled, registered and serving is still a 403 to
// everyone except cluster-admin until somebody writes a ClusterRole for its
// group — a step nothing in the projection performs and, until this existed,
// nothing in the documentation mentioned either.
//
// Writing it by hand is worse than tedious, because the projection knows
// something the author does not: exactly which verbs it can serve. A projection
// with no create query refuses create, whatever a role grants, and a role that
// grants it says a caller may do something they cannot. Generated from the
// spec, the grant matches what discovery advertises, because both come from
// pkg/projection.ServedVerbs.
package rbac

import (
	"fmt"
	"sort"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

const (
	// DefaultNamePrefix begins the name of every generated role. The group
	// follows it, so one projected group is one pair of roles no matter how
	// many projections make it up.
	DefaultNamePrefix = "kube-crisp"

	// GroupLabel records which projected API group a role was generated for, so
	// the roles belonging to a group can be found again when it is removed.
	// `kubectl crisp prune` selects on it.
	//
	// Deliberately not app.kubernetes.io/managed-by, which this project uses
	// for the APIServices and the webhook configuration the server itself
	// creates, updates and prunes. Nothing manages these: they are printed for
	// somebody to read, edit and apply, and a controller that pruned them
	// without being asked would be deleting objects a person had edited.
	GroupLabel = "crisp.kubecrisp.io/projected-group"

	aggregateToView = "rbac.authorization.k8s.io/aggregate-to-view"
	aggregateToEdit = "rbac.authorization.k8s.io/aggregate-to-edit"
)

// readVerbs and writeVerbs split the served verbs between the two roles. A verb
// belongs to exactly one of them, so the pair grants each verb once.
var (
	readVerbs  = sets.New("get", "list", "watch")
	writeVerbs = sets.New("create", "update", "patch", "delete", "deletecollection")
)

// Options configures generation.
type Options struct {
	// NamePrefix begins each generated role name. Empty means
	// DefaultNamePrefix.
	NamePrefix string

	// Aggregate labels the roles so the cluster's built-in view, edit and
	// admin roles absorb them.
	//
	// Off by default, and that is a judgement rather than caution about
	// defaults. Aggregating grants every existing holder of view in every
	// namespace the ability to read the rows behind the projection, the moment
	// the role is applied — and those rows are a production database's, not an
	// empty new resource nobody has yet. The bindings this generates without
	// it are inert until somebody binds them, which is the point at which the
	// decision is visible.
	Aggregate bool
}

// ClusterRoles generates the roles for every projected group among the
// projections given, sorted by group.
//
// Two roles per group where the group can be written and one where it cannot:
// a read-only projection has nothing to put in an edit role, and emitting an
// empty one would be a grant that looks like it does something.
func ClusterRoles(projections []crispv1alpha1.CustomResourceProjection, opts Options) ([]rbacv1.ClusterRole, error) {
	prefix := opts.NamePrefix
	if prefix == "" {
		prefix = DefaultNamePrefix
	}

	byGroup := map[string]map[string]sets.Set[string]{}
	for i := range projections {
		spec := projections[i].Spec
		group := spec.Resource.Group
		if group == "" {
			return nil, fmt.Errorf("%s: spec.resource.group is empty", projections[i].Name)
		}
		plural := spec.Resource.Plural
		if plural == "" {
			return nil, fmt.Errorf("%s: spec.resource.plural is empty", projections[i].Name)
		}

		resources, ok := byGroup[group]
		if !ok {
			resources = map[string]sets.Set[string]{}
			byGroup[group] = resources
		}

		// Union rather than assignment. Two projections declaring the same
		// group and plural is a conflict the server refuses to serve, but this
		// runs over files that were never loaded together and over a directory
		// somebody is still editing, where the useful answer is the roles for
		// what is there rather than an error about what would happen.
		add(resources, plural, projection.ServedVerbs(spec))
		for _, sub := range []string{"status", "scale"} {
			if verbs := projection.SubresourceVerbs(spec, sub); verbs != nil {
				add(resources, plural+"/"+sub, verbs)
			}
		}
	}

	groups := make([]string, 0, len(byGroup))
	for group := range byGroup {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	var out []rbacv1.ClusterRole
	for _, group := range groups {
		resources := byGroup[group]

		if rules := rulesFor(group, resources, readVerbs); len(rules) > 0 {
			out = append(out, role(prefix, group, "view", aggregateToView, opts.Aggregate, rules))
		}
		if rules := rulesFor(group, resources, writeVerbs); len(rules) > 0 {
			out = append(out, role(prefix, group, "edit", aggregateToEdit, opts.Aggregate, rules))
		}
	}

	return out, nil
}

// add unions verbs onto a resource's entry.
func add(resources map[string]sets.Set[string], resource string, verbs []string) {
	if resources[resource] == nil {
		resources[resource] = sets.New[string]()
	}
	resources[resource].Insert(verbs...)
}

// rulesFor builds the rules of one role: every resource whose served verbs
// intersect want, grouped so that resources granted the same verbs share a
// rule.
//
// Grouped, because a projected group is usually many kinds with identical
// grants — ten of them, in this repository's own Pagila example — and ten rules
// that differ only in one string is a role nobody reads.
func rulesFor(group string, resources map[string]sets.Set[string], want sets.Set[string]) []rbacv1.PolicyRule {
	byVerbs := map[string][]string{}
	for resource, served := range resources {
		granted := served.Intersection(want)
		if granted.Len() == 0 {
			continue
		}
		key := strings.Join(sortVerbs(granted), ",")
		byVerbs[key] = append(byVerbs[key], resource)
	}

	keys := make([]string, 0, len(byVerbs))
	for key := range byVerbs {
		sort.Strings(byVerbs[key])
		keys = append(keys, key)
	}
	// By the resources a rule covers rather than by its verbs, so the order is
	// the order somebody reads the group in. Sorted beforehand rather than
	// inside the comparison, which would make sorting depend on a side effect
	// of comparing.
	sort.Slice(keys, func(i, j int) bool {
		return byVerbs[keys[i]][0] < byVerbs[keys[j]][0]
	})

	rules := make([]rbacv1.PolicyRule, 0, len(keys))
	for _, key := range keys {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{group},
			Resources: byVerbs[key],
			Verbs:     strings.Split(key, ","),
		})
	}
	return rules
}

// role assembles one ClusterRole.
func role(prefix, group, tier, aggregateLabel string, aggregate bool, rules []rbacv1.PolicyRule) rbacv1.ClusterRole {
	labels := map[string]string{GroupLabel: group}
	if aggregate {
		labels[aggregateLabel] = "true"
	}

	return rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rbacv1.SchemeGroupVersion.String(),
			Kind:       "ClusterRole",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("%s:%s:%s", prefix, group, tier),
			Labels: labels,
		},
		Rules: rules,
	}
}

// sortVerbs orders verbs the way the API server lists them rather than
// alphabetically, so a generated role reads like a written one.
func sortVerbs(verbs sets.Set[string]) []string {
	order := map[string]int{
		"get": 0, "list": 1, "watch": 2,
		"create": 3, "update": 4, "patch": 5,
		"delete": 6, "deletecollection": 7,
	}
	out := verbs.UnsortedList()
	sort.Slice(out, func(i, j int) bool { return order[out[i]] < order[out[j]] })
	return out
}
