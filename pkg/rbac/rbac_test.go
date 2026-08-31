package rbac

import (
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// projectionFor builds a projection declaring the queries named in verbs.
func projectionFor(name, group, plural string, queries ...string) crispv1alpha1.CustomResourceProjection {
	p := crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = group
	p.Spec.Resource.Plural = plural
	for _, q := range queries {
		switch q {
		case "create":
			p.Spec.Queries.Create = &crispv1alpha1.Query{}
		case "update":
			p.Spec.Queries.Update = &crispv1alpha1.Query{}
		case "delete":
			p.Spec.Queries.Delete = &crispv1alpha1.Query{}
		case "deleteCollection":
			p.Spec.Queries.DeleteCollection = &crispv1alpha1.Query{}
		case "noWatch":
			p.Spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}
		}
	}
	return p
}

// ruleFor finds the rule covering resource, so a test can assert on the verbs
// one resource was granted without depending on how rules were grouped.
func ruleFor(t *testing.T, role rbacv1.ClusterRole, resource string) rbacv1.PolicyRule {
	t.Helper()
	for _, rule := range role.Rules {
		for _, r := range rule.Resources {
			if r == resource {
				return rule
			}
		}
	}
	t.Fatalf("role %s grants nothing on %s: %+v", role.Name, resource, role.Rules)
	return rbacv1.PolicyRule{}
}

func roleNamed(t *testing.T, roles []rbacv1.ClusterRole, name string) rbacv1.ClusterRole {
	t.Helper()
	for _, role := range roles {
		if role.Name == name {
			return role
		}
	}
	t.Fatalf("no role named %s in %d role(s)", name, len(roles))
	return rbacv1.ClusterRole{}
}

// TestReadOnlyProjectionGetsNoEditRole is the point of splitting the roles: a
// projection nobody can write has nothing to put in an edit role, and an empty
// one would be a grant that looks like it does something.
func TestReadOnlyProjectionGetsNoEditRole(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("films", "pagila.example.com", "films"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	if len(roles) != 1 {
		t.Fatalf("got %d role(s), want 1: %+v", len(roles), roles)
	}
	if !strings.HasSuffix(roles[0].Name, ":view") {
		t.Fatalf("the single role is %s, want the view role", roles[0].Name)
	}

	rule := ruleFor(t, roles[0], "films")
	if strings.Join(rule.Verbs, ",") != "get,list,watch" {
		t.Fatalf("verbs = %v, want get,list,watch", rule.Verbs)
	}
}

// TestVerbsFollowDeclaredQueries checks the claim the whole command rests on:
// the role grants what the projection can serve and not what it cannot.
func TestVerbsFollowDeclaredQueries(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		// Insertable and removable but never updated — an append-only table.
		projectionFor("rentals", "pagila.example.com", "rentals", "create", "delete"),
		// Updatable only: a row that exists and whose columns change.
		projectionFor("stock", "pagila.example.com", "stock", "update"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	edit := roleNamed(t, roles, "kube-crisp:pagila.example.com:edit")

	if verbs := strings.Join(ruleFor(t, edit, "rentals").Verbs, ","); verbs != "create,delete,deletecollection" {
		t.Errorf("rentals verbs = %s, want create,delete,deletecollection", verbs)
	}
	if verbs := strings.Join(ruleFor(t, edit, "stock").Verbs, ","); verbs != "update,patch" {
		t.Errorf("stock verbs = %s, want update,patch", verbs)
	}
}

// TestDeleteCollectionAloneGrantsOnlyDeleteCollection covers the asymmetry in
// the storage matrix: a collection statement serves deletecollection without
// serving delete.
func TestDeleteCollectionAloneGrantsOnlyDeleteCollection(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("orders", "store.example.com", "orders", "deleteCollection"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	edit := roleNamed(t, roles, "kube-crisp:store.example.com:edit")
	if verbs := strings.Join(ruleFor(t, edit, "orders").Verbs, ","); verbs != "deletecollection" {
		t.Fatalf("orders verbs = %s, want deletecollection alone", verbs)
	}
}

// TestWatchDisabledIsNotGranted keeps a disabled watch out of the role. An
// informer that lists, watches and is refused never syncs, and a role granting
// watch is what makes that look like a server bug rather than a projection
// that said so.
func TestWatchDisabledIsNotGranted(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("films", "pagila.example.com", "films", "noWatch"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	rule := ruleFor(t, roleNamed(t, roles, "kube-crisp:pagila.example.com:view"), "films")
	for _, verb := range rule.Verbs {
		if verb == "watch" {
			t.Fatalf("watch granted for a projection that disabled it: %v", rule.Verbs)
		}
	}
}

// TestSubresourcesSplitAcrossRoles puts the read half of a subresource in the
// view role and the write half in the edit role, the way the resource itself
// is split.
func TestSubresourcesSplitAcrossRoles(t *testing.T) {
	p := projectionFor("stock", "pagila.example.com", "stock", "update")
	p.Spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Status: &crispv1alpha1.ProjectedStatusSubresource{},
		Scale:  &crispv1alpha1.ProjectedScaleSubresource{SpecReplicasPath: ".spec.replicas"},
	}

	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{p}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	view := roleNamed(t, roles, "kube-crisp:pagila.example.com:view")
	if verbs := strings.Join(ruleFor(t, view, "stock/scale").Verbs, ","); verbs != "get" {
		t.Errorf("stock/scale in the view role = %s, want get", verbs)
	}

	edit := roleNamed(t, roles, "kube-crisp:pagila.example.com:edit")
	if verbs := strings.Join(ruleFor(t, edit, "stock/status").Verbs, ","); verbs != "update,patch" {
		t.Errorf("stock/status in the edit role = %s, want update,patch", verbs)
	}
}

// TestSubresourcesOfReadOnlyProjectionAreNotGranted covers the trap: both
// subresource storages hang off the writable storage, so declaring
// subresources.status on a projection with no write query serves nothing.
func TestSubresourcesOfReadOnlyProjectionAreNotGranted(t *testing.T) {
	p := projectionFor("films", "pagila.example.com", "films")
	p.Spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Status: &crispv1alpha1.ProjectedStatusSubresource{},
	}

	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{p}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	for _, role := range roles {
		for _, rule := range role.Rules {
			for _, resource := range rule.Resources {
				if resource == "films/status" {
					t.Fatalf("%s grants films/status, which is not served", role.Name)
				}
			}
		}
	}
}

// TestOneRolePerGroup is what makes this worth running over a directory: ten
// kinds in one group are one pair of roles, not ten.
func TestOneRolePerGroup(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("films", "pagila.example.com", "films"),
		projectionFor("actors", "pagila.example.com", "actors"),
		projectionFor("orders", "store.example.com", "orders"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	if len(roles) != 2 {
		t.Fatalf("got %d role(s), want one view role per group: %+v", len(roles), roles)
	}
	// Sorted by group, so pagila comes before store however the map iterated.
	if roles[0].Name != "kube-crisp:pagila.example.com:view" || roles[1].Name != "kube-crisp:store.example.com:view" {
		t.Fatalf("roles are not sorted by group: %s, %s", roles[0].Name, roles[1].Name)
	}

	rule := ruleFor(t, roles[0], "films")
	if strings.Join(rule.Resources, ",") != "actors,films" {
		t.Fatalf("kinds granted the same verbs should share a rule, got %v", rule.Resources)
	}
}

// TestAggregationIsOptIn is the security-relevant default: aggregating hands
// every existing holder of view the rows behind the projection.
func TestAggregationIsOptIn(t *testing.T) {
	projections := []crispv1alpha1.CustomResourceProjection{
		projectionFor("films", "pagila.example.com", "films", "update"),
	}

	off, err := ClusterRoles(projections, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range off {
		for label := range role.Labels {
			if strings.HasPrefix(label, "rbac.authorization.k8s.io/aggregate-to-") {
				t.Fatalf("%s carries %s without --aggregate", role.Name, label)
			}
		}
	}

	on, err := ClusterRoles(projections, Options{Aggregate: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := roleNamed(t, on, "kube-crisp:pagila.example.com:view").Labels[aggregateToView]; got != "true" {
		t.Errorf("view role aggregate-to-view = %q, want true", got)
	}
	if got := roleNamed(t, on, "kube-crisp:pagila.example.com:edit").Labels[aggregateToEdit]; got != "true" {
		t.Errorf("edit role aggregate-to-edit = %q, want true", got)
	}
}

// TestNamePrefix lets a cluster that already has a kube-crisp: role name of its
// own generate under another one.
func TestNamePrefix(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("films", "pagila.example.com", "films"),
	}, Options{NamePrefix: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if roles[0].Name != "acme:pagila.example.com:view" {
		t.Fatalf("name = %s, want the acme prefix", roles[0].Name)
	}
}

// TestIncompleteProjectionIsRejected: a projection with no group or no plural
// names no resource, and a role generated from it would grant "" on "".
func TestIncompleteProjectionIsRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    crispv1alpha1.CustomResourceProjection
	}{
		{"no group", projectionFor("films", "", "films")},
		{"no plural", projectionFor("films", "pagila.example.com", "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{tc.p}, Options{}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// TestSameResourceTwiceIsUnioned covers a directory somebody is still editing,
// where two files declare the same kind. The server refuses to serve that; the
// useful answer here is a role for what is there.
func TestSameResourceTwiceIsUnioned(t *testing.T) {
	roles, err := ClusterRoles([]crispv1alpha1.CustomResourceProjection{
		projectionFor("films-a", "pagila.example.com", "films", "create"),
		projectionFor("films-b", "pagila.example.com", "films", "update"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}

	edit := roleNamed(t, roles, "kube-crisp:pagila.example.com:edit")
	if verbs := strings.Join(ruleFor(t, edit, "films").Verbs, ","); verbs != "create,update,patch" {
		t.Fatalf("verbs = %s, want the union of both declarations", verbs)
	}
	if rules := len(edit.Rules); rules != 1 {
		t.Fatalf("got %d rules, want one: the same resource must not appear twice", rules)
	}
}
