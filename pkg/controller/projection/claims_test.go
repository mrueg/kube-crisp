package projection

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// claiming builds a compilation that serves one resource, which is all
// resolveClaims looks at.
func claiming(plural string, extra ...string) compilation {
	c := compilation{resources: []apidynamic.Resource{{
		Group: "warehouse.example.com", Version: "v1alpha1", Plural: plural,
	}}}
	for _, p := range extra {
		c.resources = append(c.resources, apidynamic.Resource{
			Group: "warehouse.example.com", Version: "v1alpha1", Plural: p,
		})
	}
	return c
}

func at(hour int) metav1.Time {
	return metav1.NewTime(time.Date(2026, 9, 1, hour, 0, 0, 0, time.UTC))
}

// Projections that agree keep everything they claim.
func TestResolveClaimsLeavesDistinctResourcesAlone(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{"bins": claiming("bins"), "crates": claiming("crates")},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		nil,
	)
	if len(losses) != 0 {
		t.Errorf("resolveClaims() reported %v, want nothing", losses)
	}
}

// The projection already serving keeps the resource.
//
// The mistake is in the object just applied, so that is the object that must
// fail. The alternative — newest wins — means anyone can take a working API
// group away from whoever had it by applying a projection that names it.
func TestResolveClaimsKeepsTheProjectionThatIsServing(t *testing.T) {
	surviving := map[string]compilation{"incumbent": claiming("bins"), "newcomer": claiming("bins")}
	// The newcomer is older, so that only incumbency can explain the outcome.
	created := map[string]metav1.Time{"incumbent": at(9), "newcomer": at(1)}
	serving := map[string]compilation{"incumbent": claiming("bins")}

	losses := resolveClaims(surviving, created, serving)
	if _, lost := losses["newcomer"]; !lost {
		t.Errorf("resolveClaims() = %v, want the newcomer to lose", losses)
	}
	if _, lost := losses["incumbent"]; lost {
		t.Error("the projection that was already serving lost its resource")
	}
	if msg := losses["newcomer"].Error(); !strings.Contains(msg, `"incumbent"`) ||
		!strings.Contains(msg, `resource name "bins" in group warehouse.example.com`) {
		t.Errorf("the error is %q; it should name the other projection and the resource", msg)
	}
}

// Serving is decided per resource, not per projection.
//
// A projection that already serves one resource is not the incumbent of another
// one it is edited to claim: for that resource it is the newcomer, and the
// projection serving it keeps it. Ranking by "was serving anything" let an
// older projection take a working resource away from a newer one, the moment
// somebody edited the older one -- the outage triggered by the object that was
// just applied, and landing on the object that was not.
func TestResolveClaimsRanksIncumbencyPerResource(t *testing.T) {
	// orders came later and serves orders; items came first and serves items.
	// Then items is edited to claim orders as well.
	surviving := map[string]compilation{
		"orders": claiming("orders"),
		"items":  claiming("items", "orders"),
	}
	created := map[string]metav1.Time{"orders": at(9), "items": at(1)}
	serving := map[string]compilation{"orders": claiming("orders"), "items": claiming("items")}

	losses := resolveClaims(surviving, created, serving)
	if _, lost := losses["orders"]; lost {
		t.Errorf("the projection serving orders lost it to one that was edited to claim it: %v", losses)
	}
	if _, lost := losses["items"]; !lost {
		t.Errorf("resolveClaims() = %v, want the edited projection to lose", losses)
	}
}

// And the reverse: a projection that serves a resource keeps it against one
// created later that claims it, whatever else either of them serves.
func TestResolveClaimsKeepsAResourceAgainstANewerClaimant(t *testing.T) {
	surviving := map[string]compilation{
		"orders": claiming("orders"),
		"items":  claiming("items", "orders"),
	}
	created := map[string]metav1.Time{"orders": at(1), "items": at(9)}
	serving := map[string]compilation{"orders": claiming("orders"), "items": claiming("items")}

	losses := resolveClaims(surviving, created, serving)
	if _, lost := losses["items"]; !lost || len(losses) != 1 {
		t.Errorf("resolveClaims() = %v, want just the newer claimant to lose", losses)
	}
}

// A projection gives way only to one that is going to serve.
//
// When the projection that beat it loses elsewhere, nobody is serving the name
// it lost: failing it would withdraw a working resource in favour of nobody.
func TestResolveClaimsDoesNotLoseToAProjectionThatLoses(t *testing.T) {
	// bins serves bins. middle is edited to claim bins and crates; crates
	// serves crates, so middle loses -- and bins must not lose to middle.
	surviving := map[string]compilation{
		"bins":   claiming("bins"),
		"middle": claiming("bins", "crates"),
		"crates": claiming("crates"),
	}
	created := map[string]metav1.Time{"bins": at(9), "middle": at(1), "crates": at(9)}
	serving := map[string]compilation{
		"bins":   claiming("bins"),
		"middle": claiming("middle"),
		"crates": claiming("crates"),
	}

	losses := resolveClaims(surviving, created, serving)
	if _, lost := losses["middle"]; !lost || len(losses) != 1 {
		t.Errorf("resolveClaims() = %v, want just middle to lose", losses)
	}
}

// Two projections each holding a name the other has just been edited to claim
// cannot both keep serving. The newer one gives way, so that one of them does,
// and so that every replica picks the same one.
func TestResolveClaimsSettlesAMutualClaim(t *testing.T) {
	surviving := map[string]compilation{
		"bins":   claiming("bins", "crates"),
		"crates": claiming("crates", "bins"),
	}
	created := map[string]metav1.Time{"bins": at(1), "crates": at(2)}
	serving := map[string]compilation{"bins": claiming("bins"), "crates": claiming("crates")}

	for i := 0; i < 50; i++ {
		losses := resolveClaims(surviving, created, serving)
		if _, lost := losses["crates"]; !lost || len(losses) != 1 {
			t.Fatalf("run %d: resolveClaims() = %v, want just the newer projection to lose", i, losses)
		}
	}
}

// With nobody serving yet — a cold start — the older projection wins, which
// re-elects whoever was serving before the restart.
func TestResolveClaimsPrefersTheOlderProjection(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{"older": claiming("bins"), "newer": claiming("bins")},
		map[string]metav1.Time{"older": at(1), "newer": at(2)},
		nil,
	)
	if _, lost := losses["newer"]; !lost || len(losses) != 1 {
		t.Errorf("resolveClaims() = %v, want just the newer one to lose", losses)
	}
}

// Two created in the same instant still have to settle, and settle the same way
// in every replica: an arbitrary winner is fine, an unstable one is not.
func TestResolveClaimsBreaksATieOnTheName(t *testing.T) {
	surviving := map[string]compilation{"aaa": claiming("bins"), "zzz": claiming("bins")}
	created := map[string]metav1.Time{"aaa": at(1), "zzz": at(1)}

	// Repeatedly, because the input is a map and its iteration order is not
	// stable: this is the property the sort exists for.
	for i := 0; i < 50; i++ {
		losses := resolveClaims(surviving, created, nil)
		if _, lost := losses["zzz"]; !lost || len(losses) != 1 {
			t.Fatalf("run %d: resolveClaims() = %v, want just zzz to lose", i, losses)
		}
	}
}

// A projection that conflicts on one resource serves none of them.
//
// Half a projection is worse than none: the missing half looks exactly like a
// projection nobody applied, so a client gets "the server could not find the
// requested resource" for a kind whose sibling answers.
func TestResolveClaimsFailsAProjectionWhole(t *testing.T) {
	surviving := map[string]compilation{
		"incumbent": claiming("bins"),
		"newcomer":  claiming("bins", "crates"),
	}
	created := map[string]metav1.Time{"incumbent": at(1), "newcomer": at(2)}

	losses := resolveClaims(surviving, created, map[string]compilation{"incumbent": claiming("bins")})
	if _, lost := losses["newcomer"]; !lost {
		t.Fatalf("resolveClaims() = %v, want the newcomer to lose", losses)
	}
	// crates is free again, since the projection that claimed it is not
	// installed at all.
	if _, lost := losses["incumbent"]; lost {
		t.Error("the incumbent lost a resource it does not claim")
	}
}

// named builds a compilation serving one fully named resource at each of the
// given versions, which is what a projection with versions compiles to.
func named(kind, plural string, shortNames []string, versions ...string) compilation {
	var c compilation
	for _, version := range versions {
		c.resources = append(c.resources, apidynamic.Resource{
			Group: "warehouse.example.com", Version: version, Plural: plural,
			Kind: kind, ListKind: kind + "List", Singular: strings.ToLower(kind),
			ShortNames: shortNames,
		})
	}
	return c
}

// A claim is on the plural within the group, whatever the version.
//
// Two projections serving one plural at two versions used to pass, since the
// claim was keyed on the version too. They then installed as two versions of a
// single resource, and the preferred-version sort decided which table answered
// a client that named neither -- a Bin from one projection, a Crate from the
// other, under one name.
func TestResolveClaimsIgnoresTheVersion(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{
			"bins":   named("Bin", "bins", nil, "v1"),
			"crates": named("Crate", "bins", nil, "v1beta1"),
		},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		nil,
	)
	if _, lost := losses["crates"]; !lost || len(losses) != 1 {
		t.Fatalf("resolveClaims() = %v, want just the second projection to lose", losses)
	}
	if msg := losses["crates"].Error(); !strings.Contains(msg, `resource name "bins"`) ||
		!strings.Contains(msg, `projection "bins"`) {
		t.Errorf("the error is %q; it should name the plural and the projection serving it", msg)
	}
}

// Two resources cannot present one kind under one group, whatever their
// plurals: a client asking for the kind would be answered from whichever
// happened to install.
func TestResolveClaimsRefusesASharedKind(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{
			"bins":   named("Bin", "bins", nil, "v1"),
			"crates": named("Bin", "crates", nil, "v1"),
		},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		nil,
	)
	if _, lost := losses["crates"]; !lost || len(losses) != 1 {
		t.Fatalf("resolveClaims() = %v, want just the second projection to lose", losses)
	}
	if msg := losses["crates"].Error(); !strings.Contains(msg, `kind "Bin"`) {
		t.Errorf("the error is %q; it should name the kind that collided", msg)
	}
}

// Nor one short name: kubectl resolves it to a single resource.
func TestResolveClaimsRefusesASharedShortName(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{
			"bins":   named("Bin", "bins", []string{"bn"}, "v1"),
			"crates": named("Crate", "crates", []string{"bn"}, "v1"),
		},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		nil,
	)
	if _, lost := losses["crates"]; !lost || len(losses) != 1 {
		t.Fatalf("resolveClaims() = %v, want just the second projection to lose", losses)
	}
	if msg := losses["crates"].Error(); !strings.Contains(msg, `resource name "bn"`) {
		t.Errorf("the error is %q; it should name the short name that collided", msg)
	}
}

// A short name is a resource name like any other: one projection's short name
// cannot be another's plural.
func TestResolveClaimsRefusesAShortNameThatIsAnotherPlural(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{
			"bins":   named("Bin", "bins", nil, "v1"),
			"crates": named("Crate", "crates", []string{"bins"}, "v1"),
		},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		nil,
	)
	if _, lost := losses["crates"]; !lost || len(losses) != 1 {
		t.Errorf("resolveClaims() = %v, want just the second projection to lose", losses)
	}
}

// One projection serving several versions of one kind claims each name once,
// and is not in conflict with itself. That is the supported way to serve a
// kind at more than one version.
func TestResolveClaimsAcceptsOneProjectionWithSeveralVersions(t *testing.T) {
	losses := resolveClaims(
		map[string]compilation{
			"bins":   named("Bin", "bins", []string{"bn"}, "v1", "v1beta1", "v1alpha1"),
			"crates": named("Crate", "crates", []string{"cr"}, "v1", "v1beta1"),
		},
		map[string]metav1.Time{"bins": at(1), "crates": at(2)},
		map[string]compilation{"bins": named("Bin", "bins", []string{"bn"}, "v1", "v1beta1")},
	)
	if len(losses) != 0 {
		t.Errorf("resolveClaims() reported %v, want nothing", losses)
	}
}

// One projection's mistake must not be every projection's outage.
//
// A duplicate claim used to fail the whole rebuild, which returned from sync
// before c.compiled was replaced and before hasSynced was set. On a cold start
// the projections-synced readiness gate then never closed: the server served
// nothing, for any projection, and said so only in the log.
func TestADuplicateResourceDoesNotStopTheOtherProjections(t *testing.T) {
	first := projectionObject("alpha", "bins")
	first.CreationTimestamp = at(1)
	// The same plural, in the same group and version, as alpha.
	second := projectionObject("beta", "bins")
	second.CreationTimestamp = at(2)
	unrelated := projectionObject("crates", "crates")
	unrelated.CreationTimestamp = at(3)

	f := newFixture(t, []k8sruntime.Object{first, second, unrelated})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 2 })

	if !f.controller.HasSynced() {
		t.Error("the controller never reported synced, so readiness would never close")
	}

	paths := f.router.ServedPaths()
	if len(paths) != 2 {
		t.Fatalf("served paths = %v, want the winner and the unrelated projection", paths)
	}

	// beta is the one reported, and alpha keeps the resource: it was applied
	// first, so it is the one a client is already using.
	degraded := f.controller.Degraded()
	if len(degraded) != 1 || degraded[0] != "beta" {
		t.Errorf("Degraded() = %v, want [beta]", degraded)
	}
}

// Editing a projection to name a resource another one serves fails the edited
// projection, not the one that was serving.
//
// Ranked by "was serving anything", the older projection took the resource:
// beta had been serving items since before alpha existed, so beta was processed
// first and took orders too. alpha lost, its storage was retired and the API
// group it had been serving was withdrawn -- an outage started by an edit to
// a different object. The storage beta compiled for the edit is released as
// well: nothing routes to it, and c.compiled never held it, so the retirement
// pass would not find it.
func TestAnEditedProjectionDoesNotTakeAResourceAnotherServes(t *testing.T) {
	crispmetrics.Watchers.Reset()

	alpha := projectionObject("alpha", "orders")
	alpha.CreationTimestamp = at(9)
	beta := projectionObject("beta", "items")
	beta.CreationTimestamp = at(1)

	f := newFixture(t, []k8sruntime.Object{alpha, beta})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 2 })

	// beta is edited to serve orders, which alpha already does.
	edited := beta.DeepCopy()
	edited.Spec.Resource.Plural = "orders"
	edited.Spec.Resource.Kind = kindFor("orders")
	edited.Generation = 2
	if _, err := f.client.CrispV1alpha1().CustomResourceProjections().
		Update(context.Background(), edited, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("editing beta: %v", err)
	}
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if degraded := f.controller.Degraded(); degraded[0] != "beta" {
		t.Errorf("Degraded() = %v, want [beta]: the edited projection is the one at fault", degraded)
	}
	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/orders]"; got != want {
		t.Errorf("served paths = %s, want %s: alpha keeps what it was serving", got, want)
	}

	// Closing a watch cache zeroes its watchers gauge, which is the trace a
	// released storage leaves: one for the items storage beta had, and one for
	// the orders storage it compiled and lost.
	if got := testutil.CollectAndCount(crispmetrics.Watchers); got != 2 {
		t.Errorf("%d storages were released, want both of beta's: the compilation that lost the claim was kept", got)
	}
}

// The loser's storage is released rather than left polling a table nobody
// reads.
//
// Claims are settled before the retirement pass for this reason: a projection
// dropped after it would keep its pool and its watch cache with no route to
// them.
func TestALostClaimReleasesItsPool(t *testing.T) {
	first := projectionObject("alpha", "bins")
	first.CreationTimestamp = at(1)

	f := newFixture(t, []k8sruntime.Object{first})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	// beta arrives and loses.
	second := projectionObject("beta", "bins")
	second.CreationTimestamp = at(2)
	if _, err := f.client.CrispV1alpha1().CustomResourceProjections().
		Create(context.Background(), second, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the second projection: %v", err)
	}
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if got := len(f.router.ServedPaths()); got != 1 {
		t.Errorf("served paths = %d, want the incumbent still serving alone", got)
	}
	if got := f.pools.Len(); got != 1 {
		t.Errorf("%d pools are open, want 1: the projection that lost kept its pool", got)
	}
}
