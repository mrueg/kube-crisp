package projection

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
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
	if msg := losses["newcomer"].Error(); !strings.Contains(msg, "incumbent") ||
		!strings.Contains(msg, "bins.warehouse.example.com/v1alpha1") {
		t.Errorf("the error is %q; it should name the other projection and the resource", msg)
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
