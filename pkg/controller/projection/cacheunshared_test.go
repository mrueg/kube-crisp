package projection

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

func cachingProjection(name string, ttl time.Duration) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = "store.example.com"
	p.Spec.Resource.Plural = "orders"
	p.Spec.Mapping.Name = "id"
	if ttl > 0 {
		p.Spec.CacheTTL = &metav1.Duration{Duration: ttl}
	}
	return p
}

const cacheUnsharedMetric = "kube_crisp_projections_cache_unshared"

func cacheUnsharedCount(t *testing.T) int {
	t.Helper()
	return testutil.CollectAndCount(crispmetrics.ProjectionsCacheUnshared, cacheUnsharedMetric)
}

func newCacheController(hasPeers bool) *Controller {
	return &Controller{hasPeers: hasPeers, warnedCacheUnshared: map[string]bool{}}
}

// TestCachingProjectionIsReportedWithPeers covers the other half of what
// docs/reference.md now says out loud: the invalidation a write performs
// reaches the replica that served it and no other, so a read routed elsewhere
// can be answered from an entry older than that write.
//
// Reported rather than refused, because caching on a single replica is exactly
// correct — this is only a hazard once there are peers.
func TestCachingProjectionIsReportedWithPeers(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(true)
	c.warnIfCacheUnshared(cachingProjection("orders", 30*time.Second))

	if got := cacheUnsharedCount(t); got != 1 {
		t.Errorf("%d projections reported, want 1", got)
	}
}

// TestUncachedProjectionIsNotReported, so the signal means something.
func TestUncachedProjectionIsNotReported(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(true)
	c.warnIfCacheUnshared(cachingProjection("orders", 0))

	if got := cacheUnsharedCount(t); got != 0 {
		t.Errorf("%d projections reported for a projection with no cache, want 0", got)
	}
}

// TestCachingProjectionIsNotReportedWithoutPeers. One replica invalidates its
// own cache completely, which is the whole of the cache.
func TestCachingProjectionIsNotReportedWithoutPeers(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(false)
	c.warnIfCacheUnshared(cachingProjection("orders", 30*time.Second))

	if got := cacheUnsharedCount(t); got != 0 {
		t.Errorf("%d projections reported on a single-replica server, want 0", got)
	}
}

// TestZeroCacheTTLIsNotReported. A zero duration is no cache, and reporting it
// would mean an alert firing for a projection that caches nothing.
func TestZeroCacheTTLIsNotReported(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	p := cachingProjection("orders", 0)
	p.Spec.CacheTTL = &metav1.Duration{Duration: 0}

	c := newCacheController(true)
	c.warnIfCacheUnshared(p)

	if got := cacheUnsharedCount(t); got != 0 {
		t.Errorf("%d projections reported for cacheTTL: 0, want 0", got)
	}
}

// TestCacheWarningClearsWhenTheCacheIsRemoved. A gauge that can only be set
// cannot report that the condition ended, so an alert on it would fire until
// the process restarted — long after the projection was fixed.
func TestCacheWarningClearsWhenTheCacheIsRemoved(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(true)
	c.warnIfCacheUnshared(cachingProjection("orders", 30*time.Second))
	if got := cacheUnsharedCount(t); got != 1 {
		t.Fatalf("%d projections reported, want 1 before the fix", got)
	}

	c.warnIfCacheUnshared(cachingProjection("orders", 0))
	if got := cacheUnsharedCount(t); got != 0 {
		t.Fatalf("%d projections still reported after the cache was removed, want 0", got)
	}
	if c.warnedCacheUnshared["orders"] {
		t.Error("the projection is still marked as warned, so it would never warn again")
	}
}

// TestCacheWarningIsLoggedOnce, since a sync happens whenever anything changes
// and this would otherwise be most of the log.
func TestCacheWarningIsLoggedOnce(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(true)
	for i := 0; i < 3; i++ {
		c.warnIfCacheUnshared(cachingProjection("orders", 30*time.Second))
	}

	if !c.warnedCacheUnshared["orders"] {
		t.Fatal("the projection was not recorded as warned")
	}
	if got := cacheUnsharedCount(t); got != 1 {
		t.Errorf("%d series after three syncs, want 1", got)
	}
}

// TestNilProjectionIsIgnored: the candidate list carries one for a projection
// that failed to load, and warning about it would panic.
func TestNilProjectionIsIgnored(t *testing.T) {
	crispmetrics.ProjectionsCacheUnshared.Reset()

	c := newCacheController(true)
	c.warnIfCacheUnshared(nil)

	if got := cacheUnsharedCount(t); got != 0 {
		t.Errorf("%d projections reported for a nil projection, want 0", got)
	}
}
