package projection

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

func unversionedProjection(name, version string) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = "store.example.com"
	p.Spec.Resource.Plural = "orders"
	p.Spec.Mapping.Name = "id"
	p.Spec.Mapping.ResourceVersion = version
	return p
}

const unversionedMetric = "kube_crisp_projections_unversioned"

// TestUnversionedProjectionIsReportedWithPeers covers a failure that was
// documented as a limitation and checked by nothing.
//
// The resourceVersion a list reports comes from the data when a version column
// is mapped, so every replica agrees. Without one it comes from a per-process
// counter, and two replicas then give the same client versions that mean
// different things — a watch resumed against the other replica replays what the
// client has or skips what it does not. It looks like a client bug from every
// angle except this one.
func TestUnversionedProjectionIsReportedWithPeers(t *testing.T) {
	crispmetrics.ProjectionsUnversioned.Reset()

	c := &Controller{hasPeers: true, warnedUnversioned: map[string]bool{}}
	c.warnIfUnversioned(unversionedProjection("orders", ""))

	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 1 {
		t.Errorf("%d projections reported as unversioned, want 1", got)
	}
}

// TestVersionedProjectionIsNotReported, so the signal means something.
func TestVersionedProjectionIsNotReported(t *testing.T) {
	crispmetrics.ProjectionsUnversioned.Reset()

	c := &Controller{hasPeers: true, warnedUnversioned: map[string]bool{}}
	c.warnIfUnversioned(unversionedProjection("orders", "updated_at"))

	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 0 {
		t.Errorf("a projection that maps a resourceVersion was reported as unsafe (%d series)", got)
	}
}

// TestUnversionedProjectionIsFineAlone: one replica derives its versions from
// its own counter and nothing disagrees with it, which is a supported way to
// run. Warning about it would teach people to ignore the warning.
func TestUnversionedProjectionIsFineAlone(t *testing.T) {
	crispmetrics.ProjectionsUnversioned.Reset()

	c := &Controller{hasPeers: false, warnedUnversioned: map[string]bool{}}
	c.warnIfUnversioned(unversionedProjection("orders", ""))

	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 0 {
		t.Errorf("a single-replica server reported %d unsafe projection(s), want none", got)
	}
}

// TestUnversionedWarningIsNotRepeated. A sync runs whenever anything changes,
// and a warning on every one of them is a warning nobody reads.
func TestUnversionedWarningIsNotRepeated(t *testing.T) {
	crispmetrics.ProjectionsUnversioned.Reset()

	c := &Controller{hasPeers: true, warnedUnversioned: map[string]bool{}}
	for range 5 {
		c.warnIfUnversioned(unversionedProjection("orders", ""))
	}

	if !c.warnedUnversioned["orders"] {
		t.Fatal("the projection was never recorded as warned about")
	}
	// The gauge is set every time; only the log line is suppressed, so the
	// metric stays true after a restart of the sync loop.
	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 1 {
		t.Errorf("%d series after five syncs, want 1", got)
	}
}

// TestUnversionedGaugeClearsWhenFixed. A gauge that can only be set cannot
// report that the condition ended, so an alert on it would keep firing after
// the resourceVersion column was added — until the process restarted.
func TestUnversionedGaugeClearsWhenFixed(t *testing.T) {
	crispmetrics.ProjectionsUnversioned.Reset()

	c := &Controller{hasPeers: true, warnedUnversioned: map[string]bool{}}
	c.warnIfUnversioned(unversionedProjection("orders", ""))
	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 1 {
		t.Fatalf("%d series after the unsafe projection, want 1", got)
	}

	// The same projection, now mapping a version.
	c.warnIfUnversioned(unversionedProjection("orders", "updated_at"))
	if got := testutil.CollectAndCount(crispmetrics.ProjectionsUnversioned, unversionedMetric); got != 0 {
		t.Errorf("%d series after the projection was fixed, want none — the alert would keep "+
			"firing for a condition that has ended", got)
	}
}
