package apiserver

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/component-base/metrics/testutil"

	"k8s.io/client-go/kubernetes/fake"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	projectionregistry "github.com/mrueg/kube-crisp/pkg/registry/projection"
)

// fastElection keeps the test short. The library needs RenewDeadline to fit
// inside LeaseDuration and RetryPeriod inside RenewDeadline.
func fastElection(name string) LeaderElectionOptions {
	return LeaderElectionOptions{
		Enabled:       true,
		Namespace:     "kube-crisp",
		Name:          name,
		Identity:      "replica-one",
		LeaseDuration: 300 * time.Millisecond,
		RenewDeadline: 200 * time.Millisecond,
		RetryPeriod:   40 * time.Millisecond,
	}
}

// leadershipWithin waits for the process's leadership to reach want.
func leadershipWithin(t *testing.T, want bool, within time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if projectionregistry.Leading() == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return projectionregistry.Leading() == want
}

// TestLeaderElectionTakesTheLease is the ordinary case: nothing holds the
// lease, so this replica takes it and polls at the configured interval.
func TestLeaderElectionTakesTheLease(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })

	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runLeaderElection(ctx, client, fastElection("takes-the-lease")); err != nil {
		t.Fatalf("runLeaderElection() returned error: %v", err)
	}

	if !leadershipWithin(t, true, 5*time.Second) {
		t.Fatal("the replica never took an uncontested lease")
	}

	lease, err := client.CoordinationV1().Leases("kube-crisp").
		Get(ctx, "takes-the-lease", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the lease: %v", err)
	}
	if holder := lease.Spec.HolderIdentity; holder == nil || *holder != "replica-one" {
		t.Errorf("lease holder = %v, want replica-one", holder)
	}
}

// TestFollowerDoesNotClaimALeaseSomeoneElseHolds: the whole point is that only
// one replica polls at the full rate, so a replica that finds the lease taken
// has to stay a follower.
func TestFollowerDoesNotClaimALeaseSomeoneElseHolds(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })

	other := "replica-two"
	held := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-crisp", Name: "already-held"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &other,
			LeaseDurationSeconds: ptr(int32(3600)),
			AcquireTime:          &metav1.MicroTime{Time: time.Now()},
			RenewTime:            &metav1.MicroTime{Time: time.Now()},
		},
	}

	client := fake.NewSimpleClientset(held)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runLeaderElection(ctx, client, fastElection("already-held")); err != nil {
		t.Fatalf("runLeaderElection() returned error: %v", err)
	}

	// Long enough for several acquisition attempts.
	time.Sleep(500 * time.Millisecond)

	if projectionregistry.Leading() {
		t.Error("the replica claimed leadership while another holds the lease; every replica would poll at full rate")
	}
}

// TestLosingTheLeaseIsNotTerminal is the failure mode worth guarding.
//
// A replica that loses the lease and never re-enters the election would stay on
// the follower interval forever — its watchers permanently lagging, with
// nothing to indicate why. Losing it has to put the replica back in the race.
func TestLosingTheLeaseIsNotTerminal(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })

	client := fake.NewSimpleClientset()

	// Renewals start failing once the test says so, which is how a replica
	// loses a lease it holds.
	var failing atomic.Bool
	client.PrependReactor("update", "leases",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			if failing.Load() {
				return true, nil, apierrors.NewInternalError(errRenewalRefused)
			}
			return false, nil, nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runLeaderElection(ctx, client, fastElection("loses-the-lease")); err != nil {
		t.Fatalf("runLeaderElection() returned error: %v", err)
	}
	if !leadershipWithin(t, true, 5*time.Second) {
		t.Fatal("the replica never took the lease")
	}

	failing.Store(true)
	if !leadershipWithin(t, false, 5*time.Second) {
		t.Fatal("the replica kept reporting itself the leader after its renewals failed")
	}

	// And once renewals work again it can take the lease back, which is what
	// re-entering the election buys.
	failing.Store(false)
	if !leadershipWithin(t, true, 10*time.Second) {
		t.Fatal("the replica never regained the lease; it did not re-enter the election")
	}
}

// TestLeaderElectionIdentityDefaultsToTheHostname, which for a Pod is its name.
func TestLeaderElectionIdentityDefaultsToTheHostname(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })

	hostname, err := os.Hostname()
	if err != nil {
		t.Skipf("no hostname available: %v", err)
	}

	opts := fastElection("default-identity")
	opts.Identity = ""

	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runLeaderElection(ctx, client, opts); err != nil {
		t.Fatalf("runLeaderElection() returned error: %v", err)
	}
	if !leadershipWithin(t, true, 5*time.Second) {
		t.Fatal("the replica never took the lease")
	}

	lease, err := client.CoordinationV1().Leases("kube-crisp").
		Get(ctx, "default-identity", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the lease: %v", err)
	}
	if holder := lease.Spec.HolderIdentity; holder == nil || *holder != hostname {
		t.Errorf("lease holder = %v, want the hostname %q", holder, hostname)
	}
}

// TestLeadershipDefaultsToLeadingWithoutElection: with leader election off,
// nothing calls SetLeadership and every replica polls at its own interval,
// which is what this server did before there was an election at all.
func TestLeadershipDefaultsToLeadingWithoutElection(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })

	projectionregistry.SetLeadership(nil)
	if !projectionregistry.Leading() {
		t.Error("a server with no leader election reported itself a follower")
	}
}

func ptr[T any](v T) *T { return &v }

// errRenewalRefused stands in for whatever an API server would say.
var errRenewalRefused = &refusedError{}

type refusedError struct{}

func (*refusedError) Error() string { return "renewal refused" }

// TestDefaultLeaderElectionOptions: the lease lives in the server's own
// namespace, which it learns the same way everything else does.
func TestDefaultLeaderElectionOptions(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "crisp-system")
	opts := DefaultLeaderElectionOptions()

	if opts.Enabled {
		t.Error("leader election is on by default; it needs Lease permissions the base RBAC did not always grant")
	}
	if opts.Namespace != "crisp-system" {
		t.Errorf("namespace = %q, want the pod's own", opts.Namespace)
	}
	if opts.Name == "" {
		t.Error("the lease has no name")
	}

	// The library requires this ordering, and getting it wrong fails at
	// runtime rather than here.
	if opts.RetryPeriod >= opts.RenewDeadline || opts.RenewDeadline >= opts.LeaseDuration {
		t.Errorf("timings are not retry < renew < duration: %v, %v, %v",
			opts.RetryPeriod, opts.RenewDeadline, opts.LeaseDuration)
	}
}

// TestDefaultLeaderElectionNamespaceFallback: outside a Pod there is no
// POD_NAMESPACE, and the lease still has to land somewhere sensible.
func TestDefaultLeaderElectionNamespaceFallback(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	if got := DefaultLeaderElectionOptions().Namespace; got != "kube-crisp" {
		t.Errorf("namespace = %q with no POD_NAMESPACE, want kube-crisp", got)
	}
}

// TestLeadershipIsPublishedAsAMetric. A missing series and a follower look the
// same on a dashboard, and only one of them is a problem — so a replica that
// never wins has to read 0 rather than be absent.
func TestLeadershipIsPublishedAsAMetric(t *testing.T) {
	t.Cleanup(func() { projectionregistry.SetLeadership(nil) })
	crispmetrics.PollLeader.Reset()

	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := fastElection("metric-lease")
	if err := runLeaderElection(ctx, client, opts); err != nil {
		t.Fatalf("runLeaderElection() returned error: %v", err)
	}

	lease := opts.Namespace + "/" + opts.Name
	gauge := crispmetrics.PollLeader.WithLabelValues(lease)

	// Published from the start, before any election has been won.
	if _, err := testutil.GetGaugeMetricValue(gauge); err != nil {
		t.Fatalf("the gauge was absent before the election resolved: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if value, err := testutil.GetGaugeMetricValue(gauge); err == nil && value == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	value, _ := testutil.GetGaugeMetricValue(gauge)
	t.Errorf("the leadership gauge reads %v after taking the lease, want 1", value)
}
