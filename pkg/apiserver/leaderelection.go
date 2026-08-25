package apiserver

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	projectionregistry "github.com/mrueg/kube-crisp/pkg/registry/projection"
)

// LeaderElectionOptions configures the lease that decides which replica polls.
type LeaderElectionOptions struct {
	// Enabled turns it on. Off, every replica polls at its projection's own
	// interval, which is the behaviour this server has always had.
	Enabled bool

	// Namespace and Name locate the Lease.
	Namespace string
	Name      string

	// Identity distinguishes this replica in the Lease. Defaults to the
	// hostname, which for a Pod is the Pod name.
	Identity string

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// DefaultLeaderElectionOptions returns the conventional settings.
func DefaultLeaderElectionOptions() LeaderElectionOptions {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "kube-crisp"
	}

	return LeaderElectionOptions{
		Namespace:     namespace,
		Name:          "kube-crisp-poller",
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
	}
}

// runLeaderElection keeps this replica's view of whether it leads up to date.
//
// Nothing about serving depends on the outcome: every replica answers every
// request either way. The lease decides only how often this one re-runs the
// watch queries, which is the single piece of work a projection does with no
// request behind it and therefore the only load that N replicas multiply for
// nothing.
//
// Losing the lease is deliberately not fatal. A server that stopped on losing
// it would take a working API down over a renewal it could not make; all that
// is at stake here is a poll interval, so a replica that loses the lease slows
// down and carries on.
func runLeaderElection(ctx context.Context, client kubernetes.Interface, opts LeaderElectionOptions) error {
	identity := opts.Identity
	if identity == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("determining this replica's identity: %w", err)
		}
		identity = hostname
	}

	var leader atomic.Bool
	projectionregistry.SetLeadership(leader.Load)

	// Published from the start, so a replica that never wins reads 0 rather
	// than being absent — a missing series and a follower look the same on a
	// dashboard, and only one of them is a problem.
	lease := opts.Namespace + "/" + opts.Name
	crispmetrics.PollLeader.WithLabelValues(lease).Set(0)

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Namespace: opts.Namespace, Name: opts.Name},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: opts.LeaseDuration,
		RenewDeadline: opts.RenewDeadline,
		RetryPeriod:   opts.RetryPeriod,
		// The poll rate is all that changes, so giving up the lease cleanly on
		// shutdown lets the next replica speed up immediately.
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				leader.Store(true)
				crispmetrics.PollLeader.WithLabelValues(lease).Set(1)
				klog.InfoS("leading: this replica polls watched projections at their configured interval",
					"lease", lease, "identity", identity)
			},
			OnStoppedLeading: func() {
				leader.Store(false)
				crispmetrics.PollLeader.WithLabelValues(lease).Set(0)
				klog.InfoS("no longer leading: this replica falls back to the follower poll interval",
					"lease", lease, "identity", identity)
			},
		},
	})
	if err != nil {
		return fmt.Errorf("building the leader elector: %w", err)
	}

	// Run returns when the context is cancelled; it is restarted by the caller
	// only on shutdown, so a lost election simply leaves this replica a
	// follower until it wins one.
	go func() {
		for {
			elector.Run(ctx)
			if ctx.Err() != nil {
				return
			}
			// Run returns on losing the lease too. Re-entering the election is
			// what lets this replica take it back later.
			leader.Store(false)
			crispmetrics.PollLeader.WithLabelValues(lease).Set(0)
		}
	}()
	return nil
}
