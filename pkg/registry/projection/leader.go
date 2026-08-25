package projection

import (
	"sync/atomic"
	"time"
)

// DefaultFollowerPollInterval is how often a replica that is not the leader
// re-runs a projection's watch query.
//
// It is deliberately far slower than the leader's interval rather than off: a
// watcher is served from the cache of whichever replica it happened to connect
// to, so a follower that stopped polling would leave its watchers seeing
// nothing at all, with no error to notice.
const DefaultFollowerPollInterval = time.Minute

// leaderCheck reports whether this process currently holds the lease. Nil means
// nobody is asking the question — leader election is off — and every replica
// polls at its projection's own interval, which is the behaviour without it.
var leaderCheck atomic.Pointer[func() bool]

// SetLeadership tells the poller how to find out whether this process leads.
//
// Polling is the one thing a projection does with no request behind it, so with
// several replicas it is also the only load that multiplies for nothing. The
// leader keeps the configured interval; the others fall back to a slow one, and
// their watchers lag rather than stop.
func SetLeadership(isLeader func() bool) {
	if isLeader == nil {
		leaderCheck.Store(nil)
		return
	}
	leaderCheck.Store(&isLeader)
}

// Leading reports whether this process should poll at the full rate. It is
// exported so that the leader election that drives it can be tested against
// what the poller actually sees.
func Leading() bool { return leading() }

// leading reports whether this process should poll at the full rate.
func leading() bool {
	check := leaderCheck.Load()
	if check == nil {
		return true
	}
	return (*check)()
}
