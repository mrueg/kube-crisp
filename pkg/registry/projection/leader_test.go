package projection

import (
	"testing"
	"time"
)

// TestFollowerPollsSlowly: polling is the one thing a projection does with no
// request behind it, so with several replicas it is the only load that
// multiplies for nothing. The leader keeps its interval; the others slow down.
//
// A follower slows down rather than stopping because a watcher is served from
// the cache of whichever replica it connected to — one that stopped polling
// would leave its watchers seeing nothing, with no error to notice.
func TestFollowerPollsSlowly(t *testing.T) {
	t.Cleanup(func() { SetLeadership(nil) })

	cache := newWatchCache(time.Second, "orders", nil, nil)
	cache.followerInterval = time.Minute

	// Leader election off: every replica polls at its own interval.
	SetLeadership(nil)
	if got := cache.effectiveInterval(); got != time.Second {
		t.Errorf("interval with leader election off = %v, want 1s", got)
	}

	leading := true
	SetLeadership(func() bool { return leading })

	if got := cache.effectiveInterval(); got != time.Second {
		t.Errorf("leader interval = %v, want 1s", got)
	}

	leading = false
	if got := cache.effectiveInterval(); got != time.Minute {
		t.Errorf("follower interval = %v, want 1m", got)
	}

	// A follower interval that is not actually slower would be no reduction,
	// and would only make a leader change visible for nothing.
	cache.followerInterval = time.Millisecond
	if got := cache.effectiveInterval(); got != time.Second {
		t.Errorf("interval = %v with a faster follower interval, want the configured 1s", got)
	}
}

// TestDueRespectsTheFollowerInterval checks the poller actually consults it.
func TestDueRespectsTheFollowerInterval(t *testing.T) {
	t.Cleanup(func() { SetLeadership(nil) })

	cache := newWatchCache(time.Millisecond, "orders", nil, nil)
	cache.followerInterval = time.Hour
	cache.lastPoll = time.Now()

	SetLeadership(func() bool { return true })
	time.Sleep(5 * time.Millisecond)
	if !cache.due(time.Millisecond) {
		t.Error("the leader was not due a poll after its interval elapsed")
	}

	SetLeadership(func() bool { return false })
	if cache.due(time.Millisecond) {
		t.Error("a follower was due a poll after 5ms, with an hour-long follower interval")
	}
}

// TestPollGroupTicksAtTheFollowerRate: consulting the follower interval when
// deciding whether a poll is due is only half of it. The group's timer was
// still set from the configured interval, so a follower woke at the leader's
// rate — once a second, per projection — to conclude every time that it had
// nothing to do.
func TestPollGroupTicksAtTheFollowerRate(t *testing.T) {
	t.Cleanup(func() { SetLeadership(nil) })

	group := newPollGroup(nil, "orders.store.example.com")
	cache := newWatchCache(time.Second, "orders", group, nil)
	cache.followerInterval = time.Minute

	group.mu.Lock()
	group.members[cache] = struct{}{}
	group.mu.Unlock()

	leading := true
	SetLeadership(func() bool { return leading })

	if got := group.desired(); got != time.Second {
		t.Errorf("the leader's group ticks at %v, want the configured 1s", got)
	}

	leading = false
	if got := group.desired(); got != time.Minute {
		t.Errorf("a follower's group ticks at %v, want the follower interval 1m", got)
	}

	leading = true
	if got := group.desired(); got != time.Second {
		t.Errorf("the group ticks at %v after winning the lease back, want 1s", got)
	}
}

// TestPollGroupTicksForItsFastestMember: the group is shared by every version
// of a projection, and a member wanting a slower poll skips ticks rather than
// slowing everyone down.
func TestPollGroupTicksForItsFastestMember(t *testing.T) {
	t.Cleanup(func() { SetLeadership(nil) })
	SetLeadership(nil)

	group := newPollGroup(nil, "orders.store.example.com")
	quick := newWatchCache(time.Second, "orders", group, nil)
	slow := newWatchCache(30*time.Second, "orders", group, nil)

	group.mu.Lock()
	group.members[quick] = struct{}{}
	group.members[slow] = struct{}{}
	group.mu.Unlock()

	if got := group.desired(); got != time.Second {
		t.Errorf("the group ticks at %v, want its fastest member's 1s", got)
	}
}
