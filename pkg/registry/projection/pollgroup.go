package projection

import (
	"context"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// Shared is the state every version of one projection has in common.
//
// A kind served at two versions is still one table. Reads and polls that differ
// only in how the rows will be mapped should cost the database once, not once
// per version.
type Shared struct {
	flights *flightGroup
	polls   *pollGroup
}

// Notifier subscribes to a database's change notifications, returning a channel
// that receives a wake-up whenever something may have changed.
//
// It is a function rather than a channel because a subscription holds a
// connection, and nothing should hold one for a projection nobody is watching.
// The poll group calls it when its first watcher arrives and cancels the
// context when its last one leaves.
type Notifier func(ctx context.Context) (<-chan struct{}, error)

// NewShared creates the state to hand to each version of a projection.
//
// resource is the projected resource's label — plural.group — which is what
// every other metric in this package is keyed by. Passing the projection's name
// for both left kube_crisp_query_coalesced_total{resource=...} carrying
// something no other series did, so it could not be joined against any of them.
//
// notify is optional. Given one, a change wakes the poll instead of the timer
// waiting for it, which is the difference between a watch that lags by a poll
// interval and one that lags by a round trip.
func NewShared(projection, resource string, notify Notifier) *Shared {
	return &Shared{
		flights: newFlightGroup(projection, resource),
		polls:   newPollGroup(notify, resource),
	}
}

// pollGroup drives every watch cache of a projection from one timer.
//
// The point is not the timer. It is that the caches poll at the same instant,
// so their queries — identical, because the versions differ only in mapping —
// arrive together and are collapsed into one round trip by the flight group. On
// separate timers they would miss each other and read the table once per
// version.
type pollGroup struct {
	// notify subscribes to the database's change notifications, when the
	// projection has somewhere to subscribe to. resource labels its metrics.
	notify   Notifier
	resource string

	mu      sync.Mutex
	members map[*watchCache]struct{}
	tick    time.Duration
	cancel  context.CancelFunc
}

func newPollGroup(notify Notifier, resource string) *pollGroup {
	return &pollGroup{notify: notify, resource: resource, members: map[*watchCache]struct{}{}}
}

// add starts polling a cache, starting the timer if it is the first.
func (g *pollGroup) add(c *watchCache) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.members[c] = struct{}{}
	g.retimeLocked()
}

// remove stops polling a cache, stopping the timer once none are left.
func (g *pollGroup) remove(c *watchCache) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.members, c)
	if len(g.members) == 0 {
		if g.cancel != nil {
			g.cancel()
			g.cancel = nil
		}
		g.tick = 0
		return
	}
	g.retimeLocked()
}

// retimeLocked runs the timer at the shortest interval any member asked for.
// A member wanting a slower poll simply skips the ticks that come too soon.
func (g *pollGroup) retimeLocked() {
	shortest := g.shortestLocked()
	if shortest == g.tick && g.cancel != nil {
		return
	}

	if g.cancel != nil {
		g.cancel()
		g.cancel = nil
	}
	g.tick = shortest

	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel

	// Subscribed alongside the timer and cancelled with it, so a projection
	// nobody is watching holds no connection — the same property polling has.
	var notifications <-chan struct{}
	if g.notify != nil {
		subscribed, err := g.notify(ctx)
		if err != nil {
			// Not fatal: the timer is still the thing that guarantees a poll
			// happens, and a notification only ever made it happen sooner.
			klog.ErrorS(err, "could not subscribe to change notifications; polling on the timer alone",
				"resource", g.resource)
		} else {
			notifications = subscribed
		}
	}

	go g.run(ctx, shortest, notifications)
}

// shortestLocked is the shortest interval any member currently wants, or zero
// when there are none.
//
// It asks for the effective interval rather than the configured one, so a
// replica that does not hold the polling lease actually ticks at the follower
// rate instead of ticking at the leader's and deciding, every time, that it has
// nothing to do.
func (g *pollGroup) shortestLocked() time.Duration {
	shortest := time.Duration(0)
	for member := range g.members {
		if interval := member.effectiveInterval(); shortest == 0 || interval < shortest {
			shortest = interval
		}
	}
	return shortest
}

// desired is the tick the group would choose right now.
func (g *pollGroup) desired() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.shortestLocked()
}

// run ticks until cancelled.
//
// The interval is recomputed after every round rather than fixed when the timer
// started. Membership changes go through retimeLocked, but leadership does not:
// winning or losing the lease changes how often every member wants to poll, and
// without this the group would keep whichever rate it happened to start with
// until a watcher arrived or left.
func (g *pollGroup) run(ctx context.Context, tick time.Duration, notifications <-chan struct{}) {
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case _, ok := <-notifications:
			if !ok {
				// The subscription gave up for good. The timer carries on,
				// which is what it was always the backstop for.
				notifications = nil
				continue
			}
			crispmetrics.WatchNotifications.WithLabelValues(g.resource).Inc()

			// Every member polls, not only the ones the timer would have judged
			// due: a notification says the data moved, and a cache that skips
			// this round would sit on a snapshot it has been told is stale.
			g.pollAll(ctx, tick)

			// The timer restarts from here, so a stream of changes does not also
			// produce a poll on the old schedule immediately afterwards.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(tick)

		case <-timer.C:
			g.pollOnce(ctx, tick)

			next := g.desired()
			if next <= 0 {
				// The last member left while this round was running; remove has
				// already cancelled this goroutine's context.
				return
			}
			tick = next
			timer.Reset(tick)
		}
	}
}

// pollAll refreshes every member regardless of whether the timer would have
// judged it due, which is what a notification asks for.
func (g *pollGroup) pollAll(ctx context.Context, tick time.Duration) {
	g.poll(ctx, tick, false)
}

// pollOnce refreshes every member due for a poll, concurrently, so their
// queries overlap and can be shared.
func (g *pollGroup) pollOnce(ctx context.Context, tick time.Duration) {
	g.poll(ctx, tick, true)
}

func (g *pollGroup) poll(ctx context.Context, tick time.Duration, onlyDue bool) {
	g.mu.Lock()
	members := make([]*watchCache, 0, len(g.members))
	for member := range g.members {
		members = append(members, member)
	}
	g.mu.Unlock()

	var wg sync.WaitGroup
	for _, member := range members {
		if onlyDue && !member.due(tick) {
			continue
		}
		wg.Add(1)
		go func(c *watchCache) {
			defer wg.Done()
			err := c.refresh(ctx)
			if err == nil {
				return
			}

			// A watch that stops advancing is indistinguishable from a
			// projection where nothing is happening, so a failed poll is
			// counted rather than only logged. Shedding is separated out
			// because it points at this server's own limit rather than at the
			// database.
			reason := crispmetrics.PollFailed
			if apierrors.IsTooManyRequests(err) {
				reason = crispmetrics.PollShed
			}
			crispmetrics.WatchPollErrors.WithLabelValues(c.resource, reason).Inc()
			klog.V(2).InfoS("watch poll failed", "resource", c.resource, "reason", reason, "err", err)
		}(member)
	}
	wg.Wait()
}

// members reports how many caches are being polled, for tests.
func (g *pollGroup) size() int {
	if g == nil {
		return 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.members)
}
