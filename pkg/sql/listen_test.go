package sql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// fakeNotifier stands in for a pgx connection, so the delivery loop can be
// tested without a database behind it.
type fakeNotifier struct {
	subscribed chan string
	deliver    chan struct{}
	fail       error
}

func (f *fakeNotifier) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.subscribed <- sql
	return pgconn.CommandTag{}, nil
}

func (f *fakeNotifier) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _, ok := <-f.deliver:
		if !ok {
			return nil, f.fail
		}
		return &pgconn.Notification{Channel: "orders_changed"}, nil
	}
}

func TestValidateNotifyChannel(t *testing.T) {
	for _, name := range []string{"orders_changed", "o", "_private", "a1", "Orders"} {
		if err := ValidateNotifyChannel(name); err != nil {
			t.Errorf("ValidateNotifyChannel(%q) = %v, want it accepted", name, err)
		}
	}
	for _, name := range []string{
		"", "1orders", "orders changed", "orders-changed", "orders;DROP TABLE orders",
		`orders"; LISTEN "x`, "app.orders",
	} {
		if err := ValidateNotifyChannel(name); err == nil {
			t.Errorf("ValidateNotifyChannel(%q) was accepted", name)
		}
	}
	// Longer than an identifier may be.
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'a'
	}
	if err := ValidateNotifyChannel(string(long)); err == nil {
		t.Error("a 64-character channel was accepted")
	}
}

// TestQuoteIdentifier covers the other half of that defence: the name reaches
// the statement quoted, so even a validation that let something through could
// not close the identifier and start a new statement.
func TestQuoteIdentifier(t *testing.T) {
	for input, want := range map[string]string{
		"orders_changed": `"orders_changed"`,
		"Orders":         `"Orders"`,
		`a"b`:            `"a""b"`,
		`"; DROP`:        `"""; DROP"`,
	} {
		if got := quoteIdentifier(input); got != want {
			t.Errorf("quoteIdentifier(%q) = %s, want %s", input, got, want)
		}
	}
}

// TestNotificationsSubscribeAndDeliver is the loop's contract: subscribe once,
// then turn every notification into a wake-up.
func TestNotificationsSubscribeAndDeliver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &fakeNotifier{subscribed: make(chan string, 1), deliver: make(chan struct{})}
	notifications := make(chan struct{}, 1)

	done := make(chan error, 1)
	go func() { done <- waitForNotifications(ctx, conn, "orders_changed", notifications, nil) }()

	select {
	case statement := <-conn.subscribed:
		if statement != `LISTEN "orders_changed"` {
			t.Errorf("subscribed with %q", statement)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never subscribed")
	}

	conn.deliver <- struct{}{}
	select {
	case <-notifications:
	case <-time.After(5 * time.Second):
		t.Fatal("a notification produced no wake-up")
	}

	// A cancelled context is a clean stop, not a failure to report.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("waitForNotifications() = %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop when the context was cancelled")
	}
}

// TestNotificationsCoalesce: the channel holds one, and a notification arriving
// while another is already waiting says the same thing it does. Blocking here
// would stall the connection every notification is delivered on.
func TestNotificationsCoalesce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &fakeNotifier{subscribed: make(chan string, 1), deliver: make(chan struct{})}
	notifications := make(chan struct{}, 1)

	go func() { _ = waitForNotifications(ctx, conn, "orders_changed", notifications, nil) }()
	<-conn.subscribed

	// Nobody is reading, so all but the first have nowhere to go.
	for i := 0; i < 20; i++ {
		conn.deliver <- struct{}{}
	}

	if len(notifications) != 1 {
		t.Errorf("%d wake-ups are queued, want the one they all mean", len(notifications))
	}
	<-notifications
}

// TestNotificationsReportAConnectionThatGaveUp, since a subscription that
// silently stopped delivering looks exactly like a table where nothing changes.
func TestNotificationsReportAConnectionThatGaveUp(t *testing.T) {
	conn := &fakeNotifier{
		subscribed: make(chan string, 1),
		deliver:    make(chan struct{}),
		fail:       errors.New("connection reset by peer"),
	}
	notifications := make(chan struct{}, 1)

	done := make(chan error, 1)
	go func() { done <- waitForNotifications(context.Background(), conn, "orders_changed", notifications, nil) }()
	<-conn.subscribed

	close(conn.deliver)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a dropped connection was reported as a clean stop")
		}
		if !errors.Is(err, conn.fail) {
			t.Errorf("error = %v, want it to wrap the connection failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return when the connection failed")
	}
}

// TestListenRefusesADriverThatCannotDeliver: SQLite has no notifications, and a
// projection asking for them there should be told rather than left waiting.
func TestListenRefusesADriverThatCannotDeliver(t *testing.T) {
	pool := newTestPool(t, false)

	if _, err := pool.Listen(context.Background(), "orders_changed"); err == nil {
		t.Error("Listen() was accepted on a driver that cannot deliver notifications")
	}
}

// TestListenLoopReconnects covers the behaviour that made a dead subscription
// invisible: the loop has to keep trying.
//
// A loop that gave up on the first error would leave every watch on that
// projection polling its timer for the lifetime of the process. Nothing else
// would report it — the poll underneath keeps every request working, and the
// notification counter simply stops climbing, which is what a table nobody
// writes to also looks like.
func TestListenLoopReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := make(chan int, 8)
	var stops []error

	var count int
	go listenLoop(ctx, time.Millisecond,
		func(context.Context) error {
			count++
			attempts <- count
			return fmt.Errorf("connection reset (attempt %d)", count)
		},
		func(err error) { stops = append(stops, err) },
	)

	// Three attempts is enough to show it is a loop rather than a retry.
	for want := 1; want <= 3; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("attempt %d, want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("the listener stopped after %d attempt(s); a dropped subscription is "+
				"never re-established", want-1)
		}
	}
}

// TestListenLoopStopsWhenTheContextIsDone: shutdown is not a dropped
// subscription, and reporting it as one would make every restart look like a
// fault in whatever is watching the reconnect counter.
func TestListenLoopStopsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	returned := make(chan struct{})
	var stops int

	go func() {
		defer close(returned)
		listenLoop(ctx, time.Hour,
			func(ctx context.Context) error {
				// A real attempt blocks until the connection or the context
				// gives out.
				<-ctx.Done()
				return ctx.Err()
			},
			func(error) { stops++ },
		)
	}()

	cancel()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the listener did not stop when its context was cancelled; it holds a " +
			"connection open for the lifetime of the process")
	}

	if stops != 0 {
		t.Errorf("a cancelled context was reported as %d dropped subscription(s), so an "+
			"ordinary shutdown would read as a fault", stops)
	}
}

// TestListenLoopWaitsBetweenAttempts, so a database that refuses connections
// instantly is not reconnected to in a spin.
func TestListenLoopWaitsBetweenAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := make(chan struct{}, 4)
	go listenLoop(ctx, 250*time.Millisecond,
		func(context.Context) error {
			select {
			case attempts <- struct{}{}:
			default:
			}
			return errors.New("connection refused")
		},
		func(error) {},
	)

	// Bounded receives. A bare <-attempts here would hang for the whole test
	// binary's timeout if the loop stopped retrying, reporting ten minutes
	// later and as a timeout rather than as this assertion.
	awaitAttempt(t, attempts, "first")
	start := time.Now()
	awaitAttempt(t, attempts, "second")

	// Comfortably under the interval, so this fails on a spin rather than on a
	// slow machine.
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("the second attempt came %v after the first, which is a reconnect spin "+
			"against a database that is refusing connections", elapsed)
	}
}

func awaitAttempt(t *testing.T, attempts <-chan struct{}, which string) {
	t.Helper()

	select {
	case <-attempts:
	case <-time.After(5 * time.Second):
		t.Fatalf("the %s reconnect attempt never came", which)
	}
}

// TestSubscribedIsReportedOnlyAfterListenSucceeds covers what the connected
// gauge is allowed to claim.
//
// Taking a connection is not the same as subscribing on it. A connection that
// was opened but whose LISTEN failed delivers nothing at all, which is the one
// state the gauge exists to rule out — so reporting it as connected would make
// the metric agree with the failure it was added to expose.
func TestSubscribedIsReportedOnlyAfterListenSucceeds(t *testing.T) {
	t.Run("after a successful LISTEN", func(t *testing.T) {
		conn := &fakeNotifier{subscribed: make(chan string, 1), deliver: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reported := make(chan struct{}, 1)
		go func() {
			_ = waitForNotifications(ctx, conn, "orders_changed", make(chan struct{}, 1),
				func() { reported <- struct{}{} })
		}()

		select {
		case <-reported:
		case <-time.After(5 * time.Second):
			t.Fatal("a live subscription was never reported as connected, so the gauge stays " +
				"at 0 while notifications are arriving")
		}
	})

	t.Run("not when LISTEN fails", func(t *testing.T) {
		conn := &failingNotifier{err: errors.New("permission denied for channel")}

		var reported bool
		err := waitForNotifications(context.Background(), conn, "orders_changed",
			make(chan struct{}, 1), func() { reported = true })

		if err == nil {
			t.Fatal("a failed LISTEN was reported as success")
		}
		if reported {
			t.Error("a subscription that never subscribed was reported as connected, which is " +
				"the state the gauge exists to distinguish")
		}
	})
}

// failingNotifier refuses the LISTEN.
type failingNotifier struct{ err error }

func (f *failingNotifier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.err
}

func (f *failingNotifier) WaitForNotification(context.Context) (*pgconn.Notification, error) {
	return nil, f.err
}

// TestListenerMetricsAreRemovedWhenTheSubscriptionGoesAway covers the
// difference between a subscription that is down and one that is gone.
//
// The gauge is 0 while a subscription exists but is not connected, which is
// what KubeCrispNotificationListenerDown fires on. Deleting a projection, or
// taking watch.notify off it, ends the subscription — and a gauge left behind
// at 0 would go on saying "configured for notifications and not receiving them"
// about something no longer configured for anything, so the alert would fire
// for it for as long as the process ran.
func TestListenerMetricsAreRemovedWhenTheSubscriptionGoesAway(t *testing.T) {
	crispmetrics.WatchListenerConnected.Reset()
	crispmetrics.WatchListenerReconnects.Reset()

	// Never connects: Open does not verify connectivity, so the subscription
	// starts, fails, and retries — which is the state the gauge reports as 0.
	pool, err := Open(PoolOptions{Driver: "postgres", DSN: "postgres://nobody@127.0.0.1:1/nothing"})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	notifications, err := pool.Listen(ctx, "orders_changed")
	if err != nil {
		t.Fatalf("Listen() returned error: %v", err)
	}

	if got := seriesCount(t, crispmetrics.WatchListenerConnected, "kube_crisp_watch_listener_connected"); got != 1 {
		t.Fatalf("%d connected series while a subscription exists, want 1 — a subscription "+
			"that never connects has to read as down rather than as absent", got)
	}

	cancel()

	// The goroutine owns the metric, so wait for it rather than racing it. It
	// closes the channel on its way out.
	select {
	case <-notifications:
	case <-time.After(10 * time.Second):
		t.Fatal("the listener goroutine did not stop when its context was cancelled")
	}

	if got := seriesCount(t, crispmetrics.WatchListenerConnected, "kube_crisp_watch_listener_connected"); got != 0 {
		t.Errorf("%d connected series after the subscription was torn down, want none; "+
			"KubeCrispNotificationListenerDown would fire for it forever", got)
	}
	if got := seriesCount(t, crispmetrics.WatchListenerReconnects, "kube_crisp_watch_listener_reconnects_total"); got != 0 {
		t.Errorf("%d reconnect series after teardown, want none", got)
	}
}

func seriesCount(t *testing.T, collector prometheus.Collector, name string) int {
	t.Helper()
	return prometheustestutil.CollectAndCount(collector, name)
}
