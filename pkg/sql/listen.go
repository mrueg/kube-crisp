package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/klog/v2"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// listenRetryInterval is how long to wait before reconnecting a dropped
// listener.
//
// A notification is only ever a hint that something changed, and the poll timer
// is still running underneath — so a gap here costs latency on a change, not
// the change itself. That is what makes a plain interval the right answer
// rather than a backoff to tune.
const listenRetryInterval = 5 * time.Second

// ValidateNotifyChannel rejects a channel name that could not be used safely.
//
// LISTEN takes an identifier rather than a bind parameter, so the name goes
// into the statement text. It is quoted before it gets there, which is what
// actually makes it safe; this rejects the names that would be surprising even
// quoted, and gives a better error than the database would.
func ValidateNotifyChannel(name string) error {
	invalid := fmt.Errorf(
		"notify channel %q must be letters, digits and underscores, and start with a letter", name)

	if name == "" || len(name) > 63 {
		return invalid
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case isNameStart(c):
		case c >= '0' && c <= '9':
			if i == 0 {
				return invalid
			}
		default:
			return invalid
		}
	}
	return nil
}

// Listen subscribes to a database notification channel and reports each
// notification on the returned channel.
//
// What comes back is a wake-up, not data. A notification says "something
// changed, ask again"; it does not say what, and it cannot be relied on to
// arrive — PostgreSQL delivers notifications on commit and drops them if the
// connection goes away. Which is exactly the right shape for this: the poll
// that already knows how to read what changed just stops waiting for its timer.
//
// The channel has room for one, and a send that would block is dropped. Ten
// notifications arriving while a poll is running mean the same thing as one:
// read again when it finishes.
//
// The subscription holds a connection for as long as it lives, because that is
// what LISTEN is. It reconnects on its own; the returned channel is closed only
// when ctx is done.
func (p *Pool) Listen(ctx context.Context, channel string) (<-chan struct{}, error) {
	if !SupportsNotifications(p.driver) {
		return nil, fmt.Errorf("driver %q cannot deliver notifications", p.driver)
	}
	if err := ValidateNotifyChannel(channel); err != nil {
		return nil, err
	}

	driver, ok := Lookup(p.driver)
	if !ok {
		return nil, fmt.Errorf("unsupported driver %q", p.driver)
	}

	// A connection of its own rather than one out of the pool.
	//
	// LISTEN holds a session for as long as the subscription lives, so a
	// subscription that took a pooled connection would be a connection the
	// projection could never run a query on — and on a small pool that is every
	// connection it has. A pool of one with a listener on it answers no queries
	// at all, which is a configuration nothing forbids.
	//
	// The cost is one connection per watched projection beyond
	// dataSource.maxOpenConns, which bounds the connections doing query work.
	// No lifetime on it either: a subscription is meant to be long-lived, and
	// recycling the connection underneath one would drop it on a timer.
	// Through the pool's connector when it has one, so that a subscription on a
	// data source using dataSource.auth authenticates with a minted password
	// like every other connection. Opening it from the DSN instead would open
	// it with no password at all, and the failure would be a watch that never
	// receives a notification rather than anything that looks like a
	// credential.
	listener, err := p.openConnection(driver)
	if err != nil {
		return nil, fmt.Errorf("opening a connection to listen on: %w", err)
	}
	listener.SetMaxOpenConns(1)
	listener.SetMaxIdleConns(1)
	listener.SetConnMaxLifetime(0)
	listener.SetConnMaxIdleTime(0)

	notifications := make(chan struct{}, 1)

	datasource := p.metricLabel()
	connected := crispmetrics.WatchListenerConnected.WithLabelValues(datasource, channel)
	reconnects := crispmetrics.WatchListenerReconnects.WithLabelValues(datasource, channel)

	// Both published before the first attempt. A subscription that never
	// manages to connect has to read as down rather than as absent — a metric
	// that only appears once it works cannot report that it never did — and a
	// counter that springs into existence at its first increment gives rate()
	// nothing to measure the increase from.
	connected.Set(0)
	reconnects.Add(0)

	go func() {
		defer close(notifications)
		defer func() { _ = listener.Close() }()

		// Removed rather than set to 0. This goroutine only ends when ctx does,
		// which is the subscription being torn down — the projection deleted,
		// or notify taken off it. Leaving the gauge behind at 0 would say
		// "configured for notifications and not receiving them" about something
		// that is no longer configured for anything, and
		// KubeCrispNotificationListenerDown would then fire for it forever.
		//
		// The distinction the gauge has to keep is between a subscription that
		// is down and one that is gone. Down is 0, set on every failed attempt
		// below; gone is absent.
		defer func() {
			crispmetrics.WatchListenerConnected.DeleteLabelValues(datasource, channel)
			crispmetrics.WatchListenerReconnects.DeleteLabelValues(datasource, channel)
		}()

		attempts := 0
		listenLoop(ctx, listenRetryInterval, func(ctx context.Context) error {
			attempts++
			if attempts > 1 {
				reconnects.Inc()
			}
			return p.listenOnce(ctx, listener, channel, notifications, func() { connected.Set(1) })
		}, func(err error) {
			connected.Set(0)
			if err != nil {
				klog.V(2).InfoS("notification listener stopped; reconnecting",
					"channel", channel, "datasource", datasource,
					"retryAfter", listenRetryInterval, "err", err)
			}
		})
	}()

	return notifications, nil
}

// listenLoop runs attempt until ctx is done, waiting retry between attempts and
// reporting each one that ended to stopped.
//
// Extracted from Listen so this can be tested, which the version inside it
// could not be: everything it did was behind a real connection to a real
// PostgreSQL. The behaviour worth pinning is that a failed attempt is followed
// by another one — a loop that returned on the first error would leave every
// watch on that projection quietly polling its timer for the lifetime of the
// process, with the poll underneath keeping every request working, so nothing
// would be seen to be wrong.
//
// stopped is called for every attempt that returns, including the last one
// before ctx is done, and including a clean shutdown where err is nil.
func listenLoop(
	ctx context.Context,
	retry time.Duration,
	attempt func(context.Context) error,
	stopped func(err error),
) {
	for ctx.Err() == nil {
		err := attempt(ctx)
		if ctx.Err() != nil {
			// Shutting down. Reporting this as a dropped subscription would
			// have every restart look like a fault.
			return
		}
		stopped(err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

// listenOnce holds one connection and delivers from it until it fails.
func (p *Pool) listenOnce(
	ctx context.Context,
	listener *sql.DB,
	channel string,
	notifications chan<- struct{},
	subscribed func(),
) error {
	// LISTEN is a property of a session, so a notification only reaches whoever
	// is still on the connection that subscribed.
	conn, err := listener.Conn(ctx)
	if err != nil {
		return fmt.Errorf("taking a connection to listen on: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// The whole subscription runs inside Raw, because the driver connection may
	// not be used once the callback returns.
	return conn.Raw(func(driverConn any) error {
		pgxConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("connection is %T, not a pgx connection", driverConn)
		}
		return waitForNotifications(ctx, pgxConn.Conn(), channel, notifications, subscribed)
	})
}

// notifier is the part of a pgx connection this needs, so the loop can be
// tested without a database.
type notifier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
}

// waitForNotifications subscribes and then delivers until the connection or the
// context gives out.
func waitForNotifications(
	ctx context.Context,
	conn notifier,
	channel string,
	notifications chan<- struct{},
	subscribed func(),
) error {
	// Quoted rather than interpolated: the name is validated above, and this is
	// what makes that a second line of defence rather than the only one.
	if _, err := conn.Exec(ctx, `LISTEN `+quoteIdentifier(channel)); err != nil {
		return fmt.Errorf("subscribing to %q: %w", channel, err)
	}
	// After the LISTEN rather than after the connection: a connection that was
	// taken but never subscribed delivers nothing, and reporting it as
	// connected would be reporting the one state this metric exists to rule out.
	if subscribed != nil {
		subscribed()
	}
	klog.V(2).InfoS("listening for change notifications", "channel", channel)

	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("waiting on %q: %w", channel, err)
		}

		// Room for one. A notification that arrives while another is already
		// waiting says the same thing the waiting one does.
		select {
		case notifications <- struct{}{}:
		default:
		}
	}
}

// quoteIdentifier renders a name as a quoted SQL identifier.
func quoteIdentifier(name string) string {
	out := make([]byte, 0, len(name)+2)
	out = append(out, '"')
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			out = append(out, '"')
		}
		out = append(out, name[i])
	}
	return string(append(out, '"'))
}
