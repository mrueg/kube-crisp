//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// The channel notified-orders subscribes to, and the poll interval it falls
// back to. The interval is the whole point: a change seen well inside it did
// not come from a tick.
const (
	notifyChannel      = "notified_orders_changed"
	notifyPollInterval = 60 * time.Second
)

// TestNotificationsSurviveTheSubscriptionDropping covers the failure this
// server had no way of reporting and no test for: the LISTEN connection goes
// away and does not come back.
//
// Nothing else catches it. Reads keep working, because they do not use the
// subscription. Watches keep working, because the poll timer is still running
// underneath — they just quietly return to the interval the notification was
// meant to save them from. And kube_crisp_watch_notifications_total is a
// counter, so a subscription that died leaves it flat, which is what a table
// nobody writes to also looks like.
//
// Staged by terminating the subscription's backend rather than by restarting
// PostgreSQL. It is the same event from this server's side — the connection
// holding LISTEN goes away — and it leaves the rest of the suite's data and
// every other connection untouched.
func TestNotificationsSurviveTheSubscriptionDropping(t *testing.T) {
	ctx := context.Background()

	t.Cleanup(func() {
		execSQL(t, "UPDATE notified_orders SET customer = 'ada', updated_at = '1' WHERE id = 'notified-1'")
		execSQL(t, "UPDATE polled_orders SET customer = 'ada', updated_at = '1' WHERE id = 'polled-1'")
	})

	// A watcher is what makes the projection poll, and the subscription is
	// opened alongside the poll group. Without one there is no listener to drop.
	client := dynamicClient.Resource(notifiedOrdersGVR).Namespace(acmeNamespace)
	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	watcher, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
	if err != nil {
		t.Fatalf("watching: %v", err)
	}
	defer watcher.Stop()

	// The control: the same table and the same 60s interval, with no
	// subscription and nothing done to it. Dropping a connection is a disturbance,
	// and a watch cache that responded to one by resyncing would deliver the
	// change quickly for a reason that has nothing to do with notifications.
	// This one cannot be woken that way, so if it stays quiet while the other
	// fires, the notification is what fired it.
	control := dynamicClient.Resource(polledOrdersGVR).Namespace(acmeNamespace)
	controlList, err := control.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing the control: %v", err)
	}
	controlWatcher, err := control.Watch(ctx, metav1.ListOptions{ResourceVersion: controlList.GetResourceVersion()})
	if err != nil {
		t.Fatalf("watching the control: %v", err)
	}
	defer controlWatcher.Stop()

	before := awaitListener(t, 0)
	t.Logf("the subscription is held by backend %s", before)

	// Dropped the way a restart, a failover, or an idle-connection reaper would
	// drop it.
	execSQL(t, fmt.Sprintf("SELECT pg_terminate_backend(%s)", before))

	after := awaitListener(t, mustAtoi(t, before))
	t.Logf("the subscription came back on backend %s", after)
	if after == before {
		t.Fatalf("the same backend %s is still listening, so nothing was actually dropped and "+
			"this test proves nothing about reconnecting", before)
	}

	// And it is a working subscription rather than merely a connection: a
	// change made in SQL has to reach the watch well inside the poll interval,
	// which is the only thing that distinguishes a live notification from the
	// timer underneath.
	const patience = 20 * time.Second
	if patience >= notifyPollInterval {
		t.Fatalf("the patience (%v) is not inside the poll interval (%v), so a pass could "+
			"come from a tick", patience, notifyPollInterval)
	}

	start := time.Now()
	execSQL(t, "UPDATE notified_orders SET customer = 'reconnected', updated_at = '2' WHERE id = 'notified-1'")
	execSQL(t, "UPDATE polled_orders SET customer = 'reconnected', updated_at = '2' WHERE id = 'polled-1'")

	if !awaitCustomer(watcher, "reconnected", patience) {
		t.Fatalf("the watch did not see the change within %v of a subscription being "+
			"re-established; it is back to polling every %v", patience, notifyPollInterval)
	}
	t.Logf("the reconnected watch had the change %v after the write was issued, against a "+
		"%v poll interval", time.Since(start).Round(time.Millisecond), notifyPollInterval)

	// Whatever is left of the window, spent making sure the control did not
	// also see it. If it did, something other than the notification is finding
	// changes and the measurement above means nothing.
	if awaitCustomer(controlWatcher, "reconnected", patience-time.Since(start)) {
		t.Error("the unsubscribed control saw its change inside the same window, so the " +
			"wake-up above cannot be attributed to the notification")
	} else {
		t.Logf("the unsubscribed control is still waiting, as its %v interval says it should",
			notifyPollInterval)
	}
}

// awaitListener waits for a backend to be listening on the channel and returns
// its pid, ignoring notThis so a reconnect can be told from the connection that
// was already there.
func awaitListener(t *testing.T, notThis int) string {
	t.Helper()

	// Longer than the retry interval by enough that a slow reconnect is not
	// mistaken for one that never happens.
	deadline := time.Now().Add(60 * time.Second)
	for {
		pid := strings.TrimSpace(querySQL(t, fmt.Sprintf(
			`SELECT COALESCE(max(pid)::text, '') FROM pg_stat_activity
			 WHERE query ILIKE 'LISTEN%%%s%%' AND pid <> %d`, notifyChannel, notThis)))
		if pid != "" {
			return pid
		}
		if time.Now().After(deadline) {
			if notThis == 0 {
				t.Fatal("nothing is listening on " + notifyChannel + "; the projection is " +
					"configured for notifications but never subscribed")
			}
			t.Fatalf("no backend other than %d is listening on %s; the subscription was "+
				"dropped and never re-established", notThis, notifyChannel)
		}
		time.Sleep(time.Second)
	}
}

// awaitCustomer reports whether the watch delivered an object with the given
// customer within the deadline.
func awaitCustomer(w watch.Interface, want string, within time.Duration) bool {
	deadline := time.After(within)
	for {
		select {
		case event, open := <-w.ResultChan():
			if !open {
				return false
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			if customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer"); customer == want {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()

	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n); err != nil {
		t.Fatalf("backend pid %q is not a number: %v", s, err)
	}
	return n
}
