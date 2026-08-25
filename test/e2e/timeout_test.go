//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// The same doomed query with and without spec.dataSource.statementTimeout.
	timedOrdersGVR   = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "timedorders"}
	untimedOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "untimedorders"}
)

// What PostgreSQL logs when a statement is cut short, by who cut it short.
const (
	abortedByDatabase = "canceling statement due to statement timeout"
	abortedByClient   = "canceling statement due to user request"
)

// TestStatementTimeoutIsEnforcedByTheDatabase covers the one thing
// spec.dataSource.statementTimeout claims: that the bound is the database's and
// not just this server's.
//
// The claim is hard to see from outside, because on a healthy connection the
// query dies at about the same moment either way — the driver sends a
// cancellation when the context expires, and PostgreSQL honours it. Wall clock,
// pg_stat_activity a moment later, and the error the client gets are therefore
// all the same in both configurations; the first two of those produced a
// convincing but meaningless result before this test was written this way.
//
// What differs is which mechanism did it, and PostgreSQL says so in its log.
// That is the assertion here, against a control that differs in nothing else.
//
// The reason it matters is the case this test cannot stage: a cancellation is a
// second connection carrying a request the server may never act on. When the
// network is the reason the query is slow, that is exactly when it fails to
// arrive — and then only a bound the database is already holding stops the
// query.
func TestStatementTimeoutIsEnforcedByTheDatabase(t *testing.T) {
	ctx := context.Background()

	t.Run("with statementTimeout the database aborts it", func(t *testing.T) {
		log := probeDoomedList(ctx, t, timedOrdersGVR)
		if log.databaseAborts != 1 || log.clientAborts != 0 {
			t.Errorf("PostgreSQL logged %d statement-timeout and %d user-request aborts; "+
				"want 1 and 0, or the bound is not the database's",
				log.databaseAborts, log.clientAborts)
		}
	})

	t.Run("without it the driver cancels instead", func(t *testing.T) {
		log := probeDoomedList(ctx, t, untimedOrdersGVR)
		if log.clientAborts != 1 || log.databaseAborts != 0 {
			t.Errorf("PostgreSQL logged %d user-request and %d statement-timeout aborts; "+
				"want 1 and 0 — the control must not reach the database's bound, "+
				"or the projections differ in something other than the field under test",
				log.clientAborts, log.databaseAborts)
		}
	})
}

// TestTimeoutIsNotRetriedIntoAStorm covers the load a failing read puts on a
// database that is already too slow to answer.
//
// RetryAfterSeconds on a status over 500 becomes a Retry-After header, and
// client-go retries such a response ten times by default. On a request shed at
// the concurrency limit that costs nothing, because it is refused before the
// query runs. On a timeout it costs a whole timeout budget per attempt, so one
// LIST became eleven queries and 15.6 seconds of waiting to return the error it
// was always going to return — eleven times the load, arriving exactly when the
// database can least afford it.
//
// One client request, one query. Asserted for both projections, since the
// amplification had nothing to do with which bound was being enforced.
func TestTimeoutIsNotRetriedIntoAStorm(t *testing.T) {
	ctx := context.Background()

	for name, gvr := range map[string]schema.GroupVersionResource{
		"with statementTimeout":    timedOrdersGVR,
		"without statementTimeout": untimedOrdersGVR,
	} {
		t.Run(name, func(t *testing.T) {
			log := probeDoomedList(ctx, t, gvr)

			if attempts := log.databaseAborts + log.clientAborts; attempts != 1 {
				t.Errorf("one LIST reached the database %d times; want 1. "+
					"A timeout that advertises Retry-After has every client-go client "+
					"run the slow query %d more times.", attempts, attempts-1)
			}

			// The wall clock is the same failure seen from the caller's side: a
			// retried timeout takes the retries plus their intervals, which is
			// far longer than the budget the projection was given.
			if log.elapsed > 5*time.Second {
				t.Errorf("the LIST took %v to fail against a query bounded at 500ms, "+
					"which is long enough that it was retried rather than answered", log.elapsed)
			}
		})
	}
}

// doomedList is what one LIST against a projection slower than its own timeout
// cost, measured at the database rather than inferred from the client.
type doomedList struct {
	databaseAborts int
	clientAborts   int
	elapsed        time.Duration
}

// probeDoomedList issues a LIST that cannot succeed and reports what PostgreSQL
// logged while it ran.
//
// Counted as a delta across the request rather than filtered by timestamp: the
// container log and this test do not share a clock, and a window drawn with the
// wrong one silently counts the neighbouring test's queries instead. Nothing
// else in the suite cancels a statement, so the delta is this request's.
func probeDoomedList(ctx context.Context, t *testing.T, gvr schema.GroupVersionResource) doomedList {
	t.Helper()

	beforeDatabase, beforeClient := postgresAborts(t)

	start := time.Now()
	_, err := dynamicClient.Resource(gvr).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("listing %s succeeded; the query is meant to outrun its own timeout, "+
			"so this test is measuring nothing", gvr.Resource)
	}
	if !apierrors.IsTimeout(err) {
		t.Fatalf("listing %s returned %v; want a Timeout, or the failure under test "+
			"is not the one being measured", gvr.Resource, err)
	}

	// The abort is logged when PostgreSQL gives up, which can trail the client
	// being told by a moment.
	var afterDatabase, afterClient int
	deadline := time.Now().Add(15 * time.Second)
	for {
		afterDatabase, afterClient = postgresAborts(t)
		if afterDatabase+afterClient > beforeDatabase+beforeClient || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// A restart takes the previous container's log with it, so the counts go
	// backwards and every assertion below turns into a puzzle. Say what
	// happened instead.
	if afterDatabase < beforeDatabase || afterClient < beforeClient {
		t.Fatalf("the PostgreSQL log shrank during the request (%d/%d aborts became %d/%d), "+
			"which means the database restarted; this measurement is void",
			beforeDatabase, beforeClient, afterDatabase, afterClient)
	}

	return doomedList{
		databaseAborts: afterDatabase - beforeDatabase,
		clientAborts:   afterClient - beforeClient,
		elapsed:        elapsed,
	}
}

// postgresAborts counts each kind of cancellation the database has logged so far.
func postgresAborts(t *testing.T) (database, client int) {
	t.Helper()

	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", "kube-crisp", "logs", "deploy/postgres", "--tail=-1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reading the PostgreSQL log: %v\n%s", err, output)
	}

	return strings.Count(string(output), abortedByDatabase),
		strings.Count(string(output), abortedByClient)
}
