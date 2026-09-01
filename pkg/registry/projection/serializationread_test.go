package projection

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

func contendedResource() schema.GroupResource {
	return schema.GroupResource{Group: "store.example.com", Resource: "orders"}
}

// A read the database rolled back is not an internal error.
//
// The database did its job: it refused to answer inconsistently. Reporting that
// as a 500 tells a controller the server is broken, when what happened is that
// the request may work on another attempt.
func TestAContendedReadIsNotAnInternalError(t *testing.T) {
	err := serializationFailed(contendedResource())

	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		t.Fatalf("serializationFailed() returned %T, which carries no status", err)
	}
	if got := status.Status().Code; got != http.StatusServiceUnavailable {
		t.Errorf("code = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if apierrors.IsInternalError(err) {
		t.Error("a contended read is reported as an internal error")
	}
}

// And it must not instruct every client to retry.
//
// A Retry-After is set where a retry is cheap — a shed request, a refused
// connection. This costs a whole query: the database ran the transaction and
// threw the work away, so ten retries put ten times the load on a database
// already too contended to serialise the first attempt. The status still says
// the request may be repeated; the server does not tell everybody to.
func TestAContendedReadDoesNotAskForARetry(t *testing.T) {
	var status apierrors.APIStatus
	if !errors.As(serializationFailed(contendedResource()), &status) {
		t.Fatal("no status")
	}
	if details := status.Status().Details; details != nil && details.RetryAfterSeconds != 0 {
		t.Errorf("Retry-After is %d seconds; a contended read must not ask for one",
			details.RetryAfterSeconds)
	}
}

// The metric must not call it an unreachable database.
//
// KubeCrispDatabaseUnreachable is a critical alert on result="unavailable" at
// any rate above zero, so counting a hot table there would page somebody about
// an outage that is not happening.
func TestAContendedReadIsNotCountedAsUnavailable(t *testing.T) {
	got := resultFor(serializationFailed(contendedResource()))
	if got == crispmetrics.ResultUnavailable {
		t.Fatal("a contended read is counted as an unreachable database, which pages on a hot table")
	}
	if got != crispmetrics.ResultContended {
		t.Errorf("result = %q, want %q", got, crispmetrics.ResultContended)
	}
}

// The database being genuinely unreachable still is unavailable, so the alert
// keeps meaning what it meant.
func TestAnUnreachableDatabaseIsStillUnavailable(t *testing.T) {
	if got := resultFor(unavailable(contendedResource(), errors.New("connection refused"))); got != crispmetrics.ResultUnavailable {
		t.Errorf("result = %q, want %q", got, crispmetrics.ResultUnavailable)
	}
}

// queryError has to reach for it, which is the half that was missing: the
// classifier existed and the write path used it, while a read fell through to
// the default and became a 500.
func TestQueryErrorClassifiesASerializationFailure(t *testing.T) {
	r := &REST{resource: crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Plural: "orders",
	}}

	// The text form, which is what the classifier falls back to for a driver
	// whose typed error it cannot reach — and what a test can construct without
	// a database.
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"postgres", errors.New("ERROR: could not serialize access due to concurrent update")},
		{"mysql", errors.New("Error 1213: Deadlock found when trying to get lock; try restarting transaction")},
		{"wrapped", fmt.Errorf("listing orders: %w",
			errors.New("ERROR: could not serialize access due to read/write dependencies"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.queryError(tc.err, "listing")
			if apierrors.IsInternalError(got) {
				t.Fatalf("a serialization failure became an internal error: %v", got)
			}
			if resultFor(got) != crispmetrics.ResultContended {
				t.Errorf("counted as %q", resultFor(got))
			}
		})
	}
}
