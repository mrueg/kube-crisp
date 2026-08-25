package projection

import (
	"context"
	goerrors "errors"
	"net"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestWatchKeepsTheStatusThePollProduced covers what a client is told when the
// database is down.
//
// A LIST during an outage returns 503 with a Retry-After, because queryError
// classified it. A WATCH runs the same query and was answered 500 with no
// header — so client-go, whose backoff keys on Retry-After, retried flat out
// against a database that was already struggling.
func TestWatchKeepsTheStatusThePollProduced(t *testing.T) {
	store := newTestREST(t)

	// A cache whose poll fails the way an unreachable database does.
	store.watch = newWatchCache(time.Hour, store.label, nil,
		func(context.Context) ([]unstructured.Unstructured, error) {
			return nil, store.queryError(&net.OpError{
				Op: "dial", Net: "tcp", Err: goerrors.New("connection refused"),
			}, "listing")
		})
	t.Cleanup(store.watch.Close)

	_, err := store.Watch(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err == nil {
		t.Fatal("Watch() succeeded though its poll could not reach the database")
	}
	if errors.IsInternalError(err) {
		t.Errorf("Watch() returned a 500: %v — the poll had already classified this, and the "+
			"status it produced is what the client needs to back off on", err)
	}
	if !errors.IsServiceUnavailable(err) {
		t.Errorf("Watch() returned %v, want ServiceUnavailable", err)
	}
}
