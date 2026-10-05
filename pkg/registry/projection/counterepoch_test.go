package projection

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// unversionedRow is a row from a projection that maps no resourceVersion, so
// the cache's own counter is the only version there is.
func unversionedRow(namespace, name, phase string) unstructured.Unstructured {
	obj := cachedItem(namespace, name, "")
	_ = unstructured.SetNestedField(obj.Object, phase, "status", "phase")
	return obj
}

// TestACounterVersionDoesNotOutliveItsProcess covers a projection with no
// mapped resourceVersion, whose versions are the cache's own counter.
//
// The counter started at 1 in every process, so the version a client was
// handed before a restart named a perfectly ordinary point in the next
// process's history — one that described a different state of the table. A
// client resuming from it was admitted as current and never told about what
// changed while the server was down, or was replayed history that had nothing
// to do with what it held.
func TestACounterVersionDoesNotOutliveItsProcess(t *testing.T) {
	before := []unstructured.Unstructured{unversionedRow("acme", "order-1", "pending")}
	first := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return before, nil })
	t.Cleanup(first.Close)
	first.countFromEpoch()

	resumeFrom := first.versionFor(context.Background())
	if resumeFrom == "" {
		t.Fatal("the first process stamped no version; there is nothing to resume from")
	}

	// The server restarts. While it was down a row was added, which the
	// client can only learn about by relisting.
	after := []unstructured.Unstructured{
		unversionedRow("acme", "order-1", "pending"),
		unversionedRow("acme", "order-2", "pending"),
	}
	second := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return after, nil })
	t.Cleanup(second.Close)
	second.countFromEpoch()
	if version := second.versionFor(context.Background()); version == "" {
		t.Fatal("the second process stamped no version")
	}

	w, err := second.Watch(context.Background(), "acme", nil, nil, resumeFrom, false, false, deletedTestGVK)
	if err == nil {
		w.Stop()
		t.Fatalf("a watch resuming from %q, a version handed out by an earlier process, was "+
			"admitted; it never hears about order-2", resumeFrom)
	}
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Errorf("Watch() = %v, want 410 so the client relists", err)
	}
}

// TestAMappedVersionNeverStepsBackFromTheCounter covers a projection that maps a
// resourceVersion, over a table that is empty when it is first listed.
//
// Until the table has a row the cache answers with its counter, and the first
// row's own version takes over from it. Seeded from the wall clock, that
// counter was a number far above any ordinary version column, so the version a
// client was handed went backwards the moment a row arrived — and a watch
// resuming across it was told its version was from the future.
func TestAMappedVersionNeverStepsBackFromTheCounter(t *testing.T) {
	var rows []unstructured.Unstructured
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	listed := cache.versionFor(context.Background())
	if listed == "" {
		t.Fatal("the empty table was listed with no version")
	}

	rows = []unstructured.Unstructured{cachedItem("acme", "order-1", "3")}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}
	now := cache.ResourceVersion()

	if order, ok := compareVersions(now, listed); !ok || order < 0 {
		t.Errorf("the reported version went from %q to %q when the first row arrived; it must "+
			"never step backwards", listed, now)
	}
}

// TestACounterVersionStillResumesWithinItsProcess is the other half: the
// version this process handed out is still a point it can resume from.
func TestACounterVersionStillResumesWithinItsProcess(t *testing.T) {
	rows := []unstructured.Unstructured{unversionedRow("acme", "order-1", "pending")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.countFromEpoch()

	listed := cache.versionFor(context.Background())
	w, err := cache.Watch(context.Background(), "acme", nil, nil, listed, false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from the version this process just listed at: %v", err)
	}
	w.Stop()
}
