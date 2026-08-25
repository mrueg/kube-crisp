package projection

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
)

// TestReadsAreTraced covers the gap tracing exists to close: --tracing-config-file
// has always worked, so a trace showed the apiserver's handler and then nothing
// at all where the query was.
//
// Asserted as a tree rather than as a set of names. Spans that do not nest are
// spans a trace viewer shows side by side, which is the difference between "the
// read took 900ms, 880ms of it waiting for a connection slot" and three
// unrelated durations the reader has to reassemble.
func TestReadsAreTraced(t *testing.T) {
	store := newTestREST(t)

	recorded, root := recordSpans(t)
	ctx := oteltrace.ContextWithSpan(namespacedContext("acme"), root)

	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	root.End()

	spans := recorded()

	// The verb span, carrying the projection — which the apiserver's own
	// request span cannot know.
	list := requireSpan(t, spans, "kube-crisp.list")
	if got := stringAttr(list, "kube_crisp.projection"); got == "" {
		t.Error("the verb span names no projection, so a trace cannot say which one was slow")
	}
	if got := stringAttr(list, "kube_crisp.verb"); got != "list" {
		t.Errorf("kube_crisp.verb = %q, want %q", got, "list")
	}

	// The wait for a concurrency slot, separate from the query, because they
	// fail for different reasons and send an investigation in opposite
	// directions.
	acquire := requireSpan(t, spans, "kube-crisp.acquire")

	// The database round trip itself: the hole this whole change exists to
	// fill.
	query := requireSpan(t, spans, "kube-crisp.sql.query")
	if got := stringAttr(query, "db.system"); got == "" {
		t.Error("the query span names no driver")
	}
	if got := stringAttr(query, "db.statement"); !strings.Contains(strings.ToUpper(got), "SELECT") {
		t.Errorf("db.statement = %q, want the statement that ran", got)
	}

	// And they form one tree, rather than three spans a reader has to relate by
	// timestamp.
	if acquire.Parent().SpanID() != list.SpanContext().SpanID() {
		t.Error("the acquire span is not a child of the verb span")
	}
	if query.Parent().SpanID() != list.SpanContext().SpanID() {
		t.Error("the query span is not a child of the verb span")
	}
	if list.Parent().SpanID() != root.SpanContext().SpanID() {
		t.Error("the verb span is not a child of the request span, so it starts a trace of its " +
			"own rather than appearing in the request's")
	}
}

// TestTracingIsFreeWhenItIsOff covers the reason none of this is behind a flag.
//
// tracing.Start takes its tracer from whatever span is already on the context.
// Without --tracing-config-file the apiserver puts none there, so every span
// here is derived from a no-op and records nothing. If that stopped being true,
// every projection would pay for an exporter nobody configured.
func TestTracingIsFreeWhenItIsOff(t *testing.T) {
	store := newTestREST(t)

	recorded, _ := recordSpans(t)

	// No span on the context: exactly what a server without tracing configured
	// hands to its storage.
	if _, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if spans := recorded(); len(spans) != 0 {
		t.Errorf("%d span(s) were recorded with no tracing configured; the first is %q",
			len(spans), spans[0].Name())
	}
}

// recordSpans returns a reader for everything recorded, and a root span to hang
// it from. The provider is deliberately not made global: a test that installed
// one would change what every other test in the package records.
func recordSpans(t *testing.T) (func() []sdktrace.ReadOnlySpan, oteltrace.Span) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	_, root := provider.Tracer("test").Start(context.Background(), "request")
	return recorder.Ended, root
}

func requireSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}

	var got []string
	for _, s := range spans {
		got = append(got, s.Name())
	}
	t.Fatalf("no %q span was recorded; got %v", name, got)
	return nil
}

func stringAttr(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if kv.Key == attribute.Key(key) {
			return kv.Value.AsString()
		}
	}
	return ""
}
