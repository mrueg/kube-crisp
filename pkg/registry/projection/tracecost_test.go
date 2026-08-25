package projection

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/component-base/tracing"
	utiltrace "k8s.io/utils/trace"
)

// What a span costs when nobody is collecting them.
//
// Worth pinning, because the answer is not zero and the comment on startQuery
// says tracing is free when it is off. That is true of the export and of the
// sampling; it is not true of the span object. component-base's tracing.Start
// always nests into k8s.io/utils/trace and always allocates a Span, whether or
// not --tracing-config-file was given — so every projection pays this on every
// statement, on a server that has never heard of an OTLP collector.
//
// Measured at 13 allocations and ~760 bytes a span. A read opens three or four
// of them, which is a fifth of what a single-object GET allocates and about a
// thousandth of the 3.3ms it takes. Small in the dimension that matters and not
// small in the other one, which is the sort of thing that is only obvious while
// someone is looking at it — hence this benchmark rather than a comment.
func BenchmarkTracingStartNoParent(b *testing.B) {
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, span := tracing.Start(ctx, "kube-crisp.list",
			attribute.String("kube_crisp.projection", "orders"),
			attribute.String("kube_crisp.resource", "orders.store.example.com"),
			attribute.String("kube_crisp.verb", "list"))
		span.End(500 * time.Millisecond)
	}
}

// The shape a served request actually has: the apiserver's own List handler
// puts a utiltrace on the context before storage is reached, configured tracing
// or not, so this is the cost that is really paid rather than the one above.
func BenchmarkTracingStartUnderRequestTrace(b *testing.B) {
	ctx := utiltrace.ContextWithTrace(context.Background(), utiltrace.New("List"))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, span := tracing.Start(ctx, "kube-crisp.list",
			attribute.String("kube_crisp.projection", "orders"),
			attribute.String("kube_crisp.resource", "orders.store.example.com"),
			attribute.String("kube_crisp.verb", "list"))
		span.End(500 * time.Millisecond)
	}
}
