package sql

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/component-base/tracing"
)

// Spans are opened through k8s.io/component-base/tracing rather than through
// the OpenTelemetry API directly, for two reasons.
//
// It takes the tracer from whatever span is already on the context, so nothing
// has to carry a TracerProvider down to the pool. With --tracing-config-file
// set, the generic apiserver's handler chain has already put a span on the
// request context and these become children of it; without it, the span there
// is a no-op whose provider is a no-op, and so are these. That is why none of
// this is behind a flag of its own.
//
// It also nests into k8s.io/utils/trace, which is what produces the
// "Trace[...] (total time: 500ms)" lines already in the log. Only the root
// trace logs — a nested one contributes steps to it — so these show up inside
// the request traces that were already being printed, rather than as new lines.

// slowSpanLog is how long a statement has to run before the trace it belongs to
// is worth printing on its own. Only consulted when this span is the root,
// which for a served request it never is.
const slowSpanLog = 500 * time.Millisecond

// querySpan is one trip to the database.
type querySpan struct {
	span *tracing.Span
}

// startSpan opens a span covering one statement.
//
// The statement text is recorded; the bind values are not. The text comes from
// the projection's spec and is already carried on the audit event, so it says
// nothing a reader of the trace could not otherwise obtain. The values are the
// request's own data — names, namespaces, whatever a mapped column holds — and
// a trace goes somewhere an audit log does not.
func (p *Pool) startSpan(ctx context.Context, operation string, statement string) (context.Context, *querySpan) {
	ctx, span := tracing.Start(ctx, "kube-crisp.sql."+operation,
		attribute.String("db.system", p.driver),
		attribute.String("db.statement", oneLine(statement)),
		attribute.String("kube_crisp.datasource", p.metricLabel()),
	)
	return ctx, &querySpan{span: span}
}

// oneLine puts a statement on a single line.
//
// Projection SQL is written as a block scalar and so arrives with newlines in
// it. component-base's tracing also writes these attributes into the request
// trace lines in the log, where an embedded newline splits one record into
// several and takes any log parser reading them with it.
func oneLine(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

// returned marks the moment the database answered, which is what separates
// time spent waiting for it from time spent decoding what it sent.
func (s *querySpan) returned() {
	s.span.AddEvent("the database answered")
}

// end closes the span, recording what the statement produced.
func (s *querySpan) end(rows int, err error) {
	if err != nil {
		s.span.RecordError(err)
	} else if rows >= 0 {
		s.span.AddEvent("rows read", attribute.Int("kube_crisp.rows", rows))
	}
	s.span.End(slowSpanLog)
}

// endAffected closes the span for a statement that reports affected rows rather
// than returning them. A driver that does not report the count gives -1, which
// is recorded as unknown rather than as zero.
func (s *querySpan) endAffected(affected int64, err error) {
	if err != nil {
		s.span.RecordError(err)
	} else if affected >= 0 {
		s.span.AddEvent("rows affected", attribute.Int64("kube_crisp.rows_affected", affected))
	}
	s.span.End(slowSpanLog)
}
