package projection

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// flightGroup collapses identical concurrent reads onto one database round
// trip.
//
// A projection's scarce resource is connections, and a read holds one for its
// whole duration. Sixteen clients asking for the same object at the same
// instant is sixteen connections doing identical work; one of them can answer
// all sixteen. Unlike the read cache this needs no configuration and adds no
// staleness: every waiter is answered by a query that was still running when it
// asked.
type flightGroup struct {
	projection string
	resource   string

	mu       sync.Mutex
	inflight map[string]*flight
}

// flight is one query several requests are waiting on.
type flight struct {
	done chan struct{}

	// namespace is what the query reads, so a write can detach the flights it
	// could invalidate.
	namespace string

	// results holds one result set per statement the read ran, so a list and
	// the count taken with it are shared as the pair they were read as.
	results [][]crispsql.Row
	err     error
}

func newFlightGroup(projection, resource string) *flightGroup {
	return &flightGroup{projection: projection, resource: resource, inflight: map[string]*flight{}}
}

// Do runs query unless an identical one is already running, in which case the
// caller waits for that one instead.
//
// The query itself is detached from the leader's cancellation: the request that
// happened to start it may go away, and the ones waiting behind it should not
// lose their answer to that accident. Each waiter still honours its own context
// and stops waiting when the client does.
func (g *flightGroup) Do(
	ctx context.Context,
	key, namespace string,
	query func(context.Context) ([][]crispsql.Row, error),
) ([][]crispsql.Row, error) {
	if g == nil {
		return query(ctx)
	}

	g.mu.Lock()
	if existing, ok := g.inflight[key]; ok {
		g.mu.Unlock()
		crispmetrics.QueriesCoalesced.WithLabelValues(g.projection, g.resource).Inc()
		return existing.wait(ctx)
	}

	current := &flight{done: make(chan struct{}), namespace: namespace}
	g.inflight[key] = current
	g.mu.Unlock()

	detached := context.WithoutCancel(ctx)
	go func() {
		defer close(current.done)
		defer g.finish(key, current)
		current.results, current.err = query(detached)
	}()

	return current.wait(ctx)
}

// wait blocks until the query answers or the caller gives up.
func (f *flight) wait(ctx context.Context) ([][]crispsql.Row, error) {
	select {
	case <-f.done:
		// The rows are shared with every other waiter, so nothing may modify
		// them. Mapping reads them and builds its own object.
		return f.results, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// finish removes a completed flight so the next request starts a fresh one.
func (g *flightGroup) finish(key string, current *flight) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.inflight[key] == current {
		delete(g.inflight, key)
	}
}

// detach stops new readers from joining the queries a write may have
// invalidated, without disturbing the ones already waiting on them.
//
// Without this, a client that writes and then reads could be answered by a
// query that started before its write and cannot possibly reflect it. The
// scoping matches the read cache: the namespace written, plus anything
// cluster-wide, and everything when the write itself had no namespace.
func (g *flightGroup) detach(namespace string) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for key, current := range g.inflight {
		if namespace == "" || current.namespace == "" || current.namespace == namespace {
			delete(g.inflight, key)
		}
	}
}

// pending reports how many queries are in flight, for tests and diagnostics.
func (g *flightGroup) pending() int {
	if g == nil {
		return 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.inflight)
}

// flightKey identifies a query by what it will actually send to the database:
// the statement, and the value bound to each of its parameters.
//
// Nothing coarser is safe. Two requests that look alike can still resolve
// differently — a projection binding the authenticated user is the obvious
// case — and answering one from the other's rows would hand a client rows it
// was never entitled to.
func flightKey(stmt *crispsql.Statement, args map[string]any) string {
	var b strings.Builder
	b.Grow(len(stmt.SQL) + 16*len(stmt.Params))
	b.WriteString(stmt.SQL)
	for _, name := range stmt.Params {
		b.WriteByte(0)
		b.WriteString(name)
		b.WriteByte('=')
		value, ok := args[name]
		if !ok {
			b.WriteString("<unset>")
			continue
		}
		writeBound(&b, value)
	}
	return b.String()
}

// writeBound renders one bound value into a key.
//
// The type is written alongside the value, so the string "1" and the number 1
// cannot collide — answering one request from another's rows is exactly what
// this key exists to prevent. The common types are handled directly because
// this runs on every read; anything else falls back to %#v, which distinguishes
// types the same way at the cost of reflection.
func writeBound(b *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		b.WriteString("nil")
	case string:
		b.WriteString("s:")
		b.WriteString(v)
	case []byte:
		b.WriteString("b:")
		b.Write(v)
	// The integer widths all bind as the same driver argument, so they share a
	// rendering: two reads that differ only in how the number was typed in Go
	// send the same statement and may share its result.
	case int64:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(v, 10))
	case int:
		b.WriteString("i:")
		b.WriteString(strconv.Itoa(v))
	case int32:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case float64:
		b.WriteString("f:")
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	case bool:
		b.WriteString("t:")
		b.WriteString(strconv.FormatBool(v))
	default:
		fmt.Fprintf(b, "%#v", value)
	}
}
