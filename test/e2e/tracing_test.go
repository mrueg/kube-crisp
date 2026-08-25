//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestQueriesAppearInTheRequestTrace covers the plumbing that the unit tests
// cannot: that the span this server opens around a query is a child of the one
// the request already carries.
//
// The failure it guards against is quiet and total. component-base's
// tracing.Start takes its tracer from whatever span is on the context, so if
// the request context reaching the storage layer had no span on it, every span
// here would still be created, still be a no-op, and never appear anywhere —
// with the unit tests passing throughout, because they put a span on the
// context themselves.
//
// Checked through the trace log rather than an OTLP collector, because
// tracing.Start feeds both and only one of them needs a collector deployed to
// observe. The nesting is the same nesting either way: it comes from the
// context, and the context is the thing under test.
func TestQueriesAppearInTheRequestTrace(t *testing.T) {
	ctx := context.Background()

	before := apiserverLogLines(t)

	// A read slow enough that the apiserver logs its trace at all. The
	// projection's query outruns its own timeout, which is what makes the
	// request reliably slower than the threshold rather than usually so.
	_, _ = dynamicClient.Resource(timedOrdersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{})

	trace := findRequestTrace(t, before, "timedorders")
	if trace == "" {
		t.Fatal("the apiserver logged no trace for the slow list; this test needs the server " +
			"running at --v=2, since that is what gates trace logging")
	}

	// The verb span, naming the projection the request was served by.
	if !strings.Contains(trace, `"kube-crisp.list"`) {
		t.Errorf("the request trace has no kube-crisp.list span, so this server's work is not "+
			"attached to the request's trace:\n%s", trace)
	}
	if !strings.Contains(trace, "kube_crisp.projection:timed-orders") {
		t.Errorf("the trace does not name the projection that served the request:\n%s", trace)
	}

	// And the query beneath it: the part a trace showed as a hole before any of
	// this existed.
	if !strings.Contains(trace, `"kube-crisp.sql.`) {
		t.Errorf("the request trace has no database span, which is the gap tracing was added "+
			"to close:\n%s", trace)
	}
	if !strings.Contains(trace, "db.system:postgres") {
		t.Errorf("the database span does not name the driver:\n%s", trace)
	}

	// The statement is recorded on one line. Written as a YAML block scalar it
	// has newlines in it, and an embedded newline splits one log record into
	// several.
	for _, line := range strings.Split(trace, "\n") {
		if strings.Contains(line, "db.statement:") && !strings.Contains(line, "SELECT") {
			t.Errorf("a db.statement attribute was split across log lines:\n%s", line)
		}
	}
}

// findRequestTrace returns the trace block the apiserver logged for a request
// against the named resource, or empty if it logged none.
func findRequestTrace(t *testing.T, since int, resource string) string {
	t.Helper()

	all := apiserverLog(t)
	lines := strings.Split(all, "\n")
	if since > len(lines) {
		since = len(lines)
	}

	// A trace is one log record spanning several lines: the header naming the
	// request, then its steps, then END.
	var block []string
	var collecting bool
	for _, line := range lines[since:] {
		switch {
		case strings.Contains(line, "Trace[") && strings.Contains(line, "resource:"+resource):
			collecting = true
			block = []string{line}
		case collecting:
			block = append(block, line)
			if strings.Contains(line, "END") {
				return strings.Join(block, "\n")
			}
		}
	}
	return strings.Join(block, "\n")
}

func apiserverLog(t *testing.T) string {
	t.Helper()

	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", "kube-crisp", "logs", "deploy/kube-crisp-apiserver", "-c", "apiserver", "--tail=-1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reading the apiserver log: %v\n%s", err, output)
	}
	return string(output)
}

func apiserverLogLines(t *testing.T) int {
	t.Helper()
	return len(strings.Split(apiserverLog(t), "\n"))
}

// TestUnencryptedDataSourcesAreReported covers the warning that is the only
// signal an operator gets about a database reached in the clear.
//
// Every connection string in this repository's documentation asks for TLS, and
// until this existed nothing noticed when a real one did not — a pasted
// sslmode=disable sent credentials and every projected row across the network
// with nothing said anywhere. The e2e fixture is exactly that case, which makes
// it the right place to check the warning fires.
func TestUnencryptedDataSourcesAreReported(t *testing.T) {
	log := apiserverLog(t)

	const warning = "data source connects without transport encryption"
	if !strings.Contains(log, warning) {
		t.Fatalf("the fixture's data sources use sslmode=disable and no MySQL tls parameter, "+
			"and nothing warned about either; looked for %q", warning)
	}

	// The warning has to say what to do, per driver, or it is only an alarm.
	for driver, hint := range map[string]string{
		"postgres": "sslmode=require",
		"mysql":    "tls=true",
	} {
		if !strings.Contains(log, hint) {
			t.Errorf("no warning names the fix for %s (%q), so the reader is told something is "+
				"wrong and not how to correct it", driver, hint)
		}
	}

	// SQLite is a local file. Warning about it would be noise that teaches
	// people to ignore the warning.
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, warning) && strings.Contains(line, `driver="sqlite"`) {
			t.Errorf("SQLite was warned about, though there is no connection to encrypt:\n%s", line)
		}
	}
}
