//go:build e2e

package e2e

import (
	"context"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// collectingWarnings records the warnings a request came back with, which is
// what kubectl prints above its own output.
type collectingWarnings struct {
	mu   sync.Mutex
	seen []string
}

func (c *collectingWarnings) HandleWarningHeader(code int, agent, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, text)
}

func (c *collectingWarnings) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.seen...)
}

// TestWritingALabelThatSharesAColumnWarns covers a write that succeeds while
// quietly doing less than it was asked to.
//
// The orders projection maps the status column twice: as the label
// store.example.com/status and as the field status.phase. Reading it that way
// is deliberate and useful. Writing is where they part company — only one can
// reach the column, and the field does.
//
// Before this the request was answered 200, kubectl said "labeled", and the row
// had not moved. Nothing about the behaviour has changed; what changed is that
// the client is now told which half was ignored.
func TestWritingALabelThatSharesAColumnWarns(t *testing.T) {
	ctx := context.Background()

	warnings := &collectingWarnings{}
	config := rest.CopyConfig(restConfig)
	config.WarningHandler = warnings
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("building a client that keeps warnings: %v", err)
	}
	orders := client.Resource(ordersGVR).Namespace(acmeNamespace)

	before, err := orders.Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the order: %v", err)
	}
	phase, _, _ := unstructured.NestedString(before.Object, "status", "phase")

	// Ask for a label that disagrees with the field owning the same column.
	patch := []byte(`{"metadata":{"labels":{"store.example.com/status":"definitely-not-the-phase"}}}`)
	if _, err := orders.Patch(ctx, "order-acme-1", types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		t.Fatalf("patching the label: %v", err)
	}

	var told bool
	for _, w := range warnings.all() {
		if strings.Contains(w, "store.example.com/status") && strings.Contains(w, "status.phase") {
			told = true
			t.Logf("warned: %s", w)
		}
	}
	if !told {
		t.Errorf("a label that could not be written was accepted silently; warnings were %v", warnings.all())
	}

	// And the behaviour is unchanged: the field still owns the column.
	after, err := orders.Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-reading the order: %v", err)
	}
	if got, _, _ := unstructured.NestedString(after.Object, "status", "phase"); got != phase {
		t.Errorf("status.phase moved from %q to %q; the label should not have written the column", phase, got)
	}
	if got := after.GetLabels()["store.example.com/status"]; got != phase {
		t.Errorf("the label reads %q, want the column's value %q", got, phase)
	}
}
