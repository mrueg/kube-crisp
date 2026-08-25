//go:build e2e

package e2e

import (
	"context"
	goerrors "errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	staticOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "staticorders"}
	configMapGVR    = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

const (
	staticProjectionConfigMap = "static-projections"
	staticProjectionKey       = "static-orders.yaml"

	// The line the reload test adds and removes. Chosen because its effect is
	// visible in the served object rather than only in the server's own state:
	// a mapped column appears, an unmapped one does not.
	totalCentsMapping = "      - {column: total_cents, path: spec.totalCents, type: integer}"
)

// TestProjectionDirIsServed covers the half of --projection-dir that needs no
// reload: a projection that exists only as a file, served alongside the ones
// watched from the cluster.
//
// It is the mode for running without a CRD at all — bootstrapping, or a cluster
// where nobody may create cluster-scoped resources — so the resource being
// absent from the CustomResourceProjection list while present in discovery is
// the point, not an oddity.
func TestProjectionDirIsServed(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(staticOrdersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing a projection loaded from --projection-dir: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatal("the file-backed projection served no rows, so a later assertion about " +
			"its mapping would be measuring nothing")
	}

	// It comes from a file, so there is no object in the cluster describing it.
	// Anything looking for one — a status condition, kubectl get — finds
	// nothing, which is worth pinning so the two sources stay distinguishable.
	if _, err := dynamicClient.Resource(crpGVR).Get(ctx, "static-orders", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("a CustomResourceProjection named static-orders exists (%v); this test would "+
			"then be exercising the watched path rather than --projection-dir", err)
	}
}

// TestProjectionDirReloadsWithoutARestart covers the other half: a change to
// the directory is picked up while the server runs.
//
// The directory is watched rather than the files in it, because a ConfigMap
// mount is updated by swapping a symlink — the files themselves are never
// written to, so a per-file watch would see nothing. This test drives exactly
// that path: it edits the ConfigMap and waits for the kubelet to project the
// change, which is how the change would arrive in a real cluster.
//
// Slow by nature. The kubelet syncs mounted ConfigMaps on its own schedule,
// which is up to a minute or two, and none of that is under this server's
// control.
//
// Written as a flip to whichever state the fixture is not currently in, rather
// than as set-then-restore. A restore has to propagate too, and a re-run
// starting before it landed would otherwise assert against the wrong baseline —
// which is the kind of order dependence that makes a suite pass alone and fail
// in CI.
func TestProjectionDirReloadsWithoutARestart(t *testing.T) {
	ctx := context.Background()
	configMaps := dynamicClient.Resource(configMapGVR).Namespace("kube-crisp")

	current, err := configMaps.Get(ctx, staticProjectionConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection ConfigMap: %v", err)
	}
	manifest, _, err := unstructured.NestedString(current.Object, "data", staticProjectionKey)
	if err != nil || manifest == "" {
		t.Fatalf("the ConfigMap holds no %s: %v", staticProjectionKey, err)
	}

	mapped := strings.Contains(manifest, totalCentsMapping)
	want := !mapped
	t.Logf("total_cents is currently %s; flipping it %s and waiting for the change to be served",
		mappedWord(mapped), mappedWord(want))

	var next string
	if want {
		next = strings.TrimRight(manifest, "\n") + "\n" + totalCentsMapping + "\n"
	} else {
		next = strings.ReplaceAll(manifest, totalCentsMapping+"\n", "")
	}
	if next == manifest {
		t.Fatalf("rewriting the manifest changed nothing; the marker %q no longer matches the "+
			"fixture, so this test would pass without reloading anything", totalCentsMapping)
	}

	updated := current.DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, next, "data", staticProjectionKey); err != nil {
		t.Fatalf("preparing the ConfigMap: %v", err)
	}
	if _, err := configMaps.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating the projection ConfigMap: %v", err)
	}

	// No restart, and the test says so rather than assuming it: a rollout would
	// serve the new mapping too, and prove nothing about the watch.
	before := apiserverStartedAt(ctx, t)

	deadline := time.Now().Add(4 * time.Minute)
	start := time.Now()
	for {
		serving, err := totalCentsIsMapped(ctx, t)
		if err == nil && serving == want {
			t.Logf("the change was served %v after the ConfigMap was updated, without a restart",
				time.Since(start).Round(time.Second))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spec.totalCents was still %s %v after the ConfigMap was updated "+
				"(last read error: %v); --projection-dir did not pick the change up",
				mappedWord(!want), time.Since(start).Round(time.Second), err)
		}
		time.Sleep(2 * time.Second)
	}

	if after := apiserverStartedAt(ctx, t); after != before {
		t.Errorf("the apiserver restarted during the test (started %s, now %s), so the new "+
			"mapping proves nothing about reloading a running server", before, after)
	}
}

// totalCentsIsMapped reports whether the served objects carry the field the
// reload test adds.
func totalCentsIsMapped(ctx context.Context, t *testing.T) (bool, error) {
	t.Helper()

	list, err := dynamicClient.Resource(staticOrdersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	if len(list.Items) == 0 {
		return false, goerrors.New("the projection served no rows")
	}
	_, found, err := unstructured.NestedInt64(list.Items[0].Object, "spec", "totalCents")
	if err != nil {
		return false, err
	}
	return found, nil
}

// apiserverStartedAt identifies the running process, so a reload can be told
// apart from a restart.
func apiserverStartedAt(ctx context.Context, t *testing.T) string {
	t.Helper()

	pods, err := dynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).
		Namespace("kube-crisp").
		List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=kube-crisp-apiserver"})
	if err != nil {
		t.Fatalf("listing the apiserver pods: %v", err)
	}
	if len(pods.Items) == 0 {
		t.Fatal("no kube-crisp apiserver pod is running")
	}

	// Name and start time together: a restart in place keeps the name and
	// changes the time, a rescheduled pod changes both.
	name := pods.Items[0].GetName()
	started, _, _ := unstructured.NestedString(pods.Items[0].Object, "status", "startTime")
	return name + "@" + started
}

func mappedWord(mapped bool) string {
	if mapped {
		return "mapped"
	}
	return "unmapped"
}
