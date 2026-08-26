//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// TestDiscoveryAdvertisesOnlyDeclaredVerbs checks that discovery offers exactly
// the verbs a projection has queries for.
//
// It used to offer all of them for every projection, because one Go type served
// them all and the endpoint installer derives verbs from which interfaces that
// type satisfies. The verbs with no query behind them were refused at request
// time with 405 — which the garbage collector retries forever, because it picks
// what to collect out of discovery, and which makes kubectl delete --all and
// apply --prune fail on a projection that never offered to be deleted.
func TestDiscoveryAdvertisesOnlyDeclaredVerbs(t *testing.T) {
	resources, err := discoveryClient.ServerResourcesForGroupVersion("store.example.com/v1alpha1")
	if err != nil {
		t.Fatalf("discovering group version: %v", err)
	}

	verbs := map[string][]string{}
	for _, r := range resources.APIResources {
		// Subresources come through as "orders/status"; the verbs under test
		// are the ones on the resource itself.
		if strings.Contains(r.Name, "/") {
			continue
		}
		got := append([]string(nil), r.Verbs...)
		sort.Strings(got)
		verbs[r.Name] = got
	}

	// Expectations read off the projections in test/e2e/manifests: what each
	// one declares under queries, plus watch unless it disables it.
	for _, tc := range []struct {
		resource string
		declares string
		want     []string
	}{
		{
			resource: "orders",
			declares: "every write query, watch enabled",
			want:     []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
		},
		{
			// The case that motivated this: one write query, and it used to
			// advertise create, delete, deletecollection and watch as well.
			resource: "splitorders",
			declares: "get, list, update; watch disabled",
			want:     []string{"get", "list", "patch", "update"},
		},
		{
			resource: "shipments",
			declares: "create, delete, get, list; watch disabled",
			want:     []string{"create", "delete", "deletecollection", "get", "list"},
		},
		{
			resource: "tombstonedorders",
			declares: "delete, get, list; watch enabled",
			want:     []string{"delete", "deletecollection", "get", "list", "watch"},
		},
		{
			// Read-only and watchable: an informer on this one can sync.
			resource: "polledorders",
			declares: "get, list; watch enabled",
			want:     []string{"get", "list", "watch"},
		},
		{
			// Read-only with watch disabled. Advertising watch here made an
			// informer list, watch, get refused, and never sync.
			resource: "borrowedorders",
			declares: "get, list; watch disabled",
			want:     []string{"get", "list"},
		},
		{
			// No get query at all: get is still served, by filtering the list,
			// so advertising it is correct.
			resource: "sloworders",
			declares: "list only; watch disabled",
			want:     []string{"get", "list"},
		},
	} {
		t.Run(tc.resource, func(t *testing.T) {
			got, ok := verbs[tc.resource]
			if !ok {
				t.Fatalf("%s is not advertised in discovery at all", tc.resource)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("%s declares %s\n advertised: %v\n      want: %v",
					tc.resource, tc.declares, got, tc.want)
			}
		})
	}
}

// TestUnadvertisedVerbsAreAlsoRefused is the other half: discovery and the
// storage have to agree in both directions. A verb that is not advertised must
// still be refused if a client asks for it anyway, and a verb that is
// advertised must not answer 405.
func TestUnadvertisedVerbsAreAlsoRefused(t *testing.T) {
	ctx := context.Background()

	// splitorders has no delete query. Discovery no longer says it does; the
	// storage must still say no to a client that tries.
	err := dynamicClient.Resource(splitOrdersGVR).Namespace(acmeNamespace).
		Delete(ctx, "does-not-matter", metav1.DeleteOptions{})
	if err == nil {
		t.Fatal("deleting a splitorder succeeded; it declares no delete query")
	}
	if !apierrors.IsMethodNotSupported(err) && !apierrors.IsNotFound(err) {
		t.Errorf("deleting a splitorder: got %v, want MethodNotSupported or NotFound", err)
	}

	// And the verb it does declare is served: a patch against a name that does
	// not exist must come back NotFound, not MethodNotSupported. The former
	// means the verb reached the storage.
	_, err = dynamicClient.Resource(splitOrdersGVR).Namespace(acmeNamespace).
		Patch(ctx, "no-such-order", types.MergePatchType, []byte(`{"spec":{}}`), metav1.PatchOptions{})
	if apierrors.IsMethodNotSupported(err) {
		t.Errorf("patching a splitorder was refused as unsupported, but it declares an update query")
	}
}

// TestWatchDisabledListsCarryAResourceVersion checks that a projection with
// watch disabled still stamps its list responses.
//
// These have no poll loop, so there is no watermark to quote, and they used to
// answer with metadata.resourceVersion unset — which ListMeta is not allowed to
// omit, and which leaves a client with nothing to pass back. They now report
// the newest version among the rows they returned.
//
// Read from the raw list response rather than through kubectl -o jsonpath:
// jsonpath comes back empty for a working projection too, so it cannot tell the
// two apart.
func TestWatchDisabledListsCarryAResourceVersion(t *testing.T) {
	ctx := context.Background()

	// listVersion returns the list's own resourceVersion and the versions of
	// the rows it returned.
	listVersion := func(t *testing.T, resource string) (string, []string) {
		t.Helper()
		raw, err := discoveryClient.RESTClient().Get().
			AbsPath("/apis/store.example.com/v1alpha1/namespaces/" + acmeNamespace + "/" + resource).
			Do(ctx).Raw()
		if err != nil {
			t.Fatalf("listing %s: %v", resource, err)
		}
		var list struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
			Items []struct {
				Metadata struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"metadata"`
			} `json:"items"`
		}
		if err := json.Unmarshal(raw, &list); err != nil {
			t.Fatalf("decoding the %s list: %v", resource, err)
		}
		if len(list.Items) == 0 {
			t.Fatalf("%s returned no rows, so there is nothing to derive a version from", resource)
		}

		rows := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			rows = append(rows, item.Metadata.ResourceVersion)
		}
		return list.Metadata.ResourceVersion, rows
	}

	// taggedorders has watch disabled and maps updated_at as its version.
	t.Run("with a mapped version", func(t *testing.T) {
		got, rows := listVersion(t, "taggedorders")
		if got == "" {
			t.Fatal("taggedorders listed with no resourceVersion, but it maps one")
		}
		// The fallback derives the version from the rows in the response, so it
		// has to be one of them rather than a number from somewhere else.
		if !slices.Contains(rows, got) {
			t.Errorf("taggedorders reported list resourceVersion %q, which is not the version of any row it returned: %v", got, rows)
		}
	})

	// sloworders has watch disabled and maps no version at all. There is
	// genuinely nothing to report, and inventing an ordering nothing in the
	// database supports would be worse than reporting none — so this asserts
	// the documented behaviour rather than a version.
	t.Run("without a mapped version", func(t *testing.T) {
		if got, _ := listVersion(t, "sloworders"); got != "" {
			t.Errorf("sloworders reported resourceVersion %q, but it maps no version column", got)
		}
	})

	// A watched projection reports the watch cache's watermark instead, which is
	// the point a watch resumes from. Deliberately not checked against the rows
	// in the response: the watermark covers the whole collection, so a
	// namespaced page need not contain the row it came from — and asserting
	// otherwise passes or fails depending on which namespace was written to
	// last.
	t.Run("watch enabled is unchanged", func(t *testing.T) {
		if got, _ := listVersion(t, "orders"); got == "" {
			t.Error("orders listed with no resourceVersion")
		}
	})
}

// TestListThenWatchAfterARestartIsNotRefused covers the very first thing an
// informer does against a server that has just come up.
//
// Polling starts with the first watcher, so before that the watch cache had
// read nothing and reported its own counter — the number 1 — as the list's
// resourceVersion. Watching from it was then refused: "too old resource
// version: 1 (10000)". Every projection with a mapped resourceVersion, every
// restart, every informer.
//
// The list is read raw rather than through kubectl -o jsonpath, which comes
// back empty for a working projection too and cannot tell the two apart.
func TestListThenWatchAfterARestartIsNotRefused(t *testing.T) {
	ctx := context.Background()

	restartAPIServer(t)

	// Immediately, before anything else has cause to watch: this is the window
	// the defect lived in.
	raw, err := discoveryClient.RESTClient().Get().
		AbsPath("/apis/store.example.com/v1alpha1/namespaces/" + acmeNamespace + "/orders").
		Do(ctx).Raw()
	if err != nil {
		t.Fatalf("listing orders: %v", err)
	}
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decoding the list: %v", err)
	}
	version := list.Metadata.ResourceVersion
	if version == "" {
		t.Fatal("the list carried no resourceVersion to watch from")
	}
	if version == "1" {
		t.Fatalf("the list reported resourceVersion %q, which is the cache's counter and not a version from the data", version)
	}

	watcher, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
		Watch(ctx, metav1.ListOptions{ResourceVersion: version})
	if err != nil {
		if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
			t.Fatalf("watching from the version the list just reported was refused: %v", err)
		}
		t.Fatalf("watching orders: %v", err)
	}
	defer watcher.Stop()

	// A 410 can also arrive as the first event on the stream rather than as an
	// error from Watch itself.
	select {
	case event, ok := <-watcher.ResultChan():
		if ok && event.Type == watch.Error {
			t.Fatalf("the watch stream opened with an error event: %#v", event.Object)
		}
	case <-time.After(3 * time.Second):
		// Nothing to report on an idle collection, which is the expected case.
	}
}

// restartAPIServer rolls the deployment and waits for it to come back, so that
// the next request meets a server whose caches are empty.
func restartAPIServer(t *testing.T) {
	t.Helper()

	for _, args := range [][]string{
		{"-n", "kube-crisp", "rollout", "restart", "deployment", "kube-crisp-apiserver"},
		{"-n", "kube-crisp", "rollout", "status", "deployment", "kube-crisp-apiserver", "--timeout=180s"},
	} {
		cmd := exec.Command("kubectl", append([]string{"--kubeconfig", kubeconfigPath}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kubectl %v: %v\n%s", args, err, output)
		}
	}

	// The Deployment reporting complete means the new Pod passed its readiness
	// probe. The aggregation layer takes a moment longer to route to it, and
	// until it does the list below fails rather than showing anything.
	ctx := context.Background()
	deadline := time.Now().Add(90 * time.Second)
	for {
		err := discoveryClient.RESTClient().Get().
			AbsPath("/apis/store.example.com/v1alpha1/namespaces/" + acmeNamespace + "/orders").
			Do(ctx).Error()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the projected API never came back after the restart: %v", err)
		}
		time.Sleep(time.Second)
	}

	awaitWebhookInForce(t)
}

// awaitWebhookInForce waits until the projection webhook refuses something
// again.
//
// A restart replaces the certificate the server signs itself with, and the
// configuration naming the old one is corrected by the pod that comes up. Until
// that lands the webhook cannot be called, and because its policy is Ignore
// that is silent: a projection it would refuse is accepted instead. A test
// after a restart that depends on the webhook then fails on the timing rather
// than on what it is testing, which is exactly what happened.
//
// Asks for a table that does not exist, which is the webhook's whole purpose
// and something only it can object to, through a server-side dry run that
// reaches admission and writes nothing.
func awaitWebhookInForce(t *testing.T) {
	t.Helper()

	probe := []byte(`{
	  "apiVersion": "crisp.kubecrisp.io/v1alpha1",
	  "kind": "CustomResourceProjection",
	  "metadata": {"name": "e2e-webhook-inforce-probe"},
	  "spec": {
	    "dataSource": {"driver": "postgres", "secretRef": {"name": "orders-db", "namespace": "kube-crisp"}},
	    "resource": {"group": "store.example.com", "version": "v1alpha1", "kind": "WebhookProbe",
	                 "plural": "webhookprobes", "scope": "Namespaced", "schema": {"type": "object"}},
	    "queries": {"list": {"sql": "SELECT id, tenant FROM no_such_table_for_the_probe WHERE tenant = :namespace"}},
	    "mapping": {"name": "id", "namespace": "tenant"}
	  }
	}`)

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := discoveryClient.RESTClient().Post().
			AbsPath("/apis/crisp.kubecrisp.io/v1alpha1/customresourceprojections").
			Param("dryRun", "All").
			Body(probe).
			Do(ctx).Error()
		if err != nil && strings.Contains(err.Error(), "no_such_table_for_the_probe") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the projection webhook never came back into force after the restart (last: %v)", err)
		}
		time.Sleep(2 * time.Second)
	}
}
