//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// conditionOfProjection reads one condition from a projection's status.
func conditionOfProjection(t *testing.T, name, conditionType string) (status, reason, message string) {
	t.Helper()

	raw, err := discoveryClient.RESTClient().Get().
		AbsPath("/apis/crisp.kubecrisp.io/v1alpha1/customresourceprojections/" + name).
		Do(context.Background()).Raw()
	if err != nil {
		t.Fatalf("reading projection %s: %v", name, err)
	}
	var obj struct {
		Status struct {
			Conditions []struct {
				Type    string `json:"type"`
				Status  string `json:"status"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decoding projection %s: %v", name, err)
	}
	for _, c := range obj.Status.Conditions {
		if c.Type == conditionType {
			return c.Status, c.Reason, c.Message
		}
	}
	return "", "", ""
}

// awaitCondition waits for a projection's condition to reach a status.
func awaitCondition(t *testing.T, name, conditionType, want string, timeout time.Duration) (reason, message string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var status string
	for {
		status, reason, message = conditionOfProjection(t, name, conditionType)
		if status == want {
			return reason, message
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s condition %s is %q after %v, want %q (reason %q: %s)",
				name, conditionType, status, timeout, want, reason, message)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestRegisteredGoesFalseWhenTheAggregatorCannotReachTheServer covers the whole
// path: the aggregation layer stops routing, and the projection says so.
//
// Ready used to be true on the strength of the compile alone, so a projection
// nothing could reach reported "Serving /apis/..." while every request for it
// returned NotFound. Breaking the Service's selector is how a real cluster gets
// there — a Service with no endpoints — and it leaves this server running and
// able to report, because it reads the APIService from the kube-apiserver
// rather than through its own Service.
func TestRegisteredGoesFalseWhenTheAggregatorCannotReachTheServer(t *testing.T) {
	// Healthy to begin with, or the rest of this proves nothing.
	if status, reason, _ := conditionOfProjection(t, "orders", "Registered"); status != "True" {
		t.Fatalf("orders is not Registered=True to begin with: %s (%s)", status, reason)
	}

	kubectl := func(t *testing.T, args ...string) {
		t.Helper()
		cmd := exec.Command("kubectl", append([]string{"--kubeconfig", kubeconfigPath}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kubectl %v: %v\n%s", args, err, out)
		}
	}

	// Point the Service at nothing. Restored by the cleanup below whatever
	// happens next, because leaving it broken would take the rest of the suite
	// with it.
	t.Cleanup(func() {
		cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
			"-n", "kube-crisp", "patch", "service", "kube-crisp-apiserver", "--type=merge",
			"-p", `{"spec":{"selector":{"app.kubernetes.io/name":"kube-crisp-apiserver"}}}`)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("restoring the Service selector: %v\n%s", err, out)
		}
		// And wait until it is actually serving again.
		awaitCondition(t, "orders", "Registered", "True", 3*time.Minute)
	})

	kubectl(t, "-n", "kube-crisp", "patch", "service", "kube-crisp-apiserver", "--type=merge",
		"-p", `{"spec":{"selector":{"app.kubernetes.io/name":"nothing-is-labelled-this"}}}`)

	reason, message := awaitCondition(t, "orders", "Registered", "False", 3*time.Minute)
	if reason != "NotRouted" {
		t.Errorf("Registered reason = %q, want NotRouted", reason)
	}
	if message == "" {
		t.Error("Registered carries no message, so an operator is told nothing about why")
	}
	t.Logf("Registered=False: %s", message)

	// And Ready follows it down, because nothing can reach the projection.
	if status, reason, _ := conditionOfProjection(t, "orders", "Ready"); status != "False" {
		t.Errorf("Ready = %q while the projection was unreachable, want False (reason %q)", status, reason)
	} else if reason != "NotRegistered" {
		t.Errorf("Ready reason = %q, want NotRegistered", reason)
	}
}

// TestManagedAPIServicesAreOwnedByLiveProjections checks the owner references
// that make an uninstall clean up after itself.
//
// The UID matters as much as the name. Kubernetes deletes a dependent whose
// owner does not exist, so a reference carrying a stale UID would have the
// garbage collector remove each registration moments after this server created
// it — and this server would create it again, forever.
func TestManagedAPIServicesAreOwnedByLiveProjections(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(apiServiceGVR).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/managed-by=kube-crisp",
	})
	if err != nil {
		t.Fatalf("listing managed APIServices: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatal("no APIService is managed by kube-crisp, so there is nothing to check")
	}

	projections, err := dynamicClient.Resource(crpGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing projections: %v", err)
	}
	uids := map[string]string{}
	for _, p := range projections.Items {
		uids[p.GetName()] = string(p.GetUID())
	}

	var owned int
	for _, apiService := range list.Items {
		owners := apiService.GetOwnerReferences()
		if len(owners) == 0 {
			// Legitimate: a group version also served from --projection-dir is
			// deliberately left unowned, because no object's deletion should
			// collect it.
			t.Logf("%s has no owner, which is expected only if a file-based projection serves that group",
				apiService.GetName())
			continue
		}
		owned++
		for _, owner := range owners {
			if owner.Kind != "CustomResourceProjection" {
				t.Errorf("%s is owned by a %s, want a CustomResourceProjection", apiService.GetName(), owner.Kind)
				continue
			}
			uid, exists := uids[owner.Name]
			if !exists {
				t.Errorf("%s is owned by projection %q, which does not exist — the garbage collector will delete it",
					apiService.GetName(), owner.Name)
				continue
			}
			if uid != string(owner.UID) {
				t.Errorf("%s names projection %q with UID %s, but the live one is %s — a stale UID makes the garbage collector delete it",
					apiService.GetName(), owner.Name, owner.UID, uid)
			}
		}
	}
	if owned == 0 {
		t.Error("no managed APIService carries an owner reference, so an uninstall would leave every one behind")
	}
}
