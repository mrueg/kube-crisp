package projection

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
)

func suffixManager(t *testing.T, suffixes ...string) *apiServiceManager {
	t.Helper()

	return newAPIServiceManager(
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{APIServiceGVR: "APIServiceList"},
		),
		APIServiceOptions{
			Enabled:              true,
			ServiceName:          "kube-crisp-apiserver",
			ServiceNamespace:     "kube-crisp",
			Port:                 443,
			AllowedGroupSuffixes: suffixes,
		},
		nil,
	)
}

// Registering a group is not a local act. An APIService is cluster-scoped and
// routes a whole group/version here, and the kube-apiserver's own controllers
// only manage the ones carrying the automanaged label — so a projection naming
// a group whose operator is not installed yet takes it, and nothing hands it
// back when the real operator arrives.
func TestAGroupOutsideThePermittedSuffixesIsNotRegistered(t *testing.T) {
	manager := suffixManager(t, "example.com", "acme.internal")

	resources := []apidynamic.Resource{
		{Group: "cert-manager.io", Version: "v1", Plural: "certificates"},
	}
	// Per-group-version, so the projection behind it reports the refusal in its
	// own Registered condition rather than one failure stopping the others.
	unregistered, err := manager.reconcile(context.Background(), resources, nil)
	if err != nil {
		t.Fatalf("reconcile() returned error: %v", err)
	}
	gv := schema.GroupVersion{Group: "cert-manager.io", Version: "v1"}
	refusal, refused := unregistered[gv]
	if !refused {
		t.Fatal("a projection claimed an API group outside the permitted suffixes")
	}
	if !strings.Contains(refusal.Error(), "cert-manager.io") {
		t.Errorf("the refusal does not name the group: %v", refusal)
	}

	if _, err := manager.client.Resource(APIServiceGVR).
		Get(context.Background(), "v1.cert-manager.io", metav1.GetOptions{}); err == nil {
		t.Error("the APIService was created anyway")
	}
}

// A group under one of them is registered as usual, including the suffix
// itself.
func TestAGroupUnderAPermittedSuffixIsRegistered(t *testing.T) {
	for _, group := range []string{"store.example.com", "example.com", "deep.nested.example.com"} {
		t.Run(group, func(t *testing.T) {
			manager := suffixManager(t, "example.com")

			resources := []apidynamic.Resource{{Group: group, Version: "v1alpha1", Plural: "orders"}}
			// Nothing reports availability behind a fake client, so a pending
			// registration is expected; what must not appear is a refusal.
			unregistered, err := manager.reconcile(context.Background(), resources, nil)
			if err != nil {
				t.Fatalf("reconcile() returned error: %v", err)
			}
			for gv, reason := range unregistered {
				if strings.Contains(reason.Error(), "not one this server may register") {
					t.Fatalf("reconcile() refused %s: %v", gv, reason)
				}
			}
			if _, err := manager.client.Resource(APIServiceGVR).
				Get(context.Background(), "v1alpha1."+group, metav1.GetOptions{}); err != nil {
				t.Errorf("the APIService for %s was not created: %v", group, err)
			}
		})
	}
}

// A suffix that merely appears inside the group is not that suffix: it is
// somebody else's domain.
func TestASuffixIsMatchedOnALabelBoundary(t *testing.T) {
	manager := suffixManager(t, "example.com")

	resources := []apidynamic.Resource{
		{Group: "example.com.evil.test", Version: "v1", Plural: "orders"},
	}
	unregistered, err := manager.reconcile(context.Background(), resources, nil)
	if err != nil {
		t.Fatalf("reconcile() returned error: %v", err)
	}
	if _, refused := unregistered[schema.GroupVersion{Group: "example.com.evil.test", Version: "v1"}]; !refused {
		t.Error("a group merely beginning with the permitted suffix was registered")
	}
}

// Unset allows any group, which is what every existing deployment had.
func TestNoPermittedSuffixesAllowsAnyGroup(t *testing.T) {
	manager := suffixManager(t)

	resources := []apidynamic.Resource{
		{Group: "cert-manager.io", Version: "v1", Plural: "certificates"},
	}
	unregistered, err := manager.reconcile(context.Background(), resources, nil)
	if err != nil {
		t.Fatalf("reconcile() returned error with no suffixes configured: %v", err)
	}
	for gv, reason := range unregistered {
		if strings.Contains(reason.Error(), "not one this server may register") {
			t.Errorf("%s was refused with no suffixes configured: %v", gv, reason)
		}
	}
	if _, err := manager.client.Resource(APIServiceGVR).
		Get(context.Background(), "v1.cert-manager.io", metav1.GetOptions{}); err != nil {
		t.Errorf("the APIService was not created: %v", err)
	}
}
