package projection

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// withStatus gives a projection the status a server wrote for it earlier, at
// the given generation, with Ready set to reason.
func withStatus(p *crispv1alpha1.CustomResourceProjection, generation int64, reason string, served ...string) *crispv1alpha1.CustomResourceProjection {
	out := p.DeepCopy()
	status := metav1.ConditionFalse
	if reason == "Serving" {
		status = metav1.ConditionTrue
	}
	out.Status.ObservedGeneration = generation
	out.Status.ServedPaths = served
	apimeta.SetStatusCondition(&out.Status.Conditions, metav1.Condition{
		Type: crispv1alpha1.ConditionReady, Status: status, Reason: reason, ObservedGeneration: generation,
	})
	return out
}

// A server that starts up keeps the resource with whoever was serving it.
//
// The winner used to be decided from what this process had compiled, which a
// process that has just started has none of: the older projection won instead,
// so a restart handed a contested resource from the projection serving it to
// the one that had lost it -- and of two replicas, the one that had been
// running and the one that had just started served the same resource from two
// different tables. What every replica sees the same way is the status the
// servers wrote, and that says who was serving.
func TestARestartKeepsAContestedResourceWithTheProjectionServingIt(t *testing.T) {
	// alpha is older, so that only the status can explain beta keeping bins.
	alpha := withStatus(projectionObject("alpha", "bins"), 1, "ResourceClaimed")
	alpha.CreationTimestamp = at(1)
	beta := withStatus(projectionObject("beta", "bins"), 1, "Serving",
		"/apis/warehouse.example.com/v1alpha1/bins")
	beta.CreationTimestamp = at(2)

	f := newFixture(t, []k8sruntime.Object{alpha, beta})
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if degraded := f.controller.Degraded(); degraded[0] != "alpha" {
		t.Errorf("Degraded() = %v, want [alpha]: beta was serving bins before the restart", degraded)
	}
	if ready := readyCondition(t, f, "beta"); ready.Status != metav1.ConditionTrue {
		t.Errorf("beta Ready = %s (%s: %s), want True", ready.Status, ready.Reason, ready.Message)
	}
	if ready := readyCondition(t, f, "alpha"); ready.Reason != "ResourceClaimed" {
		t.Errorf("alpha Ready reason = %q, want ResourceClaimed", ready.Reason)
	}
}

// A projection that lost a claim stays lost when the one that beat it is
// edited.
//
// An edit is a generation the status has not caught up with yet, so the
// projection serving the resource says nothing about it for the one sync that
// processes the edit. The projection that lost has said so in its own status,
// and that is enough to keep it from taking the resource back in that window
// on the strength of being older.
func TestAnEditToTheProjectionServingAResourceDoesNotHandItToTheLoser(t *testing.T) {
	alpha := withStatus(projectionObject("alpha", "bins"), 1, "ResourceClaimed")
	alpha.CreationTimestamp = at(1)
	beta := withStatus(projectionObject("beta", "bins"), 1, "Serving",
		"/apis/warehouse.example.com/v1alpha1/bins")
	beta.CreationTimestamp = at(2)
	// Edited since the status was written, in a way that has nothing to do
	// with the claim.
	beta.Generation = 2
	beta.Spec.Queries.List.SQL += " ORDER BY id"

	f := newFixture(t, []k8sruntime.Object{alpha, beta})
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	if degraded := f.controller.Degraded(); len(degraded) != 1 || degraded[0] != "alpha" {
		t.Errorf("Degraded() = %v, want [alpha]: beta's edit did not touch the claim", degraded)
	}
}

// Two replicas settle a contest the same way, whatever each has compiled.
//
// The first has been serving beta since before alpha arrived. The second has
// just started: it has compiled nothing, and alpha is older. They used to
// disagree, and a client was answered from beta's table or alpha's depending
// on which replica the Service picked.
func TestReplicasSettleAContestedResourceTheSameWay(t *testing.T) {
	beta := projectionObject("beta", "bins")
	beta.CreationTimestamp = at(2)

	running := newFixture(t, []k8sruntime.Object{beta})
	running.syncUntil(t, func() bool { return len(running.router.ServedPaths()) == 1 })

	// alpha arrives, older by its timestamp, through the same API.
	alpha := projectionObject("alpha", "bins")
	alpha.CreationTimestamp = at(1)
	if _, err := running.client.CrispV1alpha1().CustomResourceProjections().
		Create(context.Background(), alpha, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating alpha: %v", err)
	}
	running.syncUntil(t, func() bool { return len(running.controller.Degraded()) == 1 })

	// The second replica sees what the first wrote.
	var objects []k8sruntime.Object
	for _, name := range []string{"alpha", "beta"} {
		obj, err := running.client.CrispV1alpha1().CustomResourceProjections().
			Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		objects = append(objects, obj)
	}
	started := newFixture(t, objects)
	started.syncUntil(t, func() bool { return len(started.controller.Degraded()) == 1 })

	if a, b := running.controller.Degraded(), started.controller.Degraded(); a[0] != b[0] {
		t.Errorf("the running replica failed %v and the one that just started failed %v", a, b)
	}
	if degraded := started.controller.Degraded(); degraded[0] != "alpha" {
		t.Errorf("Degraded() = %v, want [alpha]: beta was serving bins first", degraded)
	}
}
