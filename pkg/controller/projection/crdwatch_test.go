package projection

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func borrowing(name, crd string) *crispv1alpha1.CustomResourceProjection {
	return &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			Resource: crispv1alpha1.ProjectedResource{
				Version:    "v1alpha1",
				SchemaFrom: &crispv1alpha1.CRDReference{Name: crd},
			},
		},
	}
}

// A cluster has a great many CustomResourceDefinitions, edited by things that
// have nothing to do with this server. Only the ones a projection borrows from
// are worth a sync.
func TestBorrowsFromNamesOnlyTheCRDsInUse(t *testing.T) {
	multi := borrowing("orders", "orders.example.com")
	multi.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{
		{Name: "v1beta1", SchemaFrom: &crispv1alpha1.CRDReference{Name: "orders-beta.example.com"}},
		{Name: "v1", Schema: nil},
	}

	inline := &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: "inline"},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			Resource: crispv1alpha1.ProjectedResource{Version: "v1alpha1"},
		},
	}

	for _, tc := range []struct {
		name       string
		projection *crispv1alpha1.CustomResourceProjection
		crd        string
		want       bool
	}{
		{"the primary version's CRD", multi, "orders.example.com", true},
		{"another version's CRD", multi, "orders-beta.example.com", true},
		{"a CRD nothing borrows", multi, "widgets.example.com", false},
		{"a projection with its own schema", inline, "orders.example.com", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := borrowsFrom(tc.projection, tc.crd); got != tc.want {
				t.Errorf("borrowsFrom(%s) = %v, want %v", tc.crd, got, tc.want)
			}
		})
	}
}

// A static projection read from a directory borrows a schema exactly as one in
// the cluster does, and the informer has to notice for both.
func TestACRDChangeQueuesASyncForStaticProjectionsToo(t *testing.T) {
	c := &Controller{
		queue: workqueue.NewTypedRateLimitingQueue[string](
			workqueue.DefaultTypedControllerRateLimiter[string]()),
		static: []crispv1alpha1.CustomResourceProjection{*borrowing("orders", "orders.example.com")},
	}

	c.queueIfBorrowed(&metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.example.com"},
	})
	if c.queue.Len() != 0 {
		t.Error("a CRD nothing borrows queued a sync")
	}

	c.queueIfBorrowed(&metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{Name: "orders.example.com"},
	})
	if c.queue.Len() != 1 {
		t.Error("the CRD a static projection borrows from did not queue a sync")
	}
}
