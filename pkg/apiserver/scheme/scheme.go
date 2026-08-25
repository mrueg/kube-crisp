// Package scheme builds the runtime scheme kube-crisp serves with. It is
// separate from the apiserver package so that lower layers, and their tests,
// can construct a scheme without importing the server.
package scheme

import (
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// New returns a scheme holding kube-crisp's own API plus the meta types every
// apiserver needs. Projected kinds are added to it as they are compiled.
func New() (*runtime.Scheme, serializer.CodecFactory) {
	scheme := runtime.NewScheme()

	utilruntime.Must(crispv1alpha1.AddToScheme(scheme))
	utilruntime.Must(metainternalversion.AddToScheme(scheme))

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	metav1.AddToGroupVersion(scheme, unversioned)
	scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)

	return scheme, serializer.NewCodecFactory(scheme)
}
