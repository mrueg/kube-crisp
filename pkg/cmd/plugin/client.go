package plugin

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// clientFlags are the kubeconfig flags every command that reaches a cluster
// carries, spelled the way kubectl spells them.
type clientFlags struct {
	kubeconfig  string
	kubecontext string
}

func (c *clientFlags) bind(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&c.kubeconfig, "kubeconfig", "", "Path to the kubeconfig file to use.")
	f.StringVar(&c.kubecontext, "context", "", "Name of the kubeconfig context to use.")

	// Bound here rather than per command, since these are the flags every
	// command that reaches a cluster carries. The errors are the ones
	// RegisterFlagCompletionFunc returns for a flag that does not exist, and
	// both were just declared.
	_ = cmd.MarkFlagFilename("kubeconfig")
	_ = cmd.RegisterFlagCompletionFunc("context", c.completeContexts)
}

// config resolves the kubeconfig the usual way: --kubeconfig, then $KUBECONFIG,
// then the default path, and in-cluster if there is none.
func (c *clientFlags) config() (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if c.kubeconfig != "" {
		rules.ExplicitPath = c.kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if c.kubecontext != "" {
		overrides.CurrentContext = c.kubecontext
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	return config, nil
}

// crisp returns a client for CustomResourceProjections.
func (c *clientFlags) crisp() (crispclient.Interface, error) {
	config, err := c.config()
	if err != nil {
		return nil, err
	}
	return crispclient.NewForConfig(config)
}

// kube returns a client for the cluster's own objects: the ClusterRoles a
// generated role is one of, and the access reviews that answer who may reach a
// projected kind.
func (c *clientFlags) kube() (kubernetes.Interface, error) {
	config, err := c.config()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// loadFiles parses every projection under the given paths.
//
// A path holding none is an error here, where it means a mistyped argument. An
// empty cluster is the case that is not one.
func loadFiles(paths []string) ([]crispv1alpha1.CustomResourceProjection, error) {
	var out []crispv1alpha1.CustomResourceProjection
	for _, path := range paths {
		loaded, err := projection.LoadPath(path)
		if err != nil {
			return nil, err
		}
		if len(loaded) == 0 {
			return nil, fmt.Errorf("%s: no CustomResourceProjection manifests found", path)
		}
		out = append(out, loaded...)
	}
	return out, nil
}

// projectionsFromCluster reads the named projections, or every one of them when
// no name is given.
func projectionsFromCluster(
	ctx context.Context,
	flags *clientFlags,
	names []string,
) ([]crispv1alpha1.CustomResourceProjection, error) {
	client, err := flags.crisp()
	if err != nil {
		return nil, err
	}

	if len(names) == 0 {
		list, err := client.CrispV1alpha1().CustomResourceProjections().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("listing projections: %w", err)
		}
		return list.Items, nil
	}

	out := make([]crispv1alpha1.CustomResourceProjection, 0, len(names))
	for _, name := range names {
		p, err := client.CrispV1alpha1().CustomResourceProjections().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("getting projection %s: %w", name, err)
		}
		out = append(out, *p)
	}
	return out, nil
}
