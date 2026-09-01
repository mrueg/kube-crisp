package plugin

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
)

// What a completion is allowed to cost, and what it may say.
//
// A completion runs while somebody is holding down Tab, so an unreachable
// cluster has to cost a beat rather than the shell. Two seconds is longer than
// a reachable API server takes and shorter than a person waits before deciding
// the terminal has hung.
const completionTimeout = 2 * time.Second

// noAnswer is what every completion here returns when it cannot answer.
//
// Silent, and deliberately so: during a completion request stdout is the
// protocol, read by kubectl and parsed as the list of choices. A message
// printed there becomes a suggestion, so a completion that cannot reach the
// cluster must say nothing at all rather than explain itself. The Error
// directive tells the shell the same thing without going through stdout, and
// NoFileComp stops it falling back to offering filenames -- which is what a
// projection name position did before any of this existed.
func noAnswer() ([]string, cobra.ShellCompDirective) {
	return nil, cobra.ShellCompDirectiveError | cobra.ShellCompDirectiveNoFileComp
}

// choice is one candidate: the word to insert, and what cobra shows beside it.
type choice struct {
	value       string
	description string
}

// format renders the candidates cobra's protocol expects, keeping only those
// that extend what has been typed and dropping any word already on the line.
//
// Dropping those matters because every command taking projection names is
// variadic: naming one twice reads the same projection twice, which is never
// what the second one meant.
func format(choices []choice, typed []string, toComplete string) []string {
	already := make(map[string]bool, len(typed))
	for _, word := range typed {
		already[word] = true
	}

	out := make([]string, 0, len(choices))
	for _, c := range choices {
		if already[c.value] || !strings.HasPrefix(c.value, toComplete) {
			continue
		}
		if c.description == "" {
			out = append(out, c.value)
			continue
		}
		// A tab separates the word from its description; cobra renders the
		// second half and never inserts it.
		out = append(out, c.value+"\t"+c.description)
	}
	sort.Strings(out)
	return out
}

// lister is where a completion's candidates come from. A function rather than a
// client so a test can answer without a cluster, the way prune takes its
// clients rather than building them.
type lister func(ctx context.Context) ([]choice, error)

// completeProjectionNames completes the NAME... arguments of rbac, can-i and
// schema.
//
// filenames is the command's -f, and its presence turns this off: -f reads
// manifests instead of the cluster, and the two cannot be combined -- the
// commands refuse it. Offering cluster names beside a -f would suggest an
// argument the command is about to reject.
func completeProjectionNames(
	list lister,
	filenames *[]string,
) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if filenames != nil && len(*filenames) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return completeFrom(cmd, list, args, toComplete)
	}
}

// completeFrom runs a lister under the timeout and formats what it returns.
func completeFrom(
	cmd *cobra.Command,
	list lister,
	typed []string,
	toComplete string,
) ([]string, cobra.ShellCompDirective) {
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, completionTimeout)
	defer cancel()

	choices, err := list(ctx)
	if err != nil {
		return noAnswer()
	}
	return format(choices, typed, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// fixed completes a closed set of values, for a flag whose choices are written
// down rather than looked up -- the output formats, which each command
// validates against its own list.
func fixed(values ...string) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		choices := make([]choice, 0, len(values))
		for _, v := range values {
			choices = append(choices, choice{value: v})
		}
		return format(choices, nil, toComplete), cobra.ShellCompDirectiveNoFileComp
	}
}

// projections lists the projections in the cluster as completion candidates,
// described by the resource each one serves -- which is what tells two
// similarly named projections apart, and comes free with the same List.
func (c *clientFlags) projections(ctx context.Context) ([]choice, error) {
	client, err := c.crisp()
	if err != nil {
		return nil, err
	}
	return projectionChoices(ctx, client)
}

// projectionChoices takes its client rather than building one, so that what it
// makes of a list can be tested without a cluster -- the seam prune already
// uses for the same reason.
func projectionChoices(ctx context.Context, client crispclient.Interface) ([]choice, error) {
	list, err := client.CrispV1alpha1().CustomResourceProjections().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	out := make([]choice, 0, len(list.Items))
	for i := range list.Items {
		resource := list.Items[i].Spec.Resource
		out = append(out, choice{
			value:       list.Items[i].Name,
			description: resource.Plural + "." + resource.Group + "/" + resource.Version,
		})
	}
	return out, nil
}

// namespaces lists the cluster's namespaces, for can-i's -n.
func (c *clientFlags) namespaces(ctx context.Context) ([]choice, error) {
	client, err := c.kube()
	if err != nil {
		return nil, err
	}
	return namespaceChoices(ctx, client)
}

// namespaceChoices takes its client, for the reason projectionChoices does.
func namespaceChoices(ctx context.Context, client kubernetes.Interface) ([]choice, error) {
	list, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	out := make([]choice, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, choice{value: list.Items[i].Name})
	}
	return out, nil
}

// completeContexts completes --context from the kubeconfig.
//
// A file read and no request, so this answers with no cluster at all -- which
// is the state somebody choosing a context is often in.
func (c *clientFlags) completeContexts(
	_ *cobra.Command, _ []string, toComplete string,
) ([]string, cobra.ShellCompDirective) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if c.kubeconfig != "" {
		rules.ExplicitPath = c.kubeconfig
	}
	config, err := rules.Load()
	if err != nil {
		return noAnswer()
	}

	choices := make([]choice, 0, len(config.Contexts))
	for name, ctx := range config.Contexts {
		choices = append(choices, choice{value: name, description: ctx.Cluster})
	}
	return format(choices, nil, toComplete), cobra.ShellCompDirectiveNoFileComp
}
