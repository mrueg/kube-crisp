package plugin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"
)

func projectionServing(name, group, version, plural string) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = group
	p.Spec.Resource.Version = version
	p.Spec.Resource.Plural = plural
	return p
}

// runCompletion drives a ValidArgsFunction the way kubectl drives it.
func runCompletion(
	fn func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective),
	args []string,
	toComplete string,
) ([]string, cobra.ShellCompDirective) {
	cmd := &cobra.Command{Use: "rbac"}
	cmd.SetContext(context.Background())
	return fn(cmd, args, toComplete)
}

// The names come back described by the resource each projection serves, which
// is what tells two similarly named projections apart.
func TestCompletingProjectionNames(t *testing.T) {
	list := func(context.Context) ([]choice, error) {
		return []choice{
			{value: "pagila-films", description: "films.pagila.example.com/v1alpha1"},
			{value: "pagila-actors", description: "actors.pagila.example.com/v1alpha1"},
		}, nil
	}

	got, directive := runCompletion(completeProjectionNames(list, nil), nil, "")
	want := []string{
		"pagila-actors\tactors.pagila.example.com/v1alpha1",
		"pagila-films\tfilms.pagila.example.com/v1alpha1",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("completed %q, want %q", got, want)
	}
	// Never files: a filename is not a projection name, and offering one is
	// what the position did before it completed anything.
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive = %d, want NoFileComp", directive)
	}
}

func TestCompletingProjectionNamesFiltersOnWhatIsTyped(t *testing.T) {
	list := func(context.Context) ([]choice, error) {
		return []choice{{value: "orders"}, {value: "films"}}, nil
	}
	got, _ := runCompletion(completeProjectionNames(list, nil), nil, "ord")
	if len(got) != 1 || got[0] != "orders" {
		t.Errorf("completed %q, want [orders]", got)
	}
}

// Every command taking names is variadic, and naming one twice reads the same
// projection twice.
func TestCompletingProjectionNamesDropsOnesAlreadyNamed(t *testing.T) {
	list := func(context.Context) ([]choice, error) {
		return []choice{{value: "orders"}, {value: "films"}}, nil
	}
	got, _ := runCompletion(completeProjectionNames(list, nil), []string{"orders"}, "")
	if len(got) != 1 || got[0] != "films" {
		t.Errorf("completed %q, want [films]", got)
	}
}

// -f reads manifests instead of the cluster and cannot be combined with names,
// so completing them there would suggest an argument the command refuses.
func TestCompletingProjectionNamesIsOffWithFilenames(t *testing.T) {
	called := false
	list := func(context.Context) ([]choice, error) {
		called = true
		return []choice{{value: "orders"}}, nil
	}
	filenames := []string{"manifests/"}

	got, directive := runCompletion(completeProjectionNames(list, &filenames), nil, "")
	if len(got) != 0 {
		t.Errorf("completed %q, want nothing", got)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive = %d, want NoFileComp", directive)
	}
	if called {
		t.Error("the cluster was listed for a completion that cannot use the answer")
	}
}

// An unreachable cluster must cost a beat and say nothing.
//
// Nothing, because stdout is the completion protocol during a request: anything
// written there is read back as a suggestion, so an error message would become
// one of the choices.
func TestACompletionThatCannotAnswerIsSilent(t *testing.T) {
	list := func(context.Context) ([]choice, error) {
		return nil, errors.New("connection refused")
	}
	got, directive := runCompletion(completeProjectionNames(list, nil), nil, "")
	if len(got) != 0 {
		t.Errorf("completed %q, want nothing", got)
	}
	if directive&cobra.ShellCompDirectiveError == 0 {
		t.Errorf("directive = %d, want the error bit", directive)
	}
	if directive&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("directive = %d, want NoFileComp so the shell does not offer files", directive)
	}
}

// The completion is bounded, so holding Tab against a cluster that is not
// answering costs a beat rather than the shell.
func TestACompletionIsBounded(t *testing.T) {
	list := func(ctx context.Context) ([]choice, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if got, _ := runCompletion(completeProjectionNames(list, nil), nil, ""); len(got) != 0 {
			t.Errorf("completed %q, want nothing", got)
		}
	}()

	select {
	case <-done:
	case <-time.After(completionTimeout * 4):
		t.Fatal("the completion did not give up")
	}
}

// The output formats are a closed set each command validates against, so they
// are written down rather than looked up.
func TestFixedValues(t *testing.T) {
	got, directive := runCompletion(fixed("yaml", "json"), nil, "")
	if len(got) != 2 || got[0] != "json" || got[1] != "yaml" {
		t.Errorf("completed %q, want [json yaml]", got)
	}
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive = %d, want NoFileComp", directive)
	}

	if got, _ = runCompletion(fixed("yaml", "json"), nil, "y"); len(got) != 1 || got[0] != "yaml" {
		t.Errorf("completed %q for \"y\", want [yaml]", got)
	}
}

func TestProjectionChoicesDescribeTheResource(t *testing.T) {
	client := crispfake.NewSimpleClientset(
		projectionServing("pagila-films", "pagila.example.com", "v1alpha1", "films"),
	)

	got, err := projectionChoices(context.Background(), client)
	if err != nil {
		t.Fatalf("projectionChoices() returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d choices, want 1", len(got))
	}
	if got[0].value != "pagila-films" {
		t.Errorf("value = %q, want pagila-films", got[0].value)
	}
	if got[0].description != "films.pagila.example.com/v1alpha1" {
		t.Errorf("description = %q, want the resource it serves", got[0].description)
	}
}

func TestNamespaceChoices(t *testing.T) {
	client := kubefake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "acme"}},
	)

	got, err := namespaceChoices(context.Background(), client)
	if err != nil {
		t.Fatalf("namespaceChoices() returned error: %v", err)
	}
	if len(got) != 1 || got[0].value != "acme" {
		t.Errorf("got %+v, want acme", got)
	}
}

// prune takes no arguments, and the position has to say so or the shell falls
// back to offering files for an argument the command rejects.
func TestPruneOffersNothingForAnArgument(t *testing.T) {
	cmd := NewCommandPrune(nil, nil)
	if cmd.ValidArgsFunction == nil {
		t.Fatal("prune completes its arguments with nothing at all")
	}
	got, directive := runCompletion(cmd.ValidArgsFunction, nil, "")
	if len(got) != 0 || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("completed %q with directive %d, want nothing and NoFileComp", got, directive)
	}
}

// Every command that takes NAME... completes them, and the one that does not
// says so. A command added without either is the case this catches.
func TestEveryCommandAnswersForItsArguments(t *testing.T) {
	for _, cmd := range []*cobra.Command{
		NewCommandRBAC(nil, nil),
		NewCommandCanI(nil, nil),
		NewCommandSchema(nil, nil),
		NewCommandPrune(nil, nil),
	} {
		if cmd.ValidArgsFunction == nil {
			t.Errorf("%s does not complete its arguments, so the shell offers filenames", cmd.Name())
		}
	}
}
