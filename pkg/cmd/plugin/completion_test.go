package plugin

import (
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// pluginBinary is the name kubectl finds the plugin under; CompletionName is
// what kubectl derives from it.
const pluginBinary = "kubectl-crisp"

// The name is not ours to choose. kubectl builds it by replacing the first "-"
// of the plugin's name with "_complete-", and looks up that and nothing else,
// so a constant that stops matching the rule is a completion that is never
// called — silently, since the miss only shows with completion debugging on.
func TestCompletionNameIsTheOneKubectlDerives(t *testing.T) {
	if want := strings.Replace(pluginBinary, "-", "_complete-", 1); CompletionName != want {
		t.Errorf("CompletionName = %q, kubectl looks for %q", CompletionName, want)
	}
}

func TestCompletionArgs(t *testing.T) {
	comp := cobra.ShellCompRequestCmd

	for _, tc := range []struct {
		name string
		argv []string
		want []string
	}{{
		name: "run under the plugin's own name",
		argv: []string{"kubectl-crisp", "rbac", "films"},
		want: []string{"rbac", "films"},
	}, {
		name: "run under the completion name, as kubectl runs it",
		argv: []string{CompletionName, "rbac", ""},
		want: []string{comp, "rbac", ""},
	}, {
		// kubectl passes the path exec.LookPath resolved, not a bare name.
		name: "resolved path",
		argv: []string{"/usr/local/bin/" + CompletionName, "schema"},
		want: []string{comp, "schema"},
	}, {
		// Only the extension is this package's business: splitting a path is
		// filepath.Base's, and it takes windows' separator on windows and not
		// here, where a backslash is an ordinary character in a name. The
		// slashes are the ones both agree on.
		name: "windows, where the lookup needs an extension",
		argv: []string{"C:/bin/" + CompletionName + ".exe", "can-i"},
		want: []string{comp, "can-i"},
	}, {
		// Only the whole name counts: a binary somebody renamed to end in it is
		// still that binary.
		name: "a name merely ending in the completion name",
		argv: []string{"my-" + CompletionName, "prune"},
		want: []string{"prune"},
	}, {
		name: "no arguments at all",
		argv: []string{CompletionName},
		want: []string{comp},
	}, {
		// Go always gives argv[0], but nothing here should index into an empty
		// slice if it ever does not.
		name: "empty argv",
		argv: nil,
		want: nil,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := CompletionArgs(tc.argv)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("CompletionArgs(%q) = %q, want %q", tc.argv, got, tc.want)
			}
		})
	}
}

// The plugin has to answer the protocol kubectl reads, and it is cobra's: the
// completions on stdout and a trailing :<directive> line. This is the command
// tree answering as kubectl would call it.
func TestCompletionArgsReachCobrasCompletion(t *testing.T) {
	root := &cobra.Command{Use: pluginBinary}
	root.AddCommand(NewCommandRBAC(nil, nil), NewCommandSchema(nil, nil))

	var out strings.Builder
	root.SetOut(&out)
	root.SetArgs(CompletionArgs([]string{CompletionName, ""}))
	if err := root.Execute(); err != nil {
		t.Fatalf("completing: %v", err)
	}

	for _, want := range []string{"rbac\t", "schema\t", ":"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("completion output missing %q:\n%s", want, out.String())
		}
	}
}
