package plugin

import (
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// CompletionName is the name kubectl runs to complete `kubectl crisp`.
//
// kubectl reaches a plugin's completion through one hook and no other. Its
// pluginCompletion derives this name from the plugin's own — the first `-`
// becomes `_complete-` — looks it up with exec.LookPath, runs what it finds
// with the words typed so far, and reads the output as cobra's __complete
// protocol. A plugin that could answer perfectly is never asked if no file
// carries the name, which is why `kubectl crisp <TAB>` used to offer nothing
// while `kubectl-crisp __complete ""` answered.
//
// Nothing says the file has to be a separate program. A wrapper script would be
// a second thing to build, ship, sign and keep in step with this one, so the
// plugin answers to the name itself and the two cannot drift:
//
//	ln -s kubectl-crisp kubectl_complete-crisp
const CompletionName = "kubectl_complete-crisp"

// CompletionArgs turns a process's argv into the arguments to run.
//
// Under CompletionName the arguments are the ones kubectl wants completed, so
// they become the hidden __complete call cobra already gives every command.
// Under any other name — kubectl-crisp, or `kubectl crisp`, which execs the
// same file — they are the user's own and are passed through untouched.
//
// The .exe comes off for windows, where kubectl's lookup goes through PATHEXT
// and so only finds the name with an executable extension on it.
func CompletionArgs(argv []string) []string {
	if len(argv) == 0 {
		return nil
	}
	if strings.TrimSuffix(filepath.Base(argv[0]), ".exe") != CompletionName {
		return argv[1:]
	}
	return append([]string{cobra.ShellCompRequestCmd}, argv[1:]...)
}
