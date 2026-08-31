package server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// NewCommandValidate builds the `validate` subcommand.
//
// A projection is only rejected once it reaches a cluster, which is late: the
// author has already committed the file and, with --projection-dir, has already
// rolled it out. Everything projection.Validate checks — the schema, the
// mapping, the queries, the parameters they declare — needs no database and no
// API server, so it can be checked wherever the file is written.
//
// What it deliberately does not check is whether the database can run the
// statements. That needs the database, and the server does it at compile time.
// Saying so here matters: a file this accepts is a well-formed projection, not
// a projection that is known to work.
func NewCommandValidate(out, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate PATH...",
		Short: "Check CustomResourceProjection manifests without a cluster",
		Long: "Validates CustomResourceProjection manifests the way the server does when it loads\n" +
			"them, without needing a cluster or a database. Each argument is a file or a\n" +
			"directory of them.\n\n" +
			"Exits non-zero if any projection is rejected, so this can gate a commit.\n\n" +
			"Whether the database can actually run the statements is not checked here: that\n" +
			"needs the database, and the server checks it when the projection is compiled.",
		Args:         cobra.MinimumNArgs(1),
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			return runValidate(args, out, errOut)
		},
	}
	return cmd
}

func runValidate(paths []string, out, errOut io.Writer) error {
	var (
		checked    int
		rejected   int
		unreadable int
	)

	for _, candidate := range expand(paths, errOut, &unreadable) {
		path := candidate.path
		// LoadPath parses without validating, so a projection that will not
		// serve is still reported rather than ending the run.
		projections, err := projection.LoadPath(path)
		if err != nil {
			// A path that will not parse is counted apart from a projection
			// that will not validate. They are different failures, and a
			// summary that added them together reported "1 of 0 rejected".
			_, _ = fmt.Fprintf(errOut, "%s: %v\n", path, err)
			unreadable++
			continue
		}
		if len(projections) == 0 {
			// A file named on the command line that holds no projection is a
			// mistake worth reporting — a misspelt name, or the wrong file.
			// One found by walking a directory is not: a projection directory
			// may hold Secrets and ConfigMaps too, which the loader is
			// deliberately willing to ignore.
			if candidate.explicit {
				_, _ = fmt.Fprintf(errOut, "%s: no CustomResourceProjection manifests found\n", path)
				unreadable++
			}
			continue
		}

		// Sorted, so the same directory reports in the same order twice.
		sort.Slice(projections, func(i, j int) bool {
			return projections[i].Name < projections[j].Name
		})
		for i := range projections {
			checked++
			if err := projection.Validate(&projections[i]); err != nil {
				_, _ = fmt.Fprintf(errOut, "%s: %s: %v\n", path, projections[i].Name, err)
				rejected++
				continue
			}
			_, _ = fmt.Fprintf(out, "ok  %s: %s (%s)\n", path, projections[i].Name,
				resourceDescription(&projections[i]))
		}
	}

	switch {
	case rejected > 0 && unreadable > 0:
		return fmt.Errorf("%d of %d projection(s) rejected, and %d path(s) could not be read",
			rejected, checked, unreadable)
	case rejected > 0:
		return fmt.Errorf("%d of %d projection(s) rejected", rejected, checked)
	case unreadable > 0:
		return fmt.Errorf("%d path(s) could not be read", unreadable)
	}

	_, _ = fmt.Fprintf(out, "\n%d projection(s) validated\n", checked)
	return nil
}

// expand turns any directories among the arguments into the files inside them.
//
// So that one unparseable file does not hide the rest. Reading a directory as a
// unit means the first file that will not parse ends the walk, and a checker
// that reports the first of three problems is a checker somebody has to run
// three times — which is the thing this command exists to avoid.
func expand(paths []string, errOut io.Writer, unreadable *int) []candidatePath {
	out := make([]candidatePath, 0, len(paths))

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			_, _ = fmt.Fprintf(errOut, "%s: %v\n", path, err)
			*unreadable++
			continue
		}
		if !info.IsDir() {
			out = append(out, candidatePath{path: path, explicit: true})
			continue
		}

		entries, err := os.ReadDir(path)
		if err != nil {
			_, _ = fmt.Fprintf(errOut, "%s: %v\n", path, err)
			*unreadable++
			continue
		}

		var found int
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".yaml", ".yml":
				out = append(out, candidatePath{path: filepath.Join(path, e.Name())})
				found++
			}
		}
		if found == 0 {
			_, _ = fmt.Fprintf(errOut, "%s: no YAML files found\n", path)
			*unreadable++
		}
	}

	return out
}

// candidatePath is a file to check, and whether it was named on the command
// line or found by walking a directory. The two are held to different
// standards: a named file that holds no projection is a mistake, and one
// alongside them in a directory is ordinary.
type candidatePath struct {
	path     string
	explicit bool
}

// resourceDescription names what a projection would serve, so the output says
// what was accepted rather than only that something was.
func resourceDescription(p *crispv1alpha1.CustomResourceProjection) string {
	res := p.Spec.Resource
	return fmt.Sprintf("%s.%s/%s", res.Plural, res.Group, res.Version)
}
