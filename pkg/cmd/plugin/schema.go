package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

type schemaOptions struct {
	client    clientFlags
	filenames []string
	output    string
}

// projectionSchema is one projection's requirements, for -o json.
type projectionSchema struct {
	Projection string                         `json:"projection"`
	Resource   string                         `json:"resource"`
	Tables     []string                       `json:"tables,omitempty"`
	Columns    []crispv1alpha1.RequiredColumn `json:"columns,omitempty"`
}

// NewCommandSchema builds the `schema` subcommand.
func NewCommandSchema(out, errOut io.Writer) *cobra.Command {
	o := &schemaOptions{}

	cmd := &cobra.Command{
		Use:   "schema [NAME...]",
		Short: "Show what a projection needs from its database",
		Long: "Reports the tables a projection's statements name and the columns its mapping reads\n" +
			"out of their results.\n\n" +
			"kube-crisp projects rows; it does not create or migrate tables. A projection whose\n" +
			"table is missing reports CompilationFailed with the database's own message, which\n" +
			"says what went wrong and not what would have been right. This is the other half:\n" +
			"what the table would have to contain, for handing to whatever manages the schema.\n\n" +
			"A description and not a migration. The types are the projection's own — the type a\n" +
			"column is coerced to on the way into an object — rather than SQL types, and\n" +
			"nothing here knows about nullability, keys, or indexes. Read it as a checklist for\n" +
			"whoever writes the DDL, not as the DDL.\n\n" +
			"With no arguments it reads the projections in the cluster. Named arguments select\n" +
			"projections by name; -f reads manifests instead and needs no cluster.",
		Args:         cobra.ArbitraryArgs,
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			return o.run(c.Context(), args, out, errOut)
		},
	}

	f := cmd.Flags()
	f.StringSliceVarP(&o.filenames, "filename", "f", nil,
		"Manifest file or directory to read projections from, instead of the cluster. Repeatable.")
	f.StringVarP(&o.output, "output", "o", "text", "Output format: text or json.")
	cmd.ValidArgsFunction = completeProjectionNames(o.client.projections, &o.filenames)
	_ = cmd.MarkFlagFilename("filename", "yaml", "yml")
	_ = cmd.RegisterFlagCompletionFunc("output", fixed("text", "json"))
	o.client.bind(cmd)

	return cmd
}

func (o *schemaOptions) run(ctx context.Context, names []string, out, errOut io.Writer) error {
	if len(o.filenames) > 0 && len(names) > 0 {
		return fmt.Errorf("cannot combine -f with projection names: -f reads manifests, names read the cluster")
	}
	switch o.output {
	case "text", "json":
	default:
		return fmt.Errorf("unsupported output format %q: use text or json", o.output)
	}

	var (
		projections []crispv1alpha1.CustomResourceProjection
		err         error
	)
	if len(o.filenames) > 0 {
		projections, err = loadFiles(o.filenames)
	} else {
		projections, err = projectionsFromCluster(ctx, &o.client, names)
	}
	if err != nil {
		return err
	}
	if len(projections) == 0 {
		_, _ = fmt.Fprintln(errOut, "no projections found")
		return nil
	}

	schemas := requirements(projections)
	if o.output == "json" {
		return json.NewEncoder(out).Encode(schemas)
	}
	return text(out, errOut, schemas)
}

// requirements derives what each projection needs, sorted by name.
//
// Computed from the spec rather than read from status.requiredSchema, which is
// the same answer: the controller fills that field by calling this very
// function. Computing it means a manifest that has never reached a cluster
// reports the same thing as one that has — and a projection whose status has
// not been written yet, because it is new or because it cannot compile, still
// says what it would need.
func requirements(projections []crispv1alpha1.CustomResourceProjection) []projectionSchema {
	out := make([]projectionSchema, 0, len(projections))

	for i := range projections {
		p := &projections[i]
		entry := projectionSchema{
			Projection: p.Name,
			Resource: fmt.Sprintf("%s.%s/%s",
				p.Spec.Resource.Plural, p.Spec.Resource.Group, p.Spec.Resource.Version),
		}
		if required := projection.RequiredSchema(p.Spec); required != nil {
			entry.Tables = required.Tables
			entry.Columns = required.Columns
		}
		out = append(out, entry)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Projection < out[j].Projection })
	return out
}

// text prints the checklist.
func text(out, errOut io.Writer, schemas []projectionSchema) error {
	for i, schema := range schemas {
		if i > 0 {
			if _, err := fmt.Fprintln(out); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(out, "%s\t%s\n", schema.Projection, schema.Resource); err != nil {
			return err
		}

		if len(schema.Tables) == 0 && len(schema.Columns) == 0 {
			// A projection can read neither: its statements name no table this
			// can recognise, and its mapping reads no column. Said rather than
			// left as an empty section, which reads like a bug in this command.
			if _, err := fmt.Fprintln(out, "  nothing recognised: no table named in its statements, no column in its mapping"); err != nil {
				return err
			}
			continue
		}

		if len(schema.Tables) > 0 {
			if _, err := fmt.Fprintf(out, "  tables: %s\n", strings.Join(schema.Tables, ", ")); err != nil {
				return err
			}
		}
		if len(schema.Columns) == 0 {
			continue
		}

		w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(w, "  COLUMN\tTYPE\tREAD FOR"); err != nil {
			return err
		}
		for _, column := range schema.Columns {
			fieldType := column.Type
			if fieldType == "" {
				fieldType = crispv1alpha1.FieldTypeString
			}
			if _, err := fmt.Fprintf(w, "  %s\t%s\t%s\n", column.Name, fieldType, column.UsedFor); err != nil {
				return err
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}

	_, _ = fmt.Fprintln(errOut,
		"\nWhat the projection reads, not what the database has — the two agreeing is what\n"+
			"compiling the queries answers. Types are the projection's own rather than SQL\n"+
			"types, and nothing here knows about nullability, keys, or indexes.")
	return nil
}
