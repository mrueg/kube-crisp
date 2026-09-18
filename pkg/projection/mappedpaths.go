package projection

import (
	"fmt"
	"strings"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// CheckMappedPaths refuses a projection whose mapping puts a column somewhere
// its schema does not describe.
//
// A read is never pruned, so such a field shows up in every GET and looks
// perfectly healthy. A write is: the object a client submits is pruned against
// the version's structural schema before it is mapped back to columns, exactly
// as a custom resource is, and a field the schema does not describe is dropped
// there — with a warning, unless the request asked for strict field validation.
// The mapper then reads the path, finds nothing, and binds NULL for the column,
// so "UPDATE ... SET notes = :notes" erases the value on every read-modify-write.
// kubectl edit of an unrelated field is enough. Nothing in the response says so
// beyond the warning header, which is why this is refused when the projection
// is checked rather than discovered row by row.
//
// The rule is the pruning algorithm's, restated: a path is kept when every
// segment is a declared property, or a value under additionalProperties, or
// sits below a node marked x-kubernetes-preserve-unknown-fields. Metadata is
// exempt, as it is from pruning, though the mapper already refuses to put a
// column there.
//
// borrowed carries the schemas resolved for versions that use schemaFrom, keyed
// by version name. A served version whose schema is neither declared nor in
// borrowed is skipped rather than refused: Validate runs before any cluster has
// been asked, and the compile calls this again once it has.
func CheckMappedPaths(p *crispv1alpha1.CustomResourceProjection, borrowed map[string]*apiextensionsv1.JSONSchemaProps) error {
	res := p.Spec.Resource

	// The primary version and the extras are one list here, because the
	// mistake is the same in every one of them and the error has to say
	// which. A version that is not served is not checked: nothing can be
	// written through it, and the schema it borrows is not even resolved.
	type served struct {
		name    string
		schema  *apiextensionsv1.JSONSchemaProps
		mapping *crispv1alpha1.Mapping
	}
	versions := []served{{name: res.Version, schema: res.Schema, mapping: &p.Spec.Mapping}}
	for i := range res.Versions {
		extra := &res.Versions[i]
		if extra.Served != nil && !*extra.Served {
			continue
		}
		mapping := &p.Spec.Mapping
		if extra.Mapping != nil {
			mapping = extra.Mapping
		}
		versions = append(versions, served{name: extra.Name, schema: extra.Schema, mapping: mapping})
	}

	for _, version := range versions {
		schema := version.schema
		if schema == nil {
			schema = borrowed[version.name]
		}
		if schema == nil {
			continue
		}
		if err := checkMappedPaths(schema, version.mapping); err != nil {
			return fmt.Errorf("projection %s: version %s: %w", p.Name, version.name, err)
		}
	}
	return nil
}

// checkMappedPaths is CheckMappedPaths for one version, once its schema is
// known.
func checkMappedPaths(schema *apiextensionsv1.JSONSchemaProps, mapping *crispv1alpha1.Mapping) error {
	if mapping == nil || len(mapping.Fields) == 0 {
		return nil
	}

	structural, err := toStructural(schema)
	if err != nil {
		return err
	}

	for i, field := range mapping.Fields {
		// A path the mapper would refuse anyway is left for it to refuse,
		// with its own message.
		if validatePath(field.Path) != nil {
			continue
		}
		parts := strings.Split(field.Path, ".")

		end := walkMappedPath(structural, parts)
		if end.list != "" {
			return fmt.Errorf(
				"mapping.fields[%d] puts column %q at %s, which runs through %s, and the schema describes "+
					"that as a list: a dotted path names map keys, and no element of a list can be reached "+
					"that way",
				i, field.Column, field.Path, end.list)
		}
		if !end.kept {
			return fmt.Errorf(
				"mapping.fields[%d] puts column %q at %s, but the schema does not describe %s: "+
					"a write through this version prunes the field before it is mapped, then binds NULL "+
					"for the column, and every read-modify-write erases the value. Describe the path in "+
					"the schema, or mark the object holding it x-kubernetes-preserve-unknown-fields: true",
				i, field.Column, field.Path, end.at)
		}

		// A json column carries a value the schema does not spell out, and a
		// leaf that says nothing about the keys of an object keeps none of
		// them. The same erasure by a shorter route: the value is not dropped,
		// it is emptied, and the column is written back as {}.
		if field.Type == crispv1alpha1.FieldTypeJSON && !end.preserved && emptiesObjects(end.node) {
			return fmt.Errorf(
				"mapping.fields[%d] carries column %q as json at %s, but the schema says nothing about "+
					"the keys of an object there, so a write through this version prunes every key of the "+
					"value and writes the column back empty. Describe its properties, or mark it "+
					"x-kubernetes-preserve-unknown-fields: true",
				i, field.Column, field.Path)
		}
	}
	return nil
}

// toStructural builds the structural form of a declared schema, which is the
// form pruning is defined against.
//
// A schema that has no structural form cannot be pruned against, and the
// registry refuses to serve one for that reason; saying so here as well puts
// the refusal where `validate` and the admission webhook can see it.
func toStructural(schema *apiextensionsv1.JSONSchemaProps) (*structuralschema.Structural, error) {
	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(schema, internal, nil); err != nil {
		return nil, fmt.Errorf("converting the schema: %w", err)
	}
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		return nil, fmt.Errorf("the schema is not structural, so nothing can say which fields a write keeps: %w", err)
	}
	return structural, nil
}

// pathEnd is where a mapped path comes out of a structural schema.
type pathEnd struct {
	// kept is whether a value at the path survives pruning; at is the first
	// segment pruning would drop when it does not.
	kept bool
	at   string

	// list is the array the path runs through, when it does. Such a path is
	// not pruned so much as mistyped, and it gets its own explanation.
	list string

	// preserved is whether the path ran into x-kubernetes-preserve-unknown-
	// fields, below which nothing is pruned at all. node is the schema node
	// the path lands on otherwise, and nil where the schema leaves the value
	// open — under additionalProperties: true, say — which pruning treats as
	// a scalar to keep and an object to empty.
	preserved bool
	node      *structuralschema.Structural
}

// walkMappedPath follows a dotted path through a structural schema the way
// pruning follows it through the object.
func walkMappedPath(root *structuralschema.Structural, parts []string) pathEnd {
	// Pruning treats the root as an embedded resource, so metadata, apiVersion
	// and kind are left alone whatever the schema says about them.
	if parts[0] == "metadata" {
		return pathEnd{kept: true, preserved: true}
	}

	node := root
	for i, part := range parts {
		// A dotted path names map keys all the way down, so a segment that
		// lands inside an array is a map where the schema wants a list.
		// Validation would refuse such a write outright, which is at least
		// loud, but it is not what the author meant either.
		if node != nil && node.Type == "array" {
			return pathEnd{list: strings.Join(parts[:i], ".")}
		}

		switch {
		case node == nil:
			// The schema left the level above open, and pruning empties any
			// object it finds there — which is what a further segment names.
			return pathEnd{at: strings.Join(parts[:i+1], ".")}
		case node.XPreserveUnknownFields:
			// A declared property is still pruned against its own schema; an
			// undeclared one is kept as it is, along with everything below.
			if prop, ok := node.Properties[part]; ok {
				node = &prop
				continue
			}
			if node.AdditionalProperties != nil {
				node = node.AdditionalProperties.Structural
				continue
			}
			return pathEnd{kept: true, preserved: true}
		default:
			if prop, ok := node.Properties[part]; ok {
				node = &prop
				continue
			}
			if node.AdditionalProperties != nil {
				node = node.AdditionalProperties.Structural
				continue
			}
			return pathEnd{at: strings.Join(parts[:i+1], ".")}
		}
	}
	return pathEnd{kept: true, node: node}
}

// emptiesObjects reports whether pruning against s strips every key from an
// object value: an object with nothing said about its contents, or a list of
// such objects. A scalar, a list of scalars, and anything marked
// x-kubernetes-preserve-unknown-fields come through intact.
func emptiesObjects(s *structuralschema.Structural) bool {
	switch {
	case s == nil:
		return true
	case s.XPreserveUnknownFields:
		return false
	case s.Type == "object":
		return len(s.Properties) == 0 && s.AdditionalProperties == nil
	case s.Type == "array":
		return s.Items != nil && emptiesObjects(s.Items)
	default:
		return false
	}
}
