package projection

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// crdChecker validates an object exactly as the kube-apiserver would when the
// shipped CustomResourceDefinition is installed: the OpenAPI validator for
// shapes and patterns, and the CEL validator for the cross-field rules.
type crdChecker struct {
	schema     apiservervalidation.SchemaValidator
	cel        *cel.Validator
	structural *structuralschema.Structural
}

// newCRDChecker compiles the CRD that ships in manifests/.
//
// Compiling it here is half the point of this file. A malformed CEL rule is
// rejected when the CRD is applied, which without this test would be discovered
// by whoever next ran `kubectl apply -f manifests/` rather than by CI.
func newCRDChecker(t *testing.T) *crdChecker {
	t.Helper()

	path := filepath.Join("..", "..", "manifests", "10-crd-customresourceprojection.yaml")
	// The path is a constant joined here, not anything a caller supplies.
	raw, err := os.ReadFile(path) //nolint:gosec // G304: fixed repository path
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}

	var served *apiextensionsv1.CustomResourceDefinitionVersion
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == "v1alpha1" {
			served = &crd.Spec.Versions[i]
			break
		}
	}
	if served == nil || served.Schema == nil || served.Schema.OpenAPIV3Schema == nil {
		t.Fatal("the CRD has no v1alpha1 schema")
	}

	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		served.Schema.OpenAPIV3Schema, internal, nil); err != nil {
		t.Fatalf("converting the CRD schema: %v", err)
	}

	validator, _, err := apiservervalidation.NewSchemaValidator(internal)
	if err != nil {
		t.Fatalf("compiling the CRD schema: %v", err)
	}
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		t.Fatalf("the CRD schema is not structural: %v", err)
	}

	return &crdChecker{
		schema:     validator,
		cel:        cel.NewValidator(structural, true, celconfig.PerCallLimit),
		structural: structural,
	}
}

// check returns every complaint the CRD has about a projection, as one string.
func (c *crdChecker) check(t *testing.T, p *crispv1alpha1.CustomResourceProjection) string {
	t.Helper()

	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("encoding the projection: %v", err)
	}
	obj := map[string]any{}
	if err := json.Unmarshal(encoded, &obj); err != nil {
		t.Fatalf("decoding the projection: %v", err)
	}
	obj["apiVersion"] = crispv1alpha1.GroupName + "/v1alpha1"
	obj["kind"] = "CustomResourceProjection"

	var messages []string
	for _, err := range apiservervalidation.ValidateCustomResource(nil, obj, c.schema) {
		messages = append(messages, err.Error())
	}
	celErrs, _ := c.cel.Validate(
		context.Background(), nil, c.structural, obj, nil, celconfig.RuntimeCELCostBudget)
	for _, err := range celErrs {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "; ")
}

// acceptableProjection is the smallest projection the CRD should accept.
func acceptableProjection() *crispv1alpha1.CustomResourceProjection {
	p := incrementalProjection()
	p.Spec.DataSource.SecretRef = crispv1alpha1.SecretReference{
		Name:      "orders-db",
		Namespace: "kube-crisp",
	}
	return p
}

// TestCRDAcceptsAValidProjection is the control: without it every rejection
// below could be the fixture rather than the rule under test.
func TestCRDAcceptsAValidProjection(t *testing.T) {
	checker := newCRDChecker(t)

	if got := checker.check(t, acceptableProjection()); got != "" {
		t.Errorf("the CRD rejected a valid projection: %s", got)
	}
}

// TestCRDRejectsWhatTheApiserverWouldReject covers the rules that used to be
// enforced only after the object was already in etcd, where they surfaced as a
// status condition rather than as a failed apply.
//
// Every case here is also rejected by Validate, which is the point: the CRD is
// meant to say the same thing earlier, not something different.
func TestCRDRejectsWhatTheApiserverWouldReject(t *testing.T) {
	checker := newCRDChecker(t)

	for _, tc := range []struct {
		name  string
		want  string
		apply func(p *crispv1alpha1.CustomResourceProjection)
	}{
		{
			name: "namespaced without a namespace column",
			want: "mapping.namespace is required",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.Namespace = ""
			},
		},
		{
			name: "cluster scoped with a namespace column",
			want: "mapping.namespace must be empty",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Resource.Scope = crispv1alpha1.ClusterScoped
			},
		},
		{
			name: "finalizers without a deletion timestamp",
			want: "mapping.deletionTimestamp",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.Finalizers = "finalizers"
			},
		},
		{
			name: "finalizers without markDeleted",
			want: "queries.markDeleted",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.Finalizers = "finalizers"
				p.Spec.Mapping.DeletionTimestamp = "deleted_at"
				p.Spec.Queries.Update = &crispv1alpha1.Query{SQL: "UPDATE orders SET x = :x"}
			},
		},
		{
			name: "watch query without a mapped resourceVersion",
			want: "mapping.resourceVersion",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.ResourceVersion = ""
			},
		},
		{
			name: "deletion query without a watch query",
			want: "watch.deletedQuery needs watch.query",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Watch.Query = nil
				p.Spec.Watch.DeletedQuery = &crispv1alpha1.Query{SQL: "SELECT id FROM tombstones"}
			},
		},
		{
			name: "a query with both sql and statements",
			want: "either sql or statements",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.Create = &crispv1alpha1.Query{
					SQL:        "INSERT INTO orders (id) VALUES (:id)",
					Statements: []string{"INSERT INTO orders (id) VALUES (:id)"},
				}
			},
		},
		{
			name: "a query with neither sql nor statements",
			want: "either sql or statements",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.Create = &crispv1alpha1.Query{}
			},
		},
		{
			name: "an identity that is both a column and a list of columns",
			want: "either name or nameColumns",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.NameColumns = []string{"tenant", "id"}
			},
		},
		{
			name: "an identity that is neither",
			want: "either name or nameColumns",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Mapping.Name = ""
			},
		},
		{
			name: "a session variable read from the submitted object",
			want: "Field or LabelSelector",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
					{Name: "app.tenant", From: crispv1alpha1.ParameterSourceField},
				}
			},
		},
		{
			name: "a session variable name that is not an identifier",
			want: "spec.dataSource.sessionVariables[0].name",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
					{Name: "app.tenant; DROP TABLE orders", From: crispv1alpha1.ParameterSourceValue, Value: "acme"},
				}
			},
		},
		{
			name: "an upper case plural",
			want: "spec.resource.plural",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Resource.Plural = "Orders"
			},
		},
		{
			name: "both a schema and a borrowed one",
			want: "exactly one of schema or schemaFrom",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.store.example.com"}
			},
		},
		{
			name: "neither a schema nor a borrowed one",
			want: "exactly one of schema or schemaFrom",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Resource.Schema = nil
			},
		},
		{
			name: "an extra version with no schema of its own",
			want: "exactly one of schema or schemaFrom",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{Name: "v1beta1"}}
			},
		},
		{
			name: "a list written as a transaction",
			want: "queries.list.sql is required",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.List = crispv1alpha1.Query{
					Statements: []string{"SELECT id FROM orders"},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := acceptableProjection()
			tc.apply(p)

			got := checker.check(t, p)
			if got == "" {
				t.Fatalf("the CRD accepted a projection the apiserver would refuse to compile")
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("complaint %q does not mention %q", got, tc.want)
			}
		})
	}
}

// TestCRDRejectsNotifyOnADriverThatCannotDeliver: notifications are the one
// watch setting that depends on the database rather than on the projection, so
// asking for them where they cannot happen should fail the apply rather than
// produce a watch that quietly polls on its timer.
func TestCRDRejectsNotifyOnADriverThatCannotDeliver(t *testing.T) {
	checker := newCRDChecker(t)

	for _, tc := range []struct {
		name  string
		want  string
		apply func(p *crispv1alpha1.CustomResourceProjection)
	}{
		{
			name: "a driver with no notifications",
			want: "push a change notification",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.DataSource.Driver = "sqlite"
				p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
			},
		},
		{
			name: "notifications with watching turned off",
			want: "turns watching off",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
				p.Spec.Watch.Disabled = true
			},
		},
		{
			name: "a channel that is not an identifier",
			want: "spec.watch.notify.channel",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders changed; DROP TABLE orders"}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := acceptableProjection()
			tc.apply(p)

			got := checker.check(t, p)
			if got == "" {
				t.Fatal("the CRD accepted a notify setting the apiserver would refuse")
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("complaint %q does not mention %q", got, tc.want)
			}
		})
	}

	// And the shape that works is accepted.
	valid := acceptableProjection()
	valid.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
	if got := checker.check(t, valid); got != "" {
		t.Errorf("the CRD rejected a valid notify setting: %s", got)
	}
}
