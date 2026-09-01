package v1alpha1_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// The credential provider named by a projection has to be one the API server
// will accept, and that is not the same guarantee the driver enum gives.
//
// A driver is linked into the binary, so the CRD that binary ships with can
// enumerate exactly what it accepts, and TestTheDriverEnumListsEveryRegistered
// Driver holds the two together. Providers cannot be enumerated that way: the
// build published from this repository registers none — every provider is a
// cloud SDK it would otherwise link for nothing — so an enum here would be an
// empty list that rejects everything, and a build that added one would have to
// remember to widen it. The same class of bug the enum test exists for, with
// the failure moved one step further away.
//
// So the field is constrained by shape and not by set, and this is the test
// that says the shape admits what the registry holds. A build that registers
// aws-rds-iam and does not regenerate its CRD fails here rather than in a
// cluster, which is the whole point of the enum test restated for a field that
// cannot have an enum.
func TestTheAuthProviderFieldAcceptsEveryRegisteredCredentialProvider(t *testing.T) {
	crd := readCRD(t)
	field := authProviderSchema(t, crd)

	// The registry as this build holds it. Empty in the published build, which
	// is why the table below exists as well.
	for _, name := range crispsql.RegisteredCredentialProviders() {
		if err := field.accepts(name); err != nil {
			t.Errorf("this build registers the %q credential provider and the CRD would reject it: %v", name, err)
		}
	}

	// And the shapes a provider name is likely to take, so that the published
	// build — which registers nothing — still checks that the field is usable
	// at all. Every name here is one a provider in this project's own
	// documentation carries or plausibly would.
	for _, name := range []string{"aws-rds-iam", "gcp-cloudsql-iam", "azure-entra", "vault"} {
		if err := field.accepts(name); err != nil {
			t.Errorf("the CRD rejects %q, which is the shape a credential provider name has: %v", name, err)
		}
	}

	// And what it must not admit. A name that differs from a registered one only
	// in case or in whitespace is a projection that reports a provider this
	// build does not have, for a provider it does.
	for _, name := range []string{"", "AWS-RDS-IAM", "aws rds iam", "-aws", "aws-", "aws_rds_iam", strings.Repeat("a", 64)} {
		if err := field.accepts(name); err == nil {
			t.Errorf("the CRD accepts %q as a credential provider name", name)
		}
	}
}

// A provider is the whole of an auth stanza, so an auth stanza without one is
// an object that says it authenticates differently and does not say how.
func TestAnAuthStanzaHasToNameItsProvider(t *testing.T) {
	crd := readCRD(t)

	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	for _, version := range versions {
		node, ok := version.(map[string]any)
		if !ok {
			continue
		}
		stanza := dig(node, "schema", "openAPIV3Schema", "properties", "spec", "properties",
			"dataSource", "properties", "auth")
		if stanza == nil {
			continue
		}
		required, _ := stanza["required"].([]any)
		var names []string
		for _, entry := range required {
			if name, ok := entry.(string); ok {
				names = append(names, name)
			}
		}
		if len(names) != 1 || names[0] != "provider" {
			t.Errorf("dataSource.auth requires %v, want provider and nothing else", names)
		}
		return
	}
	t.Fatal("the CRD has no spec.dataSource.auth")
}

// providerField is the CRD's constraint on the provider name, as much of it as
// a name can be checked against here.
type providerField struct {
	pattern   *regexp.Regexp
	maxLength int
	minLength int
	enum      []string
}

// accepts reports why a name would be rejected, or nil if it would be taken.
func (f providerField) accepts(name string) error {
	switch {
	case f.minLength > 0 && len(name) < f.minLength:
		return fmt.Errorf("shorter than minLength %d", f.minLength)
	case f.maxLength > 0 && len(name) > f.maxLength:
		return fmt.Errorf("longer than maxLength %d", f.maxLength)
	case f.pattern != nil && !f.pattern.MatchString(name):
		return fmt.Errorf("does not match %s", f.pattern)
	}
	if len(f.enum) > 0 {
		for _, allowed := range f.enum {
			if allowed == name {
				return nil
			}
		}
		return fmt.Errorf("not one of the enum %v", f.enum)
	}
	return nil
}

// authProviderSchema digs spec.dataSource.auth.provider out of the served
// version and reads the constraints off it.
func authProviderSchema(t *testing.T, crd map[string]any) providerField {
	t.Helper()

	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	for _, version := range versions {
		node, ok := version.(map[string]any)
		if !ok {
			continue
		}
		provider := dig(node, "schema", "openAPIV3Schema", "properties", "spec", "properties",
			"dataSource", "properties", "auth", "properties", "provider")
		if provider == nil {
			continue
		}

		field := providerField{}
		if pattern, ok := provider["pattern"].(string); ok {
			compiled, err := regexp.Compile(pattern)
			if err != nil {
				t.Fatalf("the CRD's provider pattern does not compile: %v", err)
			}
			field.pattern = compiled
		}
		if length, ok := provider["maxLength"].(float64); ok {
			field.maxLength = int(length)
		}
		if length, ok := provider["minLength"].(float64); ok {
			field.minLength = int(length)
		}
		for _, value := range asSlice(provider["enum"]) {
			if name, ok := value.(string); ok {
				field.enum = append(field.enum, name)
			}
		}
		if field.pattern == nil && len(field.enum) == 0 {
			t.Fatal("the CRD constrains spec.dataSource.auth.provider by neither a pattern nor an enum, " +
				"so any string reaches the registry lookup as typed")
		}
		return field
	}
	t.Fatal("the CRD has no spec.dataSource.auth.provider")
	return providerField{}
}

func asSlice(node any) []any {
	values, _ := node.([]any)
	return values
}
