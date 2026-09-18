package projection

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// writingProjection is a kind served at two versions that share one update
// statement. The primary maps both columns the statement sets; what the second
// version maps is what each test decides.
func writingProjection() *crispv1alpha1.CustomResourceProjection {
	return &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			DataSource: crispv1alpha1.DataSource{Driver: "postgres"},
			Resource: crispv1alpha1.ProjectedResource{
				Group:   "store.example.com",
				Version: "v1alpha1",
				Kind:    "Order",
				Plural:  "orders",
				Scope:   crispv1alpha1.NamespaceScoped,
				Schema:  preserving(),
				// Deliberately: the round-trip check would refuse the versions
				// for mapping different columns, and the point here is that
				// the write is refused even when the divergence is allowed.
				Conversion: crispv1alpha1.ConversionNone,
				Versions: []crispv1alpha1.ProjectedVersion{{
					Name:   "v2",
					Schema: preserving(),
					Mapping: &crispv1alpha1.Mapping{
						Name:      "id",
						Namespace: "tenant",
						Fields: []crispv1alpha1.FieldMapping{
							{Column: "customer", Path: "spec.customer"},
							{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
						},
					},
				}},
			},
			Queries: crispv1alpha1.Queries{
				List: crispv1alpha1.Query{SQL: "SELECT id, tenant, customer, total_cents FROM orders WHERE tenant = :namespace"},
				Update: &crispv1alpha1.Query{
					SQL: "UPDATE orders SET customer = :customer, total_cents = :total_cents WHERE tenant = :namespace AND id = :name",
				},
			},
			Mapping: crispv1alpha1.Mapping{
				Name:      "id",
				Namespace: "tenant",
				Fields: []crispv1alpha1.FieldMapping{
					{Column: "customer", Path: "spec.customer"},
					{Column: "total_cents", Path: "spec.totalCents", Type: crispv1alpha1.FieldTypeInteger},
				},
			},
		},
	}
}

// TestValidateRefusesAWriteThroughAVersionThatDoesNotMapWhatItSets is the
// scenario that lost data: v2 maps customer only, the shared update sets
// total_cents as well, and a kubectl apply through v2 bound NULL there and
// answered 200. The refusal has to say which version, which verb and which
// parameter, since the statement is shared and the mapping is what differs.
func TestValidateRefusesAWriteThroughAVersionThatDoesNotMapWhatItSets(t *testing.T) {
	p := writingProjection()
	p.Spec.Resource.Versions[0].Mapping.Fields = []crispv1alpha1.FieldMapping{{Column: "customer", Path: "spec.customer"}}

	err := Validate(p)
	if err == nil {
		t.Fatal("a version whose mapping omits a column the shared update sets was accepted")
	}
	for _, want := range []string{"version v2", "queries.update", ":total_cents"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "version v1alpha1") {
		t.Errorf("error %q blames the version that maps the column", err)
	}
}

// TestValidateRefusesATypoInAWriteStatement: the same rule catches the plain
// mistake, where the parameter is nobody's column at all.
func TestValidateRefusesATypoInAWriteStatement(t *testing.T) {
	p := writingProjection()
	p.Spec.Queries.Create = &crispv1alpha1.Query{
		SQL: "INSERT INTO orders (id, tenant, customer, total_cents) VALUES (:name, :namespace, :custmer, :total_cents)",
	}

	err := Validate(p)
	if err == nil {
		t.Fatal("a create statement naming :custmer was accepted")
	}
	for _, want := range []string{"queries.create", ":custmer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
}

// TestValidateAcceptsAWriteBoundFromWhatTheServerSupplies states the boundary:
// a mapped column, one of the server's own parameters, and a declared
// parameter are each supplied, and a statement built from all three is fine —
// on a driver with positional placeholders too, where a parameter used twice
// is reported twice.
func TestValidateAcceptsAWriteBoundFromWhatTheServerSupplies(t *testing.T) {
	for _, driver := range []string{"postgres", "mysql", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			p := writingProjection()
			p.Spec.DataSource.Driver = driver
			p.Spec.Queries.Update = &crispv1alpha1.Query{
				SQL: "UPDATE orders SET customer = :customer, total_cents = :total_cents, updated_by = :user, region = :region " +
					"WHERE tenant = :namespace AND id = :name AND (:resourceVersion IS NULL OR updated_at = :resourceVersion)",
				Parameters: []crispv1alpha1.QueryParameter{{Name: "region", From: crispv1alpha1.ParameterSourceValue, Value: "eu"}},
			}
			p.Spec.Queries.DeleteCollection = &crispv1alpha1.Query{
				SQL:        "DELETE FROM orders WHERE tenant = :namespace AND region = :region",
				Parameters: []crispv1alpha1.QueryParameter{{Name: "region", From: crispv1alpha1.ParameterSourceValue, Value: "eu"}},
			}

			if err := Validate(p); err != nil {
				t.Fatalf("Validate() refused a write bound entirely from what the server supplies: %v", err)
			}
		})
	}
}

// TestValidateRefusesAMappedColumnInADeleteCollection: there is no object
// behind a collection delete, so a mapped column is not supplied there even
// though the same name is fine in an update.
func TestValidateRefusesAMappedColumnInADeleteCollection(t *testing.T) {
	p := writingProjection()
	p.Spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: "DELETE FROM orders WHERE tenant = :namespace AND customer = :customer",
	}

	err := Validate(p)
	if err == nil {
		t.Fatal("a deleteCollection statement naming a mapped column was accepted")
	}
	for _, want := range []string{"queries.deleteCollection", ":customer"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
}

// TestValidateChecksEveryStatementOfATransactionalWrite: a prelude statement
// is bound from the same map as the last one, so it is held to the same rule.
func TestValidateChecksEveryStatementOfATransactionalWrite(t *testing.T) {
	p := writingProjection()
	p.Spec.Queries.Create = &crispv1alpha1.Query{
		Statements: []string{
			"INSERT INTO order_events (order_id, kind) VALUES (:name, :event_kind)",
			"INSERT INTO orders (id, tenant, customer, total_cents) VALUES (:name, :namespace, :customer, :total_cents)",
		},
	}

	err := Validate(p)
	if err == nil {
		t.Fatal("a prelude statement naming :event_kind was accepted")
	}
	if !strings.Contains(err.Error(), ":event_kind") {
		t.Errorf("error %q does not name the parameter", err)
	}
}

// TestValidateIgnoresAVersionThatIsNotServed: a version nobody can write
// through cannot lose anything, and the apiserver does not compile it.
func TestValidateIgnoresAVersionThatIsNotServed(t *testing.T) {
	p := writingProjection()
	served := false
	p.Spec.Resource.Versions[0].Served = &served
	p.Spec.Resource.Versions[0].Mapping.Fields = []crispv1alpha1.FieldMapping{{Column: "customer", Path: "spec.customer"}}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() refused a projection for a version it does not serve: %v", err)
	}
}

// TestValidateBindsAnInheritedMappingThroughEveryVersion: a version without a
// mapping of its own writes through the projection's, and is checked as such
// rather than as a version that maps nothing.
func TestValidateBindsAnInheritedMappingThroughEveryVersion(t *testing.T) {
	p := writingProjection()
	p.Spec.Resource.Versions[0].Mapping = nil

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() refused a version that inherits the mapping the statement is written against: %v", err)
	}
}
