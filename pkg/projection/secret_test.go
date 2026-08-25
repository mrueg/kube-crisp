package projection

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/fake"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func secret(namespace, name string, labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels},
		Data:       map[string][]byte{"dsn": []byte("postgres://user:pass@db:5432/store")},
	}
}

func dataSource(namespace, name string) crispv1alpha1.DataSource {
	return crispv1alpha1.DataSource{
		Driver:    "postgres",
		SecretRef: crispv1alpha1.SecretReference{Namespace: namespace, Name: name},
	}
}

// TestResolveRequiresOptIn is the containment that matters: a
// CustomResourceProjection is cluster-scoped and carries arbitrary SQL, so
// whoever creates one picks both the database and the statement. Requiring the
// label keeps the decision with whoever owns the credentials.
func TestResolveRequiresOptIn(t *testing.T) {
	resolver := &SecretDSNResolver{
		Client:       fake.NewSimpleClientset(secret("kube-crisp", "orders-db", nil)),
		RequireOptIn: true,
	}

	_, err := resolver.Resolve(context.Background(), dataSource("kube-crisp", "orders-db"))
	if err == nil {
		t.Fatal("a Secret without the opt-in label was used")
	}
	if !strings.Contains(err.Error(), OptInLabel) {
		t.Errorf("error %q does not say which label to add", err)
	}
}

func TestResolveAcceptsAnOptedInSecret(t *testing.T) {
	resolver := &SecretDSNResolver{
		Client: fake.NewSimpleClientset(
			secret("kube-crisp", "orders-db", map[string]string{OptInLabel: OptInValue})),
		RequireOptIn: true,
	}

	dsn, err := resolver.Resolve(context.Background(), dataSource("kube-crisp", "orders-db"))
	if err != nil {
		t.Fatalf("Resolve() returned error: %v", err)
	}
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Errorf("dsn = %q, want the Secret's value", dsn)
	}
}

// TestResolveRestrictsNamespaces stops a projection reaching credentials in a
// namespace the server was not meant to read from.
func TestResolveRestrictsNamespaces(t *testing.T) {
	resolver := &SecretDSNResolver{
		Client: fake.NewSimpleClientset(
			secret("payments", "prod-db", map[string]string{OptInLabel: OptInValue})),
		AllowedNamespaces: sets.New("kube-crisp"),
		RequireOptIn:      true,
	}

	_, err := resolver.Resolve(context.Background(), dataSource("payments", "prod-db"))
	if err == nil {
		t.Fatal("a Secret outside the allowed namespaces was used")
	}
	if !strings.Contains(err.Error(), "kube-crisp") {
		t.Errorf("error %q does not say which namespaces are allowed", err)
	}

	// The check happens before the read, so a projection cannot probe for the
	// existence of Secrets elsewhere either.
	if strings.Contains(err.Error(), "not found") {
		t.Error("the error reveals whether the Secret exists")
	}
}

func TestResolveAllowsOptingOutOfBothChecks(t *testing.T) {
	resolver := &SecretDSNResolver{
		Client: fake.NewSimpleClientset(secret("payments", "prod-db", nil)),
	}

	if _, err := resolver.Resolve(context.Background(), dataSource("payments", "prod-db")); err != nil {
		t.Fatalf("Resolve() with both checks disabled: %v", err)
	}
}

// stubSecretCache stands in for the informer-backed cache.
type stubSecretCache struct {
	secrets map[string]*corev1.Secret
	hits    int
}

func (c *stubSecretCache) Secret(namespace, name string) (*corev1.Secret, bool) {
	secret, ok := c.secrets[namespace+"/"+name]
	if ok {
		c.hits++
	}
	return secret, ok
}

// TestSecretResolverReadsThroughTheCache: the connection string is resolved
// once per projection on every sync, against Secrets the server already
// watches. Going to the API server for each of them is a request per projection
// every resync period for nothing.
func TestSecretResolverReadsThroughTheCache(t *testing.T) {
	cached := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders-db",
			Namespace: "kube-crisp",
			Labels:    map[string]string{OptInLabel: OptInValue},
		},
		Data: map[string][]byte{"dsn": []byte("postgres://cached")},
	}
	cache := &stubSecretCache{secrets: map[string]*corev1.Secret{"kube-crisp/orders-db": cached}}

	// The client holds a different value, so which one answered is visible.
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders-db",
			Namespace: "kube-crisp",
			Labels:    map[string]string{OptInLabel: OptInValue},
		},
		Data: map[string][]byte{"dsn": []byte("postgres://live")},
	})

	resolver := &SecretDSNResolver{Client: client, Cache: cache, RequireOptIn: true}
	dsn, err := resolver.Resolve(context.Background(), crispv1alpha1.DataSource{
		SecretRef: crispv1alpha1.SecretReference{Name: "orders-db", Namespace: "kube-crisp"},
	})
	if err != nil {
		t.Fatalf("Resolve() returned error: %v", err)
	}
	if dsn != "postgres://cached" {
		t.Errorf("Resolve() = %q, want the cached value; it went to the API server instead", dsn)
	}
	if cache.hits != 1 {
		t.Errorf("cache was consulted %d times, want 1", cache.hits)
	}
}

// TestSecretResolverFallsBackWhenNotCached: an unlabelled Secret is not in the
// informers, which select on the opt-in label. It still has to be read — and
// still has to be refused for not carrying the label.
func TestSecretResolverFallsBackWhenNotCached(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-db", Namespace: "kube-crisp"},
		Data:       map[string][]byte{"dsn": []byte("postgres://live")},
	})

	cache := &stubSecretCache{secrets: map[string]*corev1.Secret{}}
	ref := crispv1alpha1.DataSource{
		SecretRef: crispv1alpha1.SecretReference{Name: "orders-db", Namespace: "kube-crisp"},
	}

	// Without the opt-in requirement the fallback answers.
	relaxed := &SecretDSNResolver{Client: client, Cache: cache}
	dsn, err := relaxed.Resolve(context.Background(), ref)
	if err != nil {
		t.Fatalf("Resolve() returned error: %v", err)
	}
	if dsn != "postgres://live" {
		t.Errorf("Resolve() = %q, want the live value", dsn)
	}

	// With it, the missing label is still what decides.
	strict := &SecretDSNResolver{Client: client, Cache: cache, RequireOptIn: true}
	if _, err := strict.Resolve(context.Background(), ref); err == nil {
		t.Error("Resolve() accepted a Secret without the opt-in label")
	}
}

// TestResolveReadFindsTheReplica: the replica is resolved the same way the
// primary is, so a rotated replica credential rebuilds the storage exactly as
// the primary's would.
func TestResolveReadFindsTheReplica(t *testing.T) {
	client := fake.NewSimpleClientset(
		secret("kube-crisp", "orders-db", map[string]string{OptInLabel: OptInValue}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "orders-db-replica", Namespace: "kube-crisp",
				Labels: map[string]string{OptInLabel: OptInValue},
			},
			Data: map[string][]byte{"dsn": []byte("postgres://replica")},
		},
	)
	resolver := &SecretDSNResolver{Client: client, RequireOptIn: true}

	ds := crispv1alpha1.DataSource{
		SecretRef:     crispv1alpha1.SecretReference{Name: "orders-db", Namespace: "kube-crisp"},
		ReadSecretRef: &crispv1alpha1.SecretReference{Name: "orders-db-replica", Namespace: "kube-crisp"},
	}

	dsn, ok, err := ResolveRead(context.Background(), resolver, ds)
	if err != nil {
		t.Fatalf("ResolveRead() returned error: %v", err)
	}
	if !ok {
		t.Fatal("ResolveRead() reported no replica for a projection that names one")
	}
	if dsn != "postgres://replica" {
		t.Errorf("ResolveRead() = %q, want the replica's connection string", dsn)
	}

	// Without a replica named, there is nothing to resolve and that is not an
	// error — it is the ordinary case.
	plain := crispv1alpha1.DataSource{
		SecretRef: crispv1alpha1.SecretReference{Name: "orders-db", Namespace: "kube-crisp"},
	}
	if _, ok, err := ResolveRead(context.Background(), resolver, plain); err != nil || ok {
		t.Errorf("ResolveRead() with no replica = %v, %v; want false and no error", ok, err)
	}
}

// TestResolveReadOnAResolverThatHasNoReplicas: a resolver without the optional
// interface simply never has one to offer, rather than having to say so.
func TestResolveReadOnAResolverThatHasNoReplicas(t *testing.T) {
	if _, ok, err := ResolveRead(context.Background(), noReplicaResolver{},
		crispv1alpha1.DataSource{}); err != nil || ok {
		t.Errorf("ResolveRead() = %v, %v; want false and no error", ok, err)
	}
}

type noReplicaResolver struct{}

func (noReplicaResolver) Resolve(context.Context, crispv1alpha1.DataSource) (string, error) {
	return "dsn", nil
}

// TestReadDataSourceUsesItsOwnKey: the replica's connection string can live
// under a different key, and must not fall back to the primary's Secret.
func TestReadDataSourceUsesItsOwnKey(t *testing.T) {
	ds := crispv1alpha1.DataSource{
		SecretRef:     crispv1alpha1.SecretReference{Name: "primary", Namespace: "ns"},
		DSNKey:        "primary-dsn",
		ReadSecretRef: &crispv1alpha1.SecretReference{Name: "replica", Namespace: "ns"},
		ReadDSNKey:    "replica-dsn",
	}

	replica, ok := readDataSource(ds)
	if !ok {
		t.Fatal("readDataSource() reported no replica")
	}
	if replica.SecretRef.Name != "replica" {
		t.Errorf("secret = %q, want replica", replica.SecretRef.Name)
	}
	if replica.DSNKey != "replica-dsn" {
		t.Errorf("key = %q, want replica-dsn", replica.DSNKey)
	}
	// It must not describe a replica of its own, or resolution would recurse.
	if replica.ReadSecretRef != nil || replica.ReadDSNKey != "" {
		t.Error("the replica data source still names a replica")
	}

	// With no key of its own it inherits the primary's.
	ds.ReadDSNKey = ""
	replica, _ = readDataSource(ds)
	if replica.DSNKey != "primary-dsn" {
		t.Errorf("key = %q, want the primary's when the replica names none", replica.DSNKey)
	}

	// And a data source with no replica is reported as such.
	if _, ok := readDataSource(crispv1alpha1.DataSource{}); ok {
		t.Error("readDataSource() invented a replica")
	}
}

// TestEnvResolverReadsBothHalves, which is what makes the replica path usable
// without a cluster.
func TestEnvResolverReadsBothHalves(t *testing.T) {
	t.Setenv("ORDERS_DB_DSN", "postgres://primary")
	t.Setenv("ORDERS_REPLICA_DSN", "postgres://replica")

	resolver := EnvDSNResolver{}
	ds := crispv1alpha1.DataSource{
		SecretRef:     crispv1alpha1.SecretReference{Name: "orders-db", Namespace: "ns"},
		ReadSecretRef: &crispv1alpha1.SecretReference{Name: "orders-replica", Namespace: "ns"},
	}

	if got, err := resolver.Resolve(context.Background(), ds); err != nil || got != "postgres://primary" {
		t.Errorf("Resolve() = %q, %v", got, err)
	}
	got, ok, err := ResolveRead(context.Background(), resolver, ds)
	if err != nil || !ok || got != "postgres://replica" {
		t.Errorf("ResolveRead() = %q, %v, %v", got, ok, err)
	}

	// A variable that is not set is an error rather than an empty DSN.
	missing := crispv1alpha1.DataSource{
		SecretRef: crispv1alpha1.SecretReference{Name: "absent", Namespace: "ns"},
	}
	if _, err := resolver.Resolve(context.Background(), missing); err == nil {
		t.Error("Resolve() accepted an unset environment variable")
	}
}

// TestPoolLabelCarriesNoCredentials: it names a database in metrics, and a
// metric label is about the least private place a connection string could end
// up.
func TestPoolLabelCarriesNoCredentials(t *testing.T) {
	ds := crispv1alpha1.DataSource{Driver: "postgres"}
	dsn := "postgres://user:hunter2@db:5432/store"

	label := PoolLabel(ds, dsn)
	if label == "" {
		t.Fatal("PoolLabel() returned nothing")
	}
	for _, secret := range []string{"hunter2", "user", "db:5432", "store"} {
		if strings.Contains(label, secret) {
			t.Errorf("PoolLabel() = %q, which leaks %q", label, secret)
		}
	}
	if PoolLabel(ds, dsn) != PoolKey(ds, dsn) {
		t.Error("the metric label and the pool key disagree")
	}

	// A different connection string is a different pool, which is what makes
	// credential rotation take effect.
	if PoolKey(ds, dsn) == PoolKey(ds, "postgres://user:rotated@db:5432/store") {
		t.Error("two connection strings produced one pool key")
	}
	// A different prepared-statement setting is not: it is carried on the
	// statement, so the two projections share the database's one pool. See
	// TestPoolKeyIsTheDataSourceAlone.
	unprepared := ds
	no := false
	unprepared.PreparedStatements = &no
	if PoolKey(ds, dsn) != PoolKey(unprepared, dsn) {
		t.Error("prepared and unprepared projections got separate pools for one database")
	}
}
