package projection

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// A minted token must never reach the pool key, and the auth stanza must.
//
// This is the whole reason a credential provider is not simply a thing that
// rewrites the connection string. The key is a hash of the connection string,
// so a token inside it would be a different key every fifteen minutes: a new
// pool four times an hour, every live connection dropped and every prepared
// statement thrown away, for a credential that changed and a database that did
// not. The token is never in the string this hashes, so nothing about the key
// moves when it turns over.
func TestThePoolKeyDoesNotMoveWhenTheTokenDoes(t *testing.T) {
	const dsn = "postgres://orders@db.example:5432/store?sslmode=require"

	authed := crispv1alpha1.DataSource{
		Driver: "postgres",
		Auth: &crispv1alpha1.DataSourceAuth{
			Provider: "aws-rds-iam",
			Options:  map[string]string{"region": "eu-central-1"},
		},
	}

	// The connection string a provider is given never carries the password, so
	// this is the only string there is — and the same projection read again an
	// hour later, into a fresh object with a fresh map, is the same key.
	//
	// The two data sources here are equal and not identical, which is what the
	// controller actually has: every sync decodes the projection again.
	resynced := crispv1alpha1.DataSource{
		Driver: "postgres",
		Auth: &crispv1alpha1.DataSourceAuth{
			Provider: "aws-rds-iam",
			Options:  map[string]string{"region": "eu-central-1"},
		},
	}
	if PoolKey(authed, dsn) != PoolKey(resynced, dsn) {
		t.Fatal("the same data source decoded twice produced two pool keys, so every sync rebuilds the pool")
	}

	// The stanza itself is part of the identity, though: the same database
	// reached with two different providers is two different ways of
	// authenticating, and sharing a pool would give both of them whichever one
	// opened it first.
	other := authed
	other.Auth = &crispv1alpha1.DataSourceAuth{
		Provider: "gcp-cloudsql-iam",
		Options:  authed.Auth.Options,
	}
	if PoolKey(authed, dsn) == PoolKey(other, dsn) {
		t.Error("two credential providers on one connection string shared a pool")
	}

	// So are its options, for the same reason: two regions are two identities.
	elsewhere := authed
	elsewhere.Auth = &crispv1alpha1.DataSourceAuth{
		Provider: authed.Auth.Provider,
		Options:  map[string]string{"region": "us-east-1"},
	}
	if PoolKey(authed, dsn) == PoolKey(elsewhere, dsn) {
		t.Error("two regions on one connection string shared a pool")
	}

	// And the options are read in a fixed order, or an unchanged projection
	// would land on a different key between two syncs and rebuild its pool on a
	// coin toss.
	many := crispv1alpha1.DataSourceAuth{
		Provider: "aws-rds-iam",
		Options: map[string]string{
			"region": "eu-central-1", "user": "orders", "endpoint": "db.example:5432",
			"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
		},
	}
	first := PoolKey(crispv1alpha1.DataSource{Driver: "postgres", Auth: &many}, dsn)
	for i := 0; i < 100; i++ {
		if got := PoolKey(crispv1alpha1.DataSource{Driver: "postgres", Auth: &many}, dsn); got != first {
			t.Fatalf("the pool key for one data source came out as %s and then %s", first, got)
		}
	}
}

// The pool key is the whole digest, and this writes it out so that changing it
// again has to be deliberate: every pool in every existing deployment is
// rebuilt when it moves.
//
// It moved once, on purpose. It used to be the first four bytes, and a pool key
// is the identity of a set of live connections carrying one database's
// credentials — PoolCache hands back the existing pool on a match without
// looking at the connection string again. 2^32 is a search rather than a
// coincidence, and the value to search for was published as the datasource
// label on every pool metric.
func TestThePoolKeyIsTheWholeDigest(t *testing.T) {
	const dsn = "postgres://user:pass@db.example:5432/store?sslmode=require"
	ds := crispv1alpha1.DataSource{Driver: "postgres"}

	const want = "postgres#f9742ff06e6dd469a38beff80c38c2526a45d6166e604e1c2b8453000c0a29f5"
	if got := PoolKey(ds, dsn); got != want {
		t.Errorf("PoolKey() = %s, want %s — every pool in an existing deployment would be rebuilt", got, want)
	}
	// The old four-byte key, so a revert to it fails here rather than quietly.
	if got := PoolKey(ds, dsn); got == "postgres#f9742ff0" {
		t.Error("PoolKey() is a four-byte prefix again")
	}

	// And an explicitly empty stanza is still a stanza, so it does move.
	withAuth := ds
	withAuth.Auth = &crispv1alpha1.DataSourceAuth{Provider: "aws-rds-iam"}
	if PoolKey(withAuth, dsn) == want {
		t.Error("a data source that mints its password shared a pool with one that does not")
	}
}

// An auth stanza this build cannot serve is refused while the projection is
// being compiled, not when a connection is opened.
//
// This is the trap sessionDialectFor documents at length in pkg/sql/session.go:
// a capability granted in one place and denied in another, where the denial is
// on a path nobody watches. A projection whose provider is missing would
// otherwise compile clean, report Ready, and fail every request afterwards with
// a message about a credential provider — for a build that could never have
// served it.
func TestValidateRefusesAnAuthStanzaThisBuildCannotServe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		driver string
		auth   *crispv1alpha1.DataSourceAuth
		wants  []string
	}{
		{
			name:   "a provider nothing registered",
			driver: "postgres",
			auth:   &crispv1alpha1.DataSourceAuth{Provider: "aws-rds-iam"},
			// This test binary imports no provider, so none is registered, and
			// the message has to say that rather than trailing off after "this
			// build knows". The published binary registers aws-rds-iam and
			// token-file; what is being checked here is the empty case.
			wants: []string{"aws-rds-iam", "no credential provider at all", "linked in"},
		},
		{
			name:   "a driver that cannot be handed a password per connection",
			driver: "sqlite",
			auth:   &crispv1alpha1.DataSourceAuth{Provider: "aws-rds-iam"},
			wants:  []string{"sqlite", "per connection"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := incrementalProjection()
			p.Spec.DataSource.Driver = tc.driver
			p.Spec.DataSource.Auth = tc.auth

			err := Validate(p)
			if err == nil {
				t.Fatal("Validate() accepted an auth stanza this build cannot serve")
			}
			for _, want := range tc.wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}

	// And a projection with no auth at all is untouched by any of this.
	p := incrementalProjection()
	if err := Validate(p); err != nil {
		t.Errorf("Validate() rejected a projection that sets no auth: %v", err)
	}
}
