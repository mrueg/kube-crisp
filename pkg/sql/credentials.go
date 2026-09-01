package sql

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Credentials mint the password for one new connection.
//
// The interface exists because a cloud database increasingly does not have a
// password to put in a Secret. AWS RDS IAM, Cloud SQL and Entra all hand out a
// short-lived token instead — a quarter of an hour, typically — and expect the
// client to ask for another one when it needs another connection.
//
// The lifetime is what makes this a connection's property rather than a pool's.
// A token written into the connection string would change the string every time
// it was refreshed, and the connection string is what a pool is keyed by, so
// every refresh would build a new pool: live connections dropped, prepared
// statements thrown away, and the database asked to authenticate everything
// again, four times an hour, forever. Written in once and never refreshed, the
// pool survives and every connection opened after the token expires fails to
// authenticate instead — which is worse, because it looks like the database
// went down rather than like a credential ran out.
//
// So the token is minted here, per connection, by a Connector that database/sql
// calls when it decides it needs one more. The pool is keyed by the connection
// string without the password in it, and nothing about it changes when the
// token does.
//
// Password is called on the path that opens a connection, so it is called with
// that caller's context and must respect it. Implementations are expected to
// cache: database/sql opens connections in bursts after an idle period, and a
// provider that signs a fresh token for each of them pays for it eight times
// where it could pay once.
type Credentials interface {
	// Password returns the password to authenticate the next connection with.
	Password(ctx context.Context) (string, error)
}

// CredentialsFunc adapts a plain function to Credentials.
type CredentialsFunc func(ctx context.Context) (string, error)

// Password implements Credentials.
func (f CredentialsFunc) Password(ctx context.Context) (string, error) { return f(ctx) }

// CredentialRequest describes the data source a provider is being asked to
// authenticate.
type CredentialRequest struct {
	// Driver is the registered driver name, as spec.dataSource.driver carries
	// it — "postgres", not "pgx".
	Driver string

	// DSN is the connection string from the Secret, with everything but the
	// password in it: host, port, user, database, TLS settings. A provider
	// reads what it needs to address the database out of this — the endpoint
	// and the user, for RDS — rather than being told twice.
	DSN string

	// Options are the provider-specific settings from
	// spec.dataSource.auth.options, verbatim. A provider validates its own:
	// this is the one place a new provider can need configuration without an
	// API change, so the API cannot check it.
	Options map[string]string
}

// CredentialProvider is one way of obtaining a password for a data source.
//
// It is a registry rather than a switch for the same reason the driver registry
// is. Every provider means a cloud SDK linked into the binary, and kube-crisp
// deliberately links no dependency a given build does not need — pulling the
// AWS SDK into a build that projects a SQLite file would be several megabytes
// of code that build can never reach. So a provider registers itself, and a
// build that wants one is a build somebody assembled on purpose.
type CredentialProvider struct {
	// Name is what a projection's dataSource.auth.provider carries.
	Name string

	// Open builds the Credentials for one data source.
	//
	// Called once when the pool is opened, not once per connection, so this is
	// where a provider does the expensive part — resolving an SDK
	// configuration, discovering a region, reading an instance role — and
	// where it reports a request it cannot serve. An error here fails the
	// projection's compilation, which is where somebody is looking.
	Open func(req CredentialRequest) (Credentials, error)
}

var (
	credentialsMu sync.RWMutex
	credentials   = map[string]CredentialProvider{}
)

// RegisterCredentialProvider makes a provider available to projections.
//
// Call it before the server starts serving; a projection naming a provider this
// build did not register is refused when it is compiled.
func RegisterCredentialProvider(p CredentialProvider) error {
	switch {
	case p.Name == "":
		return fmt.Errorf("a credential provider needs a name")
	case p.Open == nil:
		return fmt.Errorf("credential provider %q does not say how to obtain a password", p.Name)
	}

	credentialsMu.Lock()
	defer credentialsMu.Unlock()

	if _, taken := credentials[p.Name]; taken {
		return fmt.Errorf("credential provider %q is already registered", p.Name)
	}
	credentials[p.Name] = p
	return nil
}

// LookupCredentialProvider returns the provider registered under a name.
func LookupCredentialProvider(name string) (CredentialProvider, bool) {
	credentialsMu.RLock()
	defer credentialsMu.RUnlock()

	p, ok := credentials[name]
	return p, ok
}

// RegisteredCredentialProviders lists what a projection may name, sorted.
//
// Short, and sometimes empty. A provider that talks to a cloud is that cloud's
// SDK linked into the binary, so the one this repository builds carries none of
// them and registers only what needs nothing — reading a credential out of a
// file. This is what an error about an unknown provider offers instead, so that
// somebody who deployed the stock image and asked it for AWS credentials is
// told what it does have rather than only what it does not.
func RegisteredCredentialProviders() []string {
	credentialsMu.RLock()
	defer credentialsMu.RUnlock()

	names := make([]string, 0, len(credentials))
	for name := range credentials {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SupportsCredentialProviders reports whether a driver can authenticate a
// connection with a password minted for it.
//
// It is Driver.AuthConnector restated as a question, so that the same check the
// CRD's CEL rule makes is one the code can be asked. A driver without one has
// no way to hand a per-connection password to the database: the password is in
// the connection string, the connection string is fixed when the pool opens,
// and a token that expires would take the pool down with it.
func SupportsCredentialProviders(driver string) bool {
	d, ok := Lookup(driver)
	return ok && d.AuthConnector != nil
}
