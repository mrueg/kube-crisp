# Extending

Drivers and credential providers are registries rather than switches, so a build of your own can add
to either. The [reference](reference.md) says how a projection uses them.

## Adding a driver

`spec.dataSource.driver` names a registered driver, and the registry is open:

```go
sql.Register(sql.Driver{
    Name:             "clickhouse",
    SQLDriver:        "clickhouse",   // what the database/sql driver registered as
    Placeholders:     sql.PlaceholderQuestion,
    SessionVariables: false,
    StatementTimeout: false,
    Notifications:    false,
})
```

Everything that differs between databases is stated there rather than scattered
through switch statements, so adding one is a registration rather than an edit in
six places. What a driver declares is what the rest of the server will offer: a
projection asking for session variables, a statement timeout, or notifications
from a driver that does not claim them is refused rather than silently served
without.

A driver that also sets `AuthConnector` — the seam a short-lived cloud credential
needs — has to answer two more questions, and registration refuses it if it does
not. `Encrypted` reports whether a connection string asks for TLS, which decides
whether to warn about a password the database already shares. `Verified` reports
whether it asks for a mode that establishes *which* server it reached, which is
what a minted credential turns on: a bearer token handed to an impersonator over
an encrypted connection is gone as surely as one sent in the clear, and more
quietly. Only the driver can answer, because only it knows what its own
connection string can say.

You are building your own binary either way — a `database/sql` driver has to be
linked in — so the same build regenerates the CRD, whose `driver` enum lists what
that build accepts.

## Adding a credential provider

`spec.dataSource.auth.provider` names a registered credential provider, and that
registry is open the same way:

```go
sql.RegisterCredentialProvider(sql.CredentialProvider{
    Name: "aws-rds-iam",
    Open: func(req sql.CredentialRequest) (sql.Credentials, error) {
        // req carries the driver, the connection string from the Secret, and
        // whatever dataSource.auth.options said. Return something that mints a
        // password; it is called once per new connection.
        return signer(req)
    },
})
```

It exists for the managed databases that have no password to put in a Secret:
AWS RDS IAM, Cloud SQL and Entra all authenticate with a token minted on demand
and good for about a quarter of an hour. The pool is opened from a
`database/sql` connector rather than from a connection string, so the token is
minted per connection and never becomes part of the pool's identity — which is
what keeps a fifteen-minute credential from rebuilding the pool four times an
hour. See [Passwords that are minted rather than
stored](reference.md#passwords-that-are-minted-rather-than-stored).

The reason this is a registration and not a flag is that the set is open, and a
provider that talks to a cloud is that cloud's SDK. A projection naming one this
build does not have is refused when it is compiled, with that said plainly,
rather than failing at the first query.

**AWS RDS IAM** is registered in
[`cmd/kube-crisp-apiserver`](../cmd/kube-crisp-apiserver/main.go), so a projection
can use it with the published image and no rebuild. That puts the AWS SDK in
this server's dependencies, which is the price of the provider being usable
rather than assemblable. A build wanting another cloud's writes the same
one-line registration against its own `main`, and carries that SDK alone. See
[AWS RDS IAM](reference.md#aws-rds-iam).

**`token-file`** is registered too, and links nothing: it mints no token, it
reads the one something else already refreshes into a file on a mounted volume —
a projected ServiceAccount token, a Vault Agent sidecar, a cloud token refresher.
The file is read per connection, so a rewritten one is picked up without
restarting anything. Which files a projection may name is an operator's decision
rather than the projection's, since a projection is a cluster object and an
unconstrained path would be a way to read the server's own identity and hand it
to a database as a password: `--credential-token-file-dirs` says where
credentials live, and defaults to one directory that exists for this and nothing
else. See [A credential kept in a file](reference.md#a-credential-kept-in-a-file).
