# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's advisory form:

**https://github.com/mrueg/kube-crisp/security/advisories/new**

Please do not open a public issue for a suspected vulnerability. A report is
most useful with the projection that triggers it (with credentials removed),
the driver and database version, and what an attacker gains.

Expect an acknowledgement within a week. Fixes are released as a new tag, and
the advisory is published once one is available.

## Supported versions

kube-crisp is pre-1.0 and interfaces still change. Fixes land on `main` and in
the next tag; earlier tags are not patched.

## What kube-crisp assumes

kube-crisp executes operator-supplied SQL against a database, using credentials
it reads from Secrets. Its security properties follow from that, and the
assumptions are worth stating because a deployment that breaks one of them is
not protected by the ones that remain.

**A `CustomResourceProjection` is as privileged as the database it names.** It
is cluster-scoped and carries arbitrary SQL, so whoever can create one chooses
both the statement and the credentials it runs with. Treat `create` on
`customresourceprojections` as equivalent to a database shell, and grant it as
narrowly as you would that.

**A data source Secret has to opt in.** `--require-datasource-optin` is on by
default and demands the label `crisp.kubecrisp.io/allow-projection=true` on any
Secret a projection connects with. That is what keeps the decision to expose a
database with whoever owns its credentials, rather than with whoever can write a
projection. `--datasource-namespaces` bounds where those Secrets may live, and
defaults to the server's own namespace.

**Values are bound, never interpolated.** Request-supplied values reach the
database as driver parameters, so nothing a client sends can change the shape of
a statement. The one identifier that is not a parameter — a session variable's
name, which no supported driver will bind — is validated against a character
allowlist before it is used. This is a property of kube-crisp, not of the SQL a
projection contains: a projection that builds a statement dynamically elsewhere
is outside what this guarantees.

**Authentication and authorization are delegated.** The kube-apiserver reviews
tokens and permissions, so existing RBAC governs who may read or write a
projected resource. kube-crisp adds no authorization of its own.

**Except when there is no cluster to delegate to.** Started with no
`--kubeconfig` and no mounted service account — the shape of a local run against
your own database — there is nothing to review a token or answer a
`SubjectAccessReview`, and delegated authorization can only deny every request.
The server allows them instead and says so at startup:

```
no kubeconfig and no service account: serving without authentication or
authorization, because there is no cluster to delegate either to. Every request
is allowed.
```

That is a development mode: the port must not be exposed. It cannot be reached
by accident from a cluster, where the service account makes it a normal
delegated server, nor when `--kubeconfig` is given — an unreachable cluster
named explicitly is an error rather than a downgrade.

**Tenancy is only as strong as where it is enforced.** Mapping a column to
`metadata.namespace` puts namespace RBAC in front of it, and that is usually
enough. When the boundary has to survive a mistake in a projection's `WHERE`
clause, use `dataSource.sessionVariables` with row-level security so the database
decides which rows exist — a policy the query cannot see past.

**Writes are audited; bound values are not recorded.** With `--audit-log-path`
set, each write carries annotations naming the projection, resource, verb, data
source, statement text, and affected rows. The values bound into the statement
are deliberately excluded: they are the caller's data.

## Hardening checklist

The shipped manifests favour a working first deployment. Before production:

- Replace `insecureSkipTLSVerify` on the `APIService` with a real `caBundle`,
  and give the server a serving certificate rather than the self-signed one it
  generates (`--tls-cert-file`, `--tls-private-key-file`).
- Narrow `manifests/20-rbac.yaml`'s `secrets` rule to named Secrets with
  `resourceNames`. It is already restricted to one namespace, but not to
  individual Secrets.
- Pin the image to a digest rather than `:latest`. Releases are signed with
  cosign and carry SBOMs and build provenance; verify them.
- Set `--manage-apiservices=false` and drop the `apiregistration.k8s.io` rule if
  you would rather register groups yourself. Managing them needs cluster-wide
  write access to APIServices.
- Give each projection a database role with only the privileges its queries
  need. kube-crisp bounds concurrency and rows; it does not bound what a
  statement is permitted to do.
- Restrict egress with `manifests/optional/networkpolicy.yaml`, after filling in
  the rule for your databases. Since a projection chooses both the destination
  and the statement, this is what turns "the SQL is arbitrary" into "the SQL is
  arbitrary, against these databases".
- Leave `--profiling=false`, as the shipped Deployment sets it. `/debug/pprof`
  is on by default and behind delegated authorization, but it is a surface
  nothing here needs, and a heap or CPU profile of this server describes the
  queries it is running.
- Consider `--enable-admission` so `ValidatingAdmissionPolicy` and admission
  webhooks apply to projected writes, and `--enable-priority-and-fairness` so
  one client cannot take a projection's whole capacity. Each needs the extra
  RBAC in `manifests/optional/admission-rbac.yaml` and
  `manifests/optional/flowcontrol-rbac.yaml`.

## Out of scope

- What a projection's own SQL does. kube-crisp runs the statements it is given.
- Denial of service from a projection configured against its own database —
  `maxConcurrentQueries`, `maxRows`, and `timeout` exist to bound it, and are
  the operator's to set.
- The database's own security posture. Transport encryption is configured through
  the connection string, and kube-crisp logs a warning at startup for any data
  source reached without it — credentials and every projected row cross that
  connection. It is a warning rather than a refusal because a unix socket, a
  sidecar proxy, or a database on the same host needs no TLS. Note that
  PostgreSQL's default `sslmode=prefer` counts as without: it tries TLS and
  continues in the clear if the server declines, without reporting which
  happened. `sslmode=require` or better is what actually insists.
