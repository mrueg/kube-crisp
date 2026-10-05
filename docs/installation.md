# Installing

Running the server locally or in a cluster, what the optional manifests are for, and getting the
`kubectl crisp` plugin.

## The server

These project a sample database this repository does not carry — run
[`./hack/fetch-pagila.sh`](../third_party/pagila) and load it into a PostgreSQL 18 server first, or
point the DSN at a database of your own and apply [`examples/orders/`](../examples/orders) instead,
which needs one `CREATE TABLE`.

Local, against a database you can already reach:

```console
$ export PAGILA_DB_DSN='postgres://user:pass@localhost:5432/pagila?sslmode=disable'
$ go run ./cmd/kube-crisp-apiserver \
    --projection-dir=examples/pagila --local-dsn-from-env \
    --watch-projections=false --secure-port=8443 --authentication-skip-lookup
```

In a cluster, with Helm — one chart per release, published beside the image:

```console
$ helm install kube-crisp oci://ghcr.io/mrueg/charts/kube-crisp --namespace kube-crisp --create-namespace
$ kubectl apply -f examples/pagila/00-secret.yaml     # edit the DSN first
$ kubectl apply -f examples/pagila/10-catalogue.yaml
$ kubectl get films                                   # the APIService registers itself
```

That last command works as cluster-admin. Authorization is the cluster's, so everyone else needs a
ClusterRole naming the group first — `kubectl crisp rbac | kubectl apply -f -` writes it.

`--version` pins a release; without it Helm takes the newest published. The chart is signed with
cosign keylessly, the same as the image, and both of its version numbers are stamped at release
time — so the image a default install deploys is always the one that release built.

The copy in this repository is the development one and is not a release: it is versioned
`0.0.0-dev`, and its `appVersion` is `latest`, so `helm install ./charts/kube-crisp` from a checkout
deploys the newest released image rather than a number frozen at whenever the file was last edited.
Use it for a change to the chart that is not released yet, with `--set image.tag=` to pin a version
or name an image you built.

`helm show values oci://ghcr.io/mrueg/charts/kube-crisp` lists what can be turned on: admission,
fair queueing, a `ServiceMonitor`, an egress `NetworkPolicy`, a real serving certificate.

A real certificate is two values set together. `crisp.servingCertSecret` names an existing
`kubernetes.io/tls` Secret in the release namespace, valid for `<fullname>.<namespace>.svc` — a
cert-manager `Certificate` is the usual way to get one — and the server presents it instead of the
certificate it signs for itself. `crisp.caBundle` is the PEM CA that signed it, written into the
APIServices the server creates and used by the `ServiceMonitor` to verify its scrape. The chart
refuses a `caBundle` without the Secret: the APIServices would trust a CA the server does not present
a certificate from, and every group would stay unavailable.

Or with plain manifests:

```console
$ kubectl apply -f manifests/
$ kubectl apply -f examples/pagila/00-secret.yaml     # edit the DSN first
$ kubectl apply -f examples/pagila/10-catalogue.yaml
$ kubectl get films                                   # the APIService registers itself
```

`manifests/optional/` is deliberately not picked up by the apply above — `kubectl apply -f` on a
directory is not recursive. Everything in it is a decision rather than a default:

| File | What it is for | Why it is not applied by default |
| --- | --- | --- |
| `networkpolicy.yaml` | Restricts traffic to and from the server | Needs your cluster's CIDRs and namespace labels |
| `servicemonitor.yaml`, `prometheusrule.yaml` | Scrape config and alert rules | Need the Prometheus Operator's CRDs |
| `admission-rbac.yaml` | Lets the API surface project admission configuration | Watches webhook configurations, admission policies and namespaces cluster-wide |
| `flowcontrol-rbac.yaml` | Lets it project FlowSchemas and PriorityLevelConfigurations | Writes to `flowschemas/status` |
| `webhook-rbac.yaml` | Lets the server manage its own `ValidatingWebhookConfiguration`, and carries the role the kube-apiserver's identity needs to call the webhook | Creates and updates a cluster-scoped admission object; the caller role is yours to bind |

The last three used to sit in the main directory as `60-`, `70-` and `80-`, which meant the base
install granted them — while the documentation described each as a grant to make deliberately.

## The kubectl plugin

`kubectl crisp` writes the RBAC a projected group needs to be reachable, shows who may reach it,
finds the roles a deleted projection left behind, says what a projection needs from its database, and
says why one is not answering.
kubectl finds it by name: any executable called `kubectl-crisp` on `PATH` becomes `kubectl crisp`.

Releases carry one archive per platform holding the plugin alone, for Linux, macOS and Windows — it
links no database driver, so unlike the server there is no reason to build your own:

```console
$ VERSION=0.2.0; OS=linux; ARCH=amd64
$ curl -sSLO "https://github.com/mrueg/kube-crisp/releases/download/v$VERSION/kubectl-crisp_${VERSION}_${OS}_${ARCH}.tar.gz"
$ tar xzf "kubectl-crisp_${VERSION}_${OS}_${ARCH}.tar.gz" kubectl-crisp
$ sudo install kubectl-crisp /usr/local/bin/
$ sudo ln -s kubectl-crisp /usr/local/bin/kubectl_complete-crisp   # optional; see below
$ kubectl crisp --help
```

The link is what makes `kubectl crisp <TAB>` complete. kubectl asks a plugin for completions by
looking up an executable named `kubectl_complete-<plugin>` on `PATH` and offers nothing without one,
however much the plugin itself knows — so the plugin answers to that name as well, and the link is
the whole of it. There is no second program to install or to keep in step. Skipping it costs the
completion and nothing else.

What completes: the subcommands, the projection names `rbac`, `can-i` and `schema` take — read from
the cluster and described by the resource each one serves — the output formats each command accepts,
`--context` from the kubeconfig, and `-n` from the cluster's namespaces. A completion that cannot
reach the cluster says nothing and offers no filenames, since a filename is never the right guess in
a position that wants a projection.

On Windows the lookup goes through `PATHEXT`, so the copy has to be named `kubectl_complete-crisp.exe`
to be found.

Each release also carries a `checksums.txt`, signed with cosign keylessly, which is what to check the
download against.

Or with Go, which reports its version as `dev` — the real one is stamped at release time:

```console
$ go install github.com/mrueg/kube-crisp/cmd/kubectl-crisp@latest
$ ln -s kubectl-crisp "$(go env GOPATH)/bin/kubectl_complete-crisp"   # completion, as above
```

From a checkout, `make build` puts it in `bin/` beside the server, with the completion link made.

It is not on [krew](https://krew.sigs.k8s.io) yet.
