#!/usr/bin/env bash
# Brings up a kind cluster running kube-crisp against PostgreSQL, seeded with
# ROWS orders. Idempotent: re-running against an existing cluster redeploys.
set -euo pipefail

CLUSTER="${CLUSTER:-kube-crisp-e2e}"
ROWS="${ROWS:-10000}"
BENCH_NAMESPACE="${BENCH_NAMESPACE:-bench}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-${REPO_ROOT}/hack/.e2e-kubeconfig}"

cd "${REPO_ROOT}"

# The image. Built here unless the caller already has one in the local docker
# daemon and names it in KUBE_CRISP_IMAGE — which is how CI builds once and runs
# five shards against it, rather than rebuilding the same commit five times.
if [[ -n "${KUBE_CRISP_IMAGE:-}" ]]; then
  IMAGE="${KUBE_CRISP_IMAGE}"
  echo "==> using ${IMAGE}"
  docker image inspect "${IMAGE}" >/dev/null 2>&1 \
    || { echo "!! KUBE_CRISP_IMAGE=${IMAGE} is not in the local docker daemon" >&2; exit 1; }
else
  IMAGE="$(./hack/e2e-image.sh)"
fi

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "==> creating kind cluster ${CLUSTER}"
  kind create cluster --name "${CLUSTER}" --wait 180s
fi

# A dedicated kubeconfig keeps the developer's default context untouched.
kind get kubeconfig --name "${CLUSTER}" > "${KUBECONFIG_PATH}"
export KUBECONFIG="${KUBECONFIG_PATH}"

echo "==> loading image into the cluster"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

echo "==> installing kube-crisp"
kubectl apply -f manifests/00-namespace.yaml
kubectl apply -f manifests/10-crd-customresourceprojection.yaml
kubectl apply -f manifests/20-rbac.yaml
kubectl apply -f manifests/optional/admission-rbac.yaml
kubectl apply -f manifests/optional/webhook-rbac.yaml
kubectl apply -f manifests/40-service.yaml
kubectl apply -f test/e2e/manifests/postgres.yaml
kubectl apply -f test/e2e/manifests/mysql.yaml
kubectl apply -f test/e2e/manifests/sqlite.yaml
kubectl apply -f test/e2e/manifests/static-projections.yaml
sed -e "s|\${KUBE_CRISP_IMAGE}|${IMAGE}|g" -e "s|\${BENCH_ROWS}|${ROWS}|g" \
  test/e2e/manifests/kube-crisp.yaml | kubectl apply -f -
kubectl apply -f test/e2e/manifests/bench-crd.yaml
kubectl create namespace "${BENCH_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace acme --dry-run=client -o yaml | kubectl apply -f -

# goreleaser tags snapshots by version, which does not change between builds of
# the same commit, so the pod spec can be identical while the image behind it is
# new. Force a rollout rather than trusting the tag.
kubectl -n kube-crisp rollout restart deployment/kube-crisp-apiserver

echo "==> waiting for postgres"
kubectl -n kube-crisp rollout status deployment/postgres --timeout=180s

# The postgres image answers pg_isready against the temporary server it runs
# during initdb, so a rollout can report finished a moment before the real
# server accepts connections. Retry rather than racing it.
wait_for_postgres() {
  for _ in $(seq 1 60); do
    if kubectl -n kube-crisp exec deploy/postgres -- \
        psql -U crisp -d store -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "!! postgres never accepted connections" >&2
  return 1
}

wait_for_mysql() {
  for _ in $(seq 1 90); do
    if kubectl -n kube-crisp exec deploy/mysql -- \
        mysql -ucrisp -pcrisp store -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "!! mysql never accepted connections" >&2
  return 1
}

echo "==> seeding ${ROWS} rows"
wait_for_postgres
kubectl -n kube-crisp exec deploy/postgres -- psql -U crisp -d store -v ON_ERROR_STOP=1 -c "
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_events;
DROP TABLE IF EXISTS applied_orders;
DROP TABLE IF EXISTS order_tombstones;
DROP TABLE IF EXISTS notified_orders;
DROP TABLE IF EXISTS polled_orders;
DROP FUNCTION IF EXISTS notified_orders_changed();
DROP TABLE IF EXISTS tagged_orders;
DROP TABLE IF EXISTS team_orders;
DROP TABLE IF EXISTS replicated_orders;
DROP TABLE IF EXISTS tombstoned_orders;
CREATE TABLE orders (
    id          TEXT PRIMARY KEY,
    tenant      TEXT   NOT NULL,
    customer    TEXT   NOT NULL,
    status      TEXT   NOT NULL,
    total_cents BIGINT NOT NULL,
    updated_at  TEXT   NOT NULL,
    -- Marked rather than removed, and a counter that advances with the spec:
    -- what the lifecycle projection maps onto deletionTimestamp and generation.
    deleted_at  TEXT,
    generation  BIGINT NOT NULL DEFAULT 1,
    -- JSON, for the finalizer flow and for garbage collection.
    finalizers  TEXT,
    owners      TEXT
);
-- Server-side apply needs somewhere to keep field ownership, or every apply
-- looks like the first one and two managers silently overwrite each other.
CREATE TABLE applied_orders (
    id             TEXT PRIMARY KEY,
    tenant         TEXT NOT NULL,
    customer       TEXT NOT NULL,
    status         TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    managed_fields TEXT
);
INSERT INTO applied_orders (id, tenant, customer, status, updated_at) VALUES
    ('applied-1', 'acme', 'ada', 'pending', '1');

-- Its own table, so deleting from it cannot disturb the shared orders fixture
-- that most of the suite reads.
CREATE TABLE tombstoned_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO tombstoned_orders VALUES
    ('doomed-1', 'acme', 'ada',   '1'),
    ('doomed-2', 'acme', 'grace', '2');

-- What a deletion query reads, so an incremental watch can see a removal
-- without the periodic full scan that exists only to notice one.
-- The mapped columns as well as the identity. A tombstone that only names the
-- row leaves the watch cache as the only place a deleted object exists, which
-- is what makes it hold the whole collection; one that describes the row lets
-- the cache keep keys and versions and still answer a deletion.
-- Keyed on (id, deleted_at), not on id alone. A name can be deleted, created
-- again and deleted again — the same object identity twice over — and a
-- primary key on id makes the second delete fail with a duplicate key for as
-- long as the first tombstone exists, which is forever. The row then cannot be
-- deleted at all.
CREATE TABLE order_tombstones (
    id         TEXT NOT NULL,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    PRIMARY KEY (id, deleted_at)
);

-- Labels that vary per row live in one JSON column rather than one column per
-- key, and a selector on a promoted key is answered by the database.
CREATE TABLE tagged_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    status     TEXT NOT NULL,
    labels     JSONB,
    updated_at TEXT NOT NULL
);
-- Built rather than written as a literal: this whole block reaches psql inside
-- a double-quoted shell string, which would eat the quotes JSON needs.
INSERT INTO tagged_orders VALUES
    ('tagged-1', 'acme', 'ada',   'shipped', jsonb_build_object('team', 'payments', 'tier', 'gold'), '1'),
    ('tagged-2', 'acme', 'grace', 'pending', jsonb_build_object('team', 'search'),                   '2'),
    ('tagged-3', 'acme', 'alan',  'shipped', NULL,                                                   '3');

-- Rows owned by a group, for a projection that scopes them to the caller's.
CREATE TABLE team_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    owner_team TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- system:authenticated rather than a cluster-admin group: every authenticated
-- caller has it, whichever way the cluster hands out its admin identity. A
-- kubeadm cluster puts its admin in kubeadm:cluster-admins, not system:masters.
INSERT INTO team_orders VALUES
    ('team-authenticated', 'acme', 'system:authenticated', '1'),
    ('team-nobody',        'acme', 'nobody-at-all',        '2');

-- The primary half of the read-split test. The replica database holds the same
-- row with a different customer, and nothing replicates between them.
CREATE TABLE replicated_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO replicated_orders VALUES ('split-1', 'acme', 'from-the-primary', '1');

-- What a pushed watch reads. The trigger is the half the database owns: without
-- something sending pg_notify, spec.watch.notify subscribes to a channel that
-- never says anything and the projection is back to its poll interval.
--
-- FOR EACH STATEMENT rather than FOR EACH ROW, because the payload is never
-- read: one notification per statement says everything a thousand would.
CREATE TABLE notified_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO notified_orders VALUES ('notified-1', 'acme', 'ada', '1');

-- The dollar-quoting is escaped because this SQL is inside a double-quoted bash
-- string: unescaped, \$notify\$ is a shell variable, and with set -u the script
-- stops rather than sending PostgreSQL something that half-expanded.
CREATE FUNCTION notified_orders_changed() RETURNS trigger AS \$notify\$
BEGIN
    PERFORM pg_notify('notified_orders_changed', '');
    RETURN NULL;
END;
\$notify\$ LANGUAGE plpgsql;

CREATE TRIGGER notified_orders_notify
    AFTER INSERT OR UPDATE OR DELETE ON notified_orders
    FOR EACH STATEMENT EXECUTE FUNCTION notified_orders_changed();

-- The control: the same rows and the same shape, with no notify and no trigger,
-- so a change there can only be found by the poll timer.
CREATE TABLE polled_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO polled_orders VALUES ('polled-1', 'acme', 'ada', '1');

-- The second table a transactional write has to reach atomically.
CREATE TABLE order_events (
    id     TEXT NOT NULL,
    tenant TEXT NOT NULL,
    event  TEXT NOT NULL
);
-- A composite key: no single column can name a row here.
DROP TABLE IF EXISTS shipments;
CREATE TABLE shipments (
    region   TEXT NOT NULL,
    order_no TEXT NOT NULL,
    tenant   TEXT NOT NULL,
    carrier  TEXT NOT NULL,
    seq      BIGSERIAL,
    PRIMARY KEY (region, order_no)
);
INSERT INTO shipments (region, order_no, tenant, carrier) VALUES
    ('eu','1042','acme','dhl'),
    ('us','1042','acme','ups'),
    ('eu','2001','acme','dpd');

-- Row-level security, so the database decides which rows a request may see
-- rather than the projection's WHERE clause. The policy reads a setting the
-- server puts on the connection from the request.
DROP TABLE IF EXISTS secured_orders;
CREATE TABLE secured_orders (
    id       TEXT PRIMARY KEY,
    tenant   TEXT NOT NULL,
    customer TEXT NOT NULL
);
INSERT INTO secured_orders VALUES
    ('secured-acme-1','acme','ada'),
    ('secured-acme-2','acme','grace'),
    ('secured-globex-1','globex','alan');
ALTER TABLE secured_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE secured_orders FORCE ROW LEVEL SECURITY;
CREATE POLICY secured_orders_tenant ON secured_orders
    USING (tenant = current_setting('app.tenant', true));
-- A policy is never enforced against a superuser, whatever FORCE says, and the
-- bootstrap role is one it cannot drop. So the secured projection connects as
-- a role with no special standing, which is what any real deployment would do.
-- The table it was granted on has just been dropped, so nothing depends on it.
DROP ROLE IF EXISTS crisp_app;
CREATE ROLE crisp_app LOGIN PASSWORD 'crisp';
GRANT SELECT ON secured_orders TO crisp_app;
INSERT INTO orders (id, tenant, customer, status, total_cents, updated_at)
SELECT
    'order-' || lpad(i::text, 6, '0'),
    '${BENCH_NAMESPACE}',
    'customer-' || (i % 997),
    CASE WHEN i % 3 = 0 THEN 'shipped' ELSE 'pending' END,
    (i * 37) % 100000,
    i::text
FROM generate_series(1, ${ROWS}) AS i;
INSERT INTO orders (id, tenant, customer, status, total_cents, updated_at) VALUES
    ('order-acme-1','acme','ada','shipped',4999,'1'),
    ('order-acme-2','acme','grace','pending',1250,'2');
CREATE INDEX orders_tenant_idx ON orders (tenant);
-- The column mapped to resourceVersion, because the watch query filters and
-- orders by it on every poll. Without this the poll is a sequential scan and a
-- sort of the whole table, once per pollInterval: measured at 10,000 rows, 6.0ms
-- and 116 buffers per poll against 0.16ms and 10 with the index, and the gap
-- grows with the table rather than staying flat.
CREATE INDEX orders_updated_at_idx ON orders (updated_at);
ANALYZE orders;
"

# MySQL is in the suite so the second supported driver is actually exercised:
# it has no RETURNING and binds ? placeholders, both different code paths.
# The stand-in read replica: a second database on the same server, holding the
# same row with a different value so a read cannot be mistaken for the primary's.
# CREATE DATABASE cannot be made conditional in plain SQL, and a second run
# finding it already there is the expected case rather than a failure.
kubectl -n kube-crisp exec deploy/postgres -- \
  psql -U crisp -d postgres -c "CREATE DATABASE storereplica" >/dev/null 2>&1 || true

kubectl -n kube-crisp exec deploy/postgres -- psql -U crisp -d storereplica -v ON_ERROR_STOP=1 -c "
DROP TABLE IF EXISTS replicated_orders;
CREATE TABLE replicated_orders (
    id         TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    customer   TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
INSERT INTO replicated_orders VALUES ('split-1', 'acme', 'from-the-replica', '1');
"

echo "==> waiting for mysql"
kubectl -n kube-crisp rollout status deployment/mysql --timeout=300s

echo "==> seeding mysql"
wait_for_mysql
kubectl -n kube-crisp exec deploy/mysql -- mysql -ucrisp -pcrisp store -e "
DROP TABLE IF EXISTS widgets;
CREATE TABLE widgets (
    id           VARCHAR(64) PRIMARY KEY,
    tenant       VARCHAR(64) NOT NULL,
    colour       VARCHAR(64) NOT NULL,
    weight_grams BIGINT      NOT NULL,
    updated_at   VARCHAR(32) NOT NULL,
    INDEX widgets_tenant_idx (tenant)
);
INSERT INTO widgets VALUES
    ('widget-1', 'acme', 'red', 120, '1'),
    ('widget-2', 'acme', 'blue', 340, '2');

-- The same collection the other drivers hold, so the benchmark compares four
-- backends over one dataset. MySQL has no generate_series; a recursive CTE is
-- the equivalent, and its depth limit has to be raised to reach ${ROWS}.
SET SESSION cte_max_recursion_depth = 1000000;
DROP TABLE IF EXISTS bench_orders;
CREATE TABLE bench_orders (
    id          VARCHAR(64) PRIMARY KEY,
    tenant      VARCHAR(64) NOT NULL,
    customer    VARCHAR(64) NOT NULL,
    status      VARCHAR(32) NOT NULL,
    total_cents BIGINT      NOT NULL,
    updated_at  VARCHAR(32) NOT NULL,
    INDEX bench_orders_tenant_idx (tenant)
);
INSERT INTO bench_orders (id, tenant, customer, status, total_cents, updated_at)
WITH RECURSIVE seq(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < ${ROWS}
)
SELECT CONCAT('order-', LPAD(i, 6, '0')),
       '${BENCH_NAMESPACE}',
       CONCAT('customer-', i % 997),
       IF(i % 3 = 0, 'shipped', 'pending'),
       (i * 37) % 100000,
       CAST(i AS CHAR)
FROM seq;
ANALYZE TABLE bench_orders;
" 2>/dev/null

echo "==> waiting for kube-crisp"
kubectl -n kube-crisp rollout status deployment/kube-crisp-apiserver --timeout=180s

echo "==> registering the projection"
kubectl apply -f test/e2e/manifests/orders-projection.yaml
kubectl apply -f test/e2e/manifests/orders-json-projection.yaml
kubectl apply -f test/e2e/manifests/mysql-projection.yaml
kubectl apply -f test/e2e/manifests/sqlite-projection.yaml
kubectl apply -f test/e2e/manifests/extra-projections.yaml
kubectl apply -f test/e2e/manifests/admission-policy.yaml

# No APIService is applied here on purpose: the server registers the groups it
# serves, so waiting for one to appear also tests that it does.
echo "==> waiting for the projected API to be registered and become available"
for _ in $(seq 1 60); do
  status="$(kubectl get apiservice v1alpha1.store.example.com \
    -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null || true)"
  if [[ "${status}" == "True" ]]; then
    echo "==> projected API is available"
    echo
    echo "KUBECONFIG=${KUBECONFIG_PATH}"
    exit 0
  fi
  sleep 2
done

echo "!! the projected API never became available" >&2
kubectl get apiservice v1alpha1.store.example.com -o yaml >&2 || true
kubectl -n kube-crisp logs deploy/kube-crisp-apiserver --tail=100 >&2 || true
exit 1
