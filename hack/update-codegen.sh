#!/usr/bin/env bash
# Regenerates deepcopy functions, the typed client, and the CRD for the
# kube-crisp API.
#
# Everything under pkg/apis/*/zz_generated.deepcopy.go, pkg/generated, and
# manifests/10-crd-customresourceprojection.yaml is produced here; edit the API
# types and re-run rather than editing the output.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

CODEGEN_PKG="$(go list -m -f '{{.Dir}}' k8s.io/code-generator)"
source "${CODEGEN_PKG}/kube_codegen.sh"

echo "==> deepcopy"
kube::codegen::gen_helpers \
  --boilerplate "${REPO_ROOT}/hack/boilerplate/boilerplate.go.txt" \
  "${REPO_ROOT}/pkg/apis"

echo "==> clientset, listers, and informers"
kube::codegen::gen_client \
  --with-watch \
  --output-dir "${REPO_ROOT}/pkg/generated" \
  --output-pkg "github.com/mrueg/kube-crisp/pkg/generated" \
  --boilerplate "${REPO_ROOT}/hack/boilerplate/boilerplate.go.txt" \
  "${REPO_ROOT}/pkg/apis"

echo "==> CRD"
# Pinned, so a new controller-gen cannot rewrite the committed CRD on somebody
# else's machine and turn an unrelated change into a diff.
CONTROLLER_GEN_VERSION="${CONTROLLER_GEN_VERSION:-v0.19.0}"
TOOLS="$(mktemp -d)"
trap 'rm -rf "${TOOLS}"' EXIT
GOBIN="${TOOLS}" go install "sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_GEN_VERSION}"

# Descriptions are kept. The doc comments on these types are the reference
# documentation for the API, and the CRD is the only place they reach a cluster:
# without them `kubectl explain customresourceprojection.spec.mapping` answers
# with nothing, and the only way to find out what a field means is to open the
# README.
#
# It costs about 90KB — 2.5k lines against 1.2k — which is unremarkable for a
# CRD and nowhere near the limit on an object in etcd.
"${TOOLS}/controller-gen" \
  crd \
  paths="${REPO_ROOT}/pkg/apis/crisp/v1alpha1/..." \
  output:crd:dir="${TOOLS}"

# Named for the order manifests are applied in rather than for the group.
cat "${REPO_ROOT}/hack/boilerplate/boilerplate.yaml.txt" \
    "${TOOLS}/crisp.kubecrisp.io_customresourceprojections.yaml" \
    > "${REPO_ROOT}/manifests/10-crd-customresourceprojection.yaml"

# The chart carries its own copy, because Helm installs whatever is in crds/
# before the templates and will not read it from anywhere else. Written here
# rather than kept in step by hand, so the two cannot drift.
cp "${REPO_ROOT}/manifests/10-crd-customresourceprojection.yaml" \
   "${REPO_ROOT}/charts/kube-crisp/crds/customresourceprojection.yaml"

# The static PrometheusRule is the chart's alert file wrapped in the CRD the
# Prometheus Operator expects. Generated rather than kept in step by hand, so a
# rule added to one cannot go missing from the other.
echo "==> alerts"
{
  cat "${REPO_ROOT}/hack/boilerplate/prometheusrule.yaml.txt"
  sed 's/__SLOW_QUERY_SECONDS__/5/' "${REPO_ROOT}/charts/kube-crisp/files/alerts.yaml" \
    | grep -v '^#'
} > "${REPO_ROOT}/manifests/optional/prometheusrule.yaml"

echo "==> done"
