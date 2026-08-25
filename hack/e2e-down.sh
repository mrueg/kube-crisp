#!/usr/bin/env bash
set -euo pipefail

CLUSTER="${CLUSTER:-kube-crisp-e2e}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kind delete cluster --name "${CLUSTER}"
rm -f "${REPO_ROOT}/hack/.e2e-kubeconfig"
