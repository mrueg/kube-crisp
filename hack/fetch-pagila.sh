#!/usr/bin/env bash
# Fetches the Pagila sample database into third_party/pagila/.
#
# Not vendored: it is somebody else's data, 2.9 MB of it, in a repository whose
# largest file is otherwise a logo. Upstream keeps the authoritative copy, so
# this takes it from there and checks that what arrived is what was pinned.
#
# Idempotent: files already present with the right checksum are left alone, so
# hack/e2e-up.sh can call this on every run.
set -euo pipefail

# Pinned. Bumping means new checksums, which is the point: the contents cannot
# change under this repository without the change being made here first.
COMMIT="fc7a86771a7ff213597139942f1f57c36125d37d"
BASE="https://raw.githubusercontent.com/xzilla/pagila/${COMMIT}"

# sha256, filename.
FILES=(
  "598f812e81bfbd2323a3943481344ce55ab9f3fb4995a612dfc09d555d32f7e1  pagila-schema.sql"
  "dc8b9998c2fd2ad2a52fde80f72805e2142db232c5b3853016d84d9efb03ba0f  pagila-data.sql"
)

DEST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/third_party/pagila"
mkdir -p "${DEST}"

for entry in "${FILES[@]}"; do
  sum="${entry%% *}"
  name="${entry##* }"
  path="${DEST}/${name}"

  if [[ -f "${path}" ]] && echo "${sum}  ${path}" | sha256sum --check --status; then
    continue
  fi

  echo "==> fetching ${name}"
  # To a temporary file first: a half-written dump that happens to load is a
  # worse failure than no dump at all.
  tmp="${path}.tmp"
  curl -sfL --retry 3 -o "${tmp}" "${BASE}/${name}"

  if ! echo "${sum}  ${tmp}" | sha256sum --check --status; then
    rm -f "${tmp}"
    echo "!! ${name} does not match the pinned checksum" >&2
    echo "   expected ${sum}" >&2
    echo "   If upstream changed on purpose, update COMMIT and the checksums here." >&2
    exit 1
  fi
  mv "${tmp}" "${path}"
done
