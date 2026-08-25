#!/usr/bin/env bash
# Builds the kube-crisp image and prints its name.
#
# Split out of e2e-up.sh so the build can happen somewhere other than the
# machine that runs the tests. CI runs five e2e shards, each needing a cluster
# of its own — kind and three databases cannot be shared across runners — and
# without this each of them rebuilt the same image from the same commit, which
# was most of the setup cost of every one of them.
#
# The image name is the only thing on stdout. Everything else goes to stderr, so
# a caller can take the name from a command substitution.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Diagnostics to stderr for the duration, so the progress goreleaser and the
# echoes below produce cannot end up in the captured name.
exec 3>&1 1>&2
# One image build path for every environment: goreleaser drives ko, so there is
# no Dockerfile to keep in sync with the release build.
#
# Signing and SBOMs are release-time concerns that need cosign and syft and, for
# keyless signing, an interactive Sigstore flow. goreleaser runs those pipes for
# snapshots too unless told not to.
echo "==> building the image with goreleaser${E2E_RACE:+ (race detector on)}"
git -C "${REPO_ROOT}" rev-parse HEAD >/dev/null 2>&1 \
  || { echo "!! goreleaser needs at least one commit to derive a version; commit first" >&2; exit 1; }

# The release config builds four platform targets. A kind cluster runs exactly
# one of them, and compiling the other three costs minutes and gigabytes of
# build cache, so the config is narrowed to the host platform here.
#
# It is derived from .goreleaser.yaml rather than duplicated: there is still one
# source of truth for how the image is built.
E2E_CONFIG="$(mktemp --suffix=.yaml)"
trap 'rm -f "${E2E_CONFIG}"' EXIT

HOST_ARCH="$(go env GOARCH)"

# E2E_RACE=1 builds the server with the race detector, which is the only way a
# data race in the router swap or the watch cache shows up under real traffic.
# It needs cgo, so the image also moves to a base with a libc.
RACE="${E2E_RACE:-0}"

python3 - "${E2E_CONFIG}" "${HOST_ARCH}" "${RACE}" <<'PYEOF'
import sys
config_path, arch, race = sys.argv[1], sys.argv[2], sys.argv[3] == "1"

with open(".goreleaser.yaml") as f:
    lines = f.read().splitlines()

# Rewrite the goos/goarch and ko platform lists in place, keeping every other
# setting exactly as the release uses it.
out, skipping = [], None
for line in lines:
    stripped = line.strip()

    if race and stripped == "- CGO_ENABLED=0":
        out.append(line.replace("CGO_ENABLED=0", "CGO_ENABLED=1"))
        continue
    if race and stripped == "- -trimpath":
        out.append(line)
        out.append(line.replace("-trimpath", "-race"))
        continue
    if race and stripped.startswith("base_image:"):
        indent = " " * (len(line) - len(line.lstrip()))
        out.append(f"{indent}base_image: gcr.io/distroless/base-debian12:nonroot")
        continue

    if stripped in ("goos:", "goarch:", "platforms:"):
        indent = " " * (len(line) - len(line.lstrip()))
        out.append(line)
        if stripped == "goos:":
            out.append(f"{indent}  - linux")
        elif stripped == "goarch:":
            out.append(f"{indent}  - {arch}")
        else:
            out.append(f"{indent}  - linux/{arch}")
        skipping = indent
        continue
    if skipping is not None:
        if stripped.startswith("- "):
            continue
        skipping = None
    out.append(line)

with open(config_path, "w") as f:
    f.write("\n".join(out) + "\n")
PYEOF

"${GORELEASER:-goreleaser}" release --snapshot --clean --config "${E2E_CONFIG}" \
  --skip=publish,announce,sbom,sign,archive >/dev/null

# goreleaser records what ko built in artifacts.json. A local snapshot produces
# a "Docker Manifest" naming the image by digest, which is exactly what should
# be deployed: the tag is stable across builds, the digest is not.
IMAGE="$(python3 - <<'PYEOF'
import json
with open("dist/artifacts.json") as f:
    artifacts = json.load(f)
wanted = ("Docker Manifest", "Docker Image", "Published Docker Image")
images = [a["name"] for a in artifacts if a.get("type") in wanted]
print(images[0] if images else "")
PYEOF
)"
[[ -n "${IMAGE}" ]] || { echo "!! goreleaser did not record an image in dist/artifacts.json" >&2; exit 1; }

# goreleaser reports the fully qualified name; the daemon, kind, and the kubelet
# all know the same image by its short form.
IMAGE="${IMAGE#index.docker.io/library/}"
echo "==> built ${IMAGE}"
# The name, on the caller's stdout.
echo "${IMAGE}" >&3
