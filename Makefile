SHELL := /usr/bin/env bash

BINARY  ?= kube-crisp-apiserver
BIN_DIR ?= bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

LDFLAGS := -X github.com/mrueg/kube-crisp/pkg/version.Version=$(VERSION)

.PHONY: all
all: verify build

.PHONY: build
build:
	go build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) ./cmd/kube-crisp-apiserver

.PHONY: test
test:
	go test ./...

# Every build goes through goreleaser, which drives ko for the image side.
# There is no Dockerfile and no separate ko config.
GORELEASER ?= goreleaser
GOVULNCHECK_VERSION ?= latest

# Signing and SBOMs are release-time concerns: they need cosign and syft, and
# keyless signing blocks on an interactive Sigstore flow outside CI. goreleaser
# runs both pipes for snapshots too, so local builds skip them explicitly.
LOCAL_SKIP := publish,announce,sbom,sign

# A local image, loaded into the docker daemon as goreleaser.ko.local:<version>.
.PHONY: image
image:
	$(GORELEASER) release --snapshot --clean --skip=$(LOCAL_SKIP),archive

# The full snapshot: binaries, archives, checksums, and the image.
.PHONY: snapshot
snapshot:
	$(GORELEASER) release --snapshot --clean --skip=$(LOCAL_SKIP)

# The e2e suite needs a provisioned environment: a kind cluster running
# kube-crisp against PostgreSQL, seeded with ROWS orders.
.PHONY: e2e
e2e: e2e-up e2e-test

.PHONY: e2e-up
e2e-up:
	./hack/e2e-up.sh

# E2E_RUN and E2E_SKIP select part of the suite, which is what lets CI shard it
# across jobs. Empty means everything.
E2E_RUN  ?=
E2E_SKIP ?=
E2E_ARGS  = $(if $(E2E_RUN),-run '$(E2E_RUN)') $(if $(E2E_SKIP),-skip '$(E2E_SKIP)')

.PHONY: e2e-test
e2e-test:
	KUBECONFIG=$(CURDIR)/hack/.e2e-kubeconfig go test -race -tags e2e -v -count=1 -timeout 45m \
		$(E2E_ARGS) ./test/e2e/...

# The correctness half, which is two or three minutes of the suite's twenty-odd.
# Worth having on its own: it is the part that says whether the code works, and
# it can answer before a benchmark has finished seeding.
#
# Most of those minutes are one test. Reloading --projection-dir waits on the
# kubelet to project an updated ConfigMap, which it does on its own schedule.
.PHONY: e2e-correctness
e2e-correctness:
	$(MAKE) e2e-test E2E_SKIP='Comparison|StorageAccounting'

# The benchmark shards. Split by measured time rather than by subject, so the
# jobs finish together: the read comparison alone was 56% of the suite.
#
# Every shard is named here rather than derived, so a benchmark added without
# being placed in one is a benchmark that stops running — which is louder than a
# regex that quietly matches nothing.
BENCH_SHARD_reads   = TestPerformanceComparison
BENCH_SHARD_drivers = TestDriverComparison|TestScalingComparison
BENCH_SHARD_queries = TestSelectorPushdownComparison|TestPagedWalkComparison|TestPagingDepthComparison|TestThroughputComparison
BENCH_SHARD_cluster = TestUnrelatedTrafficComparison|TestStorageAccounting|TestWritePerformanceComparison|TestWatchLatencyComparison|TestAuthorizationCostComparison

BENCH_SHARDS := reads drivers queries cluster

# make e2e-bench SHARD=reads
.PHONY: e2e-bench
e2e-bench:
	@[ -n "$(SHARD)" ] || { echo "set SHARD to one of: $(BENCH_SHARDS)"; exit 1; }
	@[ -n "$(BENCH_SHARD_$(SHARD))" ] || { echo "unknown shard '$(SHARD)'; one of: $(BENCH_SHARDS)"; exit 1; }
	$(MAKE) e2e-test E2E_RUN='$(BENCH_SHARD_$(SHARD))'

# Fails if a Comparison test is in no shard, which would silently stop running it.
.PHONY: e2e-bench-check
e2e-bench-check:
	@declared=$$(for s in $(BENCH_SHARDS); do \
	    $(MAKE) --no-print-directory -s e2e-print-shard SHARD=$$s; done | tr '|' '\n' | sort -u); \
	  actual=$$(grep -hoE '^func (Test\w*Comparison|TestStorageAccounting)' test/e2e/*_test.go \
	    | sed 's/^func //' | sort -u); \
	  missing=$$(comm -13 <(echo "$$declared") <(echo "$$actual")); \
	  if [ -n "$$missing" ]; then \
	    echo "these benchmarks are in no shard, so CI would not run them:"; echo "$$missing"; exit 1; \
	  fi; \
	  echo "==> every benchmark is in a shard"

.PHONY: e2e-print-shard
e2e-print-shard:
	@echo '$(BENCH_SHARD_$(SHARD))'

# The same suite against a server built with the race detector, which is where
# a race in the router swap or the watch cache would actually show up.
#
# The performance and throughput comparisons are skipped: timings taken under
# the race detector measure the race detector, and holding 10k objects — or
# saturating the server from 16 clients — with race instrumentation exhausts the
# memory of a kind node.
.PHONY: e2e-race
e2e-race:
	E2E_RACE=1 ./hack/e2e-up.sh
	KUBECONFIG=$(CURDIR)/hack/.e2e-kubeconfig go test -race -tags e2e -v -count=1 -timeout 45m \
		-skip 'PerformanceComparison|ThroughputComparison' ./test/e2e/...

# Every comparison — read and write latency, throughput under concurrency,
# watch propagation, paged walks, and the three drivers side by side. Skips the
# correctness suite.
.PHONY: bench
bench:
	KUBECONFIG=$(CURDIR)/hack/.e2e-kubeconfig go test -tags e2e -v -count=1 -timeout 45m \
		-run 'Comparison' ./test/e2e/...

.PHONY: e2e-down
e2e-down:
	./hack/e2e-down.sh

# Deepcopy functions, the typed client, and the CRD are generated from the API
# types.
.PHONY: codegen
codegen:
	./hack/update-codegen.sh

# Fails if the committed generated code or CRD is not what the API types
# produce, without leaving the regenerated output behind.
#
# The CRD is the half worth guarding: it is applied to a cluster, so a field
# added to the types and not to it is a projection the API server silently
# prunes rather than an error anybody sees.
GENERATED := pkg/apis pkg/generated manifests/10-crd-customresourceprojection.yaml \
             charts/kube-crisp/crds/customresourceprojection.yaml \
             manifests/optional/prometheusrule.yaml

.PHONY: codegen-check
codegen-check:
	@saved=$$(mktemp -d); \
	for path in $(GENERATED); do \
		mkdir -p "$$saved/$$(dirname $$path)"; cp -r "$$path" "$$saved/$$path"; \
	done; \
	./hack/update-codegen.sh >/dev/null; \
	status=0; \
	for path in $(GENERATED); do \
		if ! diff -qr "$$saved/$$path" "$$path" >/dev/null 2>&1; then \
			echo "generated output is stale: $$path"; status=1; \
		fi; \
	done; \
	for path in $(GENERATED); do rm -rf "$$path"; cp -r "$$saved/$$path" "$$path"; done; \
	rm -rf "$$saved"; \
	if [ $$status -ne 0 ]; then \
		echo "run 'make codegen' and commit the result"; \
	fi; \
	exit $$status

.PHONY: fmt
fmt:
	go fmt ./...

# CI cannot rewrite the tree, so it checks instead of formatting.
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l . ); \
	if [ -n "$$unformatted" ]; then \
		echo "these files need gofmt:"; echo "$$unformatted"; exit 1; \
	fi

# Fails if go.mod or go.sum would change, without leaving the change behind.
.PHONY: tidy-check
tidy-check:
	@cp go.mod go.mod.orig && cp go.sum go.sum.orig
	@go mod tidy
	@status=0; \
	if ! cmp -s go.mod go.mod.orig || ! cmp -s go.sum go.sum.orig; then \
		echo "go mod tidy produced changes; run 'make tidy' and commit the result"; status=1; \
	fi; \
	mv go.mod.orig go.mod; mv go.sum.orig go.sum; exit $$status

# -race is not decoration here: the watch cache is refreshed by a poll loop
# while watchers read from it, and the served API surface is swapped under live
# requests.
.PHONY: cover
cover:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint:
	golangci-lint run

# The chart renders a Deployment whose flags have to be flags the binary
# actually has, so this checks the rendered output as well as the templates.
HELM ?= helm

# The alert rules are the one thing here whose errors surface only when
# Prometheus loads them, which is long after anybody would connect the two.
PROMTOOL ?= promtool

.PHONY: alerts-check
alerts-check:
	@command -v $(PROMTOOL) >/dev/null || { echo "==> promtool not on PATH; skipping"; exit 0; }
	@set -e; \
	for rendered in \
	  "manifests/optional/prometheusrule.yaml" \
	  "$$($(HELM) template kube-crisp charts/kube-crisp --namespace kube-crisp \
	        --set prometheusRule.enabled=true --show-only templates/prometheusrule.yaml \
	        > .alerts-rendered.yaml && echo .alerts-rendered.yaml)"; do \
	  sed -n '/^spec:/,$$p' "$$rendered" | sed '1d;s/^  //' > .alerts-check.yaml; \
	  $(PROMTOOL) check rules .alerts-check.yaml; \
	done; \
	rm -f .alerts-check.yaml .alerts-rendered.yaml

.PHONY: helm-lint
helm-lint:
	$(HELM) lint charts/kube-crisp
	@for values in \
	  "" \
	  "--set serviceMonitor.enabled=true --set networkPolicy.enabled=true" \
	  "--set crisp.admission=true --set crisp.priorityAndFairness=true" \
	  "--set replicaCount=1 --set podDisruptionBudget.enabled=false --set crisp.leaderElection=false" \
	  "--set crisp.caBundle=placeholder" \
	  "--set crisp.tracing.enabled=true --set crisp.tracing.endpoint=otel:4317" \
	  "--set crisp.audit.enabled=true" \
	  "--set spreadReplicasAcrossNodes=false" \
	  "--set nameOverride=custom --set fullnameOverride=custom-crisp"; do \
	  $(HELM) template kube-crisp charts/kube-crisp --namespace kube-crisp $$values >/dev/null \
	    || { echo "==> chart failed to render with: $$values"; exit 1; }; \
	done
	@echo "==> chart renders with every combination checked"

# govulncheck is installed rather than "go run": go run exits 1 for any non-zero
# program exit, which would hide the 3 that distinguishes findings from errors.
#
# Fails only on vulnerabilities this code can actually reach. govulncheck exits
# 3 for any finding at all, including ones in required-but-uncalled modules,
# which would red-CI for something no change here can fix. A tool failure still
# fails the target.
.PHONY: vulncheck
vulncheck:
	@set +e; \
	bin=$$(mktemp -d); \
	GOBIN=$$bin go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) \
	  || { echo "==> could not install govulncheck"; rm -rf $$bin; exit 1; }; \
	output=$$($$bin/govulncheck ./... 2>&1); \
	code=$$?; \
	rm -rf $$bin; \
	echo "$$output"; \
	case $$code in \
	  0) exit 0 ;; \
	  3) if echo "$$output" | grep -q "Your code is affected by"; then \
	       echo "==> reachable vulnerabilities found"; exit 1; \
	     fi; \
	     echo "==> only unreachable vulnerabilities in required modules; not failing"; \
	     exit 0 ;; \
	  *) echo "==> govulncheck failed to run"; exit $$code ;; \
	esac

# Both tag sets: the e2e suite is behind a build tag and would otherwise never
# be vetted.
.PHONY: vet
vet:
	go vet ./...
	go vet -tags e2e ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: deploy
deploy:
	kubectl apply -f manifests/

.PHONY: verify
verify: fmt vet test

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) dist/
