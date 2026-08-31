.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: build lint fmt-check fmt test coverage-check test-cross-compile test-integration-smoke test-integration-live test-integration-cover test-integration-attended test-integration-keystore test-integration-native-browser docs-audit clean tidy vuln modernize-check audit test/cover help

## build: compile all packages
build:
	go build ./...

## lint: run pinned golangci-lint
lint:
	$(GOLANGCI_LINT) run ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

## fmt: format Go files
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## coverage-check: require 100% statement coverage with race instrumentation
coverage-check:
	go test -race -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 && $$NF == 0 { print "uncovered statement block: " $$0; missed = 1 } END { if (missed) exit 1 }' coverage.out
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-cross-compile: compile platform-specific test branches
test-cross-compile:
	rm -rf .tmp/cross
	mkdir -p .tmp/cross
	GOOS=linux GOARCH=amd64 go test -c -o .tmp/cross/pi-linux.test ./internal/pi
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/pi-darwin.test ./internal/pi
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/pi-cmd-darwin.test ./cmd/acp-go-pi
	GOOS=darwin GOARCH=arm64 go build ./...
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/pi-windows.test ./internal/pi
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/pi-cmd-windows.test ./cmd/acp-go-pi
	GOOS=freebsd GOARCH=amd64 go build ./...
	GOOS=openbsd GOARCH=amd64 go build ./...
	GOOS=windows GOARCH=amd64 go build ./...

## test-integration-smoke: run live integration tests that do not spend model tokens
test-integration-smoke:
	ACP_GO_PI_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=300s -parallel=4 -v ./integration/...

## test-integration-live: run full live integration tests
test-integration-live:
	ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=900s -parallel=4 -v ./integration/...

## test-integration-attended: run provider-auth flows a human must approve in real time
# `go test` hands the test binary a closed stdin, so the human's answer can
# never reach a tier run that way. The tier therefore compiles the binary and
# runs it directly, leaving stdin attached to whatever the operator supplied —
# a terminal, or a pipe carrying the pasted code. Output is teed rather than
# redirected so the relayed authorization URL still reaches the watching
# operator live, and the guard requires a top-level pass line, which no empty
# selection and no skip can produce.
test-integration-attended:
	rm -rf .tmp/attended
	mkdir -p .tmp/attended
	go test -race -c -tags=integration -o .tmp/attended/integration.test ./integration
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ( cd integration && ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_ATTENDED=1 \
	    ../.tmp/attended/integration.test -test.v -test.count=1 -test.timeout=1200s \
	    -test.run '^TestAttendedProviderAuth' ) 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestAttendedProviderAuth' "$$log" || true); \
	skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestAttendedProviderAuth(/| )' "$$log" || true); \
	empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -gt 0 ] || { echo 'no attended provider-auth login ran'; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'attended provider-auth login skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'attended provider-auth selector ran no tests'; exit 1; }

## test-integration-keystore: run credential-residence tests against the container fixture
test-integration-keystore:
	ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=600s -v -run '^TestKeystore' ./...

## test-integration-native-browser: require one Linux ordinary provider-auth browser-boundary proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_PI_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=1800s -v -run '^TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

## test-integration-cover: run live integration tests with compiled binary coverage
test-integration-cover:
	rm -rf .tmp/integration-cover coverage-integration.out
	mkdir -p .tmp/integration-cover/data
	go build -cover -coverpkg=./... -o .tmp/integration-cover/acp-go-pi ./cmd/acp-go-pi
	ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_AGENT_BINARY=$$(pwd)/.tmp/integration-cover/acp-go-pi GOCOVERDIR=$$(pwd)/.tmp/integration-cover/data go test -race -tags=integration -timeout=600s -parallel=4 -v ./integration/...
	go tool covdata percent -i=.tmp/integration-cover/data
	go tool covdata textfmt -i=.tmp/integration-cover/data -o coverage-integration.out

## docs-audit: check public docs, examples, required files, CLI flags, and removed terms
docs-audit:
	@missing=0; for file in README.md doc.go docs.json example_test.go AGENTS.md docs/overview.mdx docs/core/sessions.mdx docs/core/prompt-streaming.mdx docs/features/authentication.mdx docs/features/elicitation.mdx docs/features/mcp.mdx docs/features/models-config.mdx docs/features/permissions.mdx docs/features/raw-events.mdx docs/features/session-store.mdx docs/get-started/examples.mdx docs/get-started/install.mdx docs/get-started/quickstart.mdx docs/get-started/run-modes.mdx docs/operations/observability.mdx docs/operations/security.mdx docs/operations/troubleshooting.mdx docs/reference/acp-methods.mdx docs/reference/cli.mdx docs/reference/go-api.mdx docs/reference/meta.mdx docs/reference/updates.mdx examples/minimal-client/main.go examples/resume-from-file/main.go examples/interactive-chat/main.go; do if [ ! -f "$$file" ]; then echo "missing required docs file: $$file"; missing=1; fi; done; exit $$missing
	@for flag in -path -home -scratch-dir -provider-auth-root -model -seed-file -debug -version; do rg -q -- "$$flag" docs/reference/cli.mdx || { echo "missing CLI flag in docs/reference/cli.mdx: $$flag"; exit 1; }; done
	@for flag in path home scratch-dir provider-auth-root model seed-file debug version; do rg -q -- "\"$$flag\"" cmd/acp-go-pi/*.go || { echo "missing CLI flag registration in command code: $$flag"; exit 1; }; done
	@for test in TestHostAuthorityManagedLaunchTrace TestHostAuthorityPreparedTreeExclusivity TestHostAuthorityReclaimPrecedesRemoval TestHostAuthorityNoOrdinaryFallback; do rg -q "func $$test" host_authority_test.go || { echo "missing host authority test: $$test"; exit 1; }; done

## clean: remove build artifacts
clean:
	rm -rf .tmp coverage.out coverage-integration.out coverage-summary.txt

## tidy: verify module files are tidy
tidy:
	go mod tidy -diff

## vuln: run govulncheck from the go.mod tool directive
# golang.org/x/vuln v1.4.0 panics in x/tools SSA on Go 1.26 generics;
# keep the tool directive pinned at v1.5.0 or newer.
vuln:
	go tool govulncheck ./...

## modernize-check: preview Go modernizations without changing files
modernize-check:
	go fix -n ./...

## audit: run repository checks
audit: fmt-check lint build test coverage-check test-cross-compile tidy vuln modernize-check docs-audit
	go mod verify

## test/cover: open HTML coverage report
test/cover: coverage-check
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
