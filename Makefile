.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: build lint fmt-check fmt test coverage-check test-cross-compile test-integration-smoke test-integration-live test-integration-cover test-integration-attended test-integration-keystore test-integration-native-browser docs-audit clean tidy vuln modernize-check audit test/cover help

## build: compile all packages
build:
	go build ./...

## lint: run pinned golangci-lint
lint:
	$(GOLANGCI_LINT) run --timeout=10m --allow-parallel-runners ./...

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

## coverage-check: run shuffled race tests and report statement coverage
coverage-check:
	go test -race -shuffle=on -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 { found = 1 } END { if (!found) { print "coverage profile has no statement blocks"; exit 1 } }' coverage.out
	@report=$$(go tool cover -func=coverage.out) || exit $$?; printf '%s\n' "$$report" | awk '/^total:/ { found = 1; if ($$3 !~ /^[0-9]+([.][0-9]+)?%$$/) { print "invalid total coverage line"; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-cross-compile: compile platform-specific test branches
test-cross-compile:
	rm -rf .tmp/cross
	mkdir -p .tmp/cross
	GOOS=linux GOARCH=amd64 go test -c -o .tmp/cross/pi-linux.test ./internal/pi
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/pi-darwin.test ./internal/pi
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/pi-cmd-darwin.test ./cmd/acp-go-pi
	GOOS=darwin GOARCH=arm64 go build ./...
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/pi-root-windows.test .
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/pi-windows.test ./internal/pi
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/pi-cmd-windows.test ./cmd/acp-go-pi
	GOOS=freebsd GOARCH=amd64 go build ./...
	GOOS=openbsd GOARCH=amd64 go build ./...
	GOOS=windows GOARCH=amd64 go build ./...

## test-integration-smoke: run live integration tests that do not spend model tokens
test-integration-smoke:
	ACP_GO_PI_RUN_LIVE_TOKENS=0 ACP_GO_PI_RUN_ATTENDED=0 ACP_GO_PI_RUN_KEYSTORE=0 ACP_GO_PI_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=300s -parallel=4 -v ./integration/...

## test-integration-live: run full live integration tests
test-integration-live:
	ACP_GO_PI_RUN_ATTENDED=0 ACP_GO_PI_RUN_KEYSTORE=0 ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=900s -parallel=4 -v ./integration/...

## test-integration-attended: run provider-auth flows a human must approve in real time
test-integration-attended:
	@set -eu; dir=$$(mktemp -d); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	dir=$$(cd "$$dir" && pwd); \
	export ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_ATTENDED=1 ACP_GO_PI_RUN_LIVE_TOKENS=0 ACP_GO_PI_RUN_KEYSTORE=0; \
	go test -race -c -tags=integration -o "$$dir/integration.test" ./integration; \
	"$$dir/integration.test" -test.list '^TestAttendedProviderAuth' >"$$dir/selected"; \
	expected=$$(grep -Ec '^TestAttendedProviderAuth' "$$dir/selected" || true); \
	[ "$$expected" -gt 0 ] || { echo 'attended selector discovered no tests'; exit 1; }; \
	{ status=0; (cd integration && "$$dir/integration.test" -test.v -test.count=1 -test.timeout=1200s -test.run '^TestAttendedProviderAuth') 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); passed=$$(grep -Ec '^--- PASS: TestAttendedProviderAuth' "$$dir/output" || true); \
	skipped=$$(grep -Ec '^[[:space:]]*--- SKIP:' "$$dir/output" || true); empty=$$(grep -c 'no tests to run' "$$dir/output" || true); \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "attended tests passed $$passed of $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'attended test skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'attended selector ran no tests'; exit 1; }

## test-integration-keystore: run credential-residence tests against the container fixture
test-integration-keystore:
	ACP_GO_PI_RUN_LIVE_TOKENS=0 ACP_GO_PI_RUN_ATTENDED=0 ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=600s -v -run '^TestKeystore' ./...

## test-integration-native-browser: require one Linux ordinary provider-auth browser-boundary proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_PI_RUN_LIVE_TOKENS=0 ACP_GO_PI_RUN_ATTENDED=0 ACP_GO_PI_RUN_KEYSTORE=0 ACP_GO_PI_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration,browsercanary -timeout=1800s -v -run '^TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserLinuxOrdinaryProviderAuthReachesNoUnshimmedLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

## test-integration-cover: run live integration tests with compiled binary coverage
test-integration-cover:
	@set -eu; mkdir -p .tmp; dir=$$(mktemp -d "$$(pwd)/.tmp/integration-cover.XXXXXX"); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	mkdir "$$dir/data"; \
	go build -cover -coverpkg=./... -o "$$dir/acp-go-pi" ./cmd/acp-go-pi; \
	{ status=0; ACP_GO_PI_RUN_LIVE_TOKENS=0 ACP_GO_PI_RUN_ATTENDED=0 ACP_GO_PI_RUN_KEYSTORE=0 ACP_GO_PI_RUN_INTEGRATION=1 ACP_GO_PI_AGENT_BINARY="$$dir/acp-go-pi" GOCOVERDIR="$$dir/data" go test -race -count=1 -tags=integration -timeout=600s -parallel=4 -v ./integration/... 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); [ "$$status" -eq 0 ] || exit "$$status"; \
	[ -n "$$(find "$$dir/data" -name 'covcounters.*' -type f -size +0c -print -quit)" ] || { echo 'compiled adapter produced no coverage counters'; exit 1; }; \
	go tool covdata percent -i="$$dir/data"; \
	go tool covdata textfmt -i="$$dir/data" -o coverage-integration.out

## docs-audit: check required public docs, examples, and CLI flag coverage
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

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -diff ./...

## audit: run repository checks
audit: fmt-check lint build coverage-check test-cross-compile tidy vuln modernize-check docs-audit
	go mod verify

## test/cover: open HTML coverage report
test/cover: coverage-check
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
