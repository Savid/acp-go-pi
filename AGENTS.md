# AGENTS.md

Shared instructions for automated coding agents working in this repository.

## Purpose

This project is a Go implementation of an ACP agent for the pi coding agent.
It wraps the local `pi` CLI in RPC mode and builds directly on
`github.com/coder/acp-go-sdk`.

## Project Map

Organized by domain. Public surface lives in the root package; implementation
details live in `internal/`.

- **Entrypoint** (`cmd/acp-go-pi`): process entrypoint, ACP stdio mode,
  tracing setup, and signal handling.
- **ACP agent surface** (root package, e.g. `agent.go`, `options.go`,
  `ids.go`, `request_builders.go`): ACP method handlers, agent options,
  request builders, and extension method constants.
- **Session orchestration** (`session.go`, `session_meta.go`,
  `session_store.go`, `raw_events.go`): pi turn lifecycle, prompts,
  cancellation, permissions, elicitation, usage updates, session storage, and
  raw event handling.
- **pi CLI internals** (`internal/pi`): RPC-mode process management, JSONL
  codec and event decoding, per-session agent directory authoring, embedded
  wrapper-owned TypeScript extensions (permission bridge, MCP client),
  version probing, and model/thinking-level validation.
- **Live tests** (`integration`): integration tests that launch the real
  local `pi` CLI in isolated agent directories.
- **Docs** (`docs/`, `docs.json`): Mintlify guide. Update alongside public
  API, CLI flag, ACP method, or `_meta` field changes.

## Commands

```sh
go build ./...
go test ./...
go test -race ./...
```

Lint details live in `.golangci.yml`.

The Makefile wraps the main development checks:

```sh
make test
make lint
make audit
```

Run live integration tests only when a local `pi` CLI (v0.80.6 or newer) is
installed:

```sh
ACP_GO_PI_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=300s -parallel=4 -v ./integration/...
```

`ACP_GO_PI_RUN_INTEGRATION=1` gates the integration tier;
`ACP_GO_PI_RUN_LIVE_TOKENS=1` additionally opts in to tests that spend model
tokens (`make test-integration-smoke` omits it). Use
`make test-integration-cover` for compiled
`acp-go-pi` coverage through `GOCOVERDIR`. Integration tests always launch pi
with an isolated temp `PI_CODING_AGENT_DIR` and a scrubbed environment;
provider credentials for the live tier are injected into that isolated
directory, never read from a shared mutable pi home.

## Coding Rules

- Follow standard Go idioms: `ctx` first, no `ctx` in structs, and `%w` for
  wrapped errors.
- Keep the public root package small; implementation details belong in
  `internal/` unless they are part of the public API.
- Prefer structured protocol types and JSON decoding over ad hoc string
  parsing; pi RPC records are strict LF-delimited JSONL.
- Preserve ACP method names, request/response shapes, and validation
  behavior.
- Keep protocol glue narrow, documented, and close to the ACP method it
  serves.
- Keep shared code next to the domain it serves; avoid generic catch-all
  packages such as `utils`, `helpers`, or `common`.
- The wrapper-owned TypeScript extensions under `internal/pi/ext/` are part
  of the native boundary: they must stay dependency-free (node built-ins,
  `typebox`, and the pi extension API) and any protocol change there needs a
  matching change in the Go code that parses its output.
- Follow existing package patterns before introducing new abstractions.

## Ask Before

Unless explicitly requested, ask before:

- Changing the permission or elicitation flow shape.
- Adding new ACP extension methods or `_meta` fields.
- Changing the session-store contract or store format.
- Weakening the child-environment scrubbing (ambient provider API keys are
  live auth for pi).

## Testing Rules

- Use `testify/require` for assertions.
- Prefer table-driven tests for codec/protocol cases.
- Run `go test ./...` for ordinary changes.
- Run `go test -race ./...` or `make test` for session, MCP, concurrency, or
  cancellation changes.
- Run `make lint` before considering work complete.
- Unit tests fake the process boundary (in-memory pipes, scripted
  responses); they never launch a real `pi`.
- Integration tests launch the actual `pi` binary from `PATH` (or
  `-path`-style overrides) in isolated temp agent directories only.
- Keep live prompts deterministic with exact sentinel replies, and assert
  the ACP stop reason plus streamed updates where practical.
- Local helper processes in integration tests are MCP servers with
  deterministic responses.

## Security And Boundaries

- **IMPORTANT**: Do not silently bypass permission prompts. The permission
  bridge extension is the permission system for pi sessions; it is
  load-bearing for user trust in this agent.
- **IMPORTANT**: Launch pi children only with the scrubbed environment. pi
  honors ambient provider API keys as live auth, so environment leaks are
  credential leaks.
- Never read from or write to the operator's real `~/.pi`; every session
  gets an isolated agent directory.
- Do not log auth material, user secrets, prompts, tool input, tool output,
  or raw pi event bodies by default.
- Credential files such as `auth.json` are injected at session start and
  excluded from the session store. Explicit per-session `env`, including any
  provider keys it carries, is recorded in lifecycle boundary rows, so every
  session-store implementation must protect those rows as secret material.
- Reject unsupported ACP extension/provider mutation methods with explicit
  protocol errors unless this agent implements a namespaced extension.
- Avoid broad filesystem or network behavior in tests unless the test is
  explicitly about that boundary.
