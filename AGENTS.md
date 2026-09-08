# AGENTS.md

## Purpose

This Go ACP agent wraps the local `pi` CLI in RPC mode and builds on
`github.com/coder/acp-go-sdk`. Each ACP session owns a pi RPC process;
ordinary execution supports an explicitly configured durable native `Home`.

## Project Map

- `cmd/acp-go-pi`: ACP stdio entrypoint, tracing, signals, and CLI flags.
- Root `agent*.go`, `options.go`, `ids.go`, `request_builders.go`: public ACP
  surface, validation, configuration, and extension dispatch.
- Root `session*.go`, `raw_events.go`, `image*.go`: turn ownership, cancellation,
  permissions, elicitation, lifecycle updates, persistence, and media.
- Root `auth*.go`, `host_authority*.go`, `native_process.go`: provider-auth
  brokerage and the embedded host-authority boundary.
- `internal/pi`: native process management, JSONL codec, agent directories,
  version/model validation, and embedded TypeScript extensions in `ext/`.
- `internal/lifecycle`, `testdata/lifecycle`: lifecycle reducer and canonical
  fixture battery.
- `integration`: gated fake-backed wrapper tests and real-native compatibility
  tests. `examples/` contains runnable ACP clients.
- `docs/`, `docs.json`, `README.md`: local public guide and navigation. Update
  them alongside changes to the supported API, CLI, ACP methods, or `_meta`.

## Commands

```sh
go build ./...
go test ./...
make test
make coverage-check
make lint
make docs-audit
make fmt-check
make modernize-check
make audit
```

`make test` uses race detection, shuffled order, and the configured timeout.
`make lint` uses the Makefile's pinned linter. `make modernize-check` checks
`go fix -diff ./...` without writing. `make audit` is the full local gate;
choose checks for the task and run the combined gate once changes settle.
Prose-only edits need instruction/source review and relevant docs checks.

Native execution requires explicit task authorization, including smoke and
credential-free probes; installed binaries and environment gates alone do not
supply it. Use `make test-integration-smoke` for the tier without model spend,
`make test-integration-live` for token-spending prompts, and
`make test-integration-cover` for compiled command coverage. Attended,
keystore, and native-browser targets have separate execution prerequisites;
see [Development](README.md#development). Honor authorization already given.

## Coding Rules

- Follow Go idioms: `ctx` first, no stored contexts, and `%w` for wrapped errors.
  Preserve public shapes, validation order, error identities, and state ownership.
- Keep native implementation in `internal/pi`, protocol glue near its ACP
  handler, and shared code beside its domain. Follow existing patterns; avoid
  generic helper packages or unnecessary abstractions.
- Use structured protocol types and JSON decoding. Native RPC records are
  strictly LF-delimited JSONL; keep stdout reserved for ACP.
- Keep the Go parser and embedded TypeScript protocol changes synchronized.
  Extensions use Node built-ins and packages supplied by pi, including
  `typebox`, the pi extension API, and `@earendil-works/pi-ai/providers/all`;
  do not add external runtime dependencies.
- Preserve native cleanup and owed durable commits through cancellation and
  replacement. Advertise vacancy/quiescence only when the configured authority
  proves it; follow [sessions](docs/core/sessions.mdx) and
  [session storage](docs/features/session-store.mdx).
- Unless already authorized by the task, ask before changing permission or
  elicitation flow shape, adding ACP extension methods or `_meta` fields,
  changing the store contract/format, or weakening environment scrubbing.

## Testing Rules

- Use `testify/require` and focused regression tests for observable behavior or
  concrete failure boundaries. Prefer small tables for protocol cases; do not
  add production seams, unreachable branches, or coverage-only scaffolding.
- Synchronize concurrent tests with explicit barriers or observable state;
  deadlines bound failure and sleeps do not prove ordering. Parallel tests own
  their state and avoid process-global environment or directory mutation.
- Ordinary tests use deterministic fake process/authority boundaries and never
  require an installed pi, credentials, containers, or external network.
  Fake-backed integration tests prove wrapper and transport behavior; only
  tests executing the real pi establish native compatibility.
- Run `go test ./...` for ordinary behavior changes and `make test` for session,
  MCP, concurrency, or cancellation changes. Use `make lint` for Go edits.
  Reuse passing checks unless changes or failures justify repeating them.
- Run the complete behavioral suite with race detection and review the statement
  coverage reported by `make coverage-check`; it has no percentage threshold.
  Preserve `testdata/lifecycle/manifest.json` and every canonical fixture it names
  byte-for-byte, and keep the complete battery exercised.
  Coverage and fake authority traces do not prove physical containment or
  unexecuted platform/native behavior.
- For an authorized native probe, use a throwaway `PI_CODING_AGENT_DIR` with a
  scrubbed environment. Native tests use only temporary test-owned homes;
  select credential sources explicitly and copy into the isolated residence.
  Keep live prompts deterministic with exact sentinels, stop reasons, and
  streamed-update assertions. Inspect the selected target's gates first.

## Security And Boundaries

- The permission bridge is the session permission system. Never silently
  bypass its prompts or fail-open on denied/cancelled dialogs.
- Launch every pi child with the scrubbed environment. Ambient provider keys
  are live authentication; preserve the allowlist and deliberate overlays.
- Never discover the operator's `~/.pi` implicitly. Without `WithHome`, agent
  directories are ephemeral. Ordinary execution supports a host-selected,
  protected durable `Home`, shared under pi's native cross-process credential
  lock; managed execution rejects it.
- With a supplied `HostAuthority`, every native launch uses that authority and
  never falls back to ordinary execution. Fully materialize before prepare;
  prepared trees remain opaque until successful reclaim after terminal `Wait`.
  Failed preparation remains host-owned. Pin managed handoff reads to a directory
  disjoint from the complete scratch allocation domain before native work;
  preserve the host filesystem assumptions in [security](docs/operations/security.mdx).
- Keep the content-addressed extension source cache separate from secret-bearing
  session configuration. Preserve source-byte, file-type, and POSIX ownership/
  write-mode checks; Windows scratch protection is host-owned, without adapter
  ACL inspection. See [cache rules](docs/operations/security.mdx#extension-source-cache).
- Do not log auth material, secrets, prompts, tool input/output, or raw native
  event bodies by default. `auth.json` is excluded from session storage, but
  explicit per-session `env` is stored in lifecycle rows and may contain provider
  keys; every store must protect those rows as secret material.
- Reject unsupported ACP extension/provider mutation methods with explicit
  protocol errors. Avoid broad test filesystem or network access unrelated to
  the boundary being tested.
