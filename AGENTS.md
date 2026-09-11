# AGENTS.md

## Purpose

This Go module exposes the local `pi` CLI as an Agent Client Protocol agent.
Each ACP session drives one `pi --mode rpc` process that inherits the
adapter's environment and keeps its session in pi's own home, so a session
started over ACP can be continued natively with `pi --resume` afterwards.

## Project Map

- `cmd/acp-go-pi`: stdio entrypoint, OpenTelemetry setup, signals, flags.
- Root `agent*.go`, `options.go`, `request_builders.go`: the public ACP
  surface, option validation, and the transport wrapper that orders session
  publication behind the establishing response.
- Root `session*.go`, `image_output.go`: one session's process, event pump,
  prompt turns, permissions and elicitation, lifecycle stream, store mirror,
  replay, config options, and image output.
- `internal/pi`: pi's JSONL RPC codec and client, launch arguments, the
  embedded extensions in `ext/`, seed files, and pi's session-file layout.
- `internal/observer`: OpenTelemetry spans and metrics.
- `integration`: gated tests against the installed pi.
- `examples`: runnable ACP clients.

## Commands

```sh
make build
make test
make lint
make audit
make test-integration-smoke
make test-integration-live
```

`make test` runs with race detection and shuffled order. `make audit` is the
full local gate. Integration targets need an installed `pi`; the live target
spends model tokens and requires explicit operator intent.

## Coding Rules

- Follow Go idioms: `ctx` first, `%w` for wrapped errors, small interfaces at
  the consumer. Keep native protocol details in `internal/pi` and ACP glue
  beside its handler.
- Shared family behavior comes from `github.com/savid/acp-go-core`; never copy
  it here.
- The adapter does no isolation: pi inherits the process environment, the
  agent overlay, then the session env, and only the adapter's own
  `ACP_GO_PI_INTERNAL_*` markers are set for the child.
- Native state is never deleted. The session store is the durability
  boundary; pi's own session file is the native copy.
- Unit tests never require an installed pi: the test binary doubles as a
  scripted fake pi. Keep the fake's protocol in step with `internal/pi`.
- A comment states what the code does or why a constraint exists.

## Verification

Run `go test ./...` for ordinary changes and `make lint` for Go edits. Run
`make audit` once changes settle. Run the integration smoke target after
changing anything pi-facing.

## Boundaries

- The permission bridge is the session permission system. Never bypass its
  dialog or fail open on a denied or cancelled answer.
- Do not log prompts, tool input or output, or raw native event bodies by
  default.
- Reject every ACP extension method; the only extension surface is the
  outbound raw-event notification.
