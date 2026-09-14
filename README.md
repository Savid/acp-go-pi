# acp-go-pi

`acp-go-pi` exposes the [pi](https://github.com/earendil-works/pi-mono) coding
agent as an [Agent Client Protocol](https://agentclientprotocol.com) agent.
It launches one `pi --mode rpc` process per ACP session, maps ACP requests
onto pi's JSONL RPC, and streams ACP session updates back to the client.

pi inherits the adapter's environment and keeps sessions in its own home. A
session started over ACP can be continued natively:

```sh
acp-go-pi              # host runs a session in /work
cd /work && pi --resume
```

## Install

```sh
go install github.com/savid/acp-go-pi/cmd/acp-go-pi@latest
```

Requires `pi` 0.80.6 or newer on `PATH` or named with `-path`.

## Run

```sh
acp-go-pi [-path pi] [-home DIR] [-scratch-dir DIR] [-model provider/id] [-seed-file rel=host]... [-debug]
```

| Flag | Meaning |
|---|---|
| `-path` | pi executable; a bare name is searched on `PATH` |
| `-home` | pi config root, passed as `PI_CODING_AGENT_DIR`; empty inherits pi's own resolution |
| `-scratch-dir` | parent for ephemeral adapter state; empty means the system temp directory |
| `-model` | default model for new sessions as `provider/id` |
| `-seed-file` | `<relpath>=<hostpath>` written into pi's config root before launch; repeatable |
| `-debug` | debug logs to stderr |
| `-version` | print the adapter version |

OpenTelemetry exporters are configured from the standard `OTEL_*` variables.

## Embed

```go
err := piacp.Serve(ctx, os.Stdin, os.Stdout,
    piacp.WithHome("/srv/pi"),
    piacp.WithSessionStore(store),
)
```

Options: `WithExecutablePath`, `WithHome`, `WithScratchDir`,
`WithInputHandoffRoot`, `WithDefaultModel`, `WithConfiguredModels`, `WithEnv`,
`WithSeedFiles`, `WithSessionStore`, `WithSessionStoreLoadTimeout`,
`WithTurnTimeout`, `WithConcurrencyLimits`, `WithImageLimits`, `WithLogger`,
`WithTracerProvider`, `WithMeterProvider`, `WithTextMapPropagator`,
`WithAgentName`, `WithAgentTitle`, `WithAgentVersion`.

### Session options

`_meta.pi.options` on `session/new`, `session/load`, and `session/resume`, or
`WithSessionPiOptions` from Go:

| Field | Meaning |
|---|---|
| `model` | `provider/id` for the session |
| `env` | environment overlay for the session's pi process |
| `extraPathDirs` | absolute directories prepended to `PATH`, in order |
| `thinkingLevel` | reasoning level passed to pi |
| `permission` | `ask` (default) requests permission per tool call; `allow` auto-allows |
| `autoRetry` | opt in to pi's native retry of transient provider errors |

`_meta.pi.rawEvent.enabled` forwards every native pi event on the
`_pi/rawEvent` notification.

### Config options

`session/set_config_option` accepts `model` (`provider/id`, from pi's catalog
plus any `WithConfiguredModels` entries) and `thought_level` (`off`,
`minimal`, `low`, `medium`, `high`, `xhigh`, `max`).

### Session store

`WithSessionStore` mirrors pi's session JSONL rows under the main subpath and
the adapter's session record under `config`, format `pi-session-jsonl-v1`.
`session/load` and `session/resume` prefer pi's own file when it exists and
materialize it from the store otherwise.
Native rows and session configuration commit as one store generation. A
configuration change is durable even when no native rows were added.

## Development

```sh
make test
make lint
make audit
make test-integration-smoke   # needs pi installed, spends no tokens
make test-integration-live    # spends model tokens
```

Unit tests run the test binary as a scripted fake pi and need no installed
pi, credentials, or network.
