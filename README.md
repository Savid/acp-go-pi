# acp-go-pi

`acp-go-pi` exposes the [pi](https://github.com/earendil-works/pi-mono) coding
agent as an [Agent Client Protocol](https://agentclientprotocol.com) agent.
It launches one `pi --mode rpc` process per ACP session, maps ACP requests
onto pi's JSONL RPC, and streams ACP session updates back to the client.

pi inherits the adapter's environment and keeps sessions in its own home. A
session started over ACP can be continued natively:

```sh
acp-go-pi              # host runs a session in /work
cd /work && pi --session NATIVE_SESSION_ID
```

New, load, and resume responses and session-list entries expose the current
native ID as `_meta.pi.nativeSessionId`. Use it for native CLI continuation.
ACP requests continue to use the stable ACP `sessionId`. The store's configuration
record saves both IDs with the matching native history.

## Install

```sh
go install github.com/savid/acp-go-pi/cmd/acp-go-pi@latest
```

Verified against `pi` 0.85.1, found on `PATH` or named with `-path`.

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
`WithSeedFiles`, `WithSessionStore`, `WithConcurrencyLimits`,
`WithImageLimits`, `WithLogger`, `WithTracerProvider`, `WithMeterProvider`,
`WithTextMapPropagator`, `WithAgentName`, `WithAgentTitle`,
`WithAgentVersion`.

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

### Image input

Images are accepted as inline data or through `WithInputHandoffRoot`. Pi's
native prompt has separate text and image fields. Put all text, resource
links, and text resources before the images; images alone and multiple images
are accepted. Forwarded text after the first image fails with
`{"error":"unsupported","field":"prompt"}` before native dispatch.
User-only text excluded from native input does not affect ordering.

### Session store

`WithSessionStore` mirrors pi's session JSONL rows under the main subpath and
the adapter's session record under `config`, format `pi-session-jsonl-v1`.
`session/load` and `session/resume` prefer pi's own file when it exists and
materialize it from the store otherwise.
Native rows and session configuration commit as one store generation. A
configuration change is durable even when no native rows were added, and an
established conversation with no native history yet commits its configuration
with an empty main record. Replay decodes every stored image, user or
assistant, through the same output gate as live image output, so a stored
image that gate refuses fails the whole restore rather than leaving a hole.

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

## Account usage

`AccountUsageMethod` (`_pi/accountUsage`) accepts `sessionId` and `providerId`
(`opencode-go`, `openrouter`, `openai-codex`, or `anthropic`). Reads hold the session's foreground gate and
spend no model tokens. The session extension resolves effective credentials,
model endpoints, and authentication headers from Pi's native model registry.
Credentials travel only over an authenticated loopback endpoint and never
enter the conversation or ACP events.

Official provider routes use `github.com/savid/acp-go-core/usage` to read Go
percentage windows, OpenRouter USD balances and request counts, ChatGPT
subscription windows, and Claude subscription windows and reported spending.
ChatGPT account IDs come from the same native access token used for inference.
Subscription credits are not treated as dollars. Custom provider implementations and unverified
routes report `not_reported`. Missing credentials report `not_authenticated`.
The adapter revalidates the native binding after each read.
