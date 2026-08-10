# acp-go-pi

Go ACP agent that exposes the local pi coding agent CLI as an [Agent Client Protocol](https://agentclientprotocol.com/) agent.

[![Go Reference](https://pkg.go.dev/badge/github.com/savid/acp-go-pi.svg)](https://pkg.go.dev/github.com/savid/acp-go-pi)
[![CI](https://github.com/savid/acp-go-pi/actions/workflows/go-test.yml/badge.svg)](https://github.com/savid/acp-go-pi/actions/workflows/go-test.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

It wraps the local `pi` CLI in RPC mode, speaks ACP over JSON-RPC streams, and
builds on [`github.com/coder/acp-go-sdk`](https://github.com/coder/acp-go-sdk).

Use it as either:

- a standalone ACP subprocess: `acp-go-pi`
- an embedded Go adapter through `piacp.Serve`

## Install

Library:

```sh
go get github.com/savid/acp-go-pi
```

CLI:

```sh
go install github.com/savid/acp-go-pi/cmd/acp-go-pi@latest
```

The `acp-go-pi` binary speaks ACP over stdin/stdout; an editor or ACP host
launches it as a subprocess rather than a human-facing chat UI.

## Quickstart

The example programs run from a checkout of this repo, so clone it first:

```sh
git clone https://github.com/savid/acp-go-pi && cd acp-go-pi
```

Run a tiny local client against the agent:

```sh
go run ./examples/minimal-client \
  -auth-file "$HOME/.pi/agent/auth.json" \
  "Reply with a short hello from ACP."
```

Start an interactive session against the agent:

```sh
go run ./examples/interactive-chat -auth-file "$HOME/.pi/agent/auth.json"
```

Load and resume a stored session transcript:

```sh
go run ./examples/resume-from-file \
  -file ./transcript.jsonl \
  -auth-file "$HOME/.pi/agent/auth.json"
```

Each example copies the explicitly named credential file into its isolated pi
agent directory. It never inherits ambient provider keys or reads the normal pi
home implicitly.

## Embedded Go

```go
package main

import (
	"context"
	"log"
	"os"

	piacp "github.com/savid/acp-go-pi"
)

func main() {
	err := piacp.Serve(context.Background(), os.Stdin, os.Stdout,
		piacp.WithDefaultModel("openai/gpt-4o"),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

See the [Go API reference](https://pkg.go.dev/github.com/savid/acp-go-pi) for
options such as the pi executable path, scratch directory, default model,
session storage, permissions, raw events, and OpenTelemetry providers.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, resume,
  and extension-based fork.
- pi RPC-mode subprocess management with isolated per-session agent
  directories and a scrubbed child environment.
- A target-owned version-probe agent directory below `WithScratchDir`, passed
  as an exact non-empty `PI_CODING_AGENT_DIR` so policy `HOME` is never used as
  pi's fallback settings root.
- Prompt streaming for messages, thoughts, tool calls, tool results, usage,
  and session metadata.
- Permission prompts through a wrapper-owned pi bridge extension with
  ask/allow modes.
- Elicitation bridging through the wrapper-owned `question` tool and any
  explicitly seeded extension dialogs.
- MCP stdio and HTTP server declarations through a wrapper-owned,
  dependency-free pi MCP client extension.
- Deliberate provider credential injection through the child environment or a
  seeded `auth.json`.
- Optional seven-leg provider-auth brokerage backed by Pi's durable native
  credential home and a separate values-free ownership ledger.
- Optional durable mirroring through a host-provided `SessionStore`.
- Optional raw pi event extension notifications.
- OpenTelemetry spans, metrics, trace propagation, and structured logs
  without recording prompt or tool secrets by default.

## Process containment

The default is ordinary execution. Omitting `WithProcessIsolation` (or the CLI
`-process-isolation-config`) launches Pi as the adapter's current root or
non-root identity on Linux, Darwin, FreeBSD, OpenBSD, and Windows. It reports
`shared_identity`, uses portable direct-process liveness and locking, and makes
no descendant-inventory, whole-tree-quiescence, credential-separation, or
host-authority claim. It creates no identity lease or privileged supervisor.

Supplying `WithProcessIsolation` is a distinct hardened posture: a trusted root
Linux supervisor launches Pi as the configured non-root UID/GID with an empty
supplementary-group set and a closed policy environment. Any invalid policy,
missing authority, same-identity request, or unsupported platform fails closed;
it never retries as ordinary execution or as Darwin best effort. The effective
mode is available from `Agent.ContainmentMode`.

Darwin best-effort mode reaps the direct child and applies a bounded
TERM-to-KILL ladder to the captured original process group. It does not claim
that escaped descendants are absent. It is a separate embedded-only opt-in,
never an explicit-isolation fallback. The adapter prints a warning on startup
and retains runtime records that operators can inspect with `acp-go-pi
containment diagnose`; see the [CLI reference](docs/reference/cli.mdx) and
[security limits](docs/operations/security.mdx).
Only `group_absent` records expire after 30 days; `running` and
`cleanup_incomplete` records remain actionable. Forced PID-by-PID cleanup has
a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused
PID despite immediate identity revalidation.

## Slash Commands

The adapter projects only commands returned by pi's RPC command inventory.
Ambient extensions, prompt templates, and skills are disabled for isolated
sessions. Explicit `WithSeedFiles` entries under `extensions/`, `prompts/`,
and `skills/**/SKILL.md` are loaded by exact path and therefore are reachable;
the shipped wrapper extensions register no slash commands, so a default
session still advertises an empty command set. Nothing is synthesized from
the terminal UI's built-in commands.

## Docs

- [Overview](docs/overview.mdx)
- [Run modes](docs/get-started/run-modes.mdx)
- [Go API](docs/reference/go-api.mdx)
- [ACP methods](docs/reference/acp-methods.mdx)
- [Observability](docs/operations/observability.mdx)

Full Go API reference:
[pkg.go.dev/github.com/savid/acp-go-pi](https://pkg.go.dev/github.com/savid/acp-go-pi).

## Development

```sh
make audit
make test-integration-smoke
make test-integration-live
make test-integration-attended
make test-integration-keystore
make test-integration-cover
```

`make audit` runs the full local gate: format, lint, build, unit tests,
coverage, cross-compile, vuln, and docs checks. Live integration tests
require a local `pi` CLI (v0.80.6 or newer) and are double-gated: the
`integration` build tag plus `ACP_GO_PI_RUN_INTEGRATION=1`.
`make test-integration-smoke` runs the integration tier without spending
model tokens; tests that spend tokens additionally require
`ACP_GO_PI_RUN_LIVE_TOKENS=1`, which only `make test-integration-live` sets.
The attended target drives a real browser-approved provider login; the
keystore target validates durable credential residence and browser suppression
inside its Linux container fixture.
`make test-integration-cover` runs the integration tier against a
coverage-instrumented binary. Integration tests always launch pi with an
explicit test-owned `PI_CODING_AGENT_DIR` and a scrubbed environment. Provider
auth tests use only test-owned durable homes and ledgers.

## License

Distributed under the GNU General Public License v3.0. See [LICENSE](LICENSE).
