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
- pi RPC-mode subprocess management with ephemeral agent directories or an
  explicit ordinary durable Home, and a scrubbed child environment.
- Prompt streaming for messages, thoughts, tool calls, tool results, usage,
  and session metadata.
- Permission prompts through a wrapper-owned pi bridge extension with
  ask/allow modes.
- Elicitation bridging through the wrapper-owned `question` tool and any
  explicitly seeded extension dialogs.
- MCP stdio and HTTP server declarations through a wrapper-owned pi MCP
  client extension using dependencies supplied by the native installation.
- Deliberate provider credential injection through the child environment or a
  seeded `auth.json`, plus an optional seven-leg provider-auth brokerage over
  Pi's durable native credential home and a values-free ownership ledger.
- Ordinary same-account execution by default, or embedded host-authority
  execution with prepare/start/wait/reclaim ownership and no direct fallback —
  see [security](docs/operations/security.mdx).
- Optional durable mirroring through a host-provided `SessionStore` and
  optional raw pi event extension notifications.
- OpenTelemetry spans, metrics, trace propagation, and structured logs
  without recording prompt or tool secrets by default.

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
make test-integration-native-browser
make test-integration-cover
```

`make audit` runs the full local gate: formatting, pinned lint, build, the complete
race and shuffled test suite with a coverage report, cross-compilation, module tidiness,
vulnerability and modernization checks, docs checks, and module verification.
Use focused checks while editing; prose-only changes need source review and
`make docs-audit`. Preserve the canonical lifecycle fixture bytes under
`testdata/lifecycle/`; coverage alone does not prove protocol or native behavior.

Ordinary tests fake the process boundary and need no installed pi or credentials.
The integration tier includes fake-backed wrapper tests and real-native tests;
only the latter establish compatibility with the executed pi version. Native
execution, including smoke and credential-free probes, requires explicit task
authorization. Tier gates select execution and do not supply that authorization.

| Target | Execution and prerequisites |
| --- | --- |
| `make test-integration-smoke` | Wrapper fixtures and native smoke without model spend. Real-native cases use pi 0.80.6 or newer from `PATH` or `ACP_GO_PI_HARNESS_PATH`; missing pi skips those cases. |
| `make test-integration-live` | Adds model-token prompts. Requires pi and portable credentials; missing prerequisites fail. Select the source home with `ACP_GO_PI_HOME`, whose `agent/auth.json` is copied into isolated session directories. `ACP_GO_PI_MODEL` can select the live model. |
| `make test-integration-cover` | Runs the tier without model spend against a command built with `go build -cover`, collects `GOCOVERDIR` counters, and merges them with `go tool covdata`. Native cases need the smoke prerequisites. |
| `make test-integration-attended` | Real provider login with pi, provider connectivity, and a human approving and answering on stdin. Compiles the selected tests and runs the binary directly to retain stdin; requires actual passes and rejects skips or empty selection. |
| `make test-integration-keystore` | Canary-only credential-residence fixtures. Linux needs a container runtime; the macOS residence proof runs on macOS. Uses test-owned homes and no real credentials. |
| `make test-integration-native-browser` | Linux container canary against the pinned real pi distribution, selected with `integration,browsercanary` build tags. Needs a container runtime and fixture build prerequisites; execution is networkless, credential-free, and mounts no host home. Proves browser interception in ordinary execution. |

Integration execution requires the `integration` build tag and
`ACP_GO_PI_RUN_INTEGRATION=1`. Live prompts, attended logins, and keystore
fixtures additionally select `ACP_GO_PI_RUN_LIVE_TOKENS=1`,
`ACP_GO_PI_RUN_ATTENDED=1`, and `ACP_GO_PI_RUN_KEYSTORE=1`, respectively.
Inspect the named Makefile target before running it; unrelated tier gates must
be cleared. Native children use an explicit test-owned `PI_CODING_AGENT_DIR`
and a scrubbed environment. Provider-auth tests use temporary durable homes
and ledgers. These targets are separate from `make audit`.

## License

Distributed under the GNU General Public License v3.0. See [LICENSE](LICENSE).
