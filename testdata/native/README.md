## agent-origin.json

Captured from pi 0.86.0 on 2026-09-20.

An installed `pi --mode rpc -e capture-ext.ts` ran in a temporary working
directory with an isolated `PI_CODING_AGENT_DIR` holding the operator's own
`auth.json`, `models-store.json`, and `settings.json`. No RPC `prompt` command
was sent. The wrapper-owned extension below sent a user message itself 1.5 s
after `session_start`, the way an operator extension can, and pi ran a real
model turn (opencode-go/qwen3.7-plus) for it:

    import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
    export default function (pi: ExtensionAPI) {
      pi.on("session_start", async () => {
        setTimeout(() => { void pi.sendUserMessage("1+1"); }, 1500);
      });
    }

The fixture retains every stdout JSONL record in delivery order, from the
`agent_start` that opens the agent-origin cycle to the `agent_settled` that
settles it. Working-directory and home paths are normalised to `/workspace`
and `/home/operator`; payloads and usage values are otherwise unchanged. The
test proves adapter ownership and settlement for those frames, not provider
behavior or spontaneous scheduling.

## prompt-response.json

Captured from pi 0.87.1 on 2026-10-01.

The built adapter ran an installed `pi --mode rpc` with its own extensions in a
temporary working directory and an isolated `PI_CODING_AGENT_DIR` whose
`models.json` pointed the built-in `openrouter` provider at a local logging
proxy for OpenRouter. One prompt, "Reply with the word hi.", ran a real model
turn on `openrouter/qwen/qwen3.8-flash`. The fixture retains every stdout
record from `agent_start` to `agent_settled` in delivery order, command
responses excluded, including the bridge extension's `setStatus` relay of the
response id ahead of the first `message_update`. The proxy logged the same
`gen-…` id in OpenRouter's response body. Working-directory and home paths are
normalised to `/workspace` and `/home/operator`; payloads and usage values are
otherwise unchanged.
