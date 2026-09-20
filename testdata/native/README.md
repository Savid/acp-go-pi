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
