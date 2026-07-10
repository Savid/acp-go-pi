/**
 * acp-go-pi bridge extension (wrapper-owned, written per session).
 *
 * Permission gate: intercepts every tool call (built-in and
 * extension-registered, including MCP tools) and raises a select dialog over
 * the pi RPC extension UI protocol. The Go wrapper recognizes the marker
 * prefix in the dialog title, maps the payload to an ACP
 * session/request_permission call, and answers "allow" or "deny". A denied
 * or cancelled dialog fails closed: the tool call is blocked and the turn
 * continues with an error tool result.
 *
 * Mode comes from ACP_GO_PI_PERMISSION: "allow" auto-allows every tool call
 * (no dialog); any other value (including unset) asks per call.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const PERMISSION_MARKER = "acp-go-pi:permission:";

export default function (pi: ExtensionAPI) {
  if (process.env.ACP_GO_PI_PERMISSION === "allow") {
    return;
  }

  pi.on("tool_call", async (event, ctx) => {
    let payload: string;
    try {
      payload = JSON.stringify({
        toolName: event.toolName,
        input: event.input ?? {},
      });
    } catch {
      payload = JSON.stringify({ toolName: event.toolName, input: {} });
    }

    const choice = await ctx.ui.select(PERMISSION_MARKER + payload, [
      "allow",
      "deny",
    ]);
    if (choice !== "allow") {
      return { block: true, reason: "Denied by ACP client" };
    }
  });
}
