/**
 * acp-go-pi bridge extension.
 *
 * Permission gate: intercepts every tool call and raises a select dialog over
 * the pi RPC extension UI protocol. The Go adapter recognizes the marker prefix
 * in the dialog title, maps the payload to an ACP session/request_permission
 * call, and answers "allow" or "deny". A denied or cancelled dialog blocks the
 * tool call and the turn continues with an error tool result.
 *
 * Mode comes from ACP_GO_PI_INTERNAL_PERMISSION: "allow" auto-allows every
 * tool call; any other value asks per call.
 *
 * Question tool: lets the model ask the user one question; the adapter relays
 * the dialog as an ACP elicitation.
 *
 * Message identity: stamps a UUID onto each finalized assistant message before
 * pi persists it, so live updates and session/load replay carry one durable id.
 */
import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const PERMISSION_MARKER = "acp-go-pi:permission:";
const QUESTION_TOOL = "question";

export default function (pi: ExtensionAPI) {
	pi.registerTool({
		name: QUESTION_TOOL,
		label: "Question",
		description: "Ask the user one question and wait for their response.",
		parameters: Type.Object({
			question: Type.String(),
			options: Type.Optional(Type.Array(Type.String())),
		}),
		async execute(_id, params, signal, _onUpdate, ctx) {
			if (signal?.aborted) throw new Error("Question cancelled");

			const answer = params.options?.length
				? await ctx.ui.select(params.question, params.options)
				: await ctx.ui.input(params.question, "Type your answer");
			if (answer === undefined) throw new Error("Question declined by ACP client");

			return {
				content: [{ type: "text", text: answer }],
				details: { answer },
			};
		},
	});

	pi.on("message_end", (event) => {
		if (event.message.role !== "assistant") return;

		const message = event.message as typeof event.message & {
			acpMessageId?: string;
		};
		if (message.acpMessageId) return;

		return {
			message: {
				...message,
				acpMessageId: randomUUID(),
			} as typeof event.message,
		};
	});

	if (process.env.ACP_GO_PI_INTERNAL_PERMISSION === "allow") {
		return;
	}

	pi.on("tool_call", async (event, ctx) => {
		// The question tool is already mediated by ACP elicitation. It performs
		// no side effect and must not open a second permission dialog.
		if (event.toolName === QUESTION_TOOL) return;

		let payload: string;
		try {
			payload = JSON.stringify({
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				input: event.input ?? {},
			});
		} catch {
			payload = JSON.stringify({
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				input: {},
			});
		}

		const choice = await ctx.ui.select(PERMISSION_MARKER + payload, ["allow", "deny"]);
		if (choice !== "allow") {
			return { block: true, reason: "Denied by ACP client" };
		}
	});
}
