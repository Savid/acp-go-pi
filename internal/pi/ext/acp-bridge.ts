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
 *
 * Provider auth: the /acp-auth command enumerates providers, probes the
 * credential store, drives one native login, and removes one entry. It reports
 * results and asks for values over the same marker-prefixed dialogs.
 */
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { getPackageDir, readStoredCredential } from "@earendil-works/pi-coding-agent";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import { builtinProviders } from "@earendil-works/pi-ai/providers/all";
import { Type } from "typebox";

const PERMISSION_MARKER = "acp-go-pi:permission:";
const AUTH_MARKER = "acp-go-pi:auth:";
const AUTH_COMMAND = "acp-auth";
const AUTH_ACK = "ok";
const QUESTION_TOOL = "question";

/** Request the Go wrapper sends as the argument of /acp-auth. */
type AuthRequest = {
	id: string;
	op: "catalog" | "probe" | "login" | "remove";
	providerId?: string;
	method?: "oauth" | "api";
	providerIds?: string[];
};

type AuthEvent = Record<string, unknown> & { type: string };

type AuthPrompt = {
	type: "text" | "secret" | "select" | "manual_code";
	message: string;
	placeholder?: string;
	options?: readonly { id: string; label: string; description?: string }[];
};

/** Raise one marker dialog the wrapper acknowledges and continue. */
async function announce(ctx: ExtensionCommandContext, payload: unknown): Promise<void> {
	await ctx.ui.select(AUTH_MARKER + JSON.stringify(payload), [AUTH_ACK]);
}

/** Ask the wrapper for one value. An unanswered dialog aborts the flow. */
async function ask(ctx: ExtensionCommandContext, payload: unknown): Promise<string> {
	const answer = await ctx.ui.input(AUTH_MARKER + JSON.stringify(payload));
	if (answer === undefined) throw new Error("declined");

	return answer;
}

// AuthStorage is the only write path into pi's own auth.json, and it holds the
// cross-process lock every refresh takes. The package root re-exports the
// read helper but not the class, and the package export map blocks the
// subpath, so the module is loaded by absolute path under the resolved
// install root.
async function authStorage(): Promise<{
	modify(id: string, fn: (current: unknown) => Promise<unknown>): Promise<unknown>;
	delete(id: string): Promise<void>;
}> {
	const url = pathToFileURL(join(getPackageDir(), "dist", "core", "auth-storage.js")).href;
	const module = (await import(url)) as {
		AuthStorage: {
			create(authPath?: string): {
				modify(id: string, fn: (current: unknown) => Promise<unknown>): Promise<unknown>;
				delete(id: string): Promise<void>;
			};
		};
	};

	return module.AuthStorage.create();
}

function catalogPayload(id: string) {
	const providers = builtinProviders().map((provider) => ({
		id: provider.id,
		name: provider.name,
		oauth: provider.auth.oauth
			? { name: provider.auth.oauth.name, loginLabel: provider.auth.oauth.loginLabel ?? "" }
			: null,
		// A provider whose api-key auth has no login() is ambient-only: there is
		// nothing to broker, so it carries no api method.
		api: provider.auth.apiKey?.login ? { name: provider.auth.apiKey.name } : null,
	}));

	return { id, kind: "catalog", providers };
}

function probePayload(id: string, providerIds: string[]) {
	const entries: Record<string, string> = {};
	for (const providerId of providerIds) {
		const credential = readStoredCredential(providerId);
		if (credential) entries[providerId] = credential.type;
	}

	return { id, kind: "probe", entries };
}

// pi executes any credential value beginning with "!" as a shell command and
// caches its stdout, so a brokered value that reached a credential field would
// be a remote command channel into the worker.
function rejectsShellCredential(credential: unknown): boolean {
	const key = (credential as { key?: unknown } | undefined)?.key;

	return typeof key === "string" && key.startsWith("!");
}

async function runLogin(ctx: ExtensionCommandContext, request: AuthRequest) {
	const provider = builtinProviders().find((entry) => entry.id === request.providerId);
	if (!provider) return { id: request.id, kind: "result", ok: false, cause: "native_veto" };

	const login =
		request.method === "oauth" ? provider.auth.oauth?.login.bind(provider.auth.oauth) : provider.auth.apiKey?.login?.bind(provider.auth.apiKey);
	if (!login) return { id: request.id, kind: "result", ok: false, cause: "native_veto" };

	// notify() is synchronous and the presentation must reach the wrapper before
	// the prompt that waits on it, so events queue here and drain in order ahead
	// of every prompt and the terminal result.
	const queued: AuthEvent[] = [];
	const drain = async () => {
		while (queued.length > 0) {
			await announce(ctx, { id: request.id, kind: "event", event: queued.shift() });
		}
	};

	const interaction = {
		async prompt(prompt: AuthPrompt): Promise<string> {
			await drain();

			return ask(ctx, {
				id: request.id,
				kind: "prompt",
				prompt: prompt.type,
				message: prompt.message ?? "",
				placeholder: prompt.placeholder ?? "",
				options: (prompt.options ?? []).map((option) => option.id),
			});
		},
		notify(event: AuthEvent) {
			queued.push(event);
		},
	};

	let credential: unknown;
	try {
		credential = await login(interaction);
	} catch {
		await drain();

		return { id: request.id, kind: "result", ok: false, cause: "provider_refused" };
	}

	await drain();

	if (rejectsShellCredential(credential)) {
		return { id: request.id, kind: "result", ok: false, cause: "native_veto" };
	}

	try {
		const storage = await authStorage();
		await storage.modify(provider.id, async () => credential);
	} catch {
		return { id: request.id, kind: "result", ok: false, cause: "harvest_failed" };
	}

	const stored = credential as { type?: string; expires?: number };

	return {
		id: request.id,
		kind: "result",
		ok: true,
		credentialType: stored.type ?? "",
		expires: typeof stored.expires === "number" ? stored.expires : 0,
	};
}

async function runRemove(request: AuthRequest) {
	try {
		const storage = await authStorage();
		await storage.delete(request.providerId ?? "");
	} catch {
		return { id: request.id, kind: "result", ok: false, cause: "process" };
	}

	return { id: request.id, kind: "result", ok: true };
}

async function runAuthCommand(args: string, ctx: ExtensionCommandContext) {
	let request: AuthRequest;
	try {
		request = JSON.parse(args) as AuthRequest;
	} catch {
		return;
	}

	if (typeof request?.id !== "string" || request.id === "") return;

	switch (request.op) {
		case "catalog":
			await announce(ctx, catalogPayload(request.id));

			return;
		case "probe":
			await announce(ctx, probePayload(request.id, request.providerIds ?? []));

			return;
		case "login":
			await announce(ctx, await runLogin(ctx, request));

			return;
		case "remove":
			await announce(ctx, await runRemove(request));

			return;
		default:
			return;
	}
}

export default function (pi: ExtensionAPI) {
	// The wrapper drives every provider-auth leg through this command: pi's RPC
	// surface has no verb that invokes an extension, and prompt() runs a
	// registered command to completion without a model turn even mid-stream.
	pi.registerCommand(AUTH_COMMAND, {
		description: "ACP provider-auth bridge",
		handler: runAuthCommand,
	});

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

  // pi creates its session-entry id after message_end listeners run, so that
  // id is not present on the RPC event. Stamp a UUID on the finalized
  // assistant message before pi persists it instead. The Go adapter exposes
  // this durable id as ACP correlation metadata and replays the same id after
  // session/load, which lets a host reconcile crash-window turn checkpoints.
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

  if (process.env.ACP_GO_PI_PERMISSION === "allow") {
    return;
  }

  pi.on("tool_call", async (event, ctx) => {
	// The wrapper-owned question tool is already mediated by ACP elicitation.
	// It never performs a side effect and must not open a second permission UI.
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

    const choice = await ctx.ui.select(PERMISSION_MARKER + payload, [
      "allow",
      "deny",
    ]);
    if (choice !== "allow") {
      return { block: true, reason: "Denied by ACP client" };
    }
  });
}
