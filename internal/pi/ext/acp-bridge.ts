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
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { getAgentDir, getPackageDir, readStoredCredential } from "@earendil-works/pi-coding-agent";
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
	op: "catalog" | "probe" | "login" | "remove" | "quota";
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

// watchAbort raises the one dialog the wrapper leaves unanswered for the life
// of a login. Answering it aborts the login, which is what stops a device-code
// poll: those flows return no control to this extension between the
// presentation and the provider's approval, so without an abort a flow the
// wrapper has already terminalized keeps a live user code approvable at the
// provider for the whole native timeout. The controller's own abort resolves
// the dialog too, which releases it when the login ends on its own.
function watchAbort(ctx: ExtensionCommandContext, id: string): AbortController {
	const controller = new AbortController();

	void ctx.ui
		.select(AUTH_MARKER + JSON.stringify({ id, kind: "cancel" }), [AUTH_ACK], { signal: controller.signal })
		.then((answer) => {
			if (answer !== undefined) controller.abort();
		})
		.catch(() => controller.abort());

	return controller;
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

// The probe answers every provider it was asked about, holding the empty string
// where nothing is stored. The value is the residence signal; emitting only the
// occupied providers would make the presence of a key the signal instead, which
// is a coupling the reader on the Go side of this boundary cannot see it is
// relying on.
function probePayload(id: string, providerIds: string[]) {
	const entries: Record<string, string> = {};
	for (const providerId of providerIds) {
		entries[providerId] = readStoredCredential(providerId)?.type ?? "";
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

	// notify() is synchronous and returns no promise, so an event is queued and
	// drained on a chain of its own. The chain starts the moment the event
	// arrives rather than at the next prompt: a device-code login notifies its
	// presentation and then polls to completion without ever prompting, so a
	// queue drained only at a prompt boundary strands the one message the
	// authorize leg is waiting for. The chain still serialises, so events reach
	// the wrapper in order and ahead of any prompt that follows them.
	const queued: AuthEvent[] = [];
	let pump: Promise<void> = Promise.resolve();
	const drain = () => {
		pump = pump
			.then(async () => {
				while (queued.length > 0) {
					await announce(ctx, { id: request.id, kind: "event", event: queued.shift() });
				}
			})
			.catch(() => {});

		return pump;
	};

	const abort = watchAbort(ctx, request.id);
	const interaction = {
		signal: abort.signal,
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
			void drain();
		},
	};

	let credential: unknown;
	try {
		credential = await login(interaction);
	} catch {
		await drain();
		abort.abort();

		return { id: request.id, kind: "result", ok: false, cause: "provider_refused" };
	}

	await drain();
	abort.abort();

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

type QuotaMoney = { amount: number; currency: "USD" };
type QuotaWindow = { id: string; usedPercent: number; status: string; observedAt: string; resetsAt: string };
type QuotaBalance = {
	id: string; observedAt: string; used?: QuotaMoney; limit?: QuotaMoney;
	remaining?: QuotaMoney; uncapped?: boolean; resetInterval?: string;
};
type QuotaPool = { id: string; windows: QuotaWindow[]; balances?: QuotaBalance[] };
type QuotaResponse = { providerId: string; availability: string; reason?: string; pools: QuotaPool[] };
type QuotaBinding = { key: string; fence: string };
type QuotaConfigResolver = (value: string, env?: Record<string, string>) => string | undefined;

// Pi 0.84.4 exposes ModelRegistry as a compatibility facade. These are the
// read-only native fields needed to reject configured auth/routing and select
// its exact runtime override before the persisted credential. A missing shape
// refuses the read; getAuth() is deliberately avoided because it can refresh.
type QuotaRuntime = {
	config: { getProvider(id: string): unknown };
	credentials: { overrides: Map<string, string>; store: { authPath?: string } };
	providerAvailabilitySeq: Map<string, number>;
	credentialOperations: Map<string, unknown>;
};

let quotaModelRevision = 0;

function quotaUnavailable(providerId: string, reason: string): QuotaResponse {
	return { providerId, availability: "unavailable", reason, pools: [] };
}

function quotaFileRevision(path: string): string {
	try {
		const stat = statSync(path, { bigint: true });
		return [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].join(":");
	} catch (error) {
		if ((error as { code?: string }).code === "ENOENT") return "absent";
		throw new Error("read_failed");
	}
}

function quotaOfficialModel(providerId: string, model: { api: string; baseUrl: string; headers?: unknown }): boolean {
	if (model.headers && Object.keys(model.headers).length !== 0) return false;
	if (providerId === "openrouter") {
		return model.api === "openai-completions" && model.baseUrl === "https://openrouter.ai/api/v1";
	}
	return (model.api === "anthropic-messages" && model.baseUrl === "https://opencode.ai/zen/go") ||
		(["openai-completions", "openai-responses"].includes(model.api) && model.baseUrl === "https://opencode.ai/zen/go/v1");
}

function quotaBinding(ctx: ExtensionCommandContext, providerId: string, resolveConfigValue: QuotaConfigResolver): QuotaBinding {
	const registry = ctx.modelRegistry;
	const runtime = (registry as unknown as { runtime?: QuotaRuntime }).runtime;
	if (!runtime?.config?.getProvider || !(runtime.credentials?.overrides instanceof Map) ||
		!(runtime.providerAvailabilitySeq instanceof Map) || !(runtime.credentialOperations instanceof Map)) {
		throw new Error("session_required");
	}
	if (registry.getRegisteredProviderIds().includes(providerId) || runtime.config.getProvider(providerId)) throw new Error("unsupported");
	if (runtime.credentialOperations.has(providerId)) throw new Error("session_required");

	const models = registry.getAll().filter((model) => model.provider === providerId);
	if (models.length === 0) throw new Error("session_required");
	if (models.some((model) => !quotaOfficialModel(providerId, model))) throw new Error("unsupported");
	if (ctx.model?.provider === providerId && !quotaOfficialModel(providerId, ctx.model)) {
		throw new Error("unsupported");
	}

	const authPath = join(getAgentDir(), "auth.json");
	if (runtime.credentials.store.authPath !== authPath) throw new Error("session_required");
	const fileRevision = quotaFileRevision(authPath);
	const auth = fileRevision === "absent" ? {} : JSON.parse(readFileSync(authPath, "utf8").replace(/^\uFEFF/, ""));
	if (!auth || typeof auth !== "object" || Array.isArray(auth)) throw new Error("read_failed");
	const stored = auth[providerId] as {
		type?: string; key?: string; access?: string; expires?: number; env?: Record<string, string>;
	} | undefined;
	const override = runtime.credentials.overrides.get(providerId);
	let key: string | undefined;
	if (override) {
		key = override;
	} else if (stored?.type === "api_key") {
		if (typeof stored.key !== "string") throw new Error("not_authenticated");
		if (stored.key.startsWith("!")) throw new Error("unsupported");
		// Use Pi's own non-command template resolver, preserving stored env
		// substitutions without executing a credential helper or refreshing.
		key = resolveConfigValue(stored.key, stored.env);
	} else if (stored?.type === "oauth" && providerId === "openrouter") {
		if (typeof stored.expires !== "number" || stored.expires <= Date.now() + 300000) throw new Error("not_authenticated");
		key = stored.access;
	} else if (stored) {
		throw new Error("unsupported");
	} else {
		key = process.env[providerId === "openrouter" ? "OPENROUTER_API_KEY" : "OPENCODE_API_KEY"];
	}
	if (typeof key !== "string" || key.trim() === "") throw new Error("not_authenticated");
	if (quotaFileRevision(authPath) !== fileRevision) throw new Error("read_failed");

	return {
		key,
		fence: JSON.stringify({ key, stored, override, fileRevision, model: ctx.model,
			modelRevision: quotaModelRevision, providerRevision: runtime.providerAvailabilitySeq.get(providerId),
			models: models.map((model) => [model.id, model.api, model.baseUrl, model.headers]) }),
	};
}

async function quotaJSON(url: string, key: string, signal: AbortSignal): Promise<{ data: any; observedAt: string }> {
	const response = await fetch(url, { method: "GET", redirect: "manual", signal,
		headers: { Authorization: `Bearer ${key}`, Accept: "application/json" } });
	if (response.status !== 200) {
		await response.body?.cancel();
		throw new Error(response.status === 401 ? "not_authenticated" : "read_failed");
	}
	if (!response.body) throw new Error("read_failed");
	const reader = response.body.getReader();
	const chunks: Uint8Array[] = [];
	let size = 0;
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			size += value.length;
			if (size > 1048576) throw new Error("read_failed");
			chunks.push(value);
		}
	} finally {
		await reader.cancel();
	}
	return { data: JSON.parse(Buffer.concat(chunks).toString("utf8")), observedAt: new Date().toISOString() };
}

function quotaNumber(value: unknown, nonnegative = true): value is number {
	return typeof value === "number" && Number.isFinite(value) && (!nonnegative || value >= 0);
}

function quotaMoney(amount: number): QuotaMoney { return { amount, currency: "USD" }; }

function quotaGoPools(data: any, observedAt: string): QuotaPool[] {
	if (!data?.usage || typeof data.usage !== "object" || Array.isArray(data.usage) || Object.keys(data.usage).length === 0) {
		throw new Error("read_failed");
	}
	const windows: QuotaWindow[] = [];
	for (const [id, value] of Object.entries(data.usage)) {
		const window = value as { percent?: unknown; status?: unknown; resetsAt?: unknown };
		if (!["rolling", "weekly", "monthly"].includes(id) || !quotaNumber(window?.percent) ||
			!["ok", "rate-limited"].includes(String(window.status)) || typeof window.resetsAt !== "string" ||
			!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(window.resetsAt) || !Number.isFinite(Date.parse(window.resetsAt))) {
			throw new Error("read_failed");
		}
		if (Date.parse(window.resetsAt) > Date.now()) {
			windows.push({ id, usedPercent: window.percent, status: window.status === "ok" ? "ok" : "exhausted", observedAt, resetsAt: window.resetsAt });
		}
	}
	if (windows.length === 0) throw new Error("not_observed");
	windows.sort((a, b) => ["rolling", "weekly", "monthly"].indexOf(a.id) - ["rolling", "weekly", "monthly"].indexOf(b.id));
	return [{ id: "opencode-go", windows }];
}

function quotaRouterPools(body: any, observedAt: string): QuotaPool[] {
	const data = body?.data;
	if (!data || !Object.hasOwn(data, "limit") || !quotaNumber(data.usage) || (data.limit != null && !quotaNumber(data.limit)) ||
		(data.limit != null && !quotaNumber(data.limit_remaining, false)) ||
		(data.limit_remaining != null && !quotaNumber(data.limit_remaining, false)) ||
		(data.limit_reset != null && !["daily", "weekly", "monthly"].includes(data.limit_reset))) throw new Error("read_failed");
	const balance: QuotaBalance = { id: "key", observedAt };
	if (data.limit == null) {
		balance.uncapped = true;
		balance.used = quotaMoney(data.usage);
	} else {
		balance.limit = quotaMoney(data.limit);
		balance.remaining = quotaMoney(data.limit_remaining);
	}
	if (data.limit_reset != null) balance.resetInterval = data.limit_reset;
	return [{ id: "openrouter", windows: [], balances: [balance] }];
}

async function quotaRead(ctx: ExtensionCommandContext, providerId: string, signal: AbortSignal): Promise<QuotaResponse> {
	try {
		// Load the native helper before capturing identity. Both binding reads
		// then complete synchronously, with no yield between checking routing,
		// selecting a credential, and dispatching the account request.
		const url = pathToFileURL(join(getPackageDir(), "dist", "core", "resolve-config-value.js")).href;
		const resolver = await import(url) as { resolveConfigValue: QuotaConfigResolver };
		const binding = quotaBinding(ctx, providerId, resolver.resolveConfigValue);
		signal.throwIfAborted();
		const endpoint = providerId === "openrouter" ? "https://openrouter.ai/api/v1/key" : "https://opencode.ai/zen/go/v1/usage";
		const result = await quotaJSON(endpoint, binding.key, signal);
		const pools = providerId === "openrouter" ? quotaRouterPools(result.data, result.observedAt) : quotaGoPools(result.data, result.observedAt);
		if (providerId === "openrouter") {
			try {
				const credits = await quotaJSON("https://openrouter.ai/api/v1/credits", binding.key, AbortSignal.any([signal, AbortSignal.timeout(5000)]));
				const data = credits.data?.data;
				if (quotaNumber(data?.total_credits) && quotaNumber(data?.total_usage)) {
					pools.push({ id: "openrouter-account", windows: [], balances: [{ id: "credits", observedAt: credits.observedAt,
						used: quotaMoney(data.total_usage), remaining: quotaMoney(data.total_credits - data.total_usage) }] });
				}
			} catch { /* Optional account credits never discard a valid key allowance. */ }
		}
		let current: QuotaBinding;
		try { current = quotaBinding(ctx, providerId, resolver.resolveConfigValue); }
		catch { throw new Error("read_failed"); }
		if (signal.aborted || current.fence !== binding.fence) throw new Error("read_failed");
		for (const pool of pools) pool.windows = pool.windows.filter((window) => Date.parse(window.resetsAt) > Date.now());
		if (providerId === "opencode-go" && pools[0].windows.length === 0) throw new Error("not_observed");
		return { providerId, availability: "available", pools };
	} catch (error) {
		const reason = (error as Error).message;
		if (reason === "unsupported") return { providerId, availability: "unsupported", pools: [] };
		return quotaUnavailable(providerId, ["not_authenticated", "session_required", "not_observed"].includes(reason) ? reason : "read_failed");
	}
}

async function runQuotaCommand(ctx: ExtensionCommandContext, request: AuthRequest): Promise<void> {
	const providerId = request.providerId ?? "";
	if (!["opencode-go", "openrouter"].includes(providerId)) {
		await announce(ctx, { id: request.id, kind: "quota", response: { providerId, availability: "unsupported", pools: [] } });
		return;
	}
	const abort = new AbortController();
	void ctx.ui.select(AUTH_MARKER + JSON.stringify({ id: request.id, kind: "quota_cancel" }), [AUTH_ACK], { signal: abort.signal })
		.then(() => abort.abort()).catch(() => abort.abort());
	const signal = AbortSignal.any([abort.signal, AbortSignal.timeout(30000)]);
	try {
		const response = await quotaRead(ctx, providerId, signal);
		await announce(ctx, { id: request.id, kind: "quota", response });
	} finally {
		abort.abort();
	}
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
		case "quota":
			await runQuotaCommand(ctx, request);

			return;
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
	pi.on("model_select", () => { quotaModelRevision++; });
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
