import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { ctx, loadAuthCommand, raised, pending, hooks } from "./host.mjs";

const command = await loadAuthCommand(process.argv[2]);
const authPath = join(process.env.PI_CODING_AGENT_DIR, "auth.json");
const routerModel = { provider: "openrouter", id: "model", api: "openai-completions", baseUrl: "https://openrouter.ai/api/v1" };
const goModel = { provider: "opencode-go", id: "model", api: "anthropic-messages", baseUrl: "https://opencode.ai/zen/go" };
const keyData = { data: { limit: null, limit_remaining: null, limit_reset: null, usage: 3 } };
const future = new Date(Date.now() + 3600000).toISOString();
let calls, replies, mutations, serial = 0;
let runtime, models, registered;

function response(body, status = 200) { return new Response(typeof body === "string" ? body : JSON.stringify(body), { status }); }

function setup(provider = "openrouter", credential = { type: "api_key", key: "stored-key" }) {
	writeFileSync(authPath, JSON.stringify(credential ? { [provider]: credential } : {}));
	delete process.env.OPENROUTER_API_KEY;
	delete process.env.OPENCODE_API_KEY;
	calls = []; replies = []; mutations = [];
	runtime = { config: { getProvider: () => undefined }, credentials: { overrides: new Map(), store: { authPath } },
		providerAvailabilitySeq: new Map(), credentialOperations: new Map() };
	models = [provider === "openrouter" ? { ...routerModel } : { ...goModel }];
	registered = [];
	ctx.model = models[0];
	ctx.modelRegistry = { runtime, getAll: () => models, getRegisteredProviderIds: () => registered };
	globalThis.fetch = async (url, options) => {
		assert.equal(options.method, "GET"); assert.equal(options.redirect, "manual");
		assert.equal(options.headers.Accept, "application/json");
		assert.equal(options.signal.aborted, false);
		calls.push({ url, key: options.headers.Authorization });
		mutations.shift()?.();
		const next = replies.shift();
		if (typeof next === "function") return next(options.signal);
		return next ?? response("denied", 403);
	};
}

async function run(provider = "openrouter") {
	const id = `quota-${serial++}`;
	await command(JSON.stringify({ id, op: "quota", providerId: provider }), ctx);
	const message = raised.find((entry) => entry.payload?.id === id && entry.payload.kind === "quota");
	assert.ok(message);
	assert.equal(pending.size, 0);
	assert.ok(!JSON.stringify(message.payload).includes("stored-key"));
	return message.payload.response;
}

for (const credential of [{ type: "api_key", key: "stored-key" }, { type: "oauth", access: "stored-key", refresh: "", expires: Number.MAX_SAFE_INTEGER }]) {
	setup("openrouter", credential);
	process.env.OPENROUTER_API_KEY = "different-ambient-key";
	replies.push(response(keyData), response({ data: { total_credits: 50, total_usage: 40.812047315 } }));
	const actual = await run();
	assert.equal(actual.availability, "available");
	assert.equal(actual.pools[0].balances[0].uncapped, true);
	assert.deepEqual(actual.pools[0].balances[0].used, { amount: 3, currency: "USD" });
	const account = actual.pools[1];
	assert.equal(account.id, "openrouter-account");
	assert.deepEqual(account.windows, []);
	assert.deepEqual(account.balances[0].used, { amount: 40.812047315, currency: "USD" });
	assert.equal(account.balances[0].remaining.amount, 50 - 40.812047315);
	assert.equal(account.balances[0].limit, undefined);
	assert.equal(account.balances[0].uncapped, undefined);
	assert.ok(Date.parse(account.balances[0].observedAt) >= Date.parse(actual.pools[0].balances[0].observedAt));
	assert.deepEqual(calls, [{ url: "https://openrouter.ai/api/v1/key", key: "Bearer stored-key" }, { url: "https://openrouter.ai/api/v1/credits", key: "Bearer stored-key" }]);
}

setup();
runtime.credentials.overrides.set("openrouter", "runtime-key");
replies.push(response(keyData));
assert.equal((await run()).availability, "available");
assert.equal(calls[0].key, "Bearer runtime-key");

setup("openrouter", { type: "api_key", key: "${SAVED_KEY}", env: { SAVED_KEY: "native-template-key" } });
replies.push(response(keyData));
assert.equal((await run()).availability, "available");
assert.equal(calls[0].key, "Bearer native-template-key");

setup("openrouter", null);
process.env.OPENROUTER_API_KEY = "native-environment-key";
replies.push(response(keyData));
assert.equal((await run()).availability, "available");
assert.equal(calls[0].key, "Bearer native-environment-key");

for (const [total, used] of [[0, 0], [1, 3]]) {
	setup();
	replies.push(response({ data: { limit: 10, limit_remaining: 0, usage: 27, limit_reset: "daily" } }), response({ data: { total_credits: total, total_usage: used } }));
	const actual = await run();
	assert.deepEqual(actual.pools[0].balances[0].limit, { amount: 10, currency: "USD" });
	assert.deepEqual(actual.pools[0].balances[0].remaining, { amount: 0, currency: "USD" });
	assert.equal(actual.pools[0].balances[0].used, undefined);
	assert.equal(actual.pools[0].balances[0].resetInterval, "daily");
	assert.equal(actual.pools[1].balances[0].remaining.amount, total - used);
}

for (const configure of [
	() => { runtime.config.getProvider = () => ({ apiKey: "configured-key" }); },
	() => { registered.push("openrouter"); },
	() => { models[0].baseUrl = "https://gateway.invalid/v1"; },
	() => { models[0].headers = { Authorization: "Bearer header-key" }; },
	() => { models[0].api = "custom"; },
	() => { models.push({ ...routerModel, id: "gateway", baseUrl: "https://gateway.invalid/v1" }); },
	() => { writeFileSync(authPath, JSON.stringify({ openrouter: { type: "api_key", key: "!must-not-run" } })); },
]) {
	setup(); configure();
	assert.equal((await run()).availability, "unsupported"); assert.equal(calls.length, 0);
}

for (const configure of [
	() => { runtime.credentialOperations.set("openrouter", Promise.resolve()); },
	() => { runtime.credentials.store.authPath = "/unrelated/auth.json"; },
	() => { delete ctx.modelRegistry.runtime; },
	() => { models = []; },
]) {
	setup(); configure(); assert.equal((await run()).reason, "session_required"); assert.equal(calls.length, 0);
}

for (const [credential, availability, reason] of [
	[null, "unavailable", "not_authenticated"],
	[{ type: "oauth", access: "expired", expires: 0 }, "unavailable", "not_authenticated"],
	[{ type: "custom", key: "unknown" }, "unsupported", undefined],
]) {
	setup("openrouter", credential);
	const actual = await run(); assert.equal(actual.availability, availability); assert.equal(actual.reason, reason); assert.equal(calls.length, 0);
}
setup(); writeFileSync(authPath, "malformed"); process.env.OPENROUTER_API_KEY = "must-not-fallback";
assert.equal((await run()).reason, "read_failed"); assert.equal(calls.length, 0);

for (const mutate of [
	() => writeFileSync(authPath, JSON.stringify({ openrouter: { type: "api_key", key: "changed-key" } })),
	() => { const before = JSON.stringify({ openrouter: { type: "api_key", key: "stored-key" } }); writeFileSync(authPath, "{}"); writeFileSync(authPath, before); },
	() => { models[0].baseUrl = "https://gateway.invalid"; },
	() => { runtime.credentials.overrides.set("openrouter", "new-runtime-key"); },
	() => { runtime.providerAvailabilitySeq.set("openrouter", 2); },
	() => { hooks.get("model_select")(); hooks.get("model_select")(); },
]) {
	setup(); mutations.push(() => {}, mutate); replies.push(response(keyData), response({ data: { total_credits: 10, total_usage: 2 } }));
	const actual = await run(); assert.equal(actual.reason, "read_failed"); assert.deepEqual(actual.pools, []);
}

for (const [body, status, reason] of [["unauthorized", 401, "not_authenticated"], ["forbidden", 403, "read_failed"], ["redirect", 302, "read_failed"], ["busy", 429, "read_failed"], ["error", 500, "read_failed"], ["malformed", 200, "read_failed"], ["x".repeat(1048577), 200, "read_failed"]]) {
	setup(); replies.push(response(body, status)); const actual = await run(); assert.equal(actual.reason, reason); assert.equal(calls.length, 1);
}

for (const optional of [response("unauthorized", 401), response("denied", 403), response("redirect", 302), response("malformed"), response({ data: { total_credits: null, total_usage: 2 } }), response({ data: { total_credits: 1, total_usage: -1 } }), response("x".repeat(1048577))]) {
	setup(); replies.push(response(keyData), optional); const actual = await run(); assert.equal(actual.availability, "available"); assert.equal(actual.pools.length, 1);
}

for (const body of [
	{ data: { usage: 3 } },
	{ data: { usage: null, limit: null } },
	{ data: { usage: -1, limit: null } },
	{ data: { usage: 1, limit: -1, limit_remaining: 1 } },
	{ data: { usage: 1, limit: 5, limit_remaining: null } },
	{ data: { usage: 1, limit: null, limit_reset: "unknown" } },
	'{"data":{"usage":1e309,"limit":null}}',
]) {
	setup(); replies.push(response(body)); assert.equal((await run()).reason, "read_failed");
}

setup("opencode-go");
replies.push(response({ usage: { rolling: { percent: 0, status: "ok", resetsAt: future }, weekly: { percent: 120, status: "rate-limited", resetsAt: future } } }));
let actual = await run("opencode-go");
assert.equal(actual.availability, "available"); assert.equal(actual.pools[0].windows[0].usedPercent, 0); assert.equal(actual.pools[0].windows[1].usedPercent, 120);
assert.equal(actual.pools[0].windows[1].status, "exhausted"); assert.equal(actual.pools[0].windows[0].durationSeconds, undefined);
assert.deepEqual(calls, [{ url: "https://opencode.ai/zen/go/v1/usage", key: "Bearer stored-key" }]);

setup("opencode-go", null); process.env.OPENCODE_API_KEY = "go-environment-key";
replies.push(response({ usage: { monthly: { percent: 5, status: "ok", resetsAt: future } } }));
assert.equal((await run("opencode-go")).availability, "available"); assert.equal(calls[0].key, "Bearer go-environment-key");

for (const [usage, reason] of [
	[{ rolling: { percent: 0, status: "ok", resetsAt: "2000-01-01T00:00:00Z" } }, "not_observed"],
	[{ rolling: { percent: null, status: "ok", resetsAt: future } }, "read_failed"],
	[{ rolling: { percent: -1, status: "ok", resetsAt: future } }, "read_failed"],
	[{ rolling: { percent: 1, status: "new-status", resetsAt: future } }, "read_failed"],
	[{ unknown: { percent: 1, status: "ok", resetsAt: future } }, "read_failed"],
]) {
	setup("opencode-go"); replies.push(response({ usage })); assert.equal((await run("opencode-go")).reason, reason);
}

// Cancellation uses the parked dialog, not a model abort or a conversation turn.
setup();
let entered;
const fetchEntered = new Promise((resolve) => { entered = resolve; });
let aborted = false;
replies.push((signal) => new Promise((_, reject) => {
	signal.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); }, { once: true });
	entered();
}));
const reading = run();
await fetchEntered;
for (const answer of pending.values()) answer("ok");
assert.equal((await reading).reason, "read_failed"); assert.equal(aborted, true);

// A deadline on the optional read aborts only that request and retains the key.
setup();
let optionalAborted = false;
replies.push(response(keyData), (signal) => new Promise((_, reject) => {
	signal.addEventListener("abort", () => { optionalAborted = true; reject(new Error("optional timeout")); }, { once: true });
}));
const guard = setTimeout(() => { throw new Error("optional quota deadline did not finish"); }, 8000);
try {
	actual = await run(); assert.equal(actual.availability, "available"); assert.equal(actual.pools.length, 1); assert.equal(optionalAborted, true);
} finally { clearTimeout(guard); }

console.log("QUOTA_OK");
