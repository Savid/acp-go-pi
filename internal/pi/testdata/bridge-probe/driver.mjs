// Drives the real bridge extension against a fake pi extension host, standing
// in for the wrapper on the other side of the marker dialogs. It exists because
// the Go suite scripts the bridge's messages rather than running it: a defect
// that strands a message inside the extension is invisible from there.
//
// Usage: node driver.mjs <path to acp-bridge.ts> <stub dir>
// Prints one line per observation and exits non-zero on a failed expectation.
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const [, , extensionPath, stubDir] = process.argv;

const stubs = new Map([
	["@earendil-works/pi-coding-agent", pathToFileURL(join(stubDir, "pi-coding-agent.mjs")).href],
	["@earendil-works/pi-ai/providers/all", pathToFileURL(join(stubDir, "pi-ai-providers.mjs")).href],
	["typebox", pathToFileURL(join(stubDir, "typebox.mjs")).href],
]);

registerHooks({
	resolve(specifier, context, nextResolve) {
		const stub = stubs.get(specifier);
		if (stub) return { url: stub, shortCircuit: true };

		return nextResolve(specifier, context);
	},
});

const AUTH_MARKER = "acp-go-pi:auth:";

const raised = [];
const pending = new Map();

let nextDialog = 0;

// The wrapper answers every marker dialog with "ok" except the abort watch,
// which it holds open for the life of the login.
function select(title, _options, opts) {
	const id = String(nextDialog++);
	const payload = title.startsWith(AUTH_MARKER) ? JSON.parse(title.slice(AUTH_MARKER.length)) : null;

	raised.push({ id, payload });

	return new Promise((resolve) => {
		if (opts?.signal) {
			opts.signal.addEventListener(
				"abort",
				() => {
					pending.delete(id);
					resolve(undefined);
				},
				{ once: true },
			);
		}

		if (payload?.kind === "cancel") {
			pending.set(id, resolve);

			return;
		}

		resolve("ok");
	});
}

const ctx = { ui: { select, input: async () => undefined } };

const extension = await import(pathToFileURL(extensionPath).href);

let command;
extension.default({
	registerCommand(name, spec) {
		if (name === "acp-auth") command = spec.handler;
	},
	registerTool() {},
	on() {},
});

if (typeof command !== "function") {
	console.log("NO-COMMAND");
	process.exit(1);
}

const finished = command(JSON.stringify({ id: "probe-1", op: "login", providerId: "probe", method: "oauth" }), ctx);

function find(match) {
	return raised.find((entry) => entry.payload && match(entry.payload));
}

async function waitFor(match, label) {
	for (let attempt = 0; attempt < 200; attempt++) {
		const entry = find(match);
		if (entry) return entry;

		await new Promise((resolve) => setTimeout(resolve, 10));
	}

	console.log(`TIMEOUT ${label}`);
	process.exit(1);
}

// The login notifies its presentation and then polls without ever prompting, so
// a queue drained only at a prompt boundary never delivers this.
const presentation = await waitFor(
	(payload) => payload.kind === "event" && payload.event?.type === "device_code",
	"device-code presentation",
);
console.log(`PRESENTED ${presentation.payload.event.userCode}`);

const watch = await waitFor((payload) => payload.kind === "cancel", "abort watch");
console.log("WATCHING");

// Answering the watch is the wrapper's only way to stop a poll a terminal flow
// can no longer report on.
pending.get(watch.id)("ok");

await finished;

const result = find((payload) => payload.kind === "result");
if (!result) {
	console.log("NO-RESULT");
	process.exit(1);
}

console.log(`ABORTED ${result.payload.ok === false}`);
