// The fake pi extension host both drivers run the real bridge extension
// against. It aliases the three imports the extension makes onto the stubs
// beside this file, and answers the marker dialogs the way the Go wrapper does:
// "ok" to every one except the abort watch, which the wrapper holds open for
// the life of a login.
import { registerHooks } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const stubDir = dirname(fileURLToPath(import.meta.url));

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

export const raised = [];
export const pending = new Map();

let nextDialog = 0;

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

export const ctx = { ui: { select, input: async () => undefined } };

export function find(match) {
	return raised.find((entry) => entry.payload && match(entry.payload));
}

export async function waitFor(match, label) {
	for (let attempt = 0; attempt < 200; attempt++) {
		const entry = find(match);
		if (entry) return entry;

		await new Promise((resolve) => setTimeout(resolve, 10));
	}

	console.log(`TIMEOUT ${label}`);
	process.exit(1);
}

// loadAuthCommand runs the extension's registration entrypoint and hands back
// the /acp-auth handler, which is the only surface the wrapper drives.
export async function loadAuthCommand(extensionPath) {
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

	return command;
}
