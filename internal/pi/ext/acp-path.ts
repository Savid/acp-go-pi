/**
 * acp-go-pi native-shell PATH extension (wrapper-owned, written per session).
 *
 * pi prepends PI_CODING_AGENT_DIR/bin when it creates the built-in bash tool's
 * environment. Restore the adapter-owned ExtraPathDirs after that native
 * rewrite so the per-session executable carrier stays first for both agent
 * tool calls and RPC/user bash commands.
 */
import { delimiter } from "node:path";
import {
	createBashTool,
	createLocalBashOperations,
	type BashOperations,
	type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";

const extraPathDirs = (process.env.ACP_GO_PI_EXTRA_PATH_DIRS ?? "")
	.split(delimiter)
	.filter(Boolean);

// Windows path comparison is case-insensitive, so C:\Tools and c:\tools name
// one directory there and must dedupe against the inherited tail; every other
// platform compares byte-exact. Only the identity key is folded — the adapter's
// own ordered list keeps the caller's exact spelling, order, and duplicates.
const identityKey =
	process.platform === "win32"
		? (entry: string) => entry.toLowerCase()
		: (entry: string) => entry;

function withExtraPathDirs(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
	const owned = new Set(extraPathDirs.map(identityKey));
	const inherited = (env.PATH ?? "")
		.split(delimiter)
		.filter((entry) => entry && !owned.has(identityKey(entry)));

	return { ...env, PATH: [...extraPathDirs, ...inherited].join(delimiter) };
}

export default function (pi: ExtensionAPI) {
	if (extraPathDirs.length === 0) return;

	const bashTool = createBashTool(process.cwd(), {
		spawnHook: ({ command, cwd, env }) => ({
			command,
			cwd,
			env: withExtraPathDirs(env),
		}),
	});

	pi.registerTool({
		...bashTool,
		execute: async (id, params, signal, onUpdate, _ctx) =>
			bashTool.execute(id, params, signal, onUpdate),
	});

	const local = createLocalBashOperations();
	const operations: BashOperations = {
		exec: (command, cwd, options) =>
			local.exec(command, cwd, {
				...options,
				env: withExtraPathDirs(options.env ?? process.env),
			}),
	};
	pi.on("user_bash", () => ({ operations }));
}
