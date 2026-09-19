import { createServer } from "node:http";
import { rename, rm, writeFile } from "node:fs/promises";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
	const endpointFile = process.env.ACP_GO_PI_INTERNAL_USAGE_FILE;
	const token = process.env.ACP_GO_PI_INTERNAL_USAGE_TOKEN;
	if (!endpointFile || !token) throw new Error("Account usage endpoint missing");
	let context: ExtensionContext | undefined;
	const server = createServer(async (request, response) => {
		response.setHeader("Cache-Control", "no-store");
		if (request.method !== "GET" || request.headers.authorization !== `Bearer ${token}`) {
			response.writeHead(403).end();
			return;
		}
		const query = new URL(request.url ?? "/", "http://127.0.0.1");
		const provider = query.searchParams.get("provider") ?? "";
		const modelId = query.searchParams.get("model");
		if (query.pathname !== "/access" || !["opencode-go", "openrouter", "openai-codex", "anthropic"].includes(provider)) {
			response.writeHead(404).end();
			return;
		}
		try {
			if (!context) throw new Error("Session not ready");
			const registry = context.modelRegistry;
			const configured = registry.getProviderAuthStatus(provider).configured;
			const models = registry.getAll().filter(model => model.provider === provider && (!modelId || model.id === modelId));
			const extension = registry.getRegisteredProviderConfig(provider);
			const custom = !!registry.getRegisteredNativeProvider(provider) || !!extension?.streamSimple || !!extension?.oauth;
			const routes = new Set<string>();
			if (configured && !custom) {
				for (const model of models) {
					if (response.destroyed) return;
					const auth = await registry.getApiKeyAndHeaders(model);
					if (!auth.ok) throw new Error("Provider authentication unavailable");
					const headers = { ...registry.getProvider(provider)?.headers, ...model.headers, ...auth.headers };
					const bearer = Object.entries(headers).find(([name]) => name.toLowerCase() === "authorization")?.[1];
					const apiKey = auth.apiKey ?? (provider === "anthropic" && bearer?.startsWith("Bearer ") ? bearer.slice(7) : "");
					routes.add(JSON.stringify({
						api: model.api,
						baseUrl: auth.baseUrl ?? model.baseUrl,
						apiKey,
						accountId: provider === "openai-codex" ? codexAccountId(apiKey) : "",
						headers,
					}));
				}
			}
			response.setHeader("Content-Type", "application/json");
			response.end(JSON.stringify({ configured, custom, routes: [...routes].map(route => JSON.parse(route)) }));
		} catch {
			response.writeHead(500).end();
		}
	});
	server.requestTimeout = 10_000;
	server.headersTimeout = 10_000;
	server.unref();
	pi.on("session_start", async (_event, ctx) => {
		context = ctx;
		if (server.listening) return;
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(0, "127.0.0.1", () => {
				server.off("error", reject);
				resolve();
			});
		});
		const staging = `${endpointFile}.tmp`;
		try {
			const address = server.address();
			if (!address || typeof address === "string") throw new Error("Account usage endpoint unavailable");
			await writeFile(staging, `http://127.0.0.1:${address.port}`, { mode: 0o600, flag: "wx" });
			await rename(staging, endpointFile);
		} catch (error) {
			server.closeAllConnections();
			server.close();
			throw error;
		} finally {
			await rm(staging, { force: true });
		}
	});
	pi.on("session_shutdown", async () => {
		context = undefined;
		server.closeAllConnections();
		server.close();
		await rm(endpointFile, { force: true });
	});
}

function codexAccountId(token: string): string {
	const parts = token.split(".");
	if (parts.length !== 3) throw new Error("Invalid Codex credential");
	const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
	const accountId = claims["https://api.openai.com/auth"]?.chatgpt_account_id;
	if (typeof accountId !== "string" || !accountId.trim()) throw new Error("Codex account missing");
	return accountId;
}
