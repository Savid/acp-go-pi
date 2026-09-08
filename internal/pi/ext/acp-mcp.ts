/**
 * acp-go-pi MCP client extension (wrapper-owned, shared immutable source).
 *
 * Dependency-free MCP client: node built-ins for stdio servers, global fetch
 * for streamable HTTP servers. Reads a config file (path in
 * ACP_GO_PI_MCP_CONFIG) shaped like:
 *   { "servers": [
 *       {"name": "calc", "type": "stdio", "command": "python3",
 *        "args": ["/path/server.py"], "env": {"K": "V"}},
 *       {"name": "web",  "type": "http", "url": "http://127.0.0.1:8973/mcp",
 *        "headers": {"Authorization": "Bearer x"}}
 *   ]}
 *
 * Connects to every server at factory time (pi awaits async factories before
 * session_start), lists tools, and registers each as `mcp__<server>__<tool>`
 * (the double-underscore MCP naming convention hosts already parse).
 * A server that fails to connect throws, which makes pi exit at startup with
 * the real cause on stderr; the Go wrapper maps that to a structured
 * session/new failure. MCP tool errors (isError) are re-thrown because pi
 * custom tools signal failure by throwing. Aborts fail the local call; stdio
 * sends best-effort notifications/cancelled and HTTP aborts its fetch.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { readFileSync } from "node:fs";
import { Type } from "typebox";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const PROTOCOL_VERSION = "2025-06-18";

type McpContent = {
  type: string;
  text?: string;
  data?: string;
  mimeType?: string;
};
type McpToolInfo = { name: string; description?: string; inputSchema?: any };

interface Transport {
  request(method: string, params: any, signal?: AbortSignal): Promise<any>;
  notify(method: string, params?: any): Promise<void>;
  close(): void;
}

/* ---------- stdio transport: newline-delimited JSON-RPC ---------- */
class StdioTransport implements Transport {
  private proc: ChildProcess;
  private nextId = 1;
  private pending = new Map<
    number,
    { resolve: (v: any) => void; reject: (e: Error) => void }
  >();
  private buffer = "";
  private stderrTail: string[] = [];
  private failure: Error | undefined;

  constructor(command: string, args: string[], env: Record<string, string>) {
    this.proc = spawn(command, args, {
      env: { ...process.env, ...env },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.proc.stdout!.setEncoding("utf8");
    this.proc.stdout!.on("data", (chunk: string) => {
      this.buffer += chunk;
      let idx: number;
      while ((idx = this.buffer.indexOf("\n")) !== -1) {
        const line = this.buffer.slice(0, idx).trim();
        this.buffer = this.buffer.slice(idx + 1);
        if (!line) continue;
        let msg: any;
        try {
          msg = JSON.parse(line);
        } catch {
          continue; // skip noise lines
        }
        const waiter = this.pending.get(msg?.id);
        if (waiter) {
          if (msg.error)
            waiter.reject(
              new Error(`MCP error ${msg.error.code}: ${msg.error.message}`),
            );
          else waiter.resolve(msg.result);
        }
      }
    });
    this.proc.stderr!.on("data", (c: Buffer) => {
      this.stderrTail.push(c.toString("utf8"));
      if (this.stderrTail.length > 20) this.stderrTail.shift();
    });
    this.proc.on("error", (err) => this.fail(err));
    this.proc.stdin!.on("error", (err) => this.fail(err));
    this.proc.on("close", (code) => {
      const err = new Error(
        `MCP stdio server exited (code ${code}): ${this.stderrTail.join("").slice(-500)}`,
      );
      this.fail(err);
    });
  }

  private fail(err: Error): void {
    this.failure ??= err;
    for (const [, waiter] of this.pending) waiter.reject(this.failure);
  }

  request(method: string, params: any, signal?: AbortSignal): Promise<any> {
    if (signal?.aborted) return Promise.reject(new Error("MCP tool call aborted"));
    if (this.failure) return Promise.reject(this.failure);
    const id = this.nextId++;
    return new Promise<any>((resolve, reject) => {
      const finish = (err?: Error, result?: any) => {
        if (!this.pending.delete(id)) return;
        clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
        if (err) reject(err);
        else resolve(result);
      };
      const abort = () => {
        void this.notify("notifications/cancelled", {
          requestId: id,
          reason: "aborted",
        }).catch(() => {});
        finish(new Error("MCP tool call aborted"));
      };
      const timer = setTimeout(() => finish(new Error(`MCP request timeout: ${method}`)), 60_000);
      this.pending.set(id, {
        resolve: (result) => finish(undefined, result),
        reject: (err) => finish(err),
      });
      signal?.addEventListener("abort", abort, { once: true });
      this.proc.stdin!.write(
        JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n",
        (err) => { if (err) finish(err); },
      );
    });
  }

  async notify(method: string, params?: any): Promise<void> {
    if (this.failure) throw this.failure;
    await new Promise<void>((resolve, reject) => {
      this.proc.stdin!.write(
        JSON.stringify({ jsonrpc: "2.0", method, ...(params ? { params } : {}) }) + "\n",
        (err) => { if (err) reject(err); else resolve(); },
      );
    });
  }

  close(): void {
    this.fail(new Error("MCP transport closed"));
    try {
      this.proc.stdin!.end();
    } catch {}
    this.proc.kill("SIGTERM");
  }
}

/* ---------- streamable HTTP transport ---------- */
class HttpTransport implements Transport {
  private nextId = 1;
  private sessionId: string | undefined;
  private shutdown = new AbortController();

  constructor(
    private url: string,
    private headers: Record<string, string>,
  ) {}

  private async post(body: any, signal?: AbortSignal): Promise<any | undefined> {
    this.shutdown.signal.throwIfAborted();
    const headers: Record<string, string> = {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": PROTOCOL_VERSION,
      ...this.headers,
    };
    if (this.sessionId) headers["mcp-session-id"] = this.sessionId;
    const res = await fetch(this.url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.any([
        this.shutdown.signal,
        AbortSignal.timeout(60_000),
        ...(signal ? [signal] : []),
      ]),
    });
    const sid = res.headers.get("mcp-session-id");
    if (sid) this.sessionId = sid;
    if (res.status === 202) return undefined; // accepted notification
    if (!res.ok)
      throw new Error(
        `MCP HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`,
      );
    const ctype = res.headers.get("content-type") || "";
    if (ctype.includes("text/event-stream")) {
      // single-response SSE framing: collect data: lines until a message with an id
      const text = await res.text();
      for (const chunk of text.split("\n\n")) {
        for (const line of chunk.split("\n")) {
          if (line.startsWith("data:")) {
            const msg = JSON.parse(line.slice(5).trim());
            if (msg.id !== undefined) return msg;
          }
        }
      }
      throw new Error("MCP HTTP: no response message in SSE stream");
    }
    return await res.json();
  }

  async request(method: string, params: any, signal?: AbortSignal): Promise<any> {
    signal?.throwIfAborted();
    const id = this.nextId++;
    const msg = await this.post({ jsonrpc: "2.0", id, method, params }, signal);
    if (!msg) throw new Error(`MCP HTTP: empty response for ${method}`);
    if (msg.id !== id) throw new Error(`MCP HTTP: mismatched response for ${method}`);
    if (msg.error)
      throw new Error(`MCP error ${msg.error.code}: ${msg.error.message}`);
    return msg.result;
  }

  async notify(method: string, params?: any): Promise<void> {
    await this.post({
      jsonrpc: "2.0",
      method,
      ...(params ? { params } : {}),
    });
  }

  close(): void { this.shutdown.abort(); }
}

/* ---------- content mapping: MCP result -> pi tool result ---------- */
function mapContent(content: McpContent[] | undefined) {
  const out: any[] = [];
  for (const c of content || []) {
    if (c.type === "text") out.push({ type: "text", text: c.text ?? "" });
    else if (c.type === "image")
      out.push({ type: "image", data: c.data, mimeType: c.mimeType });
    else out.push({ type: "text", text: JSON.stringify(c) });
  }
  if (out.length === 0) out.push({ type: "text", text: "(empty result)" });
  return out;
}

export default async function (pi: ExtensionAPI) {
  const configPath = process.env.ACP_GO_PI_MCP_CONFIG;
  if (!configPath) return;
  const config = JSON.parse(readFileSync(configPath, "utf8"));
  const transports: Transport[] = [];

  try {
    for (const server of config.servers || []) {
      const transport: Transport =
        server.type === "http"
          ? new HttpTransport(server.url, server.headers || {})
          : new StdioTransport(server.command, server.args || [], server.env || {});
      transports.push(transport);

      await transport.request("initialize", {
        protocolVersion: PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: {
          name: "acp-go-pi",
          version: process.env.ACP_GO_PI_VERSION || "dev",
        },
      });
      await transport.notify("notifications/initialized");
      const listed = await transport.request("tools/list", {});

      for (const tool of (listed.tools || []) as McpToolInfo[]) {
        const schema =
          tool.inputSchema
            ? Type.Unsafe(tool.inputSchema)
            : Type.Object({});
        pi.registerTool({
          name: `mcp__${server.name}__${tool.name}`,
          label: `MCP ${server.name}/${tool.name}`,
          description: tool.description || `MCP tool ${tool.name} from ${server.name}`,
          parameters: schema as any,
          async execute(_toolCallId: string, params: any, signal?: AbortSignal) {
            const result = await transport.request(
              "tools/call",
              { name: tool.name, arguments: params ?? {} },
              signal,
            );
            if (result.isError === true) {
              // pi custom tools signal failure by throwing; the message becomes
              // the error tool result the model sees.
              const text = (result.content || [])
                .map((c: McpContent) => c.text ?? "")
                .join("\n");
              throw new Error(text || "MCP tool failed");
            }
            return {
              content: mapContent(result.content),
              details: { mcpServer: server.name, mcpTool: tool.name },
            };
          },
        } as any);
      }
    }
  } catch (err) {
    for (const transport of transports) transport.close();
    throw err;
  }

  pi.on("session_shutdown", async () => {
    for (const t of transports) t.close();
  });
}
