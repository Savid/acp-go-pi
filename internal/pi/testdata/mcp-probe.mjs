// Exercise the embedded MCP extension with in-memory transports only.
import assert from "node:assert/strict";
import { EventEmitter, getEventListeners } from "node:events";
import { readFileSync, writeFileSync } from "node:fs";
import { registerHooks, stripTypeScriptTypes } from "node:module";
import { join } from "node:path";
import { PassThrough, Writable } from "node:stream";
import { pathToFileURL } from "node:url";

const [, , extensionPath, root] = process.argv;
const extensionURL = pathToFileURL(extensionPath).href;
registerHooks({
  resolve(specifier, context, next) {
    if (specifier === "typebox") return {
      url: "data:text/javascript,export const Type={Unsafe:x=>x,Object:x=>x};", shortCircuit: true,
    };
    if (specifier === "node:child_process") return {
      url: "data:text/javascript,export const spawn=(...args)=>globalThis.mcpSpawn(...args);", shortCircuit: true,
    };
    return next(specifier, context);
  },
  load(url, context, next) {
    if (url === extensionURL) return {
      format: "module", shortCircuit: true,
      source: stripTypeScriptTypes(readFileSync(extensionPath, "utf8"), { mode: "transform" }),
    };
    return next(url, context);
  },
});

const timers = new Set();
const setTimer = globalThis.setTimeout;
const clearTimer = globalThis.clearTimeout;
globalThis.setTimeout = (fn, delay) => {
  const timer = setTimer(() => { timers.delete(timer); fn(); }, delay);
  timers.add(timer);
  return timer;
};
globalThis.clearTimeout = (timer) => { timers.delete(timer); clearTimer(timer); };

const schema = { type: "object", additionalProperties: { type: "string" } };
const processes = [];
const requests = [];
let stdioMode = "reply";
globalThis.mcpSpawn = (command) => {
  const proc = new EventEmitter();
  proc.stdout = new PassThrough();
  proc.stderr = new PassThrough();
  proc.killed = false;
  proc.kill = () => { proc.killed = true; };
  proc.stdin = new Writable({
    write(chunk, _encoding, done) {
      const request = JSON.parse(chunk.toString());
      requests.push(request);
      done();
      if (request.id === undefined || (request.method === "tools/call" && stdioMode === "wait")) return;
      if (request.method === "tools/call" && stdioMode === "error") {
        queueMicrotask(() => proc.stdin.emit("error", new Error("broken pipe")));
        return;
      }
      const result = request.method === "tools/list"
        ? { tools: [{ name: "echo", inputSchema: schema }] }
        : request.method === "tools/call" ? { content: [{ type: "text", text: "héllo 🌍" }] } : {};
      const bytes = Buffer.from(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }) + "\n");
      queueMicrotask(() => {
        // Split every UTF-8 sequence across stdout chunks.
        for (const byte of bytes) proc.stdout.write(Buffer.from([byte]));
      });
    },
  });
  processes.push(proc);
  if (command === "missing") queueMicrotask(() => proc.emit("error", new Error("spawn refused")));
  return proc;
};

const extension = (await import(extensionURL)).default;
const configPath = join(root, "mcp.json");
process.env.ACP_GO_PI_MCP_CONFIG = configPath;
async function start(servers) {
  writeFileSync(configPath, JSON.stringify({ servers }));
  const tools = [];
  let shutdown;
  await extension({ registerTool: (tool) => tools.push(tool), on: (_event, fn) => { shutdown = fn; } });
  return { tool: tools[0], shutdown };
}

const stdio = await start([{ name: "stdio", command: "fake" }]);
assert.deepEqual(stdio.tool.parameters, schema);
assert.equal((await stdio.tool.execute("1", {})).content[0].text, "héllo 🌍");
const completed = new AbortController();
await stdio.tool.execute("completed", {}, completed.signal);
assert.equal(getEventListeners(completed.signal, "abort").length, 0);
assert.equal(timers.size, 0, "completed requests retained timeout handles");

const aborted = new AbortController();
aborted.abort();
const before = requests.length;
await assert.rejects(stdio.tool.execute("2", {}, aborted.signal), /aborted/);
assert.equal(requests.length, before, "pre-cancelled request reached stdin");

stdioMode = "wait";
const cancel = new AbortController();
const pending = stdio.tool.execute("3", {}, cancel.signal);
cancel.abort();
await assert.rejects(pending, /aborted/);
assert.equal(requests.at(-1).method, "notifications/cancelled");
assert.equal(getEventListeners(cancel.signal, "abort").length, 0);
assert.equal(timers.size, 0);

const closing = stdio.tool.execute("4", {});
await stdio.shutdown();
await assert.rejects(closing, /closed/);
await assert.rejects(stdio.tool.execute("5", {}), /closed/);
assert.equal(timers.size, 0);

stdioMode = "error";
const broken = await start([{ name: "broken", command: "fake" }]);
await assert.rejects(broken.tool.execute("6", {}), /broken pipe/);
await broken.shutdown();
assert.equal(timers.size, 0);

await assert.rejects(start([{ name: "one", command: "fake" }, { name: "two", command: "missing" }]), /spawn refused/);
assert.ok(processes.every((proc) => proc.killed), "startup failure retained an MCP child");
assert.equal(timers.size, 0);

let httpMode = "reply";
let httpCalls = 0;
globalThis.fetch = async (_url, options) => {
  httpCalls++;
  assert.ok(options.signal, "HTTP work has no cancellation/deadline signal");
  options.signal.throwIfAborted();
  const request = JSON.parse(options.body);
  if (request.method === "tools/call" && httpMode === "wait") {
    return await new Promise((_resolve, reject) => {
      options.signal.addEventListener("abort", () => reject(options.signal.reason), { once: true });
    });
  }
  if (request.id === undefined) return new Response(null, { status: 202 });
  return new Response(JSON.stringify({
    jsonrpc: "2.0", id: httpMode === "mismatch" ? request.id + 1 : request.id,
    result: request.method === "tools/list" ? { tools: [{ name: "echo" }] }
      : { content: [{ type: "text", text: "http reply" }] },
  }), { headers: { "content-type": "application/json" } });
};

const http = await start([{ name: "http", type: "http", url: "https://mcp.invalid" }]);
assert.equal((await http.tool.execute("7", {})).content[0].text, "http reply");
const callsBefore = httpCalls;
await assert.rejects(http.tool.execute("8", {}, aborted.signal), { name: "AbortError" });
assert.equal(httpCalls, callsBefore, "pre-cancelled request reached fetch");
httpMode = "wait";
const httpCancel = new AbortController();
const httpPending = http.tool.execute("9", {}, httpCancel.signal);
httpCancel.abort();
await assert.rejects(httpPending, { name: "AbortError" });
httpMode = "mismatch";
await assert.rejects(http.tool.execute("10", {}), /mismatched response/);
httpMode = "wait";
const httpClosing = http.tool.execute("11", {});
await http.shutdown();
await assert.rejects(httpClosing, { name: "AbortError" });
const callsAtShutdown = httpCalls;
await assert.rejects(http.tool.execute("12", {}), { name: "AbortError" });
assert.equal(httpCalls, callsAtShutdown, "closed HTTP transport reached fetch");
console.log("MCP_TRANSPORT_OK");
