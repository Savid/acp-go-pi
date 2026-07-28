// Drives the real bridge extension through a device-code login, standing in for
// the wrapper on the other side of the marker dialogs. It exists because the Go
// suite scripts the bridge's messages rather than running it: a defect that
// strands a message inside the extension is invisible from there.
//
// Usage: node driver.mjs <path to acp-bridge.ts>
// Prints one line per observation and exits non-zero on a failed expectation.
import { ctx, find, loadAuthCommand, pending, waitFor } from "./host.mjs";

const [, , extensionPath] = process.argv;

const command = await loadAuthCommand(extensionPath);

const finished = command(JSON.stringify({ id: "probe-1", op: "login", providerId: "probe", method: "oauth" }), ctx);

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

// The probe's answer is what every residence claim on the Go side is derived
// from, so its shape is a boundary contract: an entry for every provider asked
// about, and the value — not the key — saying whether a credential is there.
await command(JSON.stringify({ id: "probe-2", op: "probe", providerIds: ["probe", "absent"] }), ctx);

const probe = find((payload) => payload.kind === "probe" && payload.id === "probe-2");
if (!probe) {
	console.log("NO-PROBE");
	process.exit(1);
}

console.log(`PROBED ${JSON.stringify(probe.payload.entries)}`);
