// Drives the real bridge extension through a login that settles, which is the
// only path that hands a brokered credential to pi's own AuthStorage. Nothing
// on the Go side can see that handoff: the wrapper's ledger, the probe and the
// inventory all report from the extension's messages, and those stay identical
// when the write is gone.
//
// Usage: node login-write.mjs <path to acp-bridge.ts>
// Prints the login result and exits non-zero on a failed expectation.
import { ctx, find, loadAuthCommand } from "./host.mjs";

const [, , extensionPath] = process.argv;

const command = await loadAuthCommand(extensionPath);

await command(JSON.stringify({ id: "login-1", op: "login", providerId: "settle", method: "oauth" }), ctx);

const result = find((payload) => payload.kind === "result" && payload.id === "login-1");
if (!result) {
	console.log("NO-RESULT");
	process.exit(1);
}

console.log(`RESULT ok=${result.payload.ok} cause=${result.payload.cause ?? ""} type=${result.payload.credentialType ?? ""}`);
