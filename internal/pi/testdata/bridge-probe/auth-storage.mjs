// Stub of pi's dist/core/auth-storage.js, the module the bridge extension loads
// by absolute path under the resolved install root. It carries the two methods
// the extension calls and the same record shape auth.json holds: modify() hands
// the current entry to its caller and writes the returned one back under the
// provider id, delete() removes it. The Go side copies this file into a fixture
// install root, because the path the extension builds runs through a "dist"
// segment this repository does not track.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

// AuthStorage.create() takes no argument from the extension, so the default is
// the same one pi resolves: auth.json inside the agent directory.
function defaultAuthPath() {
	return join(process.env.PI_CODING_AGENT_DIR ?? ".", "auth.json");
}

function readRecord(path) {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return {};
	}
}

function writeRecord(path, record) {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, JSON.stringify(record, null, 2), { mode: 0o600 });
}

export const AuthStorage = {
	create(authPath = defaultAuthPath()) {
		return {
			async modify(id, fn) {
				const record = readRecord(authPath);
				record[id] = await fn(record[id]);
				writeRecord(authPath, record);

				return record[id];
			},
			async delete(id) {
				const record = readRecord(authPath);
				delete record[id];
				writeRecord(authPath, record);
			},
		};
	},
};
