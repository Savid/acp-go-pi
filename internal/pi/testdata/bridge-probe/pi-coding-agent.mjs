// Stub of the pi extension package the bridge extension imports. Only the two
// value exports the bridge uses are provided. One provider is stored and one is
// not, which is what makes the probe's answer shape observable.
export function getAgentDir() {
	return process.env.PI_CODING_AGENT_DIR ?? "/nonexistent";
}

export function getPackageDir() {
	return process.env.PROBE_PACKAGE_DIR ?? "/nonexistent";
}

export function readStoredCredential(providerId) {
	return providerId === "probe" ? { type: "api_key" } : undefined;
}
