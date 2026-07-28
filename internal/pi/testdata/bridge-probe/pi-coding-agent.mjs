// Stub of the pi extension package the bridge extension imports. Only the two
// value exports the bridge uses are provided; the probe never reaches the
// credential-storage path.
export function getPackageDir() {
	return process.env.PROBE_PACKAGE_DIR ?? "/nonexistent";
}

export function readStoredCredential() {
	return undefined;
}
