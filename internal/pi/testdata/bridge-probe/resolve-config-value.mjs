export function resolveConfigValue(value, env) {
	if (value.startsWith("!")) throw new Error("credential command must never run");
	return value.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_, name) => env?.[name] || process.env[name] || "");
}
