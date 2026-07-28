// Stub of pi's provider catalog carrying two OAuth logins. "probe" is the
// device-code shape three of pi's seven oauth providers have: the login
// notifies its presentation once and then polls the provider to completion,
// returning control to the extension only when the provider settles or the
// login aborts. "settle" is the shape that reaches the end of a login and hands
// back a credential, which is what puts the extension on its storage write
// path.
export function builtinProviders() {
	return [
		{
			id: "settle",
			name: "Settle",
			auth: {
				oauth: {
					name: "Settle OAuth",
					loginLabel: "Settle login",
					async login() {
						return { type: "oauth", access: "settled-access", refresh: "settled-refresh", expires: 4242 };
					},
				},
				apiKey: null,
			},
		},
		{
			id: "probe",
			name: "Probe",
			auth: {
				oauth: {
					name: "Probe OAuth",
					loginLabel: "Probe device login",
					async login(interaction) {
						interaction.notify({
							type: "device_code",
							userCode: "AAAA-BBBB",
							verificationUri: "https://probe.test/device",
							intervalSeconds: 5,
							expiresInSeconds: 600,
						});

						await new Promise((_resolve, reject) => {
							interaction.signal?.addEventListener("abort", () => reject(new Error("login aborted")), {
								once: true,
							});
						});

						return { type: "oauth", access: "a", refresh: "r", expires: 1 };
					},
				},
				apiKey: null,
			},
		},
	];
}
