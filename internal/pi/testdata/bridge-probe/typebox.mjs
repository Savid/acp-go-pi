// Stub of the schema builder the bridge extension uses to declare its question
// tool. The probe never inspects a schema.
export const Type = {
	Object: (value) => value,
	String: () => "string",
	Array: (value) => value,
	Optional: (value) => value,
};
