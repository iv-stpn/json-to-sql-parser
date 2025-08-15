// Cast types for SQL queries
export const castMap = {
	string: "TEXT",
	number: "FLOAT",
	boolean: "BOOLEAN",
	object: "JSONB",
	date: "DATE",
	datetime: "TIMESTAMP",
	uuid: "UUID",
} as const;
export const castTypes = Object.values(castMap);

export type CastType = (typeof castMap)[keyof typeof castMap] | null;
export type FieldType = keyof typeof castMap;
