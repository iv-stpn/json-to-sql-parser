// Cast types for SQL queries
export const baseCastMap = {
	string: "TEXT",
	number: "FLOAT",
	boolean: "BOOLEAN",
	object: "JSONB",
	date: "DATE",
	datetime: "TIMESTAMP",
	uuid: "UUID",
} as const;
export const castTypes = Object.values(baseCastMap);

export type CastType = (typeof baseCastMap)[keyof typeof baseCastMap] | null;
export type FieldType = keyof typeof baseCastMap;
