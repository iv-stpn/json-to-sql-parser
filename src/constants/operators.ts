import { applyFunction } from "../utils/function-call";

// SQL functions and operator definitions for $expr
export const unaryOperators = ["ABS", "UPPER", "LOWER", "LENGTH", "NOT", "SQRT", "CEIL", "FLOOR", "YEAR"] as const;
export const variableFunctions = ["COALESCE", "GREATEST", "LEAST", "CONCAT", "SUBSTRING"] as const;
export const binaryOperators = { ADD: "+", SUBTRACT: "-", MULTIPLY: "*", DIVIDE: "/", MOD: "%", POW: "^", AND: "AND", OR: "OR" };

type Operator = (typeof unaryOperators)[number] | (typeof variableFunctions)[number] | keyof typeof binaryOperators;

export const operatorReturnTypes: Record<Operator, CastType> = {
	ABS: "FLOAT",
	UPPER: "TEXT",
	LOWER: "TEXT",
	LENGTH: "FLOAT",
	SQRT: "FLOAT",
	CEIL: "FLOAT",
	FLOOR: "FLOAT",
	YEAR: "FLOAT",
	NOT: "BOOLEAN",
	ADD: "FLOAT",
	SUBTRACT: "FLOAT",
	MULTIPLY: "FLOAT",
	DIVIDE: "FLOAT",
	MOD: "FLOAT",
	POW: "FLOAT",
	AND: "BOOLEAN",
	OR: "BOOLEAN",
	COALESCE: "TEXT",
	GREATEST: "TEXT",
	LEAST: "TEXT",
	CONCAT: "TEXT",
	SUBSTRING: "TEXT",
};

export const isOperator = (value: string): value is Operator => value in operatorReturnTypes;

// Aggregation operators for SQL queries
export const aggregationOperators = [
	"COUNT",
	"SUM",
	"AVG",
	"MIN",
	"MAX",
	"COUNT_DISTINCT",
	"STRING_AGG",
	"STDDEV",
	"VARIANCE",
] as const;

export type AggregationOperator = (typeof aggregationOperators)[number];

export function applyAggregationOperator(field: string, operator: AggregationOperator): string {
	if (!aggregationOperators.includes(operator)) throw new Error(`Invalid aggregation operator: ${operator}`);
	if (operator === "COUNT_DISTINCT") return `COUNT(DISTINCT ${field})`;
	if (operator === "STRING_AGG") return applyFunction("STRING_AGG", [field, "','"]);
	return applyFunction(operator, [field]);
}

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
export type CastType = (typeof castMap)[keyof typeof castMap] | null;
export type FieldType = keyof typeof castMap;
