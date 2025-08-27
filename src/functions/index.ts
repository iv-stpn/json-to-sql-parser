import { v4 as uuidv4 } from "uuid";
import type { ExpressionType, FieldType } from "../constants/cast-types";
import { Dialect } from "../constants/dialects";
import { MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR } from "../constants/errors";
import { isNotNull } from "../utils";
import { removeAllWrappingParens } from "../utils/function-call";
import { parseDate, parseTimestamp } from "../utils/parse-values";
import { ensureBoolean, ensureDateString, ensureNumber, ensureText, ensureTimestampString } from "../utils/validators";

type FieldTypeToJSType<T extends FieldType> = T extends "boolean"
	? boolean
	: T extends "number"
		? number
		: T extends "string"
			? string
			: T extends "uuid"
				? { $uuid: string }
				: T extends "datetime"
					? { $timestamp: string }
					: T extends "date"
						? { $date: string }
						: T extends "object"
							? { $jsonb: Record<string, unknown> }
							: unknown;

export type FunctionDefinition<TReturn extends FieldType = FieldType> = {
	name: string;
	argumentTypes: ExpressionType[];
	variadic?: boolean;
	returnType: TReturn;
	toSQL?: (args: string[], dialect: Dialect) => string;
	toJS: (args: unknown[]) => FieldTypeToJSType<TReturn> | null;
};

// Variadic ensure functions to avoid type casting
function ensureAllText(args: unknown[]): asserts args is (string | null)[] {
	for (const argument of args) ensureText(argument);
}

function ensureAllNumbers(args: unknown[]): asserts args is (number | null)[] {
	for (const argument of args) ensureNumber(argument);
}

function ensureAllBooleans(args: unknown[]): asserts args is (boolean | null)[] {
	for (const argument of args) ensureBoolean(argument);
}

// Logical functions (9.1 / https://www.postgresql.org/docs/current/functions-logical.html)
const andFunction: FunctionDefinition<"boolean"> = {
	name: "AND",
	argumentTypes: ["boolean", "boolean"],
	returnType: "boolean",
	toSQL: (args) => `(${args[0]} AND ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("AND function expects exactly 2 arguments");
		ensureBoolean(args[0]);
		ensureBoolean(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] && args[1];
	},
};

const orFunction: FunctionDefinition<"boolean"> = {
	name: "OR",
	argumentTypes: ["boolean", "boolean"],
	returnType: "boolean",
	toSQL: (args) => `(${args[0]} OR ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("OR function expects exactly 2 arguments");
		ensureBoolean(args[0]);
		ensureBoolean(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] || args[1];
	},
};

const notFunction: FunctionDefinition<"boolean"> = {
	name: "NOT",
	argumentTypes: ["boolean"],
	returnType: "boolean",
	toSQL: (args) => `NOT (${removeAllWrappingParens(`${args[0]}`)})`,
	toJS: (args) => {
		if (args.length !== 1) throw new Error("NOT function expects exactly 1 argument");
		ensureBoolean(args[0]);
		if (args[0] === null) return null;
		return !args[0];
	},
};

// Mathematical functions (9.3 / https://www.postgresql.org/docs/current/functions-math.html)
const addFunction: FunctionDefinition<"number"> = {
	name: "ADD",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args) => `(${args[0]} + ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("ADD function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] + args[1];
	},
};

const subtractFunction: FunctionDefinition<"number"> = {
	name: "SUBTRACT",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args) => `(${args[0]} - ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("SUBTRACT function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] - args[1];
	},
};

const multiplyFunction: FunctionDefinition<"number"> = {
	name: "MULTIPLY",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args) => `(${args[0]} * ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("MULTIPLY function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] * args[1];
	},
};

const divideFunction: FunctionDefinition<"number"> = {
	name: "DIVIDE",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args) => {
		if (Number(args[1]) === 0) throw new Error("Division by zero is not allowed");
		return `(${args[0]} / ${args[1]})`;
	},
	toJS: (args) => {
		if (args.length !== 2) throw new Error("DIVIDE function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		if (args[1] === 0) throw new Error("Division by zero is not allowed");
		return args[0] / args[1];
	},
};

const modFunction: FunctionDefinition<"number"> = {
	name: "MOD",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args) => `(${args[0]} % ${args[1]})`,
	toJS: (args) => {
		if (args.length !== 2) throw new Error("MOD function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] % args[1];
	},
};

const absFunction: FunctionDefinition<"number"> = {
	name: "ABS",
	argumentTypes: ["number"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("ABS function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.abs(args[0]);
	},
};

const powFunction: FunctionDefinition<"number"> = {
	name: "POW",
	argumentTypes: ["number", "number"],
	returnType: "number",
	toSQL: (args, dialect) => {
		if (dialect === "sqlite-3.44-minimal") throw new Error(MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR("POW", dialect));
		return `POW(${args[0]}, ${args[1]})`;
	},
	toJS: (args) => {
		if (args.length !== 2) throw new Error("POW function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] ** args[1];
	},
};

const sqrtFunction: FunctionDefinition<"number"> = {
	name: "SQRT",
	argumentTypes: ["number"],
	returnType: "number",
	toSQL: (args, dialect) => {
		if (dialect === "sqlite-3.44-minimal")
			throw new Error(MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR("SQRT", dialect));
		return `SQRT(${args[0]})`;
	},
	toJS: (args) => {
		if (args.length !== 1) throw new Error("SQRT function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		if (args[0] < 0) throw new Error("Square root of a negative number is not allowed");
		return Math.sqrt(args[0]);
	},
};

const ceilFunction: FunctionDefinition<"number"> = {
	name: "CEIL",
	argumentTypes: ["number"],
	returnType: "number",
	toSQL: (args, dialect) => {
		if (dialect === "sqlite-3.44-minimal")
			throw new Error(MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR("CEIL", dialect));
		return `CEIL(${args[0]})`;
	},
	toJS: (args) => {
		if (args.length !== 1) throw new Error("CEIL function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.ceil(args[0]);
	},
};

const floorFunction: FunctionDefinition<"number"> = {
	name: "FLOOR",
	argumentTypes: ["number"],
	returnType: "number",
	toSQL: (args, dialect) => {
		if (dialect === "sqlite-3.44-minimal")
			throw new Error(MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR("FLOOR", dialect));
		return `FLOOR(${args[0]})`;
	},
	toJS: (args) => {
		if (args.length !== 1) throw new Error("FLOOR function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.floor(args[0]);
	},
};

// String functions (9.4 / https://www.postgresql.org/docs/current/functions-string.html)
const upperFunction: FunctionDefinition<"string"> = {
	name: "UPPER",
	argumentTypes: ["string"],
	returnType: "string",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("UPPER function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].toUpperCase();
	},
};

const lowerFunction: FunctionDefinition<"string"> = {
	name: "LOWER",
	argumentTypes: ["string"],
	returnType: "string",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("LOWER function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].toLowerCase();
	},
};

const lengthFunction: FunctionDefinition<"number"> = {
	name: "LENGTH",
	argumentTypes: ["string"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("LENGTH function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].length;
	},
};

const concatFunction: FunctionDefinition<"string"> = {
	name: "CONCAT",
	argumentTypes: ["string", "string"],
	returnType: "string",
	variadic: true,
	toSQL: (args) => `(${args.join(" || ")})`,
	toJS: (args) => {
		if (args.length < 2) throw new Error("CONCAT function expects at least 2 arguments");
		ensureAllText(args);
		// If any argument is null, return null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.join("");
	},
};

const substringFunction: FunctionDefinition<"string"> = {
	name: "SUBSTR",
	argumentTypes: ["string", "number", "number"],
	returnType: "string",
	toJS: (args) => {
		if (args.length !== 3) throw new Error("SUBSTR function expects exactly 3 arguments");
		ensureText(args[0]);
		ensureNumber(args[1]);
		ensureNumber(args[2]);
		if (args[0] === null || args[1] === null || args[2] === null) return null;
		// PostgreSQL SUBSTRING is 1-indexed, JavaScript is 0-indexed
		return args[0].substring(args[1] - 1, args[1] - 1 + args[2]);
	},
};

const replaceFunction: FunctionDefinition<"string"> = {
	name: "REPLACE",
	argumentTypes: ["string", "string", "string"],
	returnType: "string",
	toJS: (args) => {
		if (args.length !== 3) throw new Error("REPLACE function expects exactly 3 arguments");
		ensureText(args[0]);
		ensureText(args[1]);
		ensureText(args[2]);
		if (args[0] === null || args[1] === null || args[2] === null) return null;
		return args[0].replace(new RegExp(args[1], "g"), args[2]);
	},
};

// Date/time functions (9.9 / https://www.postgresql.org/docs/current/functions-datetime.html)
const nowFunction: FunctionDefinition<"datetime"> = {
	name: "NOW",
	argumentTypes: [],
	returnType: "datetime",
	toSQL: (_, dialect) => {
		if (dialect === Dialect.POSTGRESQL) return "NOW()";
		if (dialect === Dialect.SQLITE_MINIMAL) return "DATETIME()";
		return "DATETIME('now', 'subsec')";
	},
	toJS: (args) => {
		if (args.length > 0) throw new Error("NOW function expects exactly 0 arguments");
		const now = new Date();
		return { $timestamp: now.toISOString().replace("Z", "") };
	},
};

const currentDateFunction: FunctionDefinition<"date"> = {
	name: "CURRENT_DATE",
	argumentTypes: [],
	returnType: "date",
	toSQL: (_, dialect) => {
		if (dialect === Dialect.POSTGRESQL) return "CURRENT_DATE";
		return "DATE()";
	},
	toJS: (args) => {
		if (args.length > 0) throw new Error("CURRENT_DATE function expects exactly 0 arguments");
		const date = new Date();

		const month = (date.getMonth() + 1).toString().padStart(2, "0");
		const day = date.getDate().toString().padStart(2, "0");
		return { $date: `${date.getFullYear()}-${month}-${day}` };
	},
};

const extractYearFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_YEAR",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(YEAR FROM ${args[0]})`;
		return `CAST(STRFTIME('%Y', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["date"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_YEAR function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getFullYear();
	},
};

const extractMonthFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_MONTH",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(MONTH FROM ${args[0]})`;
		return `CAST(STRFTIME('%m', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["date"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_MONTH function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getMonth() + 1; // JavaScript months are 0-indexed
	},
};

const extractDayFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_DAY",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(DAY FROM ${args[0]})`;
		return `CAST(STRFTIME('%d', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["date"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_DAY function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getDate();
	},
};

const extractHourFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_HOUR",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(HOUR FROM ${args[0]})`;
		return `CAST(STRFTIME('%H', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["datetime"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_HOUR function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return date.getHours();
	},
};

const extractMinuteFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_MINUTE",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(MINUTE FROM ${args[0]})`;
		return `CAST(STRFTIME('%M', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["datetime"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_MINUTE function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return date.getMinutes();
	},
};

const extractEpochFunction: FunctionDefinition<"number"> = {
	name: "EXTRACT_EPOCH",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `EXTRACT(EPOCH FROM ${args[0]})`;
		return `CAST(STRFTIME('%s', ${args[0]}) AS INTEGER)`;
	},
	argumentTypes: ["datetime"],
	returnType: "number",
	toJS: (args) => {
		if (args.length !== 1) throw new Error("EXTRACT_EPOCH function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return Math.floor(date.getTime() / 1000);
	},
};

// UUID functions (9.14 / https://www.postgresql.org/docs/current/functions-uuid.html)
const genRandomUuidFunction: FunctionDefinition<"uuid"> = {
	name: "GEN_RANDOM_UUID",
	argumentTypes: [],
	returnType: "uuid",
	toSQL: (_, dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return "GEN_RANDOM_UUID()";
		throw new Error(`GEN_RANDOM_UUID function is not supported in ${dialect}`);
	},
	toJS: (args) => {
		if (args.length > 0) throw new Error("GEN_RANDOM_UUID function expects exactly 0 arguments");
		return { $uuid: uuidv4() };
	},
};

// Conditional expressions (9.18 / https://www.postgresql.org/docs/current/functions-conditional.html)
const greatestStringFunction: FunctionDefinition<"string"> = {
	name: "GREATEST_STRING",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `GREATEST(${args.join(", ")})`;
		return `MAX(${args.join(", ")})`;
	},
	argumentTypes: ["string", "string"],
	returnType: "string",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("GREATEST_STRING function expects at least 2 arguments");
		ensureAllText(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.reduce((max, current) => (current && current > max! ? current : max));
	},
};

const greatestNumberFunction: FunctionDefinition<"number"> = {
	name: "GREATEST_NUMBER",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `GREATEST(${args.join(", ")})`;
		return `MAX(${args.join(", ")})`;
	},
	argumentTypes: ["number", "number"],
	returnType: "number",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("GREATEST_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (!args.every(isNotNull)) return null;
		return Math.max(...args);
	},
};

const leastStringFunction: FunctionDefinition<"string"> = {
	name: "LEAST_STRING",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `LEAST(${args.join(", ")})`;
		return `MIN(${args.join(", ")})`;
	},
	argumentTypes: ["string", "string"],
	returnType: "string",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("LEAST_STRING function expects at least 2 arguments");
		ensureAllText(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.reduce((min, current) => (current && current < min! ? current : min));
	},
};

const leastNumberFunction: FunctionDefinition<"number"> = {
	name: "LEAST_NUMBER",
	toSQL: (args: string[], dialect: Dialect) => {
		if (dialect === Dialect.POSTGRESQL) return `LEAST(${args.join(", ")})`;
		return `MIN(${args.join(", ")})`;
	},
	argumentTypes: ["number", "number"],
	returnType: "number",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("LEAST_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (!args.every(isNotNull)) return null;
		return Math.min(...args);
	},
};

const coalesceStringFunction: FunctionDefinition<"string"> = {
	name: "COALESCE_STRING",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["string", "string"],
	returnType: "string",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("COALESCE_STRING function expects at least 2 arguments");
		ensureAllText(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

const coalesceNumberFunction: FunctionDefinition<"number"> = {
	name: "COALESCE_NUMBER",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["number", "number"],
	returnType: "number",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("COALESCE_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

const coalesceBooleanFunction: FunctionDefinition<"boolean"> = {
	name: "COALESCE_BOOLEAN",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["boolean", "boolean"],
	returnType: "boolean",
	variadic: true,
	toJS: (args) => {
		if (args.length < 2) throw new Error("COALESCE_BOOLEAN function expects at least 2 arguments");
		ensureAllBooleans(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

// Combined functions array
const functions = [
	andFunction,
	orFunction,
	notFunction,
	addFunction,
	subtractFunction,
	multiplyFunction,
	divideFunction,
	modFunction,
	powFunction,
	absFunction,
	sqrtFunction,
	ceilFunction,
	floorFunction,
	upperFunction,
	lowerFunction,
	lengthFunction,
	concatFunction,
	substringFunction,
	replaceFunction,
	nowFunction,
	currentDateFunction,
	extractYearFunction,
	extractMonthFunction,
	extractDayFunction,
	extractHourFunction,
	extractMinuteFunction,
	extractEpochFunction,
	genRandomUuidFunction,
	greatestStringFunction,
	greatestNumberFunction,
	leastStringFunction,
	leastNumberFunction,
	coalesceStringFunction,
	coalesceNumberFunction,
	coalesceBooleanFunction,
] satisfies FunctionDefinition[];

export const functionNames = functions.map(({ name }) => name);
export const allowedFunctions: FunctionDefinition[] = functions;
