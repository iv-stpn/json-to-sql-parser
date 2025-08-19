import { v4 as uuidv4 } from "uuid";
import type { castTypes } from "../constants/cast-types";
import { isNotNull } from "../utils";
import { removeAllWrappingParens } from "../utils/function-call";
import { parseDate, parseTimestamp } from "../utils/parse-values";
import { ensureBoolean, ensureDateString, ensureNumber, ensureText, ensureTimestampString } from "../utils/validators";

type CastTypeToJSType<T extends (typeof castTypes)[number]> = T extends "BOOLEAN"
	? boolean
	: T extends "FLOAT"
		? number
		: T extends "TEXT"
			? string
			: T extends "UUID"
				? { $uuid: string }
				: T extends "TIMESTAMP"
					? { $timestamp: string }
					: T extends "DATE"
						? { $date: string }
						: T extends "JSONB"
							? { $jsonb: Record<string, unknown> }
							: unknown;

export type FunctionDefinition<TReturn extends (typeof castTypes)[number] = (typeof castTypes)[number]> = {
	name: string;
	argumentTypes: ((typeof castTypes)[number] | "ANY")[];
	variadic?: boolean;
	returnType: TReturn;
	toSQL?: (args: string[]) => string;
	toJS: (...args: unknown[]) => CastTypeToJSType<TReturn> | null;
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

// Individual function definitions
const authUidFunction: FunctionDefinition<"UUID"> = {
	name: "AUTH.UID",
	argumentTypes: [],
	returnType: "UUID",
	toSQL: () => "auth.uid()",
	toJS: () => {
		throw new Error("AUTH.UID function cannot be called in JavaScript");
	},
	// Note: AUTH.UID is a database-specific function, no JS implementation
};

// Logical functions (9.1 / https://www.postgresql.org/docs/current/functions-logical.html)
const andFunction: FunctionDefinition<"BOOLEAN"> = {
	name: "AND",
	argumentTypes: ["BOOLEAN", "BOOLEAN"],
	returnType: "BOOLEAN",
	toSQL: (args) => `${args[0]} AND ${args[1]}`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("AND function expects exactly 2 arguments");
		ensureBoolean(args[0]);
		ensureBoolean(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] && args[1];
	},
};

const orFunction: FunctionDefinition<"BOOLEAN"> = {
	name: "OR",
	argumentTypes: ["BOOLEAN", "BOOLEAN"],
	returnType: "BOOLEAN",
	toSQL: (args) => `${args[0]} OR ${args[1]}`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("OR function expects exactly 2 arguments");
		ensureBoolean(args[0]);
		ensureBoolean(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] || args[1];
	},
};

const notFunction: FunctionDefinition<"BOOLEAN"> = {
	name: "NOT",
	argumentTypes: ["BOOLEAN"],
	returnType: "BOOLEAN",
	toSQL: (args) => `NOT (${removeAllWrappingParens(`${args[0]}`)})`,
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("NOT function expects exactly 1 argument");
		ensureBoolean(args[0]);
		if (args[0] === null) return null;
		return !args[0];
	},
};

// Mathematical functions (9.3 / https://www.postgresql.org/docs/current/functions-math.html)
const addFunction: FunctionDefinition<"FLOAT"> = {
	name: "ADD",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => `(${args[0]} + ${args[1]})`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("ADD function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] + args[1];
	},
};

const subtractFunction: FunctionDefinition<"FLOAT"> = {
	name: "SUBTRACT",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => `(${args[0]} - ${args[1]})`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("SUBTRACT function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] - args[1];
	},
};

const multiplyFunction: FunctionDefinition<"FLOAT"> = {
	name: "MULTIPLY",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => `(${args[0]} * ${args[1]})`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("MULTIPLY function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] * args[1];
	},
};

const divideFunction: FunctionDefinition<"FLOAT"> = {
	name: "DIVIDE",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => {
		if (Number(args[1]) === 0) throw new Error("Division by zero is not allowed");
		return `(${args[0]} / ${args[1]})`;
	},
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("DIVIDE function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		if (args[1] === 0) throw new Error("Division by zero is not allowed");
		return args[0] / args[1];
	},
};

const modFunction: FunctionDefinition<"FLOAT"> = {
	name: "MOD",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => `(${args[0]} % ${args[1]})`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("MOD function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] % args[1];
	},
};

const powFunction: FunctionDefinition<"FLOAT"> = {
	name: "POW",
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	toSQL: (args) => `${args[0]} ^ ${args[1]}`,
	toJS: (...args) => {
		if (args.length !== 2) throw new Error("POW function expects exactly 2 arguments");
		ensureNumber(args[0]);
		ensureNumber(args[1]);
		if (args[0] === null || args[1] === null) return null;
		return args[0] ** args[1];
	},
};

const absFunction: FunctionDefinition<"FLOAT"> = {
	name: "ABS",
	argumentTypes: ["FLOAT"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("ABS function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.abs(args[0]);
	},
};

const sqrtFunction: FunctionDefinition<"FLOAT"> = {
	name: "SQRT",
	argumentTypes: ["FLOAT"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("SQRT function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.sqrt(args[0]);
	},
};

const ceilFunction: FunctionDefinition<"FLOAT"> = {
	name: "CEIL",
	argumentTypes: ["FLOAT"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("CEIL function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.ceil(args[0]);
	},
};

const floorFunction: FunctionDefinition<"FLOAT"> = {
	name: "FLOOR",
	argumentTypes: ["FLOAT"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("FLOOR function expects exactly 1 argument");
		ensureNumber(args[0]);
		if (args[0] === null) return null;
		return Math.floor(args[0]);
	},
};

// String functions (9.4 / https://www.postgresql.org/docs/current/functions-string.html)
const upperFunction: FunctionDefinition<"TEXT"> = {
	name: "UPPER",
	argumentTypes: ["TEXT"],
	returnType: "TEXT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("UPPER function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].toUpperCase();
	},
};

const lowerFunction: FunctionDefinition<"TEXT"> = {
	name: "LOWER",
	argumentTypes: ["TEXT"],
	returnType: "TEXT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("LOWER function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].toLowerCase();
	},
};

const lengthFunction: FunctionDefinition<"FLOAT"> = {
	name: "LENGTH",
	argumentTypes: ["TEXT"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("LENGTH function expects exactly 1 argument");
		ensureText(args[0]);
		if (args[0] === null) return null;
		return args[0].length;
	},
};

const concatFunction: FunctionDefinition<"TEXT"> = {
	name: "CONCAT",
	argumentTypes: ["TEXT", "TEXT"],
	returnType: "TEXT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("CONCAT function expects at least 2 arguments");
		ensureAllText(args);
		// If any argument is null, return null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.join("");
	},
};

const substringFunction: FunctionDefinition<"TEXT"> = {
	name: "SUBSTRING",
	argumentTypes: ["TEXT", "FLOAT", "FLOAT"],
	returnType: "TEXT",
	toJS: (...args) => {
		if (args.length !== 3) throw new Error("SUBSTRING function expects exactly 3 arguments");
		ensureText(args[0]);
		ensureNumber(args[1]);
		ensureNumber(args[2]);
		if (args[0] === null || args[1] === null || args[2] === null) return null;
		// PostgreSQL SUBSTRING is 1-indexed, JavaScript is 0-indexed
		return args[0].substring(args[1] - 1, args[1] - 1 + args[2]);
	},
};

const replaceFunction: FunctionDefinition<"TEXT"> = {
	name: "REPLACE",
	argumentTypes: ["TEXT", "TEXT", "TEXT"],
	returnType: "TEXT",
	toJS: (...args) => {
		if (args.length !== 3) throw new Error("REPLACE function expects exactly 3 arguments");
		ensureText(args[0]);
		ensureText(args[1]);
		ensureText(args[2]);
		if (args[0] === null || args[1] === null || args[2] === null) return null;
		return args[0].replace(new RegExp(args[1], "g"), args[2]);
	},
};

// Date/time functions (9.9 / https://www.postgresql.org/docs/current/functions-datetime.html)
const nowFunction: FunctionDefinition<"TIMESTAMP"> = {
	name: "NOW",
	argumentTypes: [],
	returnType: "TIMESTAMP",
	toJS: (...args) => {
		if (args.length > 0) throw new Error("NOW function expects exactly 0 arguments");
		const now = new Date();
		return { $timestamp: now.toISOString() };
	},
};

const currentDateFunction: FunctionDefinition<"DATE"> = {
	name: "CURRENT_DATE",
	argumentTypes: [],
	returnType: "DATE",
	toJS: (...args) => {
		if (args.length > 0) throw new Error("CURRENT_DATE function expects exactly 0 arguments");
		const date = new Date();
		return {
			$date: `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, "0")}-${date.getDate().toString().padStart(2, "0")}`,
		};
	},
};

const extractYearFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_YEAR",
	toSQL: (args: string[]) => `EXTRACT(YEAR FROM ${args[0]})`,
	argumentTypes: ["DATE"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_YEAR function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getFullYear();
	},
};

const extractMonthFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_MONTH",
	toSQL: (args: string[]) => `EXTRACT(MONTH FROM ${args[0]})`,
	argumentTypes: ["DATE"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_MONTH function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getMonth() + 1; // JavaScript months are 0-indexed
	},
};

const extractDayFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_DAY",
	toSQL: (args: string[]) => `EXTRACT(DAY FROM ${args[0]})`,
	argumentTypes: ["DATE"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_DAY function expects exactly 1 argument");
		ensureDateString(args[0]);
		if (args[0] === null) return null;
		const date = parseDate(args[0]);
		return date.getDate();
	},
};

const extractHourFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_HOUR",
	toSQL: (args: string[]) => `EXTRACT(HOUR FROM ${args[0]})`,
	argumentTypes: ["TIMESTAMP"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_HOUR function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return date.getHours();
	},
};

const extractMinuteFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_MINUTE",
	toSQL: (args: string[]) => `EXTRACT(MINUTE FROM ${args[0]})`,
	argumentTypes: ["TIMESTAMP"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_MINUTE function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return date.getMinutes();
	},
};

const extractEpochFunction: FunctionDefinition<"FLOAT"> = {
	name: "EXTRACT_EPOCH",
	toSQL: (args: string[]) => `EXTRACT(EPOCH FROM ${args[0]})`,
	argumentTypes: ["TIMESTAMP"],
	returnType: "FLOAT",
	toJS: (...args) => {
		if (args.length !== 1) throw new Error("EXTRACT_EPOCH function expects exactly 1 argument");
		ensureTimestampString(args[0]);
		if (args[0] === null) return null;
		const date = parseTimestamp(args[0]);
		return Math.floor(date.getTime() / 1000);
	},
};

// UUID functions (9.14 / https://www.postgresql.org/docs/current/functions-uuid.html)
const genRandomUuidFunction: FunctionDefinition<"UUID"> = {
	name: "GEN_RANDOM_UUID",
	argumentTypes: [],
	returnType: "UUID",
	toJS: (...args) => {
		if (args.length > 0) throw new Error("GEN_RANDOM_UUID function expects exactly 0 arguments");
		return { $uuid: uuidv4() };
	},
};

// Conditional expressions (9.18 / https://www.postgresql.org/docs/current/functions-conditional.html)
const greatestStringFunction: FunctionDefinition<"TEXT"> = {
	name: "GREATEST_STRING",
	toSQL: (args: string[]) => `GREATEST(${args.join(", ")})`,
	argumentTypes: ["TEXT", "TEXT"],
	returnType: "TEXT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("GREATEST_STRING function expects at least 2 arguments");
		ensureAllText(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.reduce((max, current) => (current && current > max! ? current : max));
	},
};

const greatestNumberFunction: FunctionDefinition<"FLOAT"> = {
	name: "GREATEST_NUMBER",
	toSQL: (args: string[]) => `GREATEST(${args.join(", ")})`,
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("GREATEST_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (!args.every(isNotNull)) return null;
		return Math.max(...args);
	},
};

const leastStringFunction: FunctionDefinition<"TEXT"> = {
	name: "LEAST_STRING",
	toSQL: (args: string[]) => `LEAST(${args.join(", ")})`,
	argumentTypes: ["TEXT", "TEXT"],
	returnType: "TEXT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("LEAST_STRING function expects at least 2 arguments");
		ensureAllText(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (args.some((arg) => arg === null)) return null;
		return args.reduce((min, current) => (current && current < min! ? current : min));
	},
};

const leastNumberFunction: FunctionDefinition<"FLOAT"> = {
	name: "LEAST_NUMBER",
	toSQL: (args: string[]) => `LEAST(${args.join(", ")})`,
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("LEAST_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		// Handle null values - return null if any arg is null (SQL behavior)
		if (!args.every(isNotNull)) return null;
		return Math.min(...args);
	},
};

const coalesceStringFunction: FunctionDefinition<"TEXT"> = {
	name: "COALESCE_STRING",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["TEXT", "TEXT"],
	returnType: "TEXT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("COALESCE_STRING function expects at least 2 arguments");
		ensureAllText(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

const coalesceNumberFunction: FunctionDefinition<"FLOAT"> = {
	name: "COALESCE_NUMBER",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["FLOAT", "FLOAT"],
	returnType: "FLOAT",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("COALESCE_NUMBER function expects at least 2 arguments");
		ensureAllNumbers(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

const coalesceBooleanFunction: FunctionDefinition<"BOOLEAN"> = {
	name: "COALESCE_BOOLEAN",
	toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
	argumentTypes: ["BOOLEAN", "BOOLEAN"],
	returnType: "BOOLEAN",
	variadic: true,
	toJS: (...args) => {
		if (args.length < 2) throw new Error("COALESCE_BOOLEAN function expects at least 2 arguments");
		ensureAllBooleans(args);
		return args.find((arg) => arg !== null) ?? null;
	},
};

// Combined functions array
const functions = [
	authUidFunction,
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
