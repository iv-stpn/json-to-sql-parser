import { z } from "zod";
import { removeAllWrappingParens } from "../utils/function-call";
import { castTypes } from "./operators";

export const argumentDefinitionSchema = z.object({ type: z.enum(castTypes), variadic: z.boolean().optional() });
export type ArgumentDefinition = z.infer<typeof argumentDefinitionSchema>;

export const functionDefinitionSchema = z.object({
	name: z.string(),
	argumentTypes: z.array(z.enum(castTypes).or(z.literal("ANY"))),
	returnType: z.enum(castTypes),
	variadic: z.boolean().optional(),
});
export type FunctionDefinition = z.infer<typeof functionDefinitionSchema> & { toSQL?: (args: string[]) => string };

export const allowedFunctions: FunctionDefinition[] = [
	{
		name: "AUTH.UID",
		argumentTypes: [],
		returnType: "UUID",
		toSQL: () => "auth.uid()",
	},
	// Logical functions (9.1 / https://www.postgresql.org/docs/current/functions-logical.html)
	{
		name: "AND",
		argumentTypes: ["BOOLEAN", "BOOLEAN"],
		returnType: "BOOLEAN",
		toSQL: (args) => `${args[0]} AND ${args[1]}`,
	},
	{
		name: "OR",
		argumentTypes: ["BOOLEAN", "BOOLEAN"],
		returnType: "BOOLEAN",
		toSQL: (args) => `${args[0]} OR ${args[1]}`,
	},
	{
		name: "NOT",
		argumentTypes: ["BOOLEAN"],
		returnType: "BOOLEAN",
		toSQL: (args) => `NOT (${removeAllWrappingParens(`${args[0]}`)})`,
	},
	// Mathematical functions (9.3 / https://www.postgresql.org/docs/current/functions-math.html)
	{
		name: "ADD",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => `(${args[0]} + ${args[1]})`,
	},
	{
		name: "SUBTRACT",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => `(${args[0]} - ${args[1]})`,
	},
	{
		name: "MULTIPLY",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => `(${args[0]} * ${args[1]})`,
	},
	{
		name: "DIVIDE",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => {
			if (Number(args[1]) === 0) throw new Error("Division by zero is not allowed");
			return `(${args[0]} / ${args[1]})`;
		},
	},
	{
		name: "MOD",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => `(${args[0]} % ${args[1]})`,
	},
	{
		name: "POW",
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		toSQL: (args) => `${args[0]} ^ ${args[1]}`,
	},
	{
		name: "ABS",
		argumentTypes: ["FLOAT"],
		returnType: "FLOAT",
	},
	{
		name: "SQRT",
		argumentTypes: ["FLOAT"],
		returnType: "FLOAT",
	},
	{
		name: "CEIL",
		argumentTypes: ["FLOAT"],
		returnType: "FLOAT",
	},
	{
		name: "FLOOR",
		argumentTypes: ["FLOAT"],
		returnType: "FLOAT",
	},
	// String functions (9.4 / https://www.postgresql.org/docs/current/functions-string.html)
	{
		name: "UPPER",
		argumentTypes: ["TEXT"],
		returnType: "TEXT",
	},
	{
		name: "LOWER",
		argumentTypes: ["TEXT"],
		returnType: "TEXT",
	},
	{
		name: "LENGTH",
		argumentTypes: ["TEXT"],
		returnType: "FLOAT",
	},
	{
		name: "CONCAT",
		argumentTypes: ["TEXT", "TEXT"],
		returnType: "TEXT",
		variadic: true,
	},
	{
		name: "SUBSTRING",
		argumentTypes: ["TEXT", "FLOAT", "FLOAT"],
		returnType: "TEXT",
	},
	{
		name: "REPLACE",
		argumentTypes: ["TEXT", "TEXT", "TEXT"],
		returnType: "TEXT",
	},
	// Date/time functions (9.9 / https://www.postgresql.org/docs/current/functions-datetime.html)
	{
		name: "NOW",
		argumentTypes: [],
		returnType: "TIMESTAMP",
	},
	{
		name: "CURRENT_DATE",
		argumentTypes: [],
		returnType: "DATE",
	},
	{
		name: "EXTRACT_YEAR",
		toSQL: (args: string[]) => `EXTRACT(YEAR FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	{
		name: "EXTRACT_MONTH",
		toSQL: (args: string[]) => `EXTRACT(MONTH FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	{
		name: "EXTRACT_DAY",
		toSQL: (args: string[]) => `EXTRACT(DAY FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	{
		name: "EXTRACT_HOUR",
		toSQL: (args: string[]) => `EXTRACT(HOUR FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	{
		name: "EXTRACT_MINUTE",
		toSQL: (args: string[]) => `EXTRACT(MINUTE FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	{
		name: "EXTRACT_EPOCH",
		toSQL: (args: string[]) => `EXTRACT(EPOCH FROM ${args[0]})`,
		argumentTypes: ["TIMESTAMP"],
		returnType: "FLOAT",
	},
	// UUID functions (9.14 / https://www.postgresql.org/docs/current/functions-uuid.html)
	{
		name: "GEN_RANDOM_UUID",
		argumentTypes: [],
		returnType: "UUID",
	},
	// Conditional expressions (9.18 / https://www.postgresql.org/docs/current/functions-conditional.html)
	{
		name: "GREATEST_STRING",
		toSQL: (args: string[]) => `GREATEST(${args.join(", ")})`,
		argumentTypes: ["TEXT", "TEXT"],
		returnType: "TEXT",
		variadic: true,
	},
	{
		name: "GREATEST_NUMBER",
		toSQL: (args: string[]) => `GREATEST(${args.join(", ")})`,
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		variadic: true,
	},
	{
		name: "LEAST_STRING",
		toSQL: (args: string[]) => `LEAST(${args.join(", ")})`,
		argumentTypes: ["TEXT", "TEXT"],
		returnType: "TEXT",
		variadic: true,
	},
	{
		name: "LEAST_NUMBER",
		toSQL: (args: string[]) => `LEAST(${args.join(", ")})`,
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		variadic: true,
	},
	{
		name: "COALESCE_STRING",
		toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
		argumentTypes: ["TEXT", "TEXT"],
		returnType: "TEXT",
		variadic: true,
	},
	{
		name: "COALESCE_NUMBER",
		toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
		argumentTypes: ["FLOAT", "FLOAT"],
		returnType: "FLOAT",
		variadic: true,
	},
	{
		name: "COALESCE_BOOLEAN",
		toSQL: (args: string[]) => `COALESCE(${args.join(", ")})`,
		argumentTypes: ["BOOLEAN", "BOOLEAN"],
		returnType: "BOOLEAN",
		variadic: true,
	},
];
