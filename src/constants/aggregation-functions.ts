import z from "zod";
import { applyFunction } from "../utils/function-call";
import { castTypes } from "./cast-types";

export const aggregationFunctionSchema = z.object({
	name: z.string(),
	expressionType: z.enum(castTypes).or(z.literal("ANY")),
	additionalArgumentTypes: z.array(z.enum(castTypes).or(z.literal("ANY"))).optional(),
	variadic: z.boolean().optional(),
});
export type AggregationDefinition = z.infer<typeof aggregationFunctionSchema> & {
	toSQL?: (expression: string, args: string[]) => string;
};

export const aggregationFunctions = [
	{
		name: "COUNT",
		expressionType: "ANY",
	},
	{
		name: "SUM",
		expressionType: "FLOAT",
	},
	{
		name: "AVG",
		expressionType: "FLOAT",
	},
	{
		name: "MIN",
		expressionType: "FLOAT",
	},
	{
		name: "MAX",
		expressionType: "FLOAT",
	},
	{
		name: "STDDEV",
		expressionType: "FLOAT",
	},
	{
		name: "VARIANCE",
		expressionType: "FLOAT",
	},
	{
		name: "COUNT_DISTINCT",
		expressionType: "ANY",
		toSQL: (expression) => `COUNT(DISTINCT ${expression})`,
	},
	{
		name: "STRING_AGG",
		expressionType: "TEXT",
		additionalArgumentTypes: ["TEXT"],
		toSQL: (expression, args) => applyFunction("STRING_AGG", [expression, ...args]),
	},
] satisfies AggregationDefinition[];

export const aggregationFunctionNames = aggregationFunctions.map(({ name }) => name);
export const allowedAggregationFunctions: AggregationDefinition[] = aggregationFunctions;
