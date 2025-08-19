import type { castTypes } from "../constants/cast-types";
import { applyFunction } from "../utils/function-call";

export type AggregationDefinition = {
	name: string;
	expressionType: (typeof castTypes)[number] | "ANY";
	additionalArgumentTypes?: (typeof castTypes)[number][];
	variadic?: boolean;
	toSQL?: (expression: string, args: string[]) => string;
};

const aggregationFunctions = [
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
