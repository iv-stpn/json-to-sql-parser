import type { castTypes } from "../constants/cast-types";
import type { Dialect } from "../constants/dialects";
import { applyFunction } from "../utils/function-call";

export type AggregationDefinition = {
	name: string;
	expressionType: (typeof castTypes)[number] | "ANY";
	additionalArgumentTypes?: (typeof castTypes)[number][];
	variadic?: boolean;
	toSQL?: (expression: string, args: string[], dialect: Dialect) => string;
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
		toSQL: (expression, _, dialect) => {
			if (dialect === "postgresql") return `STDDEV(${expression})`;
			return `SQRT(AVG(POW(${expression}, 2))-POW(AVG(${expression}),2))`;
		},
	},
	{
		name: "VARIANCE",
		expressionType: "FLOAT",
		toSQL: (expression, _, dialect) => {
			if (dialect === "postgresql") return `VARIANCE(${expression})`;
			return `AVG(POW(${expression}, 2))-POW(AVG(${expression}),2)`;
		},
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
