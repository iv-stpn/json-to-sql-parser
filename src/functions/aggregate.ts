import type { ExpressionType, FieldType } from "../constants/cast-types";
import type { Dialect } from "../constants/dialects";
import { applyFunction } from "../utils/function-call";

export type AggregationDefinition = {
	name: string;
	expressionType: ExpressionType;
	additionalArgumentTypes?: FieldType[];
	variadic?: boolean;
	toSQL?: (expression: string, args: string[], dialect: Dialect) => string;
};

const aggregationFunctions = [
	{
		name: "COUNT",
		expressionType: "any",
	},
	{
		name: "SUM",
		expressionType: "number",
	},
	{
		name: "AVG",
		expressionType: "number",
	},
	{
		name: "MIN",
		expressionType: "number",
	},
	{
		name: "MAX",
		expressionType: "number",
	},
	{
		name: "STDDEV",
		expressionType: "number",
		toSQL: (expression, _, dialect) => {
			if (dialect === "postgresql") return `STDDEV(${expression})`;
			return `SQRT(AVG(POW(${expression}, 2))-POW(AVG(${expression}),2))`;
		},
	},
	{
		name: "VARIANCE",
		expressionType: "number",
		toSQL: (expression, _, dialect) => {
			if (dialect === "postgresql") return `VARIANCE(${expression})`;
			return `AVG(POW(${expression}, 2))-POW(AVG(${expression}),2)`;
		},
	},
	{
		name: "COUNT_DISTINCT",
		expressionType: "any",
		toSQL: (expression) => `COUNT(DISTINCT ${expression})`,
	},
	{
		name: "STRING_AGG",
		expressionType: "string",
		additionalArgumentTypes: ["string"],
		toSQL: (expression, args, dialect) => {
			if (dialect === "postgresql") return applyFunction("STRING_AGG", [expression, ...args]);
			return applyFunction("GROUP_CONCAT", [expression, ...args]);
		},
	},
] satisfies AggregationDefinition[];

export const aggregationFunctionNames = aggregationFunctions.map(({ name }) => name);
export const allowedAggregationFunctions: AggregationDefinition[] = aggregationFunctions;
