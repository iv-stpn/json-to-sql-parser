import { applyFunction } from "../utils/function-call";

// TODO: add arguments for aggregation functions

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
