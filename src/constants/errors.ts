import type { ExpressionType as Type } from "./field-types";

export const JSON_ACCESS_TYPE_ERROR = (path: string, field: string, type: Type): string =>
	`JSON path access '${path}' is only allowed on JSON fields, but field '${field}' is of type '${type}'`;

const invalidJsonAccessReasons = {
	format: "Expected format: ->jsonPathSegment->...->'jsonLastSegment' or ->'jsonPathSegment'->...->>jsonLastSegment",
	quote: "Unterminated quote in JSON access path",
};
export const INVALID_JSON_ACCESS_ERROR = (jsonAccess: string, reason: keyof typeof invalidJsonAccessReasons): string =>
	`Invalid JSON access path '${jsonAccess}'. ${invalidJsonAccessReasons[reason]}`;

export const INVALID_OPERATOR_VALUE_TYPE_ERROR = (operator: string, type: string): string =>
	`${operator.toUpperCase().slice(1)} operator requires a ${type} value`;

export const FUNCTION_TYPE_MISMATCH_ERROR = (functionName: string, fieldType: Type, type: Type): string =>
	`Type mismatch for '${functionName}': expected ${fieldType}, got ${type}`;

export const COMPARISON_TYPE_MISMATCH_ERROR = (operator: string, field: string, fieldType: Type, type: Type): string =>
	`Field type mismatch for '${operator}' comparison on '${field}': expected ${fieldType}, got ${type}`;

export const INVALID_ARGUMENT_COUNT_ERROR = (functionName: string, argCount: number, count: number, variadic?: boolean): string =>
	`Function '${functionName}' requires ${variadic ? "at least" : "exactly"} ${argCount} argument${argCount !== 1 ? "s" : ""}, got ${count}`;

export const MISSING_AGGREGATION_FIELD_ERROR = "Aggregation query must have at least one group by field or aggregated field";

export const FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR =
	"Evaluations on existing rows are not allowed during INSERT. Use 'NEW_ROW' as a prefix for evaluating new row values, or $exists to apply a condition on existing rows.";

export const NON_EMPTY_CONDITION_ARRAY_ERROR = (operator: string) => `${operator} condition should be a non-empty array.`;

export const MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR = (operator: string, dialect: string) =>
	`Advanced mathematical operators (such as '${operator}') are not supported in dialect '${dialect}'`;
