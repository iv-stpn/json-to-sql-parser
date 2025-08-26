import type { CastType } from "./cast-types";

export const JSON_ACCESS_TYPE_ERROR = (path: string, field: string, type: string): string =>
	`JSON path access '${path}' is only allowed on JSON fields, but field '${field}' is of type '${type}'`;

const invalidJsonAccessReasons = {
	format: "Expected format: ->jsonPathSegment->...->'jsonLastSegment' or ->'jsonPathSegment'->...->>jsonLastSegment",
	quote: "Unterminated quote in JSON access path",
};
export const INVALID_JSON_ACCESS_ERROR = (jsonAccess: string, reason: keyof typeof invalidJsonAccessReasons): string =>
	`Invalid JSON access path '${jsonAccess}'. ${invalidJsonAccessReasons[reason]}`;

export const INVALID_OPERATOR_VALUE_TYPE_ERROR = (operator: string, type: string): string =>
	`${operator.toUpperCase().slice(1)} operator requires a ${type} value`;

export const FUNCTION_TYPE_MISMATCH_ERROR = (functionName: string, fieldType: CastType | "ANY", receivedType: CastType): string =>
	`Type mismatch for '${functionName}': expected ${fieldType}, got ${receivedType}`;

export const COMPARISON_TYPE_MISMATCH_ERROR = (
	operator: string,
	fieldName: string,
	fieldType: CastType | "ANY",
	receivedType: CastType,
): string => `Field type mismatch for '${operator}' comparison on '${fieldName}': expected ${fieldType}, got ${receivedType}`;

export const INVALID_ARGUMENT_COUNT_ERROR = (
	functionName: string,
	expectedArgCount: number,
	argCount: number,
	variadic?: boolean,
): string =>
	`Function '${functionName}' requires ${variadic ? "at least" : "exactly"} ${expectedArgCount} argument${expectedArgCount !== 1 ? "s" : ""}, got ${argCount}`;

export const MISSING_AGGREGATION_FIELD_ERROR = "Aggregation query must have at least one group by field or aggregated field";

export const FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR =
	"Evaluations on existing rows are not allowed during INSERT. Use 'NEW_ROW' as a prefix for evaluating new row values, or $exists to apply a condition on existing rows.";

export const NON_EMPTY_CONDITION_ARRAY_ERROR = (operator: string) => `${operator} condition should be a non-empty array.`;

export const MATHEMATICAL_OPERATORS_NOT_SUPPORTED_IN_DIALECT_ERROR = (operator: string, dialect: string) =>
	`Advanced mathematical operators (such as '${operator}') are not supported in dialect '${dialect}'`;
