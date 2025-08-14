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

export const OPERATOR_TYPE_MISMATCH_ERROR = (
	operator: string,
	fieldName: string,
	fieldType: string,
	receivedType: string,
): string => `Field type mismatch for '${operator}' comparison on '${fieldName}': expected ${fieldType}, got ${receivedType}`;

const functionTypeName = { Unary: "exactly 1 argument", Binary: "exactly 2 arguments", Variable: "at least 1 argument" };
export const INVALID_ARGUMENT_COUNT_ERROR = (
	functionType: keyof typeof functionTypeName,
	functionName: string,
	argCount: number,
): string => `${functionType} operator '${functionName}' requires ${functionTypeName[functionType]}, got ${argCount}`;

export const MISSING_AGGREGATION_FIELD = "Aggregation query must have at least one group by field or aggregated field";
