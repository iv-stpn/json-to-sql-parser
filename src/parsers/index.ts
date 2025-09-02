import { Dialect } from "../constants/dialects";
import {
	COMPARISON_TYPE_MISMATCH_ERROR,
	FUNCTION_TYPE_MISMATCH_ERROR,
	INVALID_ARGUMENT_COUNT_ERROR,
	INVALID_OPERATOR_VALUE_TYPE_ERROR,
	JSON_ACCESS_TYPE_ERROR,
	NON_EMPTY_CONDITION_ARRAY_ERROR,
} from "../constants/errors";
import type { ExpressionType } from "../constants/field-types";
import { allowedFunctions, type FunctionDefinition } from "../functions";
import { parseJsonAccess } from "../parsers/parse-json-access";
import type {
	AnyExpression,
	AnyFieldCondition,
	AnyScalar,
	Condition,
	ConditionExpression,
	ExpressionObject,
	ScalarExpression,
	ScalarPrimitive,
} from "../schemas";
import type { Config, Field, FieldPath, ParserState } from "../types";
import { isNonEmptyArray, isNotNull, quote } from "../utils";
import { applyFunction, removeAllWrappingParens } from "../utils/function-call";
import {
	fieldNameRegex,
	isAnyScalar,
	isExpressionObject,
	isNonNullObject,
	isScalarExpression,
	isScalarPrimitive,
	isValidDate,
	isValidTimestamp,
	uuidRegex,
} from "../utils/validators";

function parseTableFieldPath(fieldPath: string, rootTable: string) {
	if (!fieldPath.includes(".")) return { table: rootTable, field: fieldPath };

	const firstDotIndex = fieldPath.indexOf(".");
	const table = fieldPath.substring(0, firstDotIndex);
	const field = fieldPath.substring(firstDotIndex + 1);
	if (!table || !field) throw new Error(`Invalid table field '${fieldPath}': must be in 'table.field' format`);
	return { table, field };
}

const fieldNamePartRegex = new RegExp(`^${fieldNameRegex}$`);

export function parseFieldPath(field: string, rootTable: string, config: Config): FieldPath {
	const { table, field: fieldPath } = parseTableFieldPath(field, rootTable);

	const tableConfig = config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	const arrowIdx = fieldPath.indexOf("->");
	const fieldName = arrowIdx === -1 ? fieldPath : fieldPath.substring(0, arrowIdx);

	if (!fieldNamePartRegex.test(fieldName)) throw new Error(`Invalid field name '${fieldName}' in table '${table}'`);

	const fieldConfig = tableConfig.allowedFields.find((allowedField) => allowedField.name === fieldName);
	if (!fieldConfig) throw new Error(`Field '${fieldName}' is not allowed or does not exist in '${table}'`);

	// No JSON path, return as is
	if (arrowIdx === -1) return { table, field: fieldPath, fieldConfig, jsonAccess: [] };

	if (fieldConfig.type !== "object") throw new Error(JSON_ACCESS_TYPE_ERROR(fieldPath, fieldName, fieldConfig.type));

	// Parse JSON access
	const { jsonAccess, jsonExtractText } = parseJsonAccess(fieldPath.substring(arrowIdx));
	return { table, field: fieldName, fieldConfig, jsonAccess, jsonExtractText }; // Return parsed field path with JSON parts
}

type ResolveFunction = { args: AnyExpression[]; functionName: string; definition: FunctionDefinition };
export function resolveFunction(functionExpression: Record<string, AnyExpression[]>): ResolveFunction {
	const entries = Object.entries(functionExpression);
	if (entries.length !== 1) throw new Error("$func must contain exactly one function");

	const entry = entries[0];
	if (!entry) throw new Error("Function name cannot be empty");

	const [functionName, args] = entry;

	const functionDefinition = allowedFunctions.find(({ name }) => name === functionName);
	if (!functionDefinition) throw new Error(`Unknown function or operator: "${functionName}"`);

	return { args, functionName, definition: functionDefinition };
}

// Parse SQL functions and operators in expressions
function parseFunctionExpression(functionExpression: { [functionName: string]: AnyExpression[] }, state: ParserState): string {
	const { args, functionName, definition } = resolveFunction(functionExpression);

	const { argumentTypes, name, toSQL, returnType, variadic } = definition;
	if (argumentTypes.length > args.length || (argumentTypes.length < args.length && !variadic))
		throw new Error(INVALID_ARGUMENT_COUNT_ERROR(name, argumentTypes.length, args.length, variadic));

	const resolvedArguments = args.map((arg, index) => {
		const expectedType = index >= argumentTypes.length ? argumentTypes.at(-1) : argumentTypes[index];
		if (!expectedType) throw new Error(`No argument type defined for function '${name}' at index ${index}`);

		const expression = parseExpression(arg, state);

		if (expectedType === "any") return expression;
		const actualType = getExpressionType(arg, state);

		if (actualType !== expectedType && actualType !== null) {
			// Every type can be cast to a string (TEXT type), automatically cast in this case
			if (expectedType === "string") return castValue(expression, "string", state.config.dialect);
			throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(functionName, expectedType, actualType));
		}

		return expression;
	});

	state.expressions.add({ $func: functionExpression }, returnType);
	return toSQL ? toSQL(resolvedArguments, state.config.dialect) : applyFunction(name, resolvedArguments);
}

export function jsonAccessPath(path: string, jsonAccess: string[], jsonExtractText = true): string {
	if (jsonAccess.length === 0) return path;

	const finalOperator = jsonExtractText ? "->>" : "->";
	if (jsonAccess.length === 1) return `${path}${finalOperator}'${jsonAccess[0]}'`;
	return `${path}->'${jsonAccess.slice(0, -1).join("'->'")}'${finalOperator}'${jsonAccess.at(-1)}'`;
}

function jsonPathAlias(path: string, jsonAccess: string[]): string {
	return jsonAccessPath(path, jsonAccess, false).replaceAll(/(?<=->)'+|'+(?=->)|'+$/g, "");
}

function getScalarFieldType(value: AnyScalar): ExpressionType {
	if (typeof value === "string") return "string";
	if (typeof value === "number") return "number";
	if (typeof value === "boolean") return "boolean";
	if (value === null) return null;
	if (isNonNullObject(value)) {
		if ("$jsonb" in value) return "object";
		if ("$date" in value) return "date";
		if ("$timestamp" in value) return "datetime";
		if ("$uuid" in value) return "uuid";
	}
	throw new Error(`Invalid value type: ${typeof value}`);
}

export function getExpressionType(expression: AnyExpression, state: ParserState): ExpressionType {
	if (isAnyScalar(expression)) return getScalarFieldType(expression);
	if (isExpressionObject(expression)) return state.expressions.get(expression);
	if (expression === null) return null;
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

function getCastType(targetType: ExpressionType, dialect: Dialect): string | null {
	if (targetType === null || targetType === "any") return null;

	// PostgreSQL
	if (dialect === Dialect.POSTGRESQL) {
		if (targetType === "string") return "TEXT";
		if (targetType === "number") return "FLOAT";
		if (targetType === "boolean") return "BOOLEAN";
		if (targetType === "object") return "JSONB";
		if (targetType === "date") return "DATE";
		if (targetType === "datetime") return "TIMESTAMP";
		if (targetType === "uuid") return "UUID";
		return null;
	}

	// SQLite
	if (targetType === "boolean" || targetType === "object") return null;
	return targetType === "number" ? "REAL" : "TEXT";
}

export const castValue = (value: string, targetType: ExpressionType, dialect: Dialect): string => {
	const castType = getCastType(targetType, dialect);
	if (!castType) return value;

	// PostgreSQL
	if (dialect === Dialect.POSTGRESQL) return `(${removeAllWrappingParens(value)})::${castType}`;

	// SQLite
	return `CAST(${value} AS ${castType})`;
};

export const aliasValue = (expression: string, alias: string): string => `${expression} AS "${alias}"`;

// Get the expected cast type for a field when a cast is applied, dealing with JSON access and data tables
export function getTargetFieldType(
	targetFieldType: ExpressionType,
	hasJsonAccess: boolean,
	fieldType: ExpressionType,
	config: Config,
): ExpressionType {
	const hasDataTable = !!config.dataTable;
	if (!targetFieldType) return hasDataTable ? (fieldType === "string" || fieldType === "object" ? null : fieldType) : null;
	if (hasDataTable || hasJsonAccess) return targetFieldType === "string" || targetFieldType === "object" ? null : targetFieldType;
	return targetFieldType === fieldType ? null : targetFieldType;
}

type SelectFieldParams = {
	fieldPath: FieldPath;
	state: ParserState;
	targetFieldType?: ExpressionType;
	jsonExtractText?: boolean;
};
function selectField({ fieldPath, state, targetFieldType = null, jsonExtractText }: SelectFieldParams) {
	const { fieldConfig, field: fieldName } = fieldPath;

	const tableName = fieldPath.table || state.rootTable;

	const field = state.config.dataTable ? state.config.dataTable.dataField : fieldName;
	const jsonAccess = state.config.dataTable ? [fieldName, ...fieldPath.jsonAccess] : fieldPath.jsonAccess;

	// A JSON object should be extracted when targetFieldType is of type "object"
	const shouldJsonExtractText = jsonExtractText ?? targetFieldType !== "object";
	const path = jsonAccessPath(`${tableName}.${field}`, jsonAccess, shouldJsonExtractText);

	const relativePath = state.rootTable === tableName ? fieldName : `${tableName}.${fieldName}`;

	const expectedCast = getTargetFieldType(targetFieldType, jsonAccess.length > 0, fieldConfig.type, state.config);
	return { alias: jsonPathAlias(relativePath, fieldPath.jsonAccess), targetType: expectedCast, field: path };
}

export function parseField(field: string, state: ParserState) {
	const fieldPath = parseFieldPath(field, state.rootTable, state.config);
	return { select: selectField({ fieldPath, state, jsonExtractText: fieldPath.jsonExtractText }), fieldPath };
}

export function parseScalarValue(value: ScalarPrimitive) {
	if (value === null) return "NULL";
	if (typeof value === "string") return quote(value);
	if (typeof value === "number") return value.toString();
	if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
	throw new Error(`Invalid scalar value: ${value}`);
}

export function parseScalarExpression(expression: ScalarExpression, dialect: Dialect): string {
	if ("$jsonb" in expression) return castValue(quote(JSON.stringify(expression.$jsonb)), "object", dialect);
	if ("$timestamp" in expression) {
		if (!isValidTimestamp(expression.$timestamp)) throw new Error(`Invalid timestamp format: ${expression.$timestamp}`);
		const timestamp = expression.$timestamp.replace("T", " ");
		return castValue(quote(timestamp), "datetime", dialect);
	}
	if ("$date" in expression) {
		if (!isValidDate(expression.$date)) throw new Error(`Invalid date format: ${expression.$date}`);
		return castValue(quote(expression.$date), "date", dialect);
	}

	if ("$uuid" in expression) {
		if (!uuidRegex.test(expression.$uuid)) throw new Error(`Invalid UUID format: ${expression.$uuid}`);
		return castValue(quote(expression.$uuid), "uuid", dialect);
	}

	throw new Error(`Invalid scalar expression: ${JSON.stringify(expression)}`);
}

function parseAnyScalarValue(value: AnyScalar, dialect: Dialect) {
	if (isScalarPrimitive(value)) return parseScalarValue(value);
	return parseScalarExpression(value, dialect);
}

export function parseExpressionObject(expression: ExpressionObject, state: ParserState): string {
	if ("$func" in expression) return parseFunctionExpression(expression.$func, state);
	if ("$cond" in expression) return parseConditionalExpression(expression.$cond, state);

	if ("$field" in expression) {
		const { select, fieldPath } = parseField(expression.$field, state);
		state.expressions.add(expression, fieldPath.fieldConfig.type);
		return castValue(select.field, select.targetType, state.config.dialect);
	}

	if ("$var" in expression) {
		const variable = state.config.variables[expression.$var];
		if (variable === undefined) throw new Error(`Variable '${expression.$var}' is not defined`);
		state.expressions.add(expression, getScalarFieldType(variable));
		return parseAnyScalarValue(variable, state.config.dialect);
	}

	return parseScalarExpression(expression, state.config.dialect);
}

export function parseExpression(expression: AnyExpression, state: ParserState): string {
	if (isScalarExpression(expression)) return parseScalarExpression(expression, state.config.dialect);
	if (isExpressionObject(expression)) return parseExpressionObject(expression, state);
	if (isScalarPrimitive(expression)) return parseScalarValue(expression);
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

function parseConditionalExpression(condObject: ConditionExpression, state: ParserState): string {
	const condition = parseCondition(condObject.if, state);
	const thenValue = parseExpression(condObject.then, state);
	const elseValue = parseExpression(condObject.else, state);

	const thenType = getExpressionType(condObject.then, state);
	const elseType = getExpressionType(condObject.else, state);
	if (thenType && elseType && thenType !== elseType)
		throw new Error(`Type mismatch in conditional expression: then type '${thenType}', else type '${elseType}'`);

	state.expressions.add({ $cond: condObject }, thenType ?? elseType);
	return `(CASE WHEN ${condition} THEN ${thenValue} ELSE ${elseValue} END)`;
}

export function mergeConditions(parsedConditions: string[]): string {
	const [firstCondition, ...restConditions] = parsedConditions;
	if (!firstCondition) throw new Error("No conditions provided for AND condition.");

	if (restConditions.length === 0) return firstCondition; // Only one condition, no need for AND
	return `(${parsedConditions.join(" AND ")})`;
}
export function parseCondition(condition: Condition, state: ParserState): string {
	const parseSubCondition = (subCondition: Condition): string => parseCondition(subCondition, state);

	if (typeof condition === "boolean") return condition ? "TRUE" : "FALSE";

	if (isScalarExpression(condition))
		throw new Error(`Condition must evaluate to a boolean (got ${JSON.stringify(condition)} instead)`);

	if (isExpressionObject(condition)) {
		const expression = parseExpression(condition, state);
		const expressionType = state.expressions.get(condition);
		if (expressionType === "boolean") return expression;
		throw new Error(`Condition expression must evaluate to boolean, got ${expressionType} type for ${JSON.stringify(condition)}`);
	}

	// Handle logical operators
	if ("$not" in condition) return `NOT (${parseSubCondition(condition.$not)})`;
	if ("$and" in condition) {
		if (!isNonEmptyArray(condition.$and)) throw new Error(NON_EMPTY_CONDITION_ARRAY_ERROR("$and"));
		return mergeConditions(condition.$and.map(parseSubCondition));
	}
	if ("$or" in condition) {
		if (!isNonEmptyArray(condition.$or)) throw new Error(NON_EMPTY_CONDITION_ARRAY_ERROR("$or"));
		return `(${condition.$or.map(parseSubCondition).join(" OR ")})`;
	}

	// Handle EXISTS subquery
	if ("$exists" in condition) {
		const { table, condition: conditions } = condition.$exists;
		const subQueryCondition = parseCondition(conditions, state); // Parse the subquery conditions
		return `EXISTS (SELECT 1 FROM ${table} WHERE ${subQueryCondition})`;
	}

	// Handle field conditions
	const conditions: string[] = [];
	for (const [field, fieldConditionExpression] of Object.entries(condition)) {
		const fieldPath = parseFieldPath(field, state.rootTable, state.config);
		const fieldConfig = fieldPath.fieldConfig;

		const fieldConditions = parseFieldConditions(fieldConditionExpression, state, fieldConfig);

		const select = selectField({ fieldPath, state, targetFieldType: fieldConditions.type, jsonExtractText: true });
		const fieldName = castValue(select.field, select.targetType, state.config.dialect);

		const clause = mergeConditions(fieldConditions.conditions.map((conditionClause) => `${fieldName} ${conditionClause}`));
		conditions.push(clause);
	}

	return mergeConditions(conditions);
}

type ParseFieldCondition = { condition: string; type: ExpressionType };
function parseComparisonCondition(value: AnyExpression, state: ParserState, operator: string, field: Field): ParseFieldCondition {
	if (isScalarExpression(value)) {
		const expression = parseScalarExpression(value, state.config.dialect);
		return { condition: `${operator} ${expression}`, type: getScalarFieldType(value) };
	}

	if (isExpressionObject(value)) {
		const expression = parseExpressionObject(value, state);
		return { condition: `${operator} ${expression}`, type: state.expressions.get(value) };
	}

	if (value !== null) {
		const fieldType = getScalarFieldType(value);
		if (field.type !== typeof value && field.type !== "object")
			throw new Error(COMPARISON_TYPE_MISMATCH_ERROR(operator, field.name, field.type, fieldType));
		return { condition: `${operator} ${parseScalarValue(value)}`, type: fieldType };
	}

	if (operator !== "=" && operator !== "!=") throw new Error(`Operator '${operator}' should not be used with NULL value`);
	return { condition: operator === "=" ? "IS NULL" : "IS NOT NULL", type: null };
}

function parseArrayCondition(value: AnyExpression[], state: ParserState, operator: "IN" | "NOT IN"): ParseFieldCondition {
	if (value.length === 0) throw new Error(`Operator '${operator}' requires a non-empty array`);
	const resolvedValues: { value: string; type: ExpressionType }[] = value.map((item) => {
		if (isScalarExpression(item)) {
			const value = parseScalarExpression(item, state.config.dialect);
			return { value, type: getScalarFieldType(item) };
		}
		if (isExpressionObject(item)) {
			const value = parseExpressionObject(item, state);
			return { value, type: state.expressions.get(item) };
		}
		if (!isScalarPrimitive(item)) throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string, number, or boolean"));
		return { value: parseScalarValue(item), type: getScalarFieldType(item) };
	});

	const valueTypes = new Set(resolvedValues.map(({ type }) => type));
	const fieldType = valueTypes.values().next().value;
	if (valueTypes.size > 1) throw new Error(`Cannot use ${operator} with mixed types: ${Array.from(valueTypes).join(", ")}`);

	if (!fieldType) throw new Error(`Cannot use ${operator} with NULL values`);
	return { condition: `${operator} (${resolvedValues.map(({ value }) => value).join(", ")})`, type: fieldType };
}

type StringOperator = "LIKE" | "ILIKE" | "~";
function parseStringCondition(
	value: AnyExpression,
	state: ParserState,
	operator: StringOperator,
	field: Field,
): ParseFieldCondition {
	if (value === null) throw new Error(`Operator '${operator}' cannot be used with NULL value`);
	if (isScalarExpression(value))
		return {
			condition: `${operator} ${castValue(parseScalarExpression(value, state.config.dialect), "string", state.config.dialect)}`,
			type: "string",
		};
	if (isExpressionObject(value))
		return {
			condition: `${operator} ${castValue(parseExpressionObject(value, state), "string", state.config.dialect)}`,
			type: "string",
		};
	if (typeof value !== "string") throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string"));
	if (field && field.type !== "string" && field.type !== "object")
		throw new Error(COMPARISON_TYPE_MISMATCH_ERROR(operator, field.name, field.type, "string"));
	return { condition: `${operator} ${parseScalarValue(value)}`, type: "string" };
}

type ParseFieldConditions = { conditions: string[]; type: ExpressionType };
function parseFieldConditions(condition: AnyFieldCondition, state: ParserState, field: Field): ParseFieldConditions {
	// Treat expression as an equality condition if no operator is provided
	if (isAnyScalar(condition) || isExpressionObject(condition)) {
		const fieldCondition = parseComparisonCondition(condition, state, "=", field);
		return { conditions: [fieldCondition.condition], type: fieldCondition.type };
	}

	const fieldConditions: { condition: string; type: ExpressionType }[] = [];
	if (condition.$eq !== undefined) fieldConditions.push(parseComparisonCondition(condition.$eq, state, "=", field));
	if (condition.$ne !== undefined) fieldConditions.push(parseComparisonCondition(condition.$ne, state, "!=", field));
	if (condition.$gt !== undefined) fieldConditions.push(parseComparisonCondition(condition.$gt, state, ">", field));
	if (condition.$gte !== undefined) fieldConditions.push(parseComparisonCondition(condition.$gte, state, ">=", field));
	if (condition.$lt !== undefined) fieldConditions.push(parseComparisonCondition(condition.$lt, state, "<", field));
	if (condition.$lte !== undefined) fieldConditions.push(parseComparisonCondition(condition.$lte, state, "<=", field));

	if (condition.$in !== undefined) fieldConditions.push(parseArrayCondition(condition.$in, state, "IN"));
	if (condition.$nin !== undefined) fieldConditions.push(parseArrayCondition(condition.$nin, state, "NOT IN"));

	if (condition.$like !== undefined) fieldConditions.push(parseStringCondition(condition.$like, state, "LIKE", field));
	if (condition.$ilike !== undefined) fieldConditions.push(parseStringCondition(condition.$ilike, state, "ILIKE", field));

	if (condition.$regex !== undefined) {
		if (state.config.dialect === Dialect.POSTGRESQL)
			fieldConditions.push(parseStringCondition(condition.$regex, state, "~", field));
		else throw new Error("Operator 'REGEXP' is not supported by default in SQLite");
	}

	const conditionTypes = new Set(fieldConditions.map(({ type: conditionType }) => conditionType).filter(isNotNull));
	if (conditionTypes.size > 1) throw new Error(`Cannot mix types in field conditions: ${Array.from(conditionTypes).join(", ")}`);

	const conditions = fieldConditions.map(({ condition }) => condition);
	return { conditions, type: conditionTypes.values().next().value ?? null };
}
