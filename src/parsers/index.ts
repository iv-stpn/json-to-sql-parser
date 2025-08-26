import type { CastType, FieldType } from "../constants/cast-types";
import { baseCastMap } from "../constants/cast-types";
import type { Dialect } from "../constants/dialects";
import {
	COMPARISON_TYPE_MISMATCH_ERROR,
	FUNCTION_TYPE_MISMATCH_ERROR,
	INVALID_ARGUMENT_COUNT_ERROR,
	INVALID_OPERATOR_VALUE_TYPE_ERROR,
	JSON_ACCESS_TYPE_ERROR,
	NON_EMPTY_CONDITION_ARRAY_ERROR,
} from "../constants/errors";
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
import { applyFunction } from "../utils/function-call";
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

		if (expectedType === "ANY") return expression;
		const actualType = getExpressionCastType(arg, state);

		if (actualType !== expectedType && actualType !== null) {
			// Every type can be cast to TEXT, automatically cast in this case
			if (expectedType === "TEXT") return castValue(expression, "TEXT", state.config.dialect);
			throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(functionName, expectedType, actualType));
		}

		return expression;
	});

	state.expressions.add({ $func: functionExpression }, returnType);
	return toSQL ? toSQL(resolvedArguments, state.config.dialect) : applyFunction(name, resolvedArguments);
}

function jsonAccessPath(path: string, jsonAccess: string[], jsonExtractText = true): string {
	if (jsonAccess.length === 0) return path;

	const finalOperator = jsonExtractText ? "->>" : "->";
	if (jsonAccess.length === 1) return `${path}${finalOperator}'${jsonAccess[0]}'`;
	return `${path}->'${jsonAccess.slice(0, -1).join("'->'")}'${finalOperator}'${jsonAccess.at(-1)}'`;
}

function jsonPathAlias(path: string, jsonAccess: string[]): string {
	return jsonAccessPath(path, jsonAccess, false).replaceAll(/(?<=->)'+|'+(?=->)|'+$/g, "");
}

function getScalarCastType(value: AnyScalar): CastType {
	if (typeof value === "string") return baseCastMap.string;
	if (typeof value === "number") return baseCastMap.number;
	if (typeof value === "boolean") return baseCastMap.boolean;
	if (value === null) return null;
	if (isNonNullObject(value)) {
		if ("$jsonb" in value) return baseCastMap.object;
		if ("$date" in value) return baseCastMap.date;
		if ("$timestamp" in value) return baseCastMap.datetime;
		if ("$uuid" in value) return baseCastMap.uuid;
	}
	throw new Error(`Invalid value type: ${typeof value}`);
}

export function getExpressionCastType(expression: AnyExpression, state: ParserState): CastType {
	if (isAnyScalar(expression)) return getScalarCastType(expression);
	if (isExpressionObject(expression)) return state.expressions.get(expression);
	if (expression === null) return null;
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

const castTypeToSqliteType: Record<Exclude<CastType, null>, string> = {
	TEXT: "TEXT",
	FLOAT: "REAL",
	BOOLEAN: "INTEGER",
	JSONB: "TEXT",
	DATE: "TEXT",
	TIMESTAMP: "TEXT",
	UUID: "TEXT",
};

export const castValue = (value: string, type: CastType, dialect: Dialect): string => {
	if (!type) return value;
	if (dialect === "postgresql") return `(${value})::${type}`;
	return `CAST(${value} AS ${castTypeToSqliteType[type]})`;
};
export const aliasValue = (expression: string, alias: string): string => `${expression} AS "${alias}"`;

// Get the expected cast type for a field when a cast is applied, dealing with JSON access and data tables
function getExpectedCast(baseCast: CastType, hasJsonAccess: boolean, fieldType: FieldType, state: ParserState): CastType {
	const hasDataTable = !!state.config.dataTable;
	if (!baseCast) return hasDataTable ? (fieldType === "string" || fieldType === "object" ? null : baseCastMap[fieldType]) : null;
	if (hasDataTable || hasJsonAccess) return baseCast === "TEXT" || baseCast === "JSONB" ? null : baseCast;
	return baseCast === baseCastMap[fieldType] ? null : baseCast;
}

type SelectFieldParams = { fieldPath: FieldPath; state: ParserState; cast?: CastType; jsonExtractText?: boolean };
function selectField({ fieldPath, state, cast = null, jsonExtractText }: SelectFieldParams) {
	const { fieldConfig, field: fieldName } = fieldPath;

	const tableName = fieldPath.table || state.rootTable;

	const dataTable = state.config.dataTable;

	const field = dataTable ? dataTable.dataField : fieldName;
	const jsonAccess = dataTable ? [fieldName, ...fieldPath.jsonAccess] : fieldPath.jsonAccess;

	const shouldJsonExtractText = jsonExtractText ?? (cast ? cast !== "JSONB" : true);
	const path = jsonAccessPath(`${tableName}.${field}`, jsonAccess, shouldJsonExtractText);
	const relativePath = state.rootTable === tableName ? fieldName : `${tableName}.${fieldName}`;

	const expectedCast = getExpectedCast(cast, jsonAccess.length > 0, fieldConfig.type, state);
	return { alias: jsonPathAlias(relativePath, fieldPath.jsonAccess), cast: expectedCast, field: path };
}

export function parseField(field: string, state: ParserState, cast?: CastType, jsonExtractText?: boolean) {
	const fieldPath = parseFieldPath(field, state.rootTable, state.config);
	return {
		select: selectField({ fieldPath, state, cast, jsonExtractText: jsonExtractText ?? fieldPath.jsonExtractText }),
		fieldPath,
	};
}

export function parseScalarValue(value: ScalarPrimitive) {
	if (value === null) return "NULL";
	if (typeof value === "string") return quote(value);
	if (typeof value === "number") return value.toString();
	if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
	throw new Error(`Invalid scalar value: ${value}`);
}

export function parseScalarExpression(expression: ScalarExpression): string {
	if ("$jsonb" in expression) return `${quote(JSON.stringify(expression.$jsonb))}::JSONB`;
	if ("$timestamp" in expression) {
		if (!isValidTimestamp(expression.$timestamp)) throw new Error(`Invalid timestamp format: ${expression.$timestamp}`);
		const timestamp = expression.$timestamp.replace("T", " ");
		return `'${timestamp}'::TIMESTAMP`;
	}
	if ("$date" in expression) {
		if (!isValidDate(expression.$date)) throw new Error(`Invalid date format: ${expression.$date}`);
		return `'${expression.$date}'::DATE`;
	}

	if ("$uuid" in expression) {
		if (!uuidRegex.test(expression.$uuid)) throw new Error(`Invalid UUID format: ${expression.$uuid}`);
		return `'${expression.$uuid}'::UUID`;
	}

	throw new Error(`Invalid scalar expression: ${JSON.stringify(expression)}`);
}

function parseAnyScalarValue(value: AnyScalar) {
	if (isScalarPrimitive(value)) return parseScalarValue(value);
	return parseScalarExpression(value);
}

export function parseExpressionObject(expression: ExpressionObject, state: ParserState): string {
	if ("$func" in expression) return parseFunctionExpression(expression.$func, state);
	if ("$cond" in expression) return parseConditionalExpression(expression.$cond, state);

	if ("$field" in expression) {
		const { select, fieldPath } = parseField(expression.$field, state);
		state.expressions.add(expression, baseCastMap[fieldPath.fieldConfig.type]);
		return castValue(select.field, select.cast, state.config.dialect);
	}

	if ("$var" in expression) {
		const variable = state.config.variables[expression.$var];
		if (variable === undefined) throw new Error(`Variable '${expression.$var}' is not defined`);
		state.expressions.add(expression, getScalarCastType(variable));
		return parseAnyScalarValue(variable);
	}

	return parseScalarExpression(expression);
}

export function parseExpression(expression: AnyExpression, state: ParserState): string {
	if (isScalarExpression(expression)) return parseScalarExpression(expression);
	if (isExpressionObject(expression)) return parseExpressionObject(expression, state);
	if (isScalarPrimitive(expression)) return parseScalarValue(expression);
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

function parseConditionalExpression(condObject: ConditionExpression, state: ParserState): string {
	const condition = parseCondition(condObject.if, state);
	const thenValue = parseExpression(condObject.then, state);
	const elseValue = parseExpression(condObject.else, state);

	const thenType = getExpressionCastType(condObject.then, state);
	const elseType = getExpressionCastType(condObject.else, state);
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

	if (isScalarExpression(condition)) throw new Error(`Condition must evaluate to BOOLEAN (got ${JSON.stringify(condition)})`);

	if (isExpressionObject(condition)) {
		const expression = parseExpression(condition, state);
		const expressionType = state.expressions.get(condition);
		if (expressionType === "BOOLEAN") return expression;
		throw new Error(`Condition expression must evaluate to BOOLEAN, got ${expressionType} type for ${JSON.stringify(condition)}`);
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

		const select = selectField({ fieldPath, state, cast: fieldConditions.conditionType, jsonExtractText: true });
		const fieldName = castValue(select.field, select.cast, state.config.dialect);

		const clause = mergeConditions(fieldConditions.conditions.map((conditionClause) => `${fieldName} ${conditionClause}`));
		conditions.push(clause);
	}

	return mergeConditions(conditions);
}

type ParseFieldCondition = { condition: string; conditionType: CastType };
function parseComparisonCondition(value: AnyExpression, state: ParserState, operator: string, field: Field): ParseFieldCondition {
	if (isScalarExpression(value)) {
		const expression = parseScalarExpression(value);
		return { condition: `${operator} ${expression}`, conditionType: getScalarCastType(value) };
	}

	if (isExpressionObject(value)) {
		const expression = parseExpressionObject(value, state);
		return { condition: `${operator} ${expression}`, conditionType: state.expressions.get(value) };
	}

	if (value !== null) {
		const castType = getScalarCastType(value);
		if (field.type !== typeof value && field.type !== "object")
			throw new Error(COMPARISON_TYPE_MISMATCH_ERROR(operator, field.name, baseCastMap[field.type], castType));
		return { condition: `${operator} ${parseScalarValue(value)}`, conditionType: castType };
	}

	if (operator !== "=" && operator !== "!=") throw new Error(`Operator '${operator}' should not be used with NULL value`);
	return { condition: operator === "=" ? "IS NULL" : "IS NOT NULL", conditionType: null };
}

function parseArrayCondition(value: AnyExpression[], state: ParserState, operator: "IN" | "NOT IN"): ParseFieldCondition {
	if (value.length === 0) throw new Error(`Operator '${operator}' requires a non-empty array`);
	const resolvedValues: { value: string; castType: CastType }[] = value.map((item) => {
		if (isScalarExpression(item)) {
			const value = parseScalarExpression(item);
			return { value, castType: getScalarCastType(item) };
		}
		if (isExpressionObject(item)) {
			const value = parseExpressionObject(item, state);
			return { value, castType: state.expressions.get(item) };
		}
		if (!isScalarPrimitive(item)) throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string, number, or boolean"));
		return { value: parseScalarValue(item), castType: getScalarCastType(item) };
	});

	const castTypes = new Set(resolvedValues.map(({ castType }) => castType));
	const castType = castTypes.values().next().value;
	if (castTypes.size > 1) throw new Error(`Cannot use ${operator} with mixed types: ${Array.from(castTypes).join(", ")}`);
	if (!castType) throw new Error(`Cannot use ${operator} with NULL values`);
	return { condition: `${operator} (${resolvedValues.map(({ value }) => value).join(", ")})`, conditionType: castType };
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
			condition: `${operator} ${castValue(parseScalarExpression(value), "TEXT", state.config.dialect)}`,
			conditionType: "TEXT",
		};
	if (isExpressionObject(value))
		return {
			condition: `${operator} ${castValue(parseExpressionObject(value, state), "TEXT", state.config.dialect)}`,
			conditionType: "TEXT",
		};
	if (typeof value !== "string") throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string"));
	if (field && field.type !== "string" && field.type !== "object")
		throw new Error(COMPARISON_TYPE_MISMATCH_ERROR(operator, field.name, baseCastMap[field.type], "TEXT"));
	return { condition: `${operator} ${parseScalarValue(value)}`, conditionType: "TEXT" };
}

type ParseFieldConditions = { conditions: string[]; conditionType: CastType };
function parseFieldConditions(condition: AnyFieldCondition, state: ParserState, field: Field): ParseFieldConditions {
	// Treat expression as an equality condition if no operator is provided
	if (isAnyScalar(condition) || isExpressionObject(condition)) {
		const fieldCondition = parseComparisonCondition(condition, state, "=", field);
		return { conditions: [fieldCondition.condition], conditionType: fieldCondition.conditionType };
	}

	const fieldConditions: { condition: string; conditionType: CastType }[] = [];
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
	if (condition.$regex !== undefined) fieldConditions.push(parseStringCondition(condition.$regex, state, "~", field));

	const conditionTypes = new Set(fieldConditions.map(({ conditionType }) => conditionType).filter(isNotNull));
	if (conditionTypes.size > 1) throw new Error(`Cannot mix types in field conditions: ${Array.from(conditionTypes).join(", ")}`);

	const conditions = fieldConditions.map(({ condition }) => condition);
	return { conditions, conditionType: conditionTypes.values().next().value ?? null };
}
