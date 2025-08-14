import {
	INVALID_ARGUMENT_COUNT_ERROR,
	INVALID_OPERATOR_VALUE_TYPE_ERROR,
	JSON_ACCESS_TYPE_ERROR,
	OPERATOR_TYPE_MISMATCH_ERROR,
} from "../constants/errors";
import type { CastType, FieldType } from "../constants/operators";
import {
	binaryOperators,
	castMap,
	isOperator,
	operatorReturnTypes,
	unaryOperators,
	variableFunctions,
} from "../constants/operators";
import { parseJsonAccess } from "../parsers/parse-json-access";
import type { AnyExpression, Condition, ConditionExpression, ExpressionObject, FieldCondition, ScalarValue } from "../schemas";
import type { Field, FieldPath, ParserState, Primitive } from "../types";
import { isInArray, isNotNull, quote } from "../utils";
import { applyFunction } from "../utils/function-call";
import { fieldNameRegex, isPrimitiveValue, isScalarValue, isValidDate, isValidTimestamp, uuidRegex } from "../utils/validators";

export const isExpressionObject = (value: unknown): value is ExpressionObject =>
	typeof value === "object" &&
	value !== null &&
	("$expr" in value || "$cond" in value || "$timestamp" in value || "$date" in value || "$uuid" in value);

function parseTableFieldPath(fieldPath: string, rootTable: string) {
	if (!fieldPath.includes(".")) return { table: rootTable, field: fieldPath };

	const firstDotIndex = fieldPath.indexOf(".");
	const table = fieldPath.substring(0, firstDotIndex);
	const field = fieldPath.substring(firstDotIndex + 1);
	if (!table || !field) throw new Error(`Invalid table field '${fieldPath}': must be in 'table.field' format`);
	return { table, field };
}

type ParseTableFieldParams = { field: string; state: ParserState };
export function parseFieldPath({ field, state }: ParseTableFieldParams): FieldPath {
	const { table, field: fieldPath } = parseTableFieldPath(field, state.rootTable);

	const tableConfig = state.config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	const arrowIdx = fieldPath.indexOf("->");
	const fieldName = arrowIdx === -1 ? fieldPath : fieldPath.substring(0, arrowIdx);

	if (!fieldNameRegex.test(fieldName)) throw new Error(`Invalid field name '${fieldName}' in table '${table}'`);

	const fieldConfig = tableConfig.allowedFields.find((allowedField) => allowedField.name === fieldName);
	if (!fieldConfig) throw new Error(`Field '${fieldName}' is not allowed or does not exist for table '${table}'`);

	// No JSON path, return as is
	if (arrowIdx === -1) return { table, field: fieldPath, fieldConfig, jsonPathSegments: [], jsonExtractText: false };

	if (fieldConfig.type !== "object") throw new Error(JSON_ACCESS_TYPE_ERROR(fieldPath, fieldName, fieldConfig.type));

	// Parse JSON access
	const { jsonPathSegments, jsonExtractText } = parseJsonAccess(fieldPath.substring(arrowIdx));
	return { table, field: fieldName, fieldConfig, jsonPathSegments, jsonExtractText }; // Return parsed field path with JSON parts
}

// Parse SQL functions and operators in expressions
function parseExpressionFunction(exprObj: { [functionName: string]: AnyExpression[] }, state: ParserState): string {
	const entries = Object.entries(exprObj);
	if (entries.length !== 1) throw new Error("$expr must contain exactly one function");

	const [operator, args] = entries[0]!;
	if (!operator) throw new Error("Function name cannot be empty");
	if (!isOperator(operator)) throw new Error(`Unknown function or operator: ${operator}`);

	state.expressions.add({ $expr: exprObj }, operatorReturnTypes[operator]);

	// Handle unary operators
	if (isInArray(unaryOperators, operator)) {
		const arg = args[0];
		if (args.length !== 1 || arg === undefined) throw new Error(INVALID_ARGUMENT_COUNT_ERROR("Unary", operator, args.length));
		return applyFunction(operator, [parseExpression(arg, state)]);
	}

	if (isInArray(variableFunctions, operator)) {
		// Handle variable functions
		if (args.length === 0) throw new Error(INVALID_ARGUMENT_COUNT_ERROR("Variable", operator, args.length));
		const resolvedArgs = args.map((arg) => parseExpression(arg, state));
		return applyFunction(operator, resolvedArgs);
	}

	// Handle binary operators
	const [arg1, arg2] = args;
	if (args.length !== 2 || arg1 === undefined || arg2 === undefined)
		throw new Error(INVALID_ARGUMENT_COUNT_ERROR("Binary", operator, args.length));
	return `(${parseExpression(arg1!, state)} ${binaryOperators[operator]} ${parseExpression(arg2!, state)})`;
}

function jsonAccess(path: string, jsonPathSegments: string[], jsonExtractText = true): string {
	if (jsonPathSegments.length === 0) return path;

	const finalOperator = jsonExtractText ? "->>" : "->";
	if (jsonPathSegments.length === 1) return `${path}${finalOperator}'${jsonPathSegments[0]}'`;
	return `${path}->'${jsonPathSegments.slice(0, -1).join("'->'")}'${finalOperator}'${jsonPathSegments.at(-1)}'`;
}

function jsonPathAlias(path: string, jsonPathSegments: string[]): string {
	return jsonAccess(path, jsonPathSegments, false).replaceAll(/(?<=->)'+|'+(?=->)|'+$/g, "");
}

function getPrimitiveCastType(value: Primitive): CastType {
	if (typeof value === "string") return castMap.string;
	if (typeof value === "number") return castMap.number;
	if (typeof value === "boolean") return castMap.boolean;
	throw new Error(`Invalid value type: ${typeof value}`);
}

function getExpressionCastType(expression: AnyExpression, state: ParserState): CastType {
	if (isExpressionObject(expression)) return state.expressions.get(expression);
	if (isPrimitiveValue(expression)) return getPrimitiveCastType(expression);
	if (expression === null) return null;
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

export const castValue = (value: string, type: CastType): string => (type ? `(${value})::${type}` : value);
export const aliasValue = (expression: string, alias: string): string => `${expression} AS "${alias}"`;

// Get the expected cast type for a field when a cast is applied, dealing with JSON access and data tables
function getExpectedCast(baseCast: CastType, hasJsonAccess: boolean, fieldType: FieldType, state: ParserState): CastType {
	const hasDataTable = !!state.config.dataTable;
	if (!baseCast) return hasDataTable ? (fieldType === "string" || fieldType === "object" ? null : castMap[fieldType]) : null;
	if (hasDataTable || hasJsonAccess) return baseCast === "TEXT" || baseCast === "JSONB" ? null : baseCast;
	return baseCast === castMap[fieldType] ? null : baseCast;
}

type SelectFieldParams = { fieldPath: FieldPath; state: ParserState; cast?: CastType; jsonExtractText?: boolean };
function selectField({ fieldPath, state, cast = null, jsonExtractText }: SelectFieldParams) {
	const { fieldConfig, field: fieldName } = fieldPath;

	const tableName = fieldPath.table || state.rootTable;

	const dataTable = state.config.dataTable;

	const field = dataTable ? dataTable.dataField : fieldName;
	const jsonPathSegments = dataTable ? [fieldName, ...fieldPath.jsonPathSegments] : fieldPath.jsonPathSegments;

	const shouldJsonExtractText = jsonExtractText ?? (cast ? cast !== "JSONB" : true);
	const path = jsonAccess(`${tableName}.${field}`, jsonPathSegments, shouldJsonExtractText);
	const relativePath = state.rootTable === tableName ? fieldName : `${tableName}.${fieldName}`;

	const expectedCast = getExpectedCast(cast, jsonPathSegments.length > 0, fieldConfig.type, state);
	return { alias: jsonPathAlias(relativePath, fieldPath.jsonPathSegments), cast: expectedCast, field: path };
}

export function parseField(field: string, state: ParserState, cast?: CastType, jsonExtractText?: boolean) {
	const fieldPath = parseFieldPath({ field, state });
	return { select: selectField({ fieldPath, state, cast, jsonExtractText }), fieldPath };
}

function parsePrimitiveValue(value: ScalarValue) {
	if (typeof value === "string") return quote(value);
	if (typeof value === "number") return value.toString();
	if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
	throw new Error(`Invalid primitive value: ${value}`);
}

export function parseExpressionObject(expression: ExpressionObject, state: ParserState): string {
	if ("$uuid" in expression) {
		if (!uuidRegex.test(expression.$uuid)) throw new Error(`Invalid UUID format: ${expression.$uuid}`);
		state.expressions.add(expression, "UUID");
		return quote(expression.$uuid);
	}

	if ("$timestamp" in expression) {
		if (!isValidTimestamp(expression.$timestamp)) throw new Error(`Invalid timestamp format: ${expression.$timestamp}`);
		const timestamp = expression.$timestamp.replace("T", " ");
		state.expressions.add(expression, "TIMESTAMP");
		return `'${timestamp}'::TIMESTAMP`;
	}

	if ("$date" in expression) {
		if (!isValidDate(expression.$date)) throw new Error(`Invalid date format: ${expression.$date}`);
		state.expressions.add(expression, "DATE");
		return `'${expression.$date}'::DATE`;
	}

	if ("$expr" in expression) {
		if (typeof expression.$expr === "string") {
			const variable = state.config.variables[expression.$expr];
			// Return variable if it exists
			if (variable !== undefined) {
				state.expressions.add(expression, getPrimitiveCastType(variable));
				return parsePrimitiveValue(variable);
			}

			const { select, fieldPath } = parseField(expression.$expr, state);
			state.expressions.add(expression, castMap[fieldPath.fieldConfig.type]);
			return castValue(select.field, select.cast);
		}
		return parseExpressionFunction(expression.$expr, state);
	}

	if ("$cond" in expression) return parseConditionalExpression(expression.$cond, state);
	throw new Error(`Invalid expression object: ${JSON.stringify(expression)}`);
}

export function parseExpression(expression: AnyExpression, state: ParserState): string {
	if (isExpressionObject(expression)) return parseExpressionObject(expression, state);
	if (isPrimitiveValue(expression)) return parsePrimitiveValue(expression);
	if (expression === null) return "NULL";
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

export const mergeConditions = (parsedConditions: string[], context: string): string => {
	const [firstCondition, ...restConditions] = parsedConditions;
	if (!firstCondition) throw new Error(`No conditions provided for ${context}`);

	if (restConditions.length === 0) return firstCondition; // Only one condition, no need for AND
	return `(${parsedConditions.join(" AND ")})`;
};

export function parseCondition(condition: Condition, state: ParserState): string {
	const parseSubCondition = (subCondition: Condition): string => parseCondition(subCondition, state);

	// Handle logical operators
	if ("$and" in condition)
		return `${mergeConditions(condition.$and.map(parseSubCondition), `$and condition (${JSON.stringify(condition.$and)})`)}`;
	if ("$or" in condition) {
		if (condition.$or.length === 0)
			throw new Error(`No conditions provided for $or condition (${JSON.stringify(condition.$or)})`);
		return `(${condition.$or.map(parseSubCondition).join(" OR ")})`;
	}
	if ("$not" in condition) return `NOT (${parseSubCondition(condition.$not)})`;

	// Handle EXISTS subquery
	if ("$exists" in condition) {
		const { table, conditions } = condition.$exists;
		const subQueryCondition = parseCondition(conditions, state); // Parse the subquery conditions
		return `EXISTS (SELECT 1 FROM ${table} WHERE ${subQueryCondition})`;
	}

	// Handle field conditions
	const conditions: string[] = [];
	for (const [field, fieldConditionExpression] of Object.entries(condition)) {
		const fieldPath = parseFieldPath({ field, state });
		const fieldConfig = fieldPath.fieldConfig;

		const fieldConditions = parseFieldConditions(fieldConditionExpression, state, fieldConfig);

		const select = selectField({ fieldPath, state, cast: fieldConditions.castType, jsonExtractText: true });
		const fieldName = castValue(select.field, select.cast);

		const clause = mergeConditions(
			fieldConditions.conditions.map((conditionClause) => `${fieldName} ${conditionClause}`),
			`field '${field}' in condition (${JSON.stringify(condition)})`,
		);
		conditions.push(clause);
	}

	return mergeConditions(conditions, `field conditions (${JSON.stringify(condition)})`);
}

function parametrize(value: Primitive, state: ParserState) {
	state.params.push(value);
	return `$${state.params.length}`;
}

type ParseFieldCondition = { condition: string; castType: CastType };
function parseComparisonCondition(value: AnyExpression, state: ParserState, operator: string, field: Field): ParseFieldCondition {
	if (isExpressionObject(value)) {
		const expression = parseExpressionObject(value, state);
		return { condition: `${operator} ${expression}`, castType: state.expressions.get(value) };
	}

	if (value !== null) {
		if (field.type !== typeof value && field.type !== "object")
			throw new Error(OPERATOR_TYPE_MISMATCH_ERROR(operator, field.name, field.type, typeof value));
		return { condition: `${operator} ${parametrize(value, state)}`, castType: getPrimitiveCastType(value) };
	}

	if (field && !field.nullable) throw new Error(`Field '${field.name}' is not nullable, and cannot be compared with NULL`);
	if (operator !== "=" && operator !== "!=") throw new Error(`Operator '${operator}' cannot be used with NULL value`);

	return { condition: operator === "=" ? "IS NULL" : "IS NOT NULL", castType: null };
}

function parseArrayCondition(value: AnyExpression[], state: ParserState, operator: "IN" | "NOT IN"): ParseFieldCondition {
	if (value.length === 0) throw new Error(`Operator '${operator}' requires a non-empty array`);
	const resolvedValues: { value: string; castType: CastType }[] = value.map((item) => {
		if (isExpressionObject(item)) {
			const value = parseExpressionObject(item, state);
			return { value, castType: state.expressions.get(item) };
		}
		if (!isPrimitiveValue(item)) throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string, number, or boolean"));
		return { value: parametrize(item, state), castType: getPrimitiveCastType(item) };
	});

	const castTypes = new Set(resolvedValues.map(({ castType }) => castType));
	const castType = castTypes.values().next().value;
	if (castTypes.size > 1) throw new Error(`Cannot use ${operator} with mixed types: ${Array.from(castTypes).join(", ")}`);
	if (!castType) throw new Error(`Cannot use ${operator} with NULL values`);
	return { condition: `${operator} (${resolvedValues.map(({ value }) => value).join(", ")})`, castType };
}

type StringOperator = "LIKE" | "ILIKE" | "~";
function parseStringCondition(
	value: AnyExpression,
	state: ParserState,
	operator: StringOperator,
	field: Field,
): ParseFieldCondition {
	if (value === null) throw new Error(`Operator '${operator}' cannot be used with NULL value`);
	if (isExpressionObject(value))
		return { condition: `${operator} (${parseExpressionObject(value, state)}::TEXT)`, castType: "TEXT" };
	if (typeof value !== "string") throw new Error(INVALID_OPERATOR_VALUE_TYPE_ERROR(operator, "string"));
	if (field && field.type !== "string" && field.type !== "object")
		throw new Error(`Field type mismatch for ${operator} operation on '${field.name}': expected string, got ${field.type}`);
	return { condition: `${operator} ${parametrize(value, state)}`, castType: "TEXT" };
}

type ParseFieldConditions = { conditions: string[]; castType: CastType };
function parseFieldConditions(condition: FieldCondition, state: ParserState, field: Field): ParseFieldConditions {
	// Treat expression as an equality condition if no operator is provided
	if (isScalarValue(condition) || isExpressionObject(condition)) {
		const fieldCondition = parseComparisonCondition(condition, state, "=", field);
		return { conditions: [fieldCondition.condition], castType: fieldCondition.castType };
	}

	const fieldConditions: { condition: string; castType: CastType }[] = [];
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

	const castTypes = new Set(fieldConditions.map(({ castType }) => castType).filter(isNotNull));
	if (castTypes.size > 1) throw new Error(`Cannot mix types in field conditions: ${Array.from(castTypes).join(", ")}`);

	const conditions = fieldConditions.map(({ condition }) => condition);
	return { conditions, castType: castTypes.values().next().value ?? null };
}
