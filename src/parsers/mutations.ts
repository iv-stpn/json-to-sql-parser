/** biome-ignore-all lint/suspicious/noThenProperty: we use then and else in our $cond spec **/

import { z } from "zod";
import { FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR, NON_EMPTY_CONDITION_ARRAY_ERROR } from "../constants/errors";
import type {
	AnyExpression,
	AnyFieldCondition,
	AnyScalar,
	Condition,
	ConditionFieldName,
	comparisonOperators,
	FieldCondition,
	stringOperators,
} from "../schemas";
import { anyScalarSchema, conditionSchema } from "../schemas";
import type { Config, Field, ParserState } from "../types";
import { objectEntries, objectSize } from "../utils";
import {
	ensureTimestampString,
	ensureUUID,
	isExpressionObject,
	isField,
	isNonNullObject,
	isScalarExpression,
	isScalarPrimitive,
	isValidDate,
	isValidTimestamp,
	uuidRegex,
} from "../utils/validators";
import { parseExpressionObject, parseFieldPath, parseScalarExpression, parseScalarValue, resolveFunction } from ".";

function getScalarExpressionValue(value: unknown, key: "$date" | "$uuid" | "$timestamp"): string {
	if (isNonNullObject(value)) {
		const fieldValue = value[key];
		if (!fieldValue) throw new Error(`Missing field '${key}' in value: ${JSON.stringify(value)}`);
		if (typeof fieldValue !== "string") throw new Error(`Field '${key}' must be a string in value: ${JSON.stringify(value)}`);
		return fieldValue;
	}

	if (typeof value === "string") return value;
	throw new Error(`Field should be an object containing '${key}' or a string representation, got: ${JSON.stringify(value)}`);
}

export function parseNewRowValue(value: unknown, fieldConfig: Field): AnyScalar {
	if (value === null) {
		if (!fieldConfig.nullable) throw new Error(`Field '${fieldConfig.name}' cannot be null`);
		return null;
	}

	if (fieldConfig.type === "date") {
		const fieldValue = getScalarExpressionValue(value, "$date");
		if (!isValidDate(fieldValue))
			throw new Error(`Invalid date value for field '${fieldConfig.name}': '${value}'. Expected format: YYYY-MM-DD`);
		return { $date: fieldValue };
	}

	if (fieldConfig.type === "uuid") {
		const uuidValue = getScalarExpressionValue(value, "$uuid");
		ensureUUID(uuidValue);
		return { $uuid: uuidValue };
	}

	if (fieldConfig.type === "datetime") {
		const datetimeValue = getScalarExpressionValue(value, "$timestamp");
		ensureTimestampString(datetimeValue);
		return { $timestamp: datetimeValue };
	}

	if (fieldConfig.type === "object") {
		if (!isNonNullObject(value))
			throw new Error(`Field '${fieldConfig.name}' expects an object value, got: ${JSON.stringify(value)}`);
		return { $jsonb: value };
	}

	if (fieldConfig.type === "boolean") {
		if (typeof value !== "boolean")
			throw new Error(`Field '${fieldConfig.name}' expects a boolean value, got: ${JSON.stringify(value)}`);
		return value;
	}

	if (fieldConfig.type === "string") {
		if (typeof value !== "string")
			throw new Error(`Field '${fieldConfig.name}' expects a string value, got: ${JSON.stringify(value)}`);
		return value;
	}

	if (fieldConfig.type === "number") {
		if (typeof value !== "number")
			throw new Error(`Field '${fieldConfig.name}' expects a number value, got: ${JSON.stringify(value)}`);
		return value;
	}

	throw new Error(`Field '${fieldConfig.name}' has unsupported type '${fieldConfig.type}'`);
}

type ParsedRow = Record<string, AnyScalar>;
export function parseNewRow(tableName: string, newRow: Record<string, unknown>, fields: Field[]): ParsedRow {
	const parsedRow: ParsedRow = {};
	for (const [fieldName, value] of Object.entries(newRow)) {
		const fieldConfig = fields.find((field) => field.name === fieldName);
		if (!fieldConfig) throw new Error(`Field '${fieldName}' is not allowed for table '${tableName}'`);
		parsedRow[fieldName] = parseNewRowValue(value, fieldConfig);
	}
	return parsedRow;
}

type UnresolvedExpression =
	| { $field: string }
	| { $func: Record<string, EvaluationResult[]> }
	| { $cond: { if: Condition; then: EvaluationResult; else: EvaluationResult } };

const unresolvedExpressionSchema: z.ZodType<UnresolvedExpression> = z.lazy(() =>
	z.union([
		z.strictObject({ $field: z.string() }).strict(),
		z.strictObject({ $func: z.record(z.string(), z.array(z.union([unresolvedExpressionSchema, anyScalarSchema]))) }).strict(),
		z.strictObject({
			$cond: z.strictObject({
				if: conditionSchema,
				then: z.union([unresolvedExpressionSchema, anyScalarSchema]),
				else: z.union([unresolvedExpressionSchema, anyScalarSchema]),
			}),
		}),
	]),
);

function isUnresolvedExpression(value: unknown): value is UnresolvedExpression {
	const parse = unresolvedExpressionSchema.safeParse(value);
	return parse.success;
}

export function parseNewRowWithDefaults(table: string, newRow: ParsedRow, config: Config, fields: Field[]): ParsedRow {
	const parsedRow = parseNewRow(table, newRow, fields);
	const missingFields = fields.filter((field) => parsedRow[field.name] === undefined);
	const remainingDefaults: { [key: string]: AnyExpression } = {};

	// First pass - resolve null values and ensure defaults
	for (const field of missingFields) {
		const resolveDefaultField = fields.find(({ name }) => name === field.name);
		if (!resolveDefaultField) throw new Error(`Field '${field.name}' does not exist in '${table}'`);

		// Default value for nullable fields
		if (resolveDefaultField.default === undefined) {
			if (!resolveDefaultField.nullable) throw new Error(`Missing default value for non-nullable field '${field.name}'`);
			parsedRow[field.name] = null;
			continue;
		}

		remainingDefaults[field.name] = resolveDefaultField.default;
	}

	// Next passes - resolve default values with field dependencies
	let lastRemainingDefaultsCount = Number.NEGATIVE_INFINITY;
	while (objectSize(remainingDefaults) > 0 && objectSize(remainingDefaults) !== lastRemainingDefaultsCount) {
		lastRemainingDefaultsCount = objectSize(remainingDefaults);
		for (const [fieldName, defaultValue] of Object.entries(remainingDefaults)) {
			const evaluation = evaluateExpression(defaultValue, { newRow: parsedRow, table, rootTable: table, fields: fields, config });
			if (isUnresolvedExpression(evaluation)) {
				remainingDefaults[fieldName] = defaultValue;
			} else {
				parsedRow[fieldName] = evaluation;
				delete remainingDefaults[fieldName];
			}
		}
	}

	if (objectSize(remainingDefaults) > 0)
		throw new Error(`Cannot resolve defaults for ${Object.keys(remainingDefaults).join(", ")} due to circular dependencies`);
	return parsedRow;
}

// First pass of resolving the default value of a field
export type EvaluationContext = {
	table?: string;
	mutationType?: "INSERT" | "UPDATE";
	rootTable: string;
	newRow: ParsedRow;
	fields: Field[];
	config: Config;
};
type EvaluationResult = AnyScalar | UnresolvedExpression;

function argumentsDoesNotHaveUnresolvedExpression(args: EvaluationResult[]): args is AnyScalar[] {
	if (args.some(isUnresolvedExpression)) return false;
	return true;
}

function resolveField(field: string, context: EvaluationContext): AnyScalar | undefined {
	const fieldPath = parseFieldPath(field, context.table ?? context.rootTable, context.config);
	if (context.mutationType === "INSERT" && !context.table && fieldPath.table === context.rootTable)
		throw new Error(FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR);

	if (fieldPath.table !== "NEW_ROW") return undefined;

	const value = context.newRow[fieldPath.field];
	if (value === undefined) return undefined;

	if (fieldPath.jsonAccess.length === 0) return value;
	if (!isNonNullObject(value) || !("$jsonb" in value)) return null;

	let currentPathValue: unknown = value.$jsonb;
	for (const accessKey of fieldPath.jsonAccess) {
		if (!isNonNullObject(currentPathValue)) return null;
		const nextValue = currentPathValue[accessKey];
		if (nextValue === undefined) return null;
		currentPathValue = nextValue;
	}

	if (currentPathValue === null) return null;
	if (isNonNullObject(currentPathValue)) return { $jsonb: currentPathValue };
	if (typeof currentPathValue === "string" || typeof currentPathValue === "number" || typeof currentPathValue === "boolean")
		return currentPathValue;
	throw new Error(`Unsupported value type: ${typeof currentPathValue}`);
}

function evaluateExpression(expression: AnyExpression, context: EvaluationContext): EvaluationResult {
	if (isScalarPrimitive(expression)) return expression;

	if ("$timestamp" in expression) {
		if (!isValidTimestamp(expression.$timestamp)) throw new Error(`Invalid timestamp format: ${expression.$timestamp}`);
		return { $timestamp: expression.$timestamp };
	}

	if ("$date" in expression) {
		if (!isValidDate(expression.$date)) throw new Error(`Invalid date format: ${expression.$date}`);
		return { $date: expression.$date };
	}

	if ("$uuid" in expression) {
		if (!uuidRegex.test(expression.$uuid)) throw new Error(`Invalid UUID format: ${expression.$uuid}`);
		return { $uuid: expression.$uuid };
	}

	if ("$jsonb" in expression) {
		if (!isNonNullObject(expression.$jsonb)) throw new Error(`Invalid JSONB format: ${expression.$jsonb}`);
		return { $jsonb: expression.$jsonb };
	}

	if ("$field" in expression) {
		const value = resolveField(expression.$field, context);
		return value === undefined
			? { $field: context.mutationType === "UPDATE" ? expression.$field.replace("NEW_ROW.", "") : expression.$field }
			: value;
	}

	if ("$var" in expression) {
		const value = context.config.variables[expression.$var];
		if (value === undefined) throw new Error(`Variable '${expression.$var}' is not defined`);
		return value;
	}

	if ("$func" in expression) {
		const { args, definition } = resolveFunction(expression.$func);
		const evaluatedArgs = args.map((arg) => evaluateExpression(arg, context));
		if (!argumentsDoesNotHaveUnresolvedExpression(evaluatedArgs)) return { $func: { [definition.name]: evaluatedArgs } };
		return definition.toJS({ config: context.config, args: evaluatedArgs });
	}

	if ("$cond" in expression) {
		const { if: condition, then: thenValue, else: elseValue } = expression.$cond;

		const thenExpression = evaluateExpression(thenValue, context);
		const elseExpression = evaluateExpression(elseValue, context);

		const conditionResult = evaluateCondition(condition, context);
		if (typeof conditionResult === "boolean") return conditionResult ? thenExpression : elseExpression;
		return { $cond: { if: conditionResult, then: thenExpression, else: elseExpression } };
	}

	throw new Error(`Unsupported expression type: ${JSON.stringify(expression)}`);
}

export function processMutationFields(newRow: Record<string, unknown>, state: ParserState): Record<string, string> {
	return Object.entries(newRow).reduce<Record<string, string>>((acc, [fieldName, value]) => {
		// Process each field in the newRow object
		if (value === null) {
			acc[fieldName] = "NULL";
		} else if (isExpressionObject(value)) {
			acc[fieldName] = parseExpressionObject(value, state);
		} else if (isScalarExpression(value)) {
			acc[fieldName] = parseScalarExpression(value);
		} else if (isScalarPrimitive(value)) {
			acc[fieldName] = parseScalarValue(value);
		} else {
			throw new Error(`Unsupported value type for field '${fieldName}': ${JSON.stringify(value)}`);
		}
		return acc;
	}, {});
}

export function evaluateCondition(condition: Condition, context: EvaluationContext): Condition {
	// Handle boolean values and expression objects
	if (typeof condition === "boolean") return condition;
	if (condition === null) return false;

	if (isExpressionObject(condition)) {
		const result = evaluateExpression(condition, context);
		if (typeof result === "boolean") return result;
		if (result === null) return false;
		throw new Error(`Expression must evaluate to boolean or null (got ${JSON.stringify(result)})`);
	}

	if (isScalarExpression(condition)) throw new Error(`Condition must evaluate to boolean (got ${JSON.stringify(condition)})`);
	if (!isNonNullObject(condition)) throw new Error(`Invalid condition type: ${typeof condition}`);

	// Handle logical operators
	if ("$not" in condition) {
		const conditionResult = evaluateCondition(condition.$not, context);
		if (typeof conditionResult === "boolean") return !conditionResult;
		return { $not: conditionResult };
	}
	if ("$exists" in condition) {
		const existCondition = evaluateCondition(condition.$exists.condition, { ...context, table: condition.$exists.table });
		if (typeof existCondition === "boolean") return existCondition;
		return { $exists: { table: condition.$exists.table, condition: existCondition } };
	}
	if ("$and" in condition) {
		if (!Array.isArray(condition.$and) || condition.$and.length === 0) throw NON_EMPTY_CONDITION_ARRAY_ERROR("$and");
		const conditionResults = condition.$and.map((subCondition) => evaluateCondition(subCondition, context));
		if (conditionResults.some((result) => typeof result === "boolean" && result === false)) return false;
		if (conditionResults.every((result) => typeof result === "boolean" && result === true)) return true;
		return { $and: conditionResults.filter((result) => typeof result !== "boolean") };
	}
	if ("$or" in condition) {
		if (!Array.isArray(condition.$or) || condition.$or.length === 0) throw NON_EMPTY_CONDITION_ARRAY_ERROR("$or");
		const conditionResults = condition.$or.map((subCondition) => evaluateCondition(subCondition, context));
		if (conditionResults.some((result) => typeof result === "boolean" && result === true)) return true;
		if (conditionResults.every((result) => typeof result === "boolean" && result === false)) return false;
		return { $or: conditionResults.filter((result) => typeof result !== "boolean") };
	}

	// Handle field conditions
	const fieldConditions: Record<ConditionFieldName, FieldCondition> = {};
	for (const [field, fieldCondition] of objectEntries(condition)) {
		const resolvedField = resolveField(field, context);
		const conditionResult = evaluateFieldCondition(resolvedField, fieldCondition, context);
		if (conditionResult === false) return false;
		if (conditionResult !== true) {
			const fieldName = resolvedField === undefined && context.mutationType === "UPDATE" ? field.replace("NEW_ROW.", "") : field;
			if (!isField(fieldName)) throw new Error(`Invalid field name: ${fieldName}`);
			fieldConditions[fieldName] = { ...fieldConditions[fieldName], ...conditionResult };
		}
	}

	if (objectSize(fieldConditions) > 0) return fieldConditions;
	return true;
}

type NonNullableScalar = Exclude<AnyScalar, null>;

function areSameScalarType(scalar1: NonNullableScalar, scalar2: NonNullableScalar): boolean {
	if (typeof scalar1 !== typeof scalar2) return false;
	if (typeof scalar1 === "object" && typeof scalar2 === "object") {
		if ("$date" in scalar1 && "$date" in scalar2) return true;
		if ("$timestamp" in scalar1 && "$timestamp" in scalar2) return true;
		if ("$uuid" in scalar1 && "$uuid" in scalar2) return true;
		if ("$jsonb" in scalar1 && "$jsonb" in scalar2) return true;
	}
	return true;
}

const comparisonFunctions: Record<
	keyof typeof comparisonOperators,
	(field: NonNullableScalar, value: NonNullableScalar) => boolean
> = {
	$eq: (field: NonNullableScalar, value: NonNullableScalar) => field === value,
	$ne: (field: NonNullableScalar, value: NonNullableScalar) => field !== value,
	$gt: (field: NonNullableScalar, value: NonNullableScalar) => field > value,
	$gte: (field: NonNullableScalar, value: NonNullableScalar) => field >= value,
	$lt: (field: NonNullableScalar, value: NonNullableScalar) => field < value,
	$lte: (field: NonNullableScalar, value: NonNullableScalar) => field <= value,
};

class ExpressionFalseAbort extends Error {}

function evalutationComparison(
	evaluation: EvaluationResult,
	fieldValue: AnyScalar | undefined,
	fieldCondition: FieldCondition,
	comparisonType: keyof typeof comparisonOperators,
) {
	if (isUnresolvedExpression(evaluation) || fieldValue === undefined) {
		fieldCondition[comparisonType] = evaluation;
		return;
	}

	const hasNull = fieldValue === null || evaluation === null;
	if (hasNull) {
		// If equality expression, treat as IS/IS NOT NULL
		if (
			(comparisonType === "$eq" || comparisonType === "$ne") &&
			(comparisonType === "$eq" ? evaluation === fieldValue : evaluation !== fieldValue)
		) {
			return true;
		}

		throw new ExpressionFalseAbort(); // Return false if NULL comparison fails or for any other comparison involving NULL
	}

	if (!areSameScalarType(fieldValue, evaluation))
		throw new Error(`Type mismatch between ${JSON.stringify(fieldValue)} and ${JSON.stringify(evaluation)}.`);

	const comparisonFn = comparisonFunctions[comparisonType];
	if (!comparisonFn) throw new Error(`Unknown comparison operator: ${comparisonType}`);

	const result = comparisonFn(fieldValue, evaluation);
	if (result === false) throw new ExpressionFalseAbort();
}

function getRegexFromStringOperator(value: string, operator: keyof typeof stringOperators) {
	if (operator === "$like") return new RegExp(`^${value.replace(/%/g, ".*").replace(/_/g, ".")}$`);
	if (operator === "$ilike") return new RegExp(`^${value.replace(/%/g, ".*").replace(/_/g, ".")}$`, "i");
	if (operator === "$regex") return new RegExp(value);
	throw new Error(`Unknown string operator: ${operator}`);
}

function evaluateTextCondition(
	evaluation: EvaluationResult,
	fieldValue: AnyScalar | undefined,
	fieldCondition: FieldCondition,
	stringOperator: keyof typeof stringOperators,
) {
	if (evaluation === null || fieldValue === null) throw new ExpressionFalseAbort();
	if (isUnresolvedExpression(evaluation) || fieldValue === undefined) {
		fieldCondition[stringOperator] = evaluation;
		return;
	}
	if (typeof evaluation !== "string")
		throw new Error(`Expected string evaluation for ${stringOperator} operator, got ${typeof evaluation}`);
	if (typeof fieldValue !== "string") throw new Error(`Field value must be a string for ${stringOperator} operator`);

	const regex = getRegexFromStringOperator(evaluation, stringOperator);
	if (!regex) throw new Error(`Failed to get regex for string operator: ${stringOperator}`);

	const result = regex.test(evaluation);
	if (result === false) throw new ExpressionFalseAbort();
}

function evaluateArrayCondition(
	items: AnyExpression[],
	fieldValue: AnyScalar | undefined,
	fieldCondition: FieldCondition,
	evaluationContext: EvaluationContext,
	notIn: boolean = false,
) {
	const evaluatedItems = items.map((item) => evaluateExpression(item, evaluationContext));
	if (evaluatedItems.some(isUnresolvedExpression) || fieldValue === undefined) {
		fieldCondition[notIn ? "$nin" : "$in"] = evaluatedItems;
		return;
	}

	const hasValue = evaluatedItems.some((item) => {
		if (isScalarPrimitive(item)) return item === fieldValue;
		if (isNonNullObject(fieldValue)) {
			if ("$uuid" in item) return "$uuid" in fieldValue && fieldValue.$uuid === item.$uuid;
			if ("$date" in item) return "$date" in fieldValue && fieldValue.$date === item.$date;
			if ("$timestamp" in item) return "$timestamp" in fieldValue && fieldValue.$timestamp === item.$timestamp;
			if ("$jsonb" in item) return "$jsonb" in fieldValue && fieldValue.$jsonb === item.$jsonb;
		}
		return false;
	});

	if ((notIn && hasValue) || (!notIn && !hasValue)) throw new ExpressionFalseAbort();
}

function evaluateFieldCondition(
	fieldValue: AnyScalar | undefined,
	condition: AnyFieldCondition,
	context: EvaluationContext,
): boolean | FieldCondition {
	const fieldCondition: FieldCondition = {};
	const evaluate = (condition: AnyExpression) => evaluateExpression(condition, context);

	try {
		if (isScalarPrimitive(condition) || isExpressionObject(condition)) {
			// Handle direct value comparison
			evalutationComparison(evaluate(condition), fieldValue, fieldCondition, "$eq");
		} else {
			// Handle field conditions
			if (condition.$eq !== undefined) evalutationComparison(evaluate(condition.$eq), fieldValue, fieldCondition, "$eq");
			if (condition.$ne !== undefined) evalutationComparison(evaluate(condition.$ne), fieldValue, fieldCondition, "$ne");

			if (condition.$gt !== undefined) evalutationComparison(evaluate(condition.$gt), fieldValue, fieldCondition, "$gt");
			if (condition.$gte !== undefined) evalutationComparison(evaluate(condition.$gte), fieldValue, fieldCondition, "$gte");
			if (condition.$lt !== undefined) evalutationComparison(evaluate(condition.$lt), fieldValue, fieldCondition, "$lt");
			if (condition.$lte !== undefined) evalutationComparison(evaluate(condition.$lte), fieldValue, fieldCondition, "$lte");
			if (condition.$like !== undefined) evaluateTextCondition(evaluate(condition.$like), fieldValue, fieldCondition, "$like");
			if (condition.$ilike !== undefined) evaluateTextCondition(evaluate(condition.$ilike), fieldValue, fieldCondition, "$ilike");
			if (condition.$regex !== undefined) evaluateTextCondition(evaluate(condition.$regex), fieldValue, fieldCondition, "$regex");
			if (condition.$in !== undefined) evaluateArrayCondition(condition.$in, fieldValue, fieldCondition, context, false);
			if (condition.$nin !== undefined) evaluateArrayCondition(condition.$nin, fieldValue, fieldCondition, context, true);
		}
	} catch (error) {
		if (error instanceof ExpressionFalseAbort) return false;
		throw error;
	}

	if (objectSize(fieldCondition) === 0) return true;
	return fieldCondition;
}
