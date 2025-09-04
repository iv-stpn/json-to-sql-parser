import type { Dialect } from "../constants/dialects";
import { FUNCTION_TYPE_MISMATCH_ERROR, INVALID_ARGUMENT_COUNT_ERROR, MISSING_AGGREGATION_FIELD_ERROR } from "../constants/errors";
import type { ExpressionType } from "../constants/field-types";
import { type AggregationDefinition, allowedAggregationFunctions } from "../functions/aggregate";
import { aliasValue, castValue, getExpressionType, parseExpression, parseField } from "../parsers";
import type { AggregatedField, AggregationQuery } from "../schemas";
import type { Config, ConfigWithForeignKeys, ParserState } from "../types";
import { objectEntries } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { applyFunction } from "../utils/function-call";
import { ensureNormalizedConfig } from "../utils/normalize-config";
import { buildJoinClause } from "./joins";
import { buildWhereClause } from "./where";

type AggregationState = ParserState & { joins: string[]; processedTables: Set<string> };
function processJoins(fields: string[], rootTable: string, state: AggregationState): void {
	// Extract all table references from field paths
	for (const fieldRef of fields) {
		const dotIndex = fieldRef.indexOf(".");
		if (dotIndex === 0) throw new Error(`Invalid field reference: ${fieldRef}. Field references cannot start with a dot.`);
		if (dotIndex <= -1) continue;

		const tableName = fieldRef.substring(0, dotIndex);
		if (tableName === rootTable) continue;

		const tableConfig = state.config.tables[tableName];
		if (!tableConfig) throw new Error(`Table '${tableName}' is not allowed or does not exist`);

		if (state.processedTables.has(tableName)) continue;

		const relationship = state.config.relationships.find(
			({ table, toTable }) => (table === rootTable && toTable === tableName) || (table === tableName && toTable === rootTable),
		);
		if (!relationship) throw new Error(`No relationship found between '${rootTable}' and '${tableName}'`);

		state.joins.push(buildJoinClause(rootTable, tableName, relationship, state.config));
		state.processedTables.add(tableName);
	}
}

function parseAggregationField(aggregation: AggregatedField, expressionType: ExpressionType, state: ParserState): string {
	if (typeof aggregation.field === "string") {
		const { select, fieldPath } = parseField(aggregation.field, state);
		if (fieldPath.fieldConfig.type !== expressionType && expressionType !== "any") {
			// Every type can be cast to TEXT, automatically cast in this case
			if (expressionType === "string") return castValue(select.field, "string", state.config.dialect);
			throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(aggregation.function, fieldPath.fieldConfig.type, expressionType));
		}

		// For COUNT operations, we don't need to cast the field
		if (aggregation.function === "COUNT") return select.field;
		return castValue(select.field, select.targetType, state.config.dialect);
	}

	return parseExpression(aggregation.field, state);
}

function applyAggregationOperator(
	operator: AggregationDefinition,
	alias: string,
	expression: string,
	args: string[],
	dialect: Dialect,
): string {
	if (operator.toSQL) return aliasValue(operator.toSQL(expression, args, dialect), alias);
	return aliasValue(applyFunction(operator.name, [expression, ...args]), alias);
}

function parseAggregation(alias: string, aggregation: AggregatedField, state: ParserState): string {
	const { function: operator, field } = aggregation;
	if (field === "*") {
		if (operator !== "COUNT")
			throw new Error(`Aggregation function '${operator}' cannot be used with '*'. Only COUNT(*) is supported.`);
		return aliasValue("COUNT(*)", alias);
	}

	const aggregationFunction = allowedAggregationFunctions.find(({ name }) => name === operator);
	if (!aggregationFunction) throw new Error(`Invalid aggregation operator: ${operator}`);

	const { additionalArgumentTypes = [], expressionType, name, variadic } = aggregationFunction;
	const aggregationArguments = aggregation.additionalArguments ?? [];

	const expression = parseAggregationField(aggregation, expressionType, state);

	if (additionalArgumentTypes.length > 0) {
		if (!additionalArgumentTypes || additionalArgumentTypes.length < aggregationArguments.length)
			throw new Error(
				INVALID_ARGUMENT_COUNT_ERROR(operator, aggregationArguments.length, additionalArgumentTypes.length, variadic),
			);

		if (additionalArgumentTypes.length > aggregationArguments.length && !variadic)
			throw new Error(
				INVALID_ARGUMENT_COUNT_ERROR(operator, additionalArgumentTypes.length, aggregationArguments.length, variadic),
			);

		const resolvedArguments = aggregationArguments.map((arg, index) => {
			const expectedType =
				index >= additionalArgumentTypes.length ? additionalArgumentTypes.at(-1) : additionalArgumentTypes[index];
			if (!expectedType) throw new Error(`No argument type defined for function '${name}' at index ${index}`);

			const argumentExpression = parseExpression(arg, state);

			const actualType = getExpressionType(arg, state);
			if (actualType !== expectedType && actualType !== null) {
				// Every type can be cast to TEXT, automatically cast in this case
				if (expectedType === "string") return castValue(argumentExpression, "string", state.config.dialect);
				throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(name, expectedType, actualType));
			}

			return argumentExpression;
		});

		return applyAggregationOperator(aggregationFunction, alias, expression, resolvedArguments, state.config.dialect);
	}

	if (aggregation.additionalArguments && aggregation.additionalArguments.length > 0)
		throw new Error(`Aggregation function '${operator}' does not support additional arguments.`);

	return applyAggregationOperator(aggregationFunction, alias, expression, [], state.config.dialect);
}

type ParsedAggregationQuery = {
	select: string[];
	from: string;
	where?: string;
	groupBy: string[];
	joins: string[];
};
export function parseAggregationQuery(query: AggregationQuery, config: Config | ConfigWithForeignKeys): ParsedAggregationQuery {
	const normalizedConfig = ensureNormalizedConfig(config);
	const expressions = new ExpressionTypeMap();
	const processedTables = new Set<string>([query.table]);

	const state: AggregationState = { config: normalizedConfig, expressions, rootTable: query.table, joins: [], processedTables };
	const { table, groupBy, aggregatedFields } = query;

	const aggregatedFieldEntries = objectEntries(aggregatedFields ?? {});
	if (aggregatedFieldEntries.length === 0 && groupBy.length === 0) throw new Error(MISSING_AGGREGATION_FIELD_ERROR);

	const tableConfig = normalizedConfig.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed`);

	const selectFields: string[] = [];
	const groupByFields: string[] = [];
	const fields: string[] = [];

	// Collect all field references
	for (const fieldName of groupBy) fields.push(fieldName);

	for (const [, aggregatedField] of aggregatedFieldEntries) {
		if (typeof aggregatedField.field === "string") fields.push(aggregatedField.field);
	}

	// Detect and add required joins
	processJoins(fields, table, state);

	// Process group by fields
	for (const fieldName of groupBy) {
		const { select } = parseField(fieldName, state);
		selectFields.push(aliasValue(castValue(select.field, select.targetType, normalizedConfig.dialect), select.alias));
		groupByFields.push(select.field);
	}

	// Process aggregated fields
	for (const [alias, aggregatedField] of aggregatedFieldEntries) {
		selectFields.push(parseAggregation(alias, aggregatedField, state));
	}

	const from = normalizedConfig.dataTable ? aliasValue(normalizedConfig.dataTable.table, table) : table;
	const where = buildWhereClause(query.condition, state);
	return { select: selectFields, from, where, groupBy: groupByFields, joins: state.joins };
}

export function compileAggregationQuery(query: ParsedAggregationQuery): string {
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.joins.length > 0) sql += ` ${query.joins.join(" ")}`;
	if (query.where) sql += ` WHERE ${query.where}`;
	if (query.groupBy.length > 0) sql += ` GROUP BY ${query.groupBy.join(", ")}`;
	return sql;
}

export function buildAggregationQuery(query: AggregationQuery, config: Config | ConfigWithForeignKeys): string {
	const parsedQuery = parseAggregationQuery(query, config);
	const sql = compileAggregationQuery(parsedQuery);
	return sql;
}
