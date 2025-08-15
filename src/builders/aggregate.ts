import { type AggregationDefinition, allowedAggregationFunctions } from "../constants/aggregation-functions";
import { type CastType, castMap } from "../constants/cast-types";
import { FUNCTION_TYPE_MISMATCH_ERROR, INVALID_ARGUMENT_COUNT_ERROR } from "../constants/errors";
import { aliasValue, castValue, getExpressionCastType, parseExpression, parseField } from "../parsers";
import type { AggregatedField, AggregationQuery } from "../schemas";
import type { BaseParsedQuery, Config, ParserState, Primitive } from "../types";
import { objectEntries } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { applyFunction } from "../utils/function-call";
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

function parseAggregationField(aggregation: AggregatedField, expressionType: CastType | "ANY", state: ParserState): string {
	if (typeof aggregation.field === "string") {
		const { select, fieldPath } = parseField(aggregation.field, state);
		const fieldCastType = castMap[fieldPath.fieldConfig.type];
		if (fieldCastType !== expressionType && expressionType !== "ANY") {
			if (expressionType === "TEXT") return castValue(select.field, "TEXT"); // Every type can be cast to TEXT, automatically cast in this case
			throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(aggregation.function, fieldCastType, expressionType));
		}

		// For COUNT operations, we don't need to cast the field
		if (aggregation.function === "COUNT") return select.field;
		return castValue(select.field, select.cast);
	}

	return parseExpression(aggregation.field, state);
}

function applyAggregationOperator(operator: AggregationDefinition, alias: string, expression: string, args: string[]): string {
	if (operator.toSQL) return aliasValue(operator.toSQL(expression, args), alias);
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

			const expression = parseExpression(arg, state);

			if (expectedType === "ANY") return expression;
			const actualType = getExpressionCastType(arg, state);

			if (actualType !== expectedType && actualType !== null) {
				if (expectedType === "TEXT") return castValue(expression, "TEXT"); // Every type can be cast to TEXT, automatically cast in this case
				throw new Error(FUNCTION_TYPE_MISMATCH_ERROR(name, expectedType, actualType));
			}

			return expression;
		});

		return applyAggregationOperator(aggregationFunction, alias, expression, resolvedArguments);
	}

	if (aggregation.additionalArguments && aggregation.additionalArguments.length > 0)
		throw new Error(`Aggregation function '${operator}' does not support additional arguments.`);

	return applyAggregationOperator(aggregationFunction, alias, expression, []);
}

type ParsedAggregationQuery = BaseParsedQuery & { groupBy: string[]; joins: string[] };
export function parseAggregationQuery(query: AggregationQuery, config: Config): ParsedAggregationQuery {
	const expressions = new ExpressionTypeMap();
	const processedTables = new Set<string>([query.table]);

	const state: AggregationState = { config, params: [], expressions, rootTable: query.table, joins: [], processedTables };
	const { table, groupBy, aggregatedFields } = query;

	const aggregatedFieldEntries = objectEntries(aggregatedFields ?? {});
	if (aggregatedFieldEntries.length === 0 && groupBy.length === 0)
		throw new Error("Aggregation query must have at least one group by field or aggregated field");

	const tableConfig = config.tables[table];
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
		selectFields.push(aliasValue(castValue(select.field, select.cast), select.alias));
		groupByFields.push(select.field);
	}

	// Process aggregated fields
	for (const [alias, aggregatedField] of aggregatedFieldEntries) {
		selectFields.push(parseAggregation(alias, aggregatedField, state));
	}

	const from = config.dataTable ? aliasValue(config.dataTable.table, table) : table;
	const where = buildWhereClause(query.condition, state);
	return { select: selectFields, from, where, groupBy: groupByFields, joins: state.joins, params: state.params };
}

export function compileAggregationQuery(query: ParsedAggregationQuery): string {
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.joins.length > 0) sql += ` ${query.joins.join(" ")}`;
	if (query.where) sql += ` WHERE ${query.where}`;
	if (query.groupBy.length > 0) sql += ` GROUP BY ${query.groupBy.join(", ")}`;
	return sql;
}

export function buildAggregationQuery(query: AggregationQuery, config: Config): { sql: string; params: Primitive[] } {
	const parsedQuery = parseAggregationQuery(query, config);
	const sql = compileAggregationQuery(parsedQuery);
	return { sql, params: parsedQuery.params };
}
