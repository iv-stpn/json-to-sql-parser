import { aggregationOperators, applyAggregationOperator } from "../constants/operators";
import { aliasValue, castValue, parseExpression, parseField } from "../parsers";
import type { AggregatedField, AggregationQuery } from "../schemas";
import type { BaseParsedQuery, Config, ParserState, Primitive } from "../types";
import { objectEntries } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
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

function parseAggregationField(aggregation: AggregatedField, state: ParserState): string {
	if (typeof aggregation.field === "string") {
		const { select } = parseField(aggregation.field, state);
		// For COUNT operations, we don't need to cast the field
		if (aggregation.operator === "COUNT") return select.field;
		return castValue(select.field, select.cast);
	}

	return parseExpression(aggregation.field, state);
}

function parseAggregation(alias: string, aggregation: AggregatedField, state: ParserState): string {
	const { operator, field } = aggregation;
	if (field === "*") {
		if (operator !== "COUNT") throw new Error(`Operator '${operator}' cannot be used with '*'. Only COUNT(*) is supported.`);
		return aliasValue("COUNT(*)", alias);
	}

	return aliasValue(applyAggregationOperator(parseAggregationField(aggregation, state), operator), alias);
}

// function getFieldsFromAggregatedField(aggregatedField: string | ExpressionObject, fields: string[] = []): string[] {
// 	if (typeof aggregatedField === "string") return [aggregatedField];
// 	if ("$expr" in aggregatedField)
// }

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
		if (!aggregationOperators.includes(aggregatedField.operator))
			throw new Error(`Invalid aggregation operator: ${aggregatedField.operator}`);
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
