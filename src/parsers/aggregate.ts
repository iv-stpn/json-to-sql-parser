import { aggregationOperators, applyAggregationOperator } from "../constants/operators";
import type { Aggregation, AggregationQuery } from "../schemas";
import type { Config, ParserState, Primitive } from "../types";
import { objectEntries } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { aliasValue, castValue, parseExpression, parseField } from ".";

export type { AggregationQuery } from "../schemas";

type ParsedAggregationQuery = { select: string[]; from: string; groupBy: string[]; params: Primitive[] };

function parseAggregationField(table: string, aggregation: Aggregation, state: ParserState): string {
	if (typeof aggregation.field === "string") {
		const { select } = parseField(table, aggregation.field, state);
		return castValue(select.field, select.cast);
	}

	return parseExpression(aggregation.field, state);
}

function parseAggregation(table: string, alias: string, aggregation: Aggregation, state: ParserState): string {
	const { operator, field } = aggregation;
	if (field === "*") {
		if (operator !== "COUNT") throw new Error(`Operator '${operator}' cannot be used with '*'. Only COUNT(*) is supported.`);
		return aliasValue("COUNT(*)", alias);
	}

	return aliasValue(applyAggregationOperator(parseAggregationField(table, aggregation, state), operator), alias);
}

export function parseAggregationQuery(query: AggregationQuery, config: Config): ParsedAggregationQuery {
	const state = { config, params: [], expressions: new ExpressionTypeMap(), rootTable: query.table };
	const { table, groupBy, aggregatedFields } = query;

	const aggregatedFieldEntries = objectEntries(aggregatedFields);
	if (aggregatedFieldEntries.length === 0 && groupBy.length === 0)
		throw new Error("Aggregation query must have at least one group by field or aggregated field");

	const tableConfig = config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed`);

	const selectFields: string[] = [];
	const groupByFields: string[] = [];

	// Process group by fields
	for (const fieldName of groupBy) {
		const { select } = parseField(table, fieldName, state);
		selectFields.push(aliasValue(castValue(select.field, select.cast), select.alias));
		groupByFields.push(select.field);
	}

	// Process aggregated fields
	for (const [alias, aggregatedField] of aggregatedFieldEntries) {
		if (!aggregationOperators.includes(aggregatedField.operator))
			throw new Error(`Invalid aggregation operator: ${aggregatedField.operator}`);
		selectFields.push(parseAggregation(table, alias, aggregatedField, state));
	}

	const from = config.dataTable ? aliasValue(config.dataTable.table, table) : table;
	return { select: selectFields, from, groupBy: groupByFields, params: state.params };
}

export function compileAggregationQuery(query: ParsedAggregationQuery): string {
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.groupBy.length > 0) sql += ` GROUP BY ${query.groupBy.join(", ")}`;
	return sql;
}

export function generateAggregationQuery(query: AggregationQuery, config: Config): { sql: string; params: Primitive[] } {
	const parsedQuery = parseAggregationQuery(query, config);
	const sql = compileAggregationQuery(parsedQuery);
	return { sql, params: parsedQuery.params };
}
