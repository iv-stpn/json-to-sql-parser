import type { Condition, Config } from "../src";
import { parseAggregationQuery } from "../src/parsers/aggregate";
import { parseSelectQuery } from "../src/parsers/select";

export function extractSelectWhereClause(
	condition: Condition,
	config: Config,
	rootTable: string,
): { sql: string; params: unknown[] } {
	const query = {
		rootTable,
		selection: { [`${rootTable}.id`]: true }, // minimal selection
		condition,
	};
	const parsedQuery = parseSelectQuery(query, config);
	return { sql: parsedQuery.where || "", params: parsedQuery.params };
}

export function extractAggregationWhereClause(
	condition: Condition,
	config: Config,
	rootTable: string,
): { sql: string; params: unknown[] } {
	const query = {
		table: rootTable,
		groupBy: [`${rootTable}.id`], // minimal grouping
		condition,
	};
	const parsedQuery = parseAggregationQuery(query, config);
	return { sql: parsedQuery.where || "", params: parsedQuery.params };
}
