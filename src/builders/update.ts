import { ensureConditionObject } from "../parsers/issues";
import type { EvaluationContext } from "../parsers/mutations";
import { evaluateCondition, parseNewRow, processMutationFields } from "../parsers/mutations";
import type { Condition, ScalarPrimitive, UpdateQuery } from "../schemas";
import type { Config, ParserState } from "../types";
import { doubleQuote } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { buildWhereClause } from "./where";

type UpdateState = ParserState & { updates: string[] };
type ParsedUpdateQuery = { table: string; params: ScalarPrimitive[]; updates: Record<string, unknown>; where?: string };

export function parseUpdateQuery(updateQuery: UpdateQuery, config: Config): ParsedUpdateQuery {
	const { table, updates, condition } = updateQuery;

	// Validate table and updates using reusable utilities
	const tableConfig = config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	config.tables.NEW_ROW = tableConfig;

	const fields = tableConfig.allowedFields;
	const newRow = parseNewRow(table, updates, fields);

	// Initialize state
	const expressions = new ExpressionTypeMap();
	const state: UpdateState = { config, rootTable: table, params: [], expressions, updates: [] };

	// Process update fields and generate WHERE clause
	const processedFields = processMutationFields(updates, state);

	let conditionResult: Condition = true;
	if (condition) {
		const evaluationContext: EvaluationContext = { newRow, rootTable: table, fields, config, mutationType: "UPDATE" };
		conditionResult = evaluateCondition(ensureConditionObject(condition), evaluationContext);
	}

	if (conditionResult === false) throw new Error("Update condition not met.");
	if (conditionResult === true) return { table, params: state.params, updates: processedFields };
	return { table, params: state.params, updates: processedFields, where: buildWhereClause(conditionResult, state) };
}

export function compileUpdateQuery(query: ParsedUpdateQuery): string {
	let sql = `UPDATE ${query.table} SET ${Object.entries(query.updates)
		.map(([column, value]) => `${doubleQuote(column)} = ${value}`)
		.join(", ")}`;

	// Add WHERE clause if one was generated
	if (query.where) sql += ` WHERE ${query.where}`;
	return sql;
}

export function buildUpdateQuery(
	updateQuery: UpdateQuery,
	config: Config,
): { sql: string; params: ScalarPrimitive[]; conditionResult?: Condition } {
	const parsedQuery = parseUpdateQuery(updateQuery, config);
	const sql = compileUpdateQuery(parsedQuery);
	return { sql, params: parsedQuery.params };
}
