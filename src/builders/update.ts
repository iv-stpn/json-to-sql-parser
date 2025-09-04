import { ensureConditionObject } from "../parsers/issues";
import type { EvaluationContext } from "../parsers/mutations";
import { evaluateCondition, parseNewRow, processMutationFields } from "../parsers/mutations";
import type { Condition, UpdateQuery } from "../schemas";
import type { Config, ConfigWithForeignKeys, ParserState } from "../types";
import { doubleQuote } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { ensureNormalizedConfig } from "../utils/normalize-config";
import { buildWhereClause } from "./where";

type UpdateState = ParserState & { updates: string[] };
type ParsedUpdateQuery = { table: string; updates: Record<string, unknown>; where?: string };

export function parseUpdateQuery(updateQuery: UpdateQuery, baseConfig: Config | ConfigWithForeignKeys): ParsedUpdateQuery {
	const config = ensureNormalizedConfig(baseConfig);

	const { table, updates, condition } = updateQuery;

	// Validate table and updates using reusable utilities
	const tableConfig = config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	config.tables.NEW_ROW = tableConfig;

	const fields = tableConfig.allowedFields;

	const expressions = new ExpressionTypeMap();
	const newRow = parseNewRow({ config, rootTable: table, fields, mutationType: "UPDATE" }, expressions, updates);

	// Initialize state
	const state: UpdateState = { config, rootTable: table, expressions, updates: [] };

	// Process update fields and generate WHERE clause
	const processedFields = processMutationFields(updates, state);

	let conditionResult: Condition = true;
	if (condition) {
		const evaluationContext: EvaluationContext = { newRow, fields, rootTable: table, config, mutationType: "UPDATE" };
		conditionResult = evaluateCondition(ensureConditionObject(condition), evaluationContext);
	}

	if (conditionResult === false) throw new Error("Update condition not met.");
	if (conditionResult === true) return { table, updates: processedFields };
	return { table, updates: processedFields, where: buildWhereClause(conditionResult, state) };
}

export function compileUpdateQuery(query: ParsedUpdateQuery): string {
	let sql = `UPDATE ${query.table} SET ${Object.entries(query.updates)
		.map(([column, value]) => `${doubleQuote(column)} = ${value}`)
		.join(", ")}`;

	// Add WHERE clause if one was generated
	if (query.where) sql += ` WHERE ${query.where}`;
	return sql;
}

export function buildUpdateQuery(updateQuery: UpdateQuery, config: Config | ConfigWithForeignKeys): string {
	const parsedQuery = parseUpdateQuery(updateQuery, config);
	const sql = compileUpdateQuery(parsedQuery);
	return sql;
}
