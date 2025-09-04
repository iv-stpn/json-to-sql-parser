import { ensureConditionObject } from "../parsers/issues";
import type { EvaluationContext } from "../parsers/mutations";
import { evaluateCondition } from "../parsers/mutations";
import type { Condition, DeleteQuery } from "../schemas";
import type { Config, ConfigWithForeignKeys, ParserState } from "../types";
import { ExpressionTypeMap } from "../utils/expression-map";
import { ensureNormalizedConfig } from "../utils/normalize-config";
import { buildWhereClause } from "./where";

type ParsedDeleteQuery = {
	table: string;
	where?: string;
};

export function parseDeleteQuery(deleteQuery: DeleteQuery, config: Config | ConfigWithForeignKeys): ParsedDeleteQuery {
	const normalizedConfig = ensureNormalizedConfig(config);
	const { table, condition } = deleteQuery;

	const tableConfig = normalizedConfig.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	// Initialize state
	const expressions = new ExpressionTypeMap();
	const state: ParserState = { config: normalizedConfig, rootTable: table, expressions };

	// Evaluate condition if present
	let conditionResult: Condition = true;
	if (condition !== undefined) {
		const evaluationContext: EvaluationContext = {
			newRow: {},
			rootTable: table,
			fields: tableConfig.allowedFields,
			config: normalizedConfig,
			mutationType: "DELETE",
		};
		conditionResult = evaluateCondition(ensureConditionObject(condition), evaluationContext);
	}

	// If condition was evaluated and returned false, throw an error
	if (conditionResult === false) throw new Error("Delete condition not met.");

	// If condition is true, delete all rows (no WHERE clause)
	if (conditionResult === true) return { table };

	// Otherwise, build WHERE clause from condition
	const where = buildWhereClause(conditionResult, state);
	return { table, where };
}

export function compileDeleteQuery(query: ParsedDeleteQuery): string {
	let sql = `DELETE FROM ${query.table}`;

	// Add WHERE clause if one was generated
	if (query.where) sql += ` WHERE ${query.where}`;

	return sql;
}

export function buildDeleteQuery(deleteQuery: DeleteQuery, config: Config | ConfigWithForeignKeys): string {
	const parsedQuery = parseDeleteQuery(deleteQuery, config);
	const sql = compileDeleteQuery(parsedQuery);
	return sql;
}
