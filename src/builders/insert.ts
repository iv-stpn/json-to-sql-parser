import { ensureConditionObject } from "../parsers/issues";
import type { EvaluationContext } from "../parsers/mutations";
import { evaluateCondition, parseNewRowWithDefaults, processMutationFields } from "../parsers/mutations";
import type { Condition, InsertQuery, ScalarPrimitive } from "../schemas";
import type { Config, ParserState } from "../types";
import { doubleQuote } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";

type ParsedInsertQuery = {
	table: string;
	params: ScalarPrimitive[];
	columns: string[];
	values: string[];
	conditionResult?: Condition;
};

export function parseInsertQuery(insertQuery: InsertQuery, config: Config): ParsedInsertQuery {
	const { table, newRow: newRowData, condition } = insertQuery;

	const tableConfig = config.tables[table];
	if (!tableConfig) throw new Error(`Table '${table}' is not allowed or does not exist`);

	config.tables.NEW_ROW = tableConfig;

	const fields = tableConfig.allowedFields;
	const newRow = parseNewRowWithDefaults(table, newRowData, config, fields);

	// Initialize state
	const expressions = new ExpressionTypeMap();
	const state: ParserState = { config, rootTable: table, params: [], expressions };

	// Process parsed newRow fields
	const processedFields = processMutationFields(newRow, state);

	// Evaluate condition if present
	let conditionResult: Condition = true;
	if (condition) {
		const evaluationContext: EvaluationContext = { rootTable: table, newRow, fields, config, mutationType: "INSERT" };
		conditionResult = evaluateCondition(ensureConditionObject(condition), evaluationContext);
	}

	const columns = Object.keys(processedFields);
	const values = Object.values(processedFields);
	return { params: state.params, table, columns, values, conditionResult };
}

export function compileInsertQuery(query: ParsedInsertQuery): string {
	// If condition was evaluated and returned false, throw an error
	if (query.conditionResult === false) throw new Error("Insert condition not met.");
	return `INSERT INTO ${query.table} (${query.columns.map(doubleQuote).join(", ")}) VALUES (${query.values.join(", ")})`;
}

export function buildInsertQuery(
	insertQuery: InsertQuery,
	config: Config,
): { sql: string; params: ScalarPrimitive[]; conditionResult?: Condition } {
	const parsedQuery = parseInsertQuery(insertQuery, config);
	const sql = compileInsertQuery(parsedQuery);
	return { sql, params: parsedQuery.params, conditionResult: parsedQuery.conditionResult };
}
