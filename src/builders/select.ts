import { aliasValue, castValue, parseExpressionObject, parseField, parseScalarExpression } from "../parsers";
import type { FieldName, FieldSelection, SelectQuery } from "../schemas";
import type { Config, ConfigWithForeignKeys, ParserState } from "../types";
import { objectSize } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { ensureNormalizedConfig } from "../utils/normalize-config";
import { isExpressionObject, isScalarExpression } from "../utils/validators";
import { buildJoinClause } from "./joins";
import { buildWhereClause } from "./where";
import { Dialect } from "../constants/dialects";

type SelectState = ParserState & { joins: string[]; select: string[]; processedTables: Set<string> };
function processField(fieldName: string, selection: FieldSelection, table: string, state: SelectState): void {
	if (selection === true) {
		const { select } = parseField(fieldName, state);
		state.select.push(aliasValue(castValue(select.field, select.targetType, state.config.dialect), select.alias));
		return;
	}

	if (isScalarExpression(selection)) {
		const expression = parseScalarExpression(selection, state.config.dialect);
		state.select.push(aliasValue(expression, fieldName));
		return;
	}

	if (isExpressionObject(selection)) {
		const expression = parseExpressionObject(selection, state);
		state.select.push(aliasValue(expression, fieldName));
		return;
	}

	if (selection === false) return;

	// Relationship field
	const dotIndex = fieldName.indexOf(".");
	const targetTable = dotIndex > -1 ? fieldName.substring(dotIndex + 1) : fieldName;
	processRelationship(targetTable, selection, table, state);
}

const isRelationship = (relationshipTable: string, targetTable: string, fromTable: string, toTable: string): boolean => {
	// Check if this relationship connects fromTable to targetTable
	// The relationship can work in either direction
	return (
		(relationshipTable === fromTable && toTable === targetTable) || (relationshipTable === targetTable && toTable === fromTable)
	);
};

type Selection = Record<FieldName, FieldSelection>;
function processRelationship(table: string, selection: Selection, fromTable: string, state: SelectState): void {
	const relationship = state.config.relationships.find(({ table: relationshipTable, toTable }) =>
		isRelationship(relationshipTable, table, fromTable, toTable),
	);
	if (!relationship) throw new Error(`No relationship found between '${fromTable}' and '${table}'`);

	// Add join
	const joinClause = buildJoinClause(fromTable, table, relationship, state.config);
	if (!state.joins.includes(joinClause)) state.joins.push(joinClause);

	// Process nested selection
	state.processedTables.add(table);
	for (const [fieldName, fieldValue] of Object.entries(selection)) {
		processField(`${table}.${fieldName}`, fieldValue, table, state);
	}
}

// Result of parsing a SELECT query
type ParsedSelectQuery = { select: string[]; from: string; where?: string; joins: string[]; limit?: number; offset?: number };
export function parseSelectQuery(selectQuery: SelectQuery, baseConfig: Config | ConfigWithForeignKeys): ParsedSelectQuery {
	const config = ensureNormalizedConfig(baseConfig);
	const { rootTable, selection, condition, pagination } = selectQuery;

	// Validate root table
	if (!config.tables[rootTable] && !config.dataTable) throw new Error(`Table '${rootTable}' is not allowed`);

	// Validate selection is not empty
	if (objectSize(selection) === 0) throw new Error("Selection cannot be empty");

	// Process the selection starting from the main table
	const processedTables = new Set([rootTable]);
	const expressions = new ExpressionTypeMap();

	const state: SelectState = { config, rootTable, expressions, select: [], joins: [], processedTables };
	for (const [fieldName, fieldValue] of Object.entries(selection)) processField(fieldName, fieldValue, rootTable, state);

	const from = config.dataTable ? aliasValue(config.dataTable.table, rootTable) : rootTable;
	const where = buildWhereClause(condition, state);

	const limit = pagination?.limit;
	const offset = pagination?.offset;

	return { select: state.select, from, where, joins: state.joins, limit, offset };
}

export function compileSelectQuery(query: ParsedSelectQuery, dialect: Dialect): string {
	// Convert alias quoting from single quotes to double quotes
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.joins.length > 0) sql += ` ${query.joins.join(" ")}`;
	if (query.where) sql += ` WHERE ${query.where}`;

	// Handle pagination with dialect-specific behavior
	const isSQLite = dialect === Dialect.SQLITE_MINIMAL || dialect === Dialect.SQLITE_EXTENSIONS;

	if (query.limit !== undefined) {
		sql += ` LIMIT ${query.limit}`;
	} else if (query.offset !== undefined && isSQLite) {
		// SQLite requires LIMIT when using OFFSET, use -1 for unlimited
		sql += ` LIMIT -1`;
	}

	if (query.offset !== undefined) {
		sql += ` OFFSET ${query.offset}`;
	}

	return sql;
}

export function buildSelectQuery(selectQuery: SelectQuery, config: Config | ConfigWithForeignKeys): string {
	const parsedQuery = parseSelectQuery(selectQuery, config);
	const normalizedConfig = ensureNormalizedConfig(config);
	const sql = compileSelectQuery(parsedQuery, normalizedConfig.dialect);
	return sql;
}
