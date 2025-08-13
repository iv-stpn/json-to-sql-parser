import { type Condition, type FieldSelection, isExpressionObject, type Selection } from "../schemas";
import type { Config, ParserState, Primitive, Relationship } from "../types";
import { ExpressionTypeMap } from "../utils/expression-map";
import { aliasValue, castValue, mergeConditions, parseExpressionObject, parseField } from ".";
import { parseWhereClause } from "./where";

function buildDataTableWhereClause(table: string, state: ParserState, whereClause?: string): string {
	const dataTable = state.config.dataTable;
	if (!dataTable) throw new Error("Data table configuration is missing");

	const fieldWhereConditions = (dataTable.whereConditions ?? []).map((condition) => `${table}.${condition}`);
	const whereConditions = [...fieldWhereConditions, ...(whereClause ? [whereClause] : [])];
	return mergeConditions([`${table}.${dataTable.tableField} = '${table}'`, ...whereConditions], "data table conditions");
}

function buildJoinClause(table: string, toTable: string, relationship: Relationship, config: Config, alias?: string): string {
	const toTableName = config.dataTable ? aliasValue(config.dataTable.table, toTable) : toTable;
	if (relationship.table === table)
		return `LEFT JOIN ${toTableName} ON ${table}.${relationship.field} = ${alias ?? toTable}.${relationship.toField}`;
	return `LEFT JOIN ${toTableName} ON ${table}.${relationship.toField} = ${alias ?? toTable}.${relationship.field}`;
}

type SelectState = ParserState & { joins: string[]; select: string[]; processedTables: Set<string> };
function processField(fieldName: string, selection: FieldSelection, table: string, state: SelectState): void {
	if (selection === true) {
		const { select } = parseField(table, fieldName, state);
		state.select.push(aliasValue(castValue(select.field, select.cast), select.alias));
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

const isRelationship = (relationshipName: string, table: string, fromTable: string, toTable: string): boolean =>
	(relationshipName === fromTable && table === toTable) || (relationshipName === toTable && table === fromTable);

function processRelationship(table: string, selection: Selection, fromTable: string, state: SelectState): void {
	const relationship = state.config.relationships.find(({ table: relationshipTable, toTable }) =>
		isRelationship(table, relationshipTable, fromTable, toTable),
	);
	if (!relationship) throw new Error(`No relationship found between '${fromTable}' and '${table}'`);

	// Generate unique alias if the same table is joined multiple times
	const alias = state.processedTables.has(table) ? `${table}_${state.processedTables.size}` : undefined;

	// Add join
	const joinClause = buildJoinClause(fromTable, table, relationship, state.config, alias);
	if (!state.joins.includes(joinClause)) state.joins.push(joinClause);

	// Process nested selection
	const tableName = alias || table;
	state.processedTables.add(tableName);
	for (const [fieldName, fieldValue] of Object.entries(selection))
		processField(`${tableName}.${fieldName}`, fieldValue, table, state);
}

// Result of parsing a SELECT query
type ParsedSelectQuery = { select: string[]; from: string; joins: string[]; where?: string; params: Primitive[] };
type SelectQuery = { rootTable: string; selection: Selection; condition?: Condition };
export function parseSelectQuery(selectQuery: SelectQuery, config: Config): ParsedSelectQuery {
	const { rootTable, selection, condition } = selectQuery;

	// Validate root table
	if (!config.tables[rootTable] && !config.dataTable) throw new Error(`Table '${rootTable}' is not allowed`);

	// Validate selection is not empty
	if (Object.keys(selection).length === 0) throw new Error("Selection cannot be empty");

	// Process the selection starting from the main table
	const processedTables = new Set([rootTable]);
	const expressions = new ExpressionTypeMap();
	const state: SelectState = { config, rootTable, params: [], expressions, select: [], joins: [], processedTables };
	for (const [fieldName, fieldValue] of Object.entries(selection)) processField(fieldName, fieldValue, rootTable, state);

	const from = config.dataTable ? aliasValue(config.dataTable.table, rootTable) : rootTable;
	const result: ParsedSelectQuery = { select: state.select, from, joins: state.joins, params: state.params };

	if (!condition) {
		// If no condition is provided, ensure we have a valid WHERE clause for schema-less data tables
		if (config.dataTable) result.where = buildDataTableWhereClause(rootTable, state);
		return result;
	}

	const { sql, params: sqlParams } = parseWhereClause(condition, config, rootTable);
	result.where = config.dataTable ? buildDataTableWhereClause(rootTable, state, sql) : sql;
	result.params = [...state.params, ...sqlParams];
	return result;
}

export function compileSelectQuery(query: ParsedSelectQuery): string {
	// Convert alias quoting from single quotes to double quotes
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.joins.length > 0) sql += ` ${query.joins.join(" ")}`;
	if (query.where) sql += ` WHERE ${query.where}`;
	return sql;
}

export function generateSelectQuery(selectQuery: SelectQuery, config: Config): { sql: string; params: Primitive[] } {
	const parsedQuery = parseSelectQuery(selectQuery, config);
	const sql = compileSelectQuery(parsedQuery);
	return { sql, params: parsedQuery.params };
}
