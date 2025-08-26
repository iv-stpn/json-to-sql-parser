import { aliasValue, castValue, parseExpressionObject, parseField, parseScalarExpression } from "../parsers";
import type { FieldName, FieldSelection, SelectQuery } from "../schemas";
import type { Config, ParserState } from "../types";
import { objectSize } from "../utils";
import { ExpressionTypeMap } from "../utils/expression-map";
import { isExpressionObject, isScalarExpression } from "../utils/validators";
import { buildJoinClause } from "./joins";
import { buildWhereClause } from "./where";

type SelectState = ParserState & { joins: string[]; select: string[]; processedTables: Set<string> };
function processField(fieldName: string, selection: FieldSelection, table: string, state: SelectState): void {
	if (selection === true) {
		const { select } = parseField(fieldName, state);
		state.select.push(aliasValue(castValue(select.field, select.cast, state.config.dialect), select.alias));
		return;
	}

	if (isScalarExpression(selection)) {
		const expression = parseScalarExpression(selection);
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

const isRelationship = (relationshipName: string, table: string, fromTable: string, toTable: string): boolean =>
	(relationshipName === fromTable && table === toTable) || (relationshipName === toTable && table === fromTable);

type Selection = Record<FieldName, FieldSelection>;
function processRelationship(table: string, selection: Selection, fromTable: string, state: SelectState): void {
	const relationship = state.config.relationships.find(({ table: relationshipTable, toTable }) =>
		isRelationship(table, relationshipTable, fromTable, toTable),
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
type ParsedSelectQuery = { select: string[]; from: string; where?: string; joins: string[] };
export function parseSelectQuery(selectQuery: SelectQuery, config: Config): ParsedSelectQuery {
	const { rootTable, selection, condition } = selectQuery;

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

	return { select: state.select, from, where, joins: state.joins };
}

export function compileSelectQuery(query: ParsedSelectQuery): string {
	// Convert alias quoting from single quotes to double quotes
	let sql = `SELECT ${query.select.join(", ")} FROM ${query.from}`;
	if (query.joins.length > 0) sql += ` ${query.joins.join(" ")}`;
	if (query.where) sql += ` WHERE ${query.where}`;
	return sql;
}

export function buildSelectQuery(selectQuery: SelectQuery, config: Config): string {
	const parsedQuery = parseSelectQuery(selectQuery, config);
	const sql = compileSelectQuery(parsedQuery);
	return sql;
}
