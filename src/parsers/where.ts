import { conditionSchema } from "../schemas";
import type { ParserState } from "../types";
import { mergeConditions, parseCondition } from ".";

export function buildDataTableWhereClause(state: ParserState, whereClause?: string): string {
	const dataTable = state.config.dataTable;
	if (!dataTable) throw new Error("Data table configuration is missing");

	const fieldWhereConditions = (dataTable.whereConditions ?? []).map((condition) => `${state.rootTable}.${condition}`);
	const whereConditions = [...fieldWhereConditions, ...(whereClause ? [whereClause] : [])];
	const baseCondition = `${state.rootTable}.${dataTable.tableField} = '${state.rootTable}'`;
	return mergeConditions([baseCondition, ...whereConditions], "data table conditions");
}

export function parseWhereClause(condition: unknown, state: ParserState) {
	if (!condition) {
		// If no condition is provided, ensure we have a valid WHERE clause for schema-less data tables
		if (state.config.dataTable) return buildDataTableWhereClause(state);
		return undefined;
	}

	const sql = parseCondition(conditionSchema.parse(condition), state);
	return state.config.dataTable ? buildDataTableWhereClause(state, sql) : sql;
}
