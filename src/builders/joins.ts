import { aliasValue, castValue, getTargetFieldType, jsonAccessPath, parseFieldPath } from "../parsers";
import type { Config, Relationship } from "../types";

export function buildJoinClause(
	table: string,
	toTable: string,
	relationship: Relationship,
	config: Config,
	alias?: string,
): string {
	// Ensure both tables are part of the relationship
	if (relationship.table !== table && relationship.toTable !== table)
		throw new Error(`Table ${table} is not part of the relationship: ${JSON.stringify(relationship)}`);

	if (relationship.table !== toTable && relationship.toTable !== toTable)
		throw new Error(`Target table ${toTable} is not part of the relationship: ${JSON.stringify(relationship)}`);

	// Ensure both fields are valid fields in the config
	const fieldPath = parseFieldPath(relationship.table === table ? relationship.field : relationship.toField, table, config);
	const toFieldPath = parseFieldPath(relationship.table === table ? relationship.toField : relationship.field, toTable, config);

	// Cast the target field path
	const toTableName = config.dataTable ? aliasValue(config.dataTable.table, toTable) : toTable;
	const toTableTargetType = getTargetFieldType(
		fieldPath.fieldConfig.type,
		fieldPath.jsonAccess.length > 0,
		toFieldPath.fieldConfig.type,
		config,
	);

	const field = jsonAccessPath(fieldPath.field, fieldPath.jsonAccess, true);
	const toField = jsonAccessPath(toFieldPath.field, toFieldPath.jsonAccess, true);

	const leftField = `${table}.${field}`;
	const rightField = castValue(`${alias ?? toTable}.${toField}`, toTableTargetType, config.dialect);

	return `LEFT JOIN ${toTableName} ON ${leftField} = ${rightField}`;
}
