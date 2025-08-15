import type { CastType } from "../constants/cast-types";
import { castMap } from "../constants/cast-types";
import { aliasValue, castValue } from "../parsers";
import type { Config, Relationship } from "../types";

export function buildJoinClause(
	table: string,
	toTable: string,
	relationship: Relationship,
	config: Config,
	alias?: string,
): string {
	// Ensure the relationship contains the table and toTable
	if (
		!relationship ||
		!relationship.table ||
		!relationship.toTable ||
		(relationship.table !== table && relationship.toTable !== table)
	) {
		throw new Error(`Invalid relationship for table ${table}: ${JSON.stringify(relationship)}`);
	}

	const toTableName = config.dataTable ? aliasValue(config.dataTable.table, toTable) : toTable;

	// Get field types for proper casting
	const getFieldType = (tableName: string, fieldName: string): CastType => {
		const tableConfig = config.tables[tableName];
		if (!tableConfig) return null;
		const fieldConfig = tableConfig.allowedFields.find((field) => field.name === fieldName);
		return fieldConfig ? castMap[fieldConfig.type] : null;
	};

	// Helper function to cast field if needed
	const castField = (tableName: string, fieldName: string): string => {
		const fieldType = getFieldType(tableName, fieldName);
		const fieldRef = `${tableName}.${fieldName}`;
		return castValue(fieldRef, fieldType);
	};

	const leftField = castField(table, relationship.table === table ? relationship.field : relationship.toField);
	const rightField = castField(alias ?? toTable, relationship.table === table ? relationship.toField : relationship.field);
	return `LEFT JOIN ${toTableName} ON ${leftField} = ${rightField}`;
}
