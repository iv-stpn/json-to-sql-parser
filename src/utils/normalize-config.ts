import type { Config, ConfigWithForeignKeys, FieldWithForeignKey, Relationship } from "../types";

function extractRelationshipsFromFields(tableName: string, fields: FieldWithForeignKey[]): Relationship[] {
	const relationships: Relationship[] = [];

	for (const field of fields) {
		if (field.foreignKey) {
			const { table: toTable, field: toField } = field.foreignKey;
			relationships.push({ table: tableName, field: field.name, toTable, toField });
		}
	}

	return relationships;
}

export function normalizeConfig(config: ConfigWithForeignKeys): Config {
	const normalizedTables: Config["tables"] = {};
	const extractedRelationships: Relationship[] = [];

	// Process each table
	for (const [tableName, tableConfig] of Object.entries(config.tables)) {
		// Normalize fields by removing foreign key information
		const normalizedFields = tableConfig.allowedFields.map(({ foreignKey, ...field }) => field);
		normalizedTables[tableName] = { allowedFields: normalizedFields };

		// Extract relationships from foreign keys
		const relationships = extractRelationshipsFromFields(tableName, tableConfig.allowedFields);
		extractedRelationships.push(...relationships);
	}

	return {
		tables: normalizedTables,
		variables: config.variables,
		dialect: config.dialect,
		dataTable: config.dataTable,
		relationships: extractedRelationships,
	};
}

export function ensureNormalizedConfig(config: Config | ConfigWithForeignKeys): Config {
	if (!("relationships" in config)) return normalizeConfig(config);
	return config;
}
