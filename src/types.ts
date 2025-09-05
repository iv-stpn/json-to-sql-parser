import type { Dialect } from "./constants/dialects";
import type { FieldType } from "./constants/field-types";
import type { AnyExpression, AnyScalar } from "./schemas";
import type { ExpressionTypeMap } from "./utils/expression-map";

export type Field = { name: string; type: FieldType; nullable: boolean; default?: AnyExpression };
export type FieldWithForeignKey = Field & {
	foreignKey?: { table: string; field: string; onDelete?: "cascade" | "set_null" | "restrict" };
};

export type FieldPath = { table: string; field: string; jsonAccess: string[]; jsonExtractText?: boolean; fieldConfig: Field };
export type Relationship = { table: string; field: string; toTable: string; toField: string };

type Tables = { [tableName: string]: { allowedFields: Field[] } };
type TablesWithForeignKeys = { [tableName: string]: { allowedFields: FieldWithForeignKey[] } };

type Variables = { [varName: string]: AnyScalar };
type DataTableConfig = { table: string; dataField: string; tableField: string; whereConditions?: string[] };
export type Config = {
	tables: Tables;
	variables: Variables;
	relationships: Relationship[];
	dialect: Dialect;
	dataTable?: DataTableConfig;
};

export type ConfigWithForeignKeys = {
	tables: TablesWithForeignKeys;
	variables: Variables;
	dialect: Dialect;
	dataTable?: DataTableConfig;
};

export type ParserState = { config: Config; rootTable: string; expressions: ExpressionTypeMap };
