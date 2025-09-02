import type { FieldType } from "./constants/field-types";
import type { Dialect } from "./constants/dialects";
import type { AnyExpression, AnyScalar } from "./schemas";
import type { ExpressionTypeMap } from "./utils/expression-map";

export type Field = { name: string; type: FieldType; nullable: boolean; default?: AnyExpression };
export type FieldPath = { table: string; field: string; jsonAccess: string[]; jsonExtractText?: boolean; fieldConfig: Field };

type Cardinality = "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many";
export type Relationship = { table: string; field: string; toTable: string; toField: string; type: Cardinality };

type Tables = { [tableName: string]: { allowedFields: Field[] } };

type Variables = { [varName: string]: AnyScalar };
type DataTableConfig = { table: string; dataField: string; tableField: string; whereConditions?: string[] };
export type Config = {
	tables: Tables;
	variables: Variables;
	relationships: Relationship[];
	dialect: Dialect;
	dataTable?: DataTableConfig;
};

export type ParserState = { config: Config; rootTable: string; expressions: ExpressionTypeMap };
