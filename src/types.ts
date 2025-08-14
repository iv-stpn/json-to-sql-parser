import type { FieldType } from "./constants/operators";
import type { ExpressionTypeMap } from "./utils/expression-map";

export type Field = { name: string; type: FieldType; nullable: boolean };
export type FieldPath = { table: string; field: string; jsonPathSegments: string[]; fieldConfig: Field };

type Cardinality = "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many";
export type Relationship = { table: string; field: string; toTable: string; toField: string; type: Cardinality };

type DataTableConfig = { table: string; dataField: string; tableField: string; whereConditions?: string[] };

export type Primitive = string | number | boolean;

type Variables = { [varName: string]: Primitive };
type Tables = { [tableName: string]: { allowedFields: Field[] } };

export type Config = { tables: Tables; variables: Variables; relationships: Relationship[]; dataTable?: DataTableConfig };

export type ParserState = { config: Config; params: Primitive[]; rootTable: string; expressions: ExpressionTypeMap };
export type BaseParsedQuery = { select: string[]; from: string; where?: string; params: Primitive[] };
