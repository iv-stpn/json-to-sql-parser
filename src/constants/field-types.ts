// Cast types for SQL queries
export const fieldTypes = ["string", "number", "boolean", "object", "date", "datetime", "uuid"] as const;
export type FieldType = (typeof fieldTypes)[number];
export type ExpressionType = FieldType | "any" | null;
