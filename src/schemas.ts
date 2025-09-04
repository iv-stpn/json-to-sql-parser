/** biome-ignore-all lint/suspicious/noThenProperty: we use `then` and `else` for conditional expressions */
import { z } from "zod";
import { aggregationFunctionNames } from "./functions/aggregate";
import { isField } from "./utils/validators";

// Scalar value types
const scalarPrimitiveSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export type ScalarPrimitive = z.infer<typeof scalarPrimitiveSchema>;

// Scalar expression types (timestamp, date, uuid, jsonb)
const scalarExpressionSchema = z.union([
	z.strictObject({ $date: z.string() }),
	z.strictObject({ $timestamp: z.string() }),
	z.strictObject({ $jsonb: z.looseObject({}) }),
	z.strictObject({ $uuid: z.uuid({ message: "Invalid UUID format" }) }),
]);
export type ScalarExpression = z.infer<typeof scalarExpressionSchema>;

export type ConditionExpression = { if: Condition; then: AnyExpression; else: AnyExpression };
export type ExpressionObject =
	| { $field: string } // Field reference
	| { $var: string } // Variable reference
	| { $func: Record<string, AnyExpression[]> } // Function call with arguments
	| { $cond: ConditionExpression } // Conditional expression
	| ScalarExpression; // Scalar expressions (timestamp, date, uuid, jsonb)

export const expressionObjectSchema: z.ZodType<ExpressionObject> = z.lazy(() =>
	z.union([
		z.strictObject({
			$cond: z.strictObject({
				if: z.lazy(() => conditionSchema),
				then: z.lazy(() => anyExpressionSchema),
				else: z.lazy(() => anyExpressionSchema),
			}),
		}),
		z.strictObject({ $var: z.string() }), // Variable reference
		z.strictObject({ $field: z.string() }), // Field reference
		z.strictObject({ $func: z.record(z.string(), z.array(z.lazy(() => anyExpressionSchema))) }), // Function call
		scalarExpressionSchema, // Scalar expressions (timestamp, date, uuid)
	]),
);

export type AnyExpression = ExpressionObject | ScalarPrimitive;
export const anyExpressionSchema: z.ZodType<AnyExpression> = z.lazy(() =>
	z.union([expressionObjectSchema, scalarPrimitiveSchema]),
);

type AnyBooleanExpression = ExpressionObject | boolean;
const anyBooleanExpressionSchema: z.ZodType<AnyBooleanExpression> = z.lazy(() => z.union([expressionObjectSchema, z.boolean()]));

const anyExpr = anyExpressionSchema.optional();
const anyArrayExpr = z.array(anyExpressionSchema).optional();
export const comparisonOperators = { $eq: anyExpr, $ne: anyExpr, $gt: anyExpr, $gte: anyExpr, $lt: anyExpr, $lte: anyExpr };
export const stringOperators = { $like: anyExpr, $ilike: anyExpr, $regex: anyExpr };
export const arrayOperators = { $in: anyArrayExpr, $nin: anyArrayExpr };

// All conditions that can be applied to a field (multiple conditions can be combined)
const fieldConditionSchema = z.strictObject({ ...comparisonOperators, ...stringOperators, ...arrayOperators });
export type FieldOperator = keyof typeof fieldConditionSchema.shape;
export type FieldCondition = z.infer<typeof fieldConditionSchema>;
export type AnyFieldCondition = FieldCondition | AnyExpression;

// Ensures field starts with lowercase letter or is a NEW_ROW prefixed field
const fieldNameSchema = z
	.string()
	.refine((field) => isField(field) || field.startsWith("NEW_ROW."), "Field name must be a valid identifier'");

export type FieldName =
	`${"a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"}${string}`;

export type ConditionFieldName = FieldName | `NEW_ROW.${string}`;

export type Condition =
	| { $and: Condition[] } // Logical AND
	| { $or: Condition[] } // Logical OR
	| { $not: Condition } // Logical NOT
	| { $exists: { table: string; condition: Condition } } // EXISTS subquery as (SELECT 1 FROM <table> WHERE <condition>)
	| AnyBooleanExpression // Expression evaluating to TRUE or FALSE
	| Record<ConditionFieldName, AnyFieldCondition>; // Field conditions

export const conditionSchema: z.ZodType<Condition> = z.lazy(() =>
	z.union([
		z.strictObject({ $not: z.lazy(() => conditionSchema) }),
		z.strictObject({ $or: z.array(z.lazy(() => conditionSchema)) }),
		z.strictObject({ $and: z.array(z.lazy(() => conditionSchema)) }),
		z.strictObject({ $exists: z.strictObject({ table: z.string(), condition: z.lazy(() => conditionSchema) }) }),
		z.record(fieldNameSchema, z.union([fieldConditionSchema, anyExpressionSchema])),
		anyBooleanExpressionSchema, // Expression evaluating to TRUE or FALSE
	]),
);

export type FieldSelection = boolean | ExpressionObject | { [key: string]: FieldSelection };

export const fieldSelectionSchema: z.ZodType<FieldSelection> = z.union([
	z.boolean(),
	z.lazy(() => expressionObjectSchema),
	z.record(
		z.string(),
		z.lazy(() => fieldSelectionSchema),
	),
]);

// Pagination schema
export const paginationSchema = z.strictObject({
	limit: z.number().int().positive().optional(),
	offset: z.number().int().nonnegative().optional(),
});

export type Pagination = z.infer<typeof paginationSchema>;

export const selectQuerySchema = z.strictObject({
	rootTable: z.string(),
	selection: z.record(z.string(), fieldSelectionSchema),
	condition: conditionSchema.optional(),
	pagination: paginationSchema.optional(),
});

export type SelectQuery = z.infer<typeof selectQuerySchema>;

// Aggregation schemas
const aggregatedFieldSchema = z.strictObject({
	field: z.union([z.string(), expressionObjectSchema]),
	function: z.enum(aggregationFunctionNames),
	additionalArguments: z.array(anyExpressionSchema).optional(),
});

export type AggregatedField = z.infer<typeof aggregatedFieldSchema>;

// Schema for aggregation query
export const aggregationQuerySchema = z.strictObject({
	table: z.string(),
	groupBy: z.array(z.string()),
	condition: conditionSchema.optional(),
	aggregatedFields: z.record(z.string(), aggregatedFieldSchema).optional(),
});

export type AggregationQuery = z.infer<typeof aggregationQuerySchema>;

// Union of scalar values and scalar expressions
export const anyScalarSchema = z.union([scalarPrimitiveSchema, scalarExpressionSchema]);
export type AnyScalar = z.infer<typeof anyScalarSchema>;

// Insert query schema
export const insertQuerySchema = z.strictObject({
	table: z.string(),
	newRow: z.record(z.string(), anyScalarSchema),
	condition: conditionSchema.optional(),
});

export type InsertQuery = z.infer<typeof insertQuerySchema>;

// Update query schema
export const updateQuerySchema = z.strictObject({
	table: z.string(),
	updates: z.record(z.string(), anyExpressionSchema),
	condition: conditionSchema.optional(),
});

export type UpdateQuery = z.infer<typeof updateQuerySchema>;

// Delete query schema
export const deleteQuerySchema = z.strictObject({
	table: z.string(),
	condition: conditionSchema.optional(),
});

export type DeleteQuery = z.infer<typeof deleteQuerySchema>;
