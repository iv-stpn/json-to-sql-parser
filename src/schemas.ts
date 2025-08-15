/** biome-ignore-all lint/suspicious/noThenProperty: we use `then` and `else` for conditional expressions */
import { z } from "zod";
import { aggregationOperators } from "./constants/aggregation-functions";
import { isField } from "./utils/validators";

// Primitive value types
export const scalarValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export type ScalarValue = z.infer<typeof scalarValueSchema>;

export type ConditionExpression = { if: Condition; then: AnyExpression; else: AnyExpression };
export type ExpressionObject =
	| { $field: string } // Field reference
	| { $var: string } // Variable reference
	| { $func: { [functionName: string]: AnyExpression[] } } // Function call with arguments
	| { $timestamp: string } // Timestamp value
	| { $date: string } // Date value
	| { $uuid: string } // UUID value
	| { $cond: ConditionExpression };

export const expressionObjectSchema: z.ZodType<ExpressionObject> = z.lazy(() =>
	z.union([
		z.object({
			$cond: z.object({
				if: z.lazy(() => conditionSchema),
				then: z.lazy(() => anyExpressionSchema),
				else: z.lazy(() => anyExpressionSchema),
			}),
		}),
		z.object({ $func: z.record(z.string(), z.array(z.lazy(() => anyExpressionSchema))) }), // Function call
		z.object({ $field: z.string() }), // Field reference
		z.object({ $var: z.string() }), // Variable reference
		z.object({ $timestamp: z.string() }), // Timestamp value
		z.object({ $date: z.string() }), // Date value
		z.object({ $uuid: z.uuid({ error: "Invalid UUID format" }) }), // UUID value
	]),
);

export type AnyExpression = ExpressionObject | ScalarValue;
export const anyExpressionSchema: z.ZodType<AnyExpression> = z.lazy(() => z.union([expressionObjectSchema, scalarValueSchema]));

const $func = anyExpressionSchema.optional();
const comparisonOperators = { $eq: $func, $ne: $func, $gt: $func, $gte: $func, $lt: $func, $lte: $func };
const stringOperators = { $like: $func, $ilike: $func, $regex: $func };

const $arrayExpr = z.array(anyExpressionSchema).optional();
const arrayOperators = { $in: $arrayExpr, $nin: $arrayExpr };

// All conditions that can be applied to a field (multiple conditions can be combined)
const fieldConditionSchema = z.union([
	z.object({ ...comparisonOperators, ...stringOperators, ...arrayOperators }),
	anyExpressionSchema,
]);

export type FieldCondition = z.infer<typeof fieldConditionSchema>;

// Ensures field starts with lowercase letter
const fieldNameSchema = z.string().refine(isField, "Field name must be a valid identifier");
export type FieldName =
	`${"a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"}${string}`;

export type Condition =
	| { $and: Condition[] } // Logical AND
	| { $or: Condition[] } // Logical OR
	| { $not: Condition } // Logical NOT
	| { $exists: { table: string; conditions: Condition } } // EXISTS subquery
	| Record<FieldName, FieldCondition>; // Field conditions

export const conditionSchema: z.ZodType<Condition> = z.lazy(() =>
	z.union([
		z.object({ $and: z.array(z.lazy(() => conditionSchema)) }),
		z.object({ $or: z.array(z.lazy(() => conditionSchema)) }),
		z.object({ $not: z.lazy(() => conditionSchema) }),
		z.object({ $exists: z.object({ table: z.string(), conditions: z.lazy(() => conditionSchema) }) }),
		z.record(fieldNameSchema, fieldConditionSchema),
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

export const selectQuerySchema = z.object({
	rootTable: z.string(),
	selection: z.record(z.string(), fieldSelectionSchema),
	condition: conditionSchema.optional(),
});

export type SelectQuery = z.infer<typeof selectQuerySchema>;

// Aggregation schemas
export const aggregatedFieldSchema = z.object({
	operator: z.enum(aggregationOperators),
	field: z.union([z.string(), expressionObjectSchema]),
});

export type AggregatedField = z.infer<typeof aggregatedFieldSchema>;

// Schema for aggregation query
export const aggregationQuerySchema = z.object({
	table: z.string(),
	groupBy: z.array(z.string()),
	condition: conditionSchema.optional(),
	aggregatedFields: z.record(z.string(), aggregatedFieldSchema).optional(),
});

export type AggregationQuery = z.infer<typeof aggregationQuerySchema>;
