/** biome-ignore-all lint/suspicious/noThenProperty: we use `then` and `else` for conditional expressions */
import { z } from "zod";
import { type AggregationOperator, aggregationOperators } from "./constants/operators";

// Primitive value types
export const equalityValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export type EqualityValue = z.infer<typeof equalityValueSchema>;

export type ConditionExpression = { if: Condition; then: AnyExpression; else: AnyExpression };
export type ExpressionObject =
	| { $expr: string } // Field reference or context variable
	| { $expr: { [functionName: string]: AnyExpression[] } } // Function call with arguments
	| { $timestamp: string } // Timestamp value
	| { $date: string } // Date value
	| { $uuid: string } // UUID value
	| { $cond: ConditionExpression };

function record<T extends z.ZodTypeAny>(type: T): z.ZodRecord<z.ZodString, T> {
	return z.record(z.string(), type);
}

export const expressionObjectSchema: z.ZodType<ExpressionObject> = z.lazy(() =>
	z.union([
		z.object({
			$cond: z.object({
				if: z.lazy(() => conditionSchema),
				then: z.lazy(() => anyExpressionSchema),
				else: z.lazy(() => anyExpressionSchema),
			}),
		}),
		z.object({ $expr: record(z.array(z.lazy(() => anyExpressionSchema))) }), // Function call
		z.object({ $expr: z.string() }), // Field reference or context variable
		z.object({ $timestamp: z.string() }), // Timestamp value
		z.object({ $date: z.string() }), // Date value
		z.object({ $uuid: z.string() }), // UUID value
	]),
);

export const isExpressionObject = (value: unknown): value is ExpressionObject =>
	typeof value === "object" &&
	value !== null &&
	("$expr" in value || "$cond" in value || "$timestamp" in value || "$date" in value || "$uuid" in value);

export type AnyExpression = ExpressionObject | EqualityValue;
export const anyExpressionSchema: z.ZodType<AnyExpression> = z.lazy(() => z.union([expressionObjectSchema, equalityValueSchema]));

const $expr = anyExpressionSchema.optional();
const comparisonOperators = { $eq: $expr, $ne: $expr, $gt: $expr, $gte: $expr, $lt: $expr, $lte: $expr };
const stringOperators = { $like: $expr, $ilike: $expr, $regex: $expr };

const $arrayExpr = z.array(anyExpressionSchema).optional();
const arrayOperators = { $in: $arrayExpr, $nin: $arrayExpr };

const fieldConditionSchema = z.union([
	z.object({ ...comparisonOperators, ...stringOperators, ...arrayOperators }),
	anyExpressionSchema,
]);

export type FieldCondition = z.infer<typeof fieldConditionSchema>;
export type Condition =
	| { $and: Condition[] } // Logical AND
	| { $or: Condition[] } // Logical OR
	| { $not: Condition } // Logical NOT
	| { $exists: { table: string; conditions: Condition } } // EXISTS subquery
	| Record<FieldName, FieldCondition>; // Field conditions

type FieldName =
	`${"a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"}${string}`;

const fieldNameRegex = /^[a-z]/; // Field names must start with a lowercase letter and can contain alphanumeric characters and underscores
const isFieldName = (field: string): field is FieldName => fieldNameRegex.test(field);

// Ensures field starts with lowercase letter
const fieldNameSchema = z.string().refine(isFieldName, "Field name must start with a lowercase letter");

export const conditionSchema: z.ZodType<Condition> = z.lazy(() =>
	z.union([
		z.object({ $not: conditionSchema }),
		z.object({ $or: z.array(conditionSchema) }),
		z.object({ $and: z.array(conditionSchema) }),
		z.object({ $exists: z.object({ table: z.string(), conditions: conditionSchema }) }),
		z.record(fieldNameSchema, fieldConditionSchema),
	]),
);

export type FieldSelection = boolean | ExpressionObject | { [key: string]: FieldSelection };
export type Selection = { [key: FieldName]: FieldSelection };

export const fieldSelectionSchema: z.ZodType<FieldSelection> = z.union([
	z.boolean(),
	expressionObjectSchema,
	record(z.lazy(() => fieldSelectionSchema)),
]);

// Aggregation schemas
export const aggregatedSchema = z.object({
	operator: z.enum(aggregationOperators),
	field: z.union([z.string(), expressionObjectSchema]),
});

export type Aggregation = { operator: AggregationOperator; field: string | ExpressionObject };
export type AggregationQuery = { table: string; groupBy: string[]; aggregatedFields: Record<string, Aggregation> };

// Schema for aggregation query
export const aggregationQuerySchema = z.object({
	table: z.string(),
	groupByFields: z.array(z.string()),
	aggregatedFields: record(aggregatedSchema),
});
