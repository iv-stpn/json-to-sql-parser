/** biome-ignore-all lint/suspicious/noThenProperty: we use `then` and `else` for conditional expressions */
import { z } from "zod";
import { type AggregationOperator, aggregationOperators } from "./operators";

// Primitive value types
export const equalityValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export type EqualityValue = z.infer<typeof equalityValueSchema>;

export type ConditionExpression = { if: Condition; then: AnyExpression; else: AnyExpression };
export type ExpressionObject =
	| { $expr: string } // Field reference or context variable
	| { $expr: { [functionName: string]: AnyExpression[] } } // Function call with arguments
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
		z.object({ $expr: z.record(z.string(), z.array(z.lazy(() => anyExpressionSchema))) }), // Function call
		z.object({ $expr: z.string() }), // Field reference or context variable
	]),
);

export const isExpressionObject = (value: unknown): value is ExpressionObject =>
	typeof value === "object" && value !== null && ("$expr" in value || "$cond" in value);

export type AnyExpression = ExpressionObject | EqualityValue;
export const anyExpressionSchema: z.ZodType<AnyExpression> = z.lazy(() => z.union([expressionObjectSchema, equalityValueSchema]));

const $eq = anyExpressionSchema.optional();
const $in = z.array(anyExpressionSchema).optional();
const fieldConditionSchema = z.union([
	z.object({ $eq, $ne: $eq, $gt: $eq, $gte: $eq, $lt: $eq, $lte: $eq, $in, $nin: $in, $like: $eq, $ilike: $eq, $regex: $eq }),
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
		z.object({ $and: z.array(conditionSchema) }),
		z.object({ $or: z.array(conditionSchema) }),
		z.object({ $not: conditionSchema }),
		z.object({ $exists: z.object({ table: z.string(), conditions: conditionSchema }) }),
		z.record(fieldNameSchema, fieldConditionSchema),
	]),
);

export type FieldSelection = boolean | ExpressionObject | { [key: string]: FieldSelection };
export type Selection = { [key: FieldName]: FieldSelection };

export const fieldSelectionSchema: z.ZodType<FieldSelection> = z.union([
	z.boolean(),
	expressionObjectSchema,
	z.record(
		z.string(),
		z.lazy(() => fieldSelectionSchema),
	),
]);

// Aggregation schemas
export const aggregatedSchema = z.object({
	operator: z.enum(aggregationOperators),
	field: z.union([z.string(), expressionObjectSchema]),
});

export type Aggregation = { operator: AggregationOperator; field: string | ExpressionObject };
export type AggregationQuery = { table: string; groupBy: string[]; aggregatedFields: Record<string, Aggregation> };

// Schema for aggregation query
export const aggregatedFieldsSchema = z.record(z.string(), aggregatedSchema);
export const aggregationQuerySchema = z.object({
	table: z.string(),
	groupByFields: z.array(z.string()),
	aggregatedFields: aggregatedFieldsSchema,
});
