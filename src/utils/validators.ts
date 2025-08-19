import { fieldPathRegex } from "../parsers/parse-json-access";
import {
	type AnyScalar,
	arrayOperators,
	comparisonOperators,
	type ExpressionObject,
	type FieldName,
	type FieldOperator,
	type ScalarExpression,
	type ScalarPrimitive,
	stringOperators,
} from "../schemas";

// Internal validators
export const isExpressionObject = (value: unknown): value is ExpressionObject =>
	isNonNullObject(value) && ("$func" in value || "$cond" in value || "$field" in value || "$var" in value);

export const isScalarExpression = (value: unknown): value is ScalarExpression =>
	isNonNullObject(value) && ("$jsonb" in value || "$timestamp" in value || "$date" in value || "$uuid" in value);

export const isScalarPrimitive = (value: unknown): value is ScalarPrimitive =>
	typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null;

export const isAnyScalar = (value: unknown): value is AnyScalar => isScalarPrimitive(value) || isScalarExpression(value);

export const isFieldOperator = (value: string): value is FieldOperator =>
	value in comparisonOperators || value in stringOperators || value in arrayOperators;

// Type guards for better type narrowing
export const isNonNullObject = (value: unknown): value is Record<string, unknown> =>
	value !== null && typeof value === "object" && !Array.isArray(value);

// SQL validators
const tableNameRegex = `[a-z][a-z_]+`;
export const fieldNameRegex = `[a-z][a-z_0-9]*`;

const fieldRegex = new RegExp(`^(?:${tableNameRegex}\\.)?${fieldNameRegex}(?:${fieldPathRegex})?$`);
export const isField = (field: string): field is FieldName => fieldRegex.test(field);

// Common utility functions for validating data types
const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
const timestampRegex = /^\d{4}-\d{2}-\d{2}(?:T| )\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$/;

export function isValidDate(value: string): boolean {
	if (!dateRegex.test(value)) return false;

	const [year, month, day] = value.split("-").map(Number);
	if (year === undefined || month === undefined || day === undefined) return false;

	const date = new Date(year, month - 1, day);
	return date.getFullYear() === year && date.getMonth() + 1 === month && date.getDate() === day;
}

const timeSeparatorRegex = /[:.]/;
export function isValidTimestamp(value: string): boolean {
	if (!timestampRegex.test(value)) return false;

	const datePart = value.slice(0, 10);
	if (!isValidDate(datePart)) return false;

	const timePart = value.slice(11);
	const [hours, minutes, seconds] = timePart.split(timeSeparatorRegex).map(Number);
	if (hours === undefined || minutes === undefined) return false;
	if (hours < 0 || hours >= 24 || minutes < 0 || minutes >= 60) return false;
	if (seconds !== undefined && (seconds < 0 || seconds >= 60)) return false;
	return true;
}

export const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function ensureBoolean(value: unknown): asserts value is boolean | null {
	if (typeof value !== "boolean" && value !== null) throw new Error("Expected a boolean or null value");
}
export function ensureNumber(value: unknown): asserts value is number | null {
	if (typeof value !== "number" && value !== null) throw new Error("Expected a number or null value");
}
export function ensureText(value: unknown): asserts value is string | null {
	if (typeof value !== "string" && value !== null) throw new Error("Expected a string or null value");
}
export function ensureUUID(value: unknown): asserts value is string | null {
	if (typeof value !== "string" && value !== null) throw new Error("Expected a UUID string or null value");
	if (value !== null && !uuidRegex.test(value)) throw new Error("Invalid UUID format");
}
export function ensureTimestampString(value: unknown): asserts value is string | null {
	if (typeof value !== "string" && value !== null) throw new Error("Expected a timestamp string or null value");
	if (value !== null && !isValidTimestamp(value)) throw new Error("Invalid timestamp format");
}
export function ensureDateString(value: unknown): asserts value is string | null {
	if (typeof value !== "string" && value !== null) throw new Error("Expected a date string or null value");
	if (value !== null && !isValidDate(value)) throw new Error("Invalid date format");
}
