import { NON_EMPTY_CONDITION_ARRAY_ERROR } from "../constants/errors";
import { allowedFunctions } from "../functions";
import { type Condition, conditionSchema } from "../schemas";
import { isNonEmptyArray, isNotNull, objectSize } from "../utils";
import {
	isAnyScalar,
	isExpressionObject,
	isField,
	isFieldOperator,
	isNonNullObject,
	isScalarExpression,
	isScalarPrimitive,
	isValidDate,
	isValidTimestamp,
	uuidRegex,
} from "../utils/validators";

const atPath = (path: string) => (path === "" ? "" : `at "${path[0] === "." ? path.slice(1) : path}"`);

const invalidExpression = (path: string, message: string) => `Invalid expression${atPath(path)}: ${message}`;

export function findIssueInExpressionSchema(expression: unknown, path = ""): string | null {
	if (isScalarPrimitive(expression)) return null;
	if (isScalarExpression(expression)) {
		if (objectSize(expression) !== 1) return invalidExpression(path, "scalar expressions must have exactly one property");
		return null;
	}
	if (isExpressionObject(expression)) {
		if (objectSize(expression) !== 1) return invalidExpression(path, "expression objects must have exactly one property");
		if ("$uuid" in expression) {
			if (typeof expression.$uuid !== "string") return invalidExpression(`${path}.$uuid`, "should be a string");
			if (!uuidRegex.test(expression.$uuid)) return invalidExpression(`${path}.$uuid`, "invalid UUID format");
		} else if ("$field" in expression) {
			if (typeof expression.$field !== "string") return invalidExpression(`${path}.$field`, "should be a string");
			if (!isField(expression.$field)) return invalidExpression(`${path}.$field`, "invalid field format");
		} else if ("$date" in expression) {
			if (typeof expression.$date !== "string") return invalidExpression(`${path}.$date`, "should be a string");
			if (!isValidDate(expression.$date)) return invalidExpression(`${path}.$date`, "invalid date format");
		} else if ("$timestamp" in expression) {
			if (typeof expression.$timestamp !== "string") return invalidExpression(`${path}.$timestamp`, "should be a string");
			if (!isValidTimestamp(expression.$timestamp)) return invalidExpression(`${path}.$timestamp`, "invalid timestamp format");
		} else if ("$cond" in expression) {
			if (!isNonNullObject(expression.$cond))
				return invalidExpression(`${path}.$cond`, "should be an object with if, then, else properties");
			if (expression.$cond.if === undefined) return invalidExpression(`${path}.$cond.if`, "missing if property");
			if (expression.$cond.then === undefined) return invalidExpression(`${path}.$cond.then`, "missing then property");
			if (expression.$cond.else === undefined) return invalidExpression(`${path}.$cond.else`, "missing else property");

			const ifIssue = findIssueInConditionSchema(expression.$cond.if, `${path}.$cond.if`);
			if (ifIssue) return ifIssue;
			const thenIssue = findIssueInExpressionSchema(expression.$cond.then, `${path}.$cond.then`);
			if (thenIssue) return thenIssue;
			const elseIssue = findIssueInExpressionSchema(expression.$cond.else, `${path}.$cond.else`);
			if (elseIssue) return elseIssue;
		} else if ("$func" in expression) {
			const entries = Object.entries(expression.$func);
			if (entries.length !== 1 || !entries[0])
				return invalidExpression(`${path}.$func`, "should have exactly one function entry");

			const [name, args] = entries[0];
			if (!allowedFunctions.some((fn) => fn.name === name)) return invalidExpression(`${path}.$func`, "unknown function");
			if (!Array.isArray(args)) return invalidExpression(`${path}.$func`, "arguments should be an array");
			for (const arg of args) {
				const issue = findIssueInExpressionSchema(arg, `${path}.$func.args`);
				if (issue) return issue;
			}
		} else if ("$jsonb" in expression) {
			if (!isNonNullObject(expression.$jsonb)) return invalidExpression(`${path}.$jsonb`, "should be an object");
		} else if ("$var" in expression && typeof expression.$var !== "string") {
			return invalidExpression(`${path}.$var`, "should be a string");
		}
		return null;
	}

	return invalidExpression(path, `invalid expression value (got ${JSON.stringify(expression)})`);
}

type ConditionIssueType = "expression" | "$and" | "$or" | "$not" | "$exists";
const invalidCondition = (type: ConditionIssueType, path: string, message: string) =>
	`Invalid ${type === "expression" ? "condition expression" : `${type} expression`}${atPath(path)}: ${message}`;

export function findIssueInConditionSchema(condition: unknown, path = ""): string | null {
	const invalid = (type: ConditionIssueType, message: string) => invalidCondition(type, path, message);

	if (condition === undefined) return invalid("expression", "condition is undefined");

	if (!isNonNullObject(condition) || isScalarExpression(condition)) {
		if (typeof condition === "boolean") return null;
		return invalid("expression", `only boolean scalar values allowed for conditions (got ${JSON.stringify(condition)})`);
	}

	if (isExpressionObject(condition)) {
		const issue = findIssueInExpressionSchema(condition, path);
		return issue ? issue : null;
	}

	const conditionKeysCount = Object.keys(condition).filter((key) => key.startsWith("")).length;
	if (conditionKeysCount > 1) return invalid("expression", 'only one condition ("" key) is allowed at each level');

	if ("$and" in condition) {
		if (!isNonEmptyArray(condition.$and)) return invalid("$and", NON_EMPTY_CONDITION_ARRAY_ERROR("$and"));
		const issues = condition.$and
			.map((subCondition, index) => findIssueInConditionSchema(subCondition, `${path}.$and[${index}]`))
			.filter(isNotNull);
		if (issues.length > 0) return issues.join(", ");
	}

	if ("$or" in condition) {
		if (!isNonEmptyArray(condition.$or)) return `Invalid $or condition at ${path}: ${NON_EMPTY_CONDITION_ARRAY_ERROR("$or")}`;
		const issues = condition.$or
			.map((subCondition, index) => findIssueInConditionSchema(subCondition, `${path}.$or[${index}]`))
			.filter(isNotNull);
		if (issues.length > 0) return issues.join("\n");
	}

	if ("$not" in condition) return findIssueInConditionSchema(condition.$not, `${path}.$not`);

	if ("$exists" in condition) {
		if (!isNonNullObject(condition.$exists))
			return invalid("$exists", 'should be an object with "table" and "condition" properties');
		if (typeof condition.$exists.table !== "string") return invalid("$exists", '"table" should be a string');
		if (condition.$exists.condition === undefined) return invalid("$exists", 'missing "condition" property');
		if (objectSize(condition.$exists) !== 2) return invalid("$exists", 'should only contain "table" and "condition" properties');
		return findIssueInConditionSchema(condition.$exists.condition, `${path}.$exists.condition`);
	}

	for (const [key, value] of Object.entries(condition)) {
		if (!isField(key)) return invalid("expression", `invalid field name at ${path}: ${key}`);
		if (isAnyScalar(value) || isExpressionObject(value)) {
			findIssueInExpressionSchema(value, `${path}.${key}`);
			continue;
		}

		if (!isNonNullObject(value))
			return invalid(
				"expression",
				`a field condition should be an expression or an object with comparison, array or string expression operators (got ${JSON.stringify(value)})`,
			);

		for (const [key, subValue] of Object.entries(value)) {
			if (!isFieldOperator(key)) throw new Error(`Invalid field operator at ${path}.${key}: ${key}`);
			const issue = findIssueInConditionSchema(subValue, `${path}.${key}`);
			if (issue) return issue;
		}

		const issue = findIssueInConditionSchema(value, `${path}.${key}`);
		if (issue) return issue;
	}

	return null;
}

export function ensureConditionObject(condition: unknown): Condition {
	try {
		return conditionSchema.parse(condition);
	} catch {
		const issue = findIssueInConditionSchema(condition);
		throw new Error(issue ?? "Invalid condition: the condition object should match the expected schema");
	}
}
