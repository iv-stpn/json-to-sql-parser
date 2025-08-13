/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { beforeEach, describe, expect, it } from "bun:test";
import { ExpressionTypeMap } from "../src/expression-map";
import { parseExpression } from "../src/parsers";
import { parseWhereClause } from "../src/parsers/conditions";
import type { AnyExpression, Condition } from "../src/schemas";
import type { Config, ParserState } from "../src/types";

// Test configuration
let testConfig: Config;

beforeEach(() => {
	testConfig = {
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: true },
					{ name: "age", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "created_at", type: "string", nullable: false },
					{ name: "metadata", type: "object", nullable: true },
				],
			},
			posts: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "title", type: "string", nullable: false },
					{ name: "user_id", type: "number", nullable: false },
					{ name: "published", type: "boolean", nullable: false },
				],
			},
		},
		variables: {
			"auth.uid": 123,
			current_user: 456,
		},
		relationships: [],
	};
});

describe("Expression Parser Advanced Tests", () => {
	describe("Complex nested expressions", () => {
		it("should handle deeply nested conditional expressions", () => {
			const condition: Condition = {
				"users.status": {
					$eq: {
						$cond: {
							if: {
								$and: [{ "users.active": { $eq: true } }, { "users.age": { $gte: 18 } }],
							},
							then: {
								$cond: {
									if: { "users.age": { $gte: 65 } },
									then: "senior",
									else: "adult",
								},
							},
							else: "inactive",
						},
					},
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toContain("CASE WHEN");
			expect(result.sql).toContain("CASE WHEN users.age >= $3 THEN 'senior' ELSE 'adult' END");
			expect(result.params).toEqual([true, 18, 65]);
		});

		it("should handle expressions with mixed argument types", () => {
			const condition: Condition = {
				"users.name": {
					$eq: {
						$expr: {
							CONCAT: [{ $expr: "users.name" }, " (", { $expr: "auth.uid" }, ")"],
						},
					},
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name = CONCAT(users.name, ' (', 123, ')')");
		});

		it("should handle function calls with expression arguments", () => {
			const condition: Condition = {
				"users.age": {
					$gt: {
						$expr: {
							ADD: [{ $expr: { YEAR: [{ $expr: "users.created_at" }] } }, 5],
						},
					},
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.age > (YEAR(users.created_at) + 5)");
		});
	});

	describe("String literal handling", () => {
		it("should handle strings in expressions", () => {
			const condition: Condition = {
				"users.name": {
					$eq: { $expr: { CONCAT: ["Hello", "World"] } },
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name = CONCAT('Hello', 'World')");
		});

		it("should handle numeric literals in expressions", () => {
			const condition: Condition = {
				"users.age": {
					$eq: { $expr: { ADD: [25, 5.5] } },
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.age = (25 + 5.5)");
		});
	});

	describe("Error handling for expressions", () => {
		it("should throw error for invalid expression structure", () => {
			const condition: Condition = {
				"users.name": {
					$eq: { $expr: {} },
				},
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow("$expr must contain exactly one function");
		});

		it("should throw error for multiple functions in $expr", () => {
			const condition: Condition = {
				"users.name": {
					$eq: { $expr: { UPPER: ["test"], LOWER: ["test"] } },
				},
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow("$expr must contain exactly one function");
		});

		it("should throw error for empty function name", () => {
			const condition: Condition = {
				"users.name": {
					$eq: { $expr: { "": ["test"] } },
				},
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow("Function name cannot be empty");
		});
	});

	describe("Direct expression evaluation", () => {
		let testState: ParserState;

		beforeEach(() => {
			testState = { config: testConfig, params: [], expressions: new ExpressionTypeMap(), rootTable: "users" };
		});

		it("should evaluate primitive values directly", () => {
			expect(parseExpression("test", testState)).toBe("'test'");
			expect(parseExpression(42, testState)).toBe("42");
			expect(parseExpression(true, testState)).toBe("TRUE");
			expect(parseExpression(null, testState)).toBe("NULL");
		});

		it("should handle invalid expression types", () => {
			const invalidExpr = { $invalid: "test" } as unknown as AnyExpression;
			expect(() => parseExpression(invalidExpr, testState)).toThrow('Invalid expression object: {"$invalid":"test"}');
		});
	});
});

describe("Complex logical conditions", () => {
	describe("Nested logical operators", () => {
		it("should handle complex AND/OR combinations", () => {
			const condition: Condition = {
				$or: [
					{
						$and: [{ "users.active": { $eq: true } }, { "users.age": { $gte: 18 } }],
					},
					{
						$and: [{ "users.name": { $like: "Admin%" } }, { "users.email": { $ne: null } }],
					},
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("((users.active = $1 AND users.age >= $2) OR (users.name LIKE $3 AND users.email IS NOT NULL))");
			expect(result.params).toEqual([true, 18, "Admin%"]);
		});

		it("should handle nested NOT conditions", () => {
			const condition: Condition = {
				$not: {
					$or: [{ "users.active": { $eq: false } }, { "users.email": { $eq: null } }],
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("NOT ((users.active = $1 OR users.email IS NULL))");
			expect(result.params).toEqual([false]);
		});
	});

	describe("Mixed operator combinations", () => {
		it("should handle multiple operators on same field", () => {
			const condition: Condition = {
				"users.age": { $gte: 18, $lte: 65, $ne: 30 },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.age != $1 AND users.age >= $2 AND users.age <= $3)");
			expect(result.params).toEqual([30, 18, 65]);
		});

		it("should handle array operations with expressions", () => {
			const condition: Condition = {
				"users.id": {
					$in: [1, { $expr: "auth.uid" }, { $expr: "current_user" }],
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.id IN ($1, 123, 456)");
			expect(result.params).toEqual([1]);
		});
	});
});

describe("Null handling", () => {
	describe("Null comparisons", () => {
		it("should handle IS NULL for equality", () => {
			const condition: Condition = {
				"users.email": { $eq: null },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.email IS NULL");
			expect(result.params).toEqual([]);
		});

		it("should handle IS NOT NULL for inequality", () => {
			const condition: Condition = {
				"users.email": { $ne: null },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.email IS NOT NULL");
			expect(result.params).toEqual([]);
		});

		it("should throw error for null on non-nullable field", () => {
			const condition: Condition = {
				"users.name": { $eq: null },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Field 'name' is not nullable, and cannot be compared with NULL",
			);
		});
	});
});
