import { beforeEach, describe, expect, it } from "bun:test";
import { parseAggregationQuery } from "../src/builders/aggregate";
import { parseSelectQuery } from "../src/builders/select";
import { parseExpression } from "../src/parsers";
import type { AnyExpression, Condition } from "../src/schemas";
import type { Config, ParserState } from "../src/types";
import { ExpressionTypeMap } from "../src/utils/expression-map";
import { extractSelectWhereClause } from "./_helpers";

describe("Edge Case Tests", () => {
	let testConfig: Config;
	let testState: ParserState;

	beforeEach(() => {
		testConfig = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "age", type: "number", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "score", type: "number", nullable: true },
						{ name: "balance", type: "number", nullable: true },
						{ name: "description", type: "string", nullable: true },
					],
				},
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
						{ name: "tags", type: "object", nullable: true },
						{ name: "rating", type: "number", nullable: true },
					],
				},
			},
			variables: {
				"auth.uid": 123,
				current_user: 456,
				"system.version": "1.0.0",
				max_limit: 1000,
				is_admin: true,
			},
			relationships: [
				{
					table: "users",
					field: "id",
					toTable: "posts",
					toField: "user_id",
					type: "one-to-many",
				},
			],
		};

		testState = {
			config: testConfig,
			rootTable: "users",
			params: [],
			expressions: new ExpressionTypeMap(),
		};
	});

	describe("Complex Nested Conditions", () => {
		it("should handle deeply nested AND/OR conditions", () => {
			const condition: Condition = {
				$and: [
					{
						$or: [{ "users.name": { $eq: "John" } }, { "users.name": { $eq: "Jane" } }],
					},
					{
						$and: [{ "users.age": { $gt: 18 } }, { "users.age": { $lt: 65 } }],
					},
					{
						$or: [{ "users.active": { $eq: true } }, { "users.status": { $eq: "pending" } }],
					},
				],
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toContain("AND");
			expect(result.sql).toContain("OR");
			expect(result.params).toEqual(["John", "Jane", 18, 65, true, "pending"]);
		});

		it("should handle mixed NOT conditions with complex nesting", () => {
			const condition: Condition = {
				$not: {
					$and: [
						{
							$or: [{ "users.name": { $eq: "blocked" } }, { "users.status": { $eq: "banned" } }],
						},
						{ "users.age": { $lt: 13 } },
					],
				},
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toContain("NOT");
			expect(result.params).toEqual(["blocked", "banned", 13]);
		});

		it("should handle empty AND/OR arrays", () => {
			expect(() => {
				const condition: Condition = { $and: [] };
				extractSelectWhereClause(condition, testConfig, "users");
			}).toThrow();

			expect(() => {
				const condition: Condition = { $or: [] };
				extractSelectWhereClause(condition, testConfig, "users");
			}).toThrow();
		});

		it("should handle single-element AND/OR arrays", () => {
			const andCondition: Condition = {
				$and: [{ "users.name": { $eq: "John" } }],
			};

			const orCondition: Condition = {
				$or: [{ "users.age": { $gt: 18 } }],
			};

			const andResult = extractSelectWhereClause(andCondition, testConfig, "users");
			const orResult = extractSelectWhereClause(orCondition, testConfig, "users");

			expect(andResult.sql).toBe("users.name = $1");
			expect(orResult.sql).toBe("(users.age > $1)");
		});
	});

	describe("Extreme Value Testing", () => {
		it("should handle very large numbers", () => {
			const largeNumbers = [
				Number.MAX_SAFE_INTEGER,
				Number.MAX_VALUE,
				1e308,
				9007199254740991, // MAX_SAFE_INTEGER
			];

			for (const largeNumber of largeNumbers) {
				const condition: Condition = {
					"users.age": { $eq: largeNumber },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([largeNumber]);
			}
		});

		it("should handle very small numbers", () => {
			const smallNumbers = [
				Number.MIN_SAFE_INTEGER,
				Number.MIN_VALUE,
				-1e308,
				-9007199254740991, // MIN_SAFE_INTEGER
				Number.EPSILON,
			];

			for (const smallNumber of smallNumbers) {
				const condition: Condition = {
					"users.age": { $eq: smallNumber },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([smallNumber]);
			}
		});

		it("should handle special numeric values", () => {
			const specialNumbers = [0, -0, Infinity, -Infinity, NaN];

			for (const specialNumber of specialNumbers) {
				const condition: Condition = {
					"users.age": { $eq: specialNumber },
				};

				if (Number.isNaN(specialNumber) || !Number.isFinite(specialNumber)) {
					expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
				} else {
					const result = extractSelectWhereClause(condition, testConfig, "users");
					expect(result.params).toEqual([specialNumber]);
				}
			}
		});

		it("should handle empty strings and whitespace", () => {
			const stringValues = [
				"",
				" ",
				"  ",
				"\t",
				"\n",
				"\r\n",
				"\u0020", // Space
				"\u00A0", // Non-breaking space
			];

			for (const stringValue of stringValues) {
				const condition: Condition = {
					"users.name": { $eq: stringValue },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([stringValue]);
			}
		});

		it("should handle unicode strings", () => {
			const unicodeStrings = [
				"🚀 Unicode",
				"中文字符",
				"العربية",
				"русский",
				"हिन्दी",
				"日本語",
				"한국어",
				"🎉🎊✨",
				"\u{1F600}", // Emoji
				"\uD83D\uDE00", // Emoji UTF-16
			];

			for (const unicodeString of unicodeStrings) {
				const condition: Condition = {
					"users.name": { $eq: unicodeString },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([unicodeString]);
			}
		});
	});

	describe("Complex Expression Edge Cases", () => {
		it("should handle deeply nested function calls", () => {
			const nestedExpression: AnyExpression = {
				$func: {
					UPPER: [{ $func: { LOWER: [{ $func: { CONCAT: ["users.name", " - ", "users.status"] } }] } }],
				},
			};

			const result = parseExpression(nestedExpression, testState);
			expect(result).toContain("UPPER");
			expect(result).toContain("LOWER");
			expect(result).toContain("CONCAT");
		});

		it("should handle mathematical expressions with edge cases", () => {
			const addResult = parseExpression({ $func: { ADD: [0, 0] } }, testState);
			expect(addResult).toMatch("0 + 0");

			const subtractResult = parseExpression({ $func: { SUBTRACT: [0, 0] } }, testState);
			expect(subtractResult).toMatch("0 - 0");

			const multiplyResult = parseExpression({ $func: { MULTIPLY: [0, 1] } }, testState);
			expect(multiplyResult).toMatch("0 * 1");

			const divideResult = parseExpression({ $func: { DIVIDE: [1, 1] } }, testState);
			expect(divideResult).toMatch("1 / 1");

			const modResult = parseExpression({ $func: { MOD: [10, 3] } }, testState);
			expect(modResult).toMatch("10 % 3");

			const powResult = parseExpression({ $func: { POW: [2, 0] } }, testState);
			expect(powResult).toMatch("2 ^ 0");
		});

		it("should handle division by zero attempts", () => {
			const divisionByZero: AnyExpression = {
				$func: { DIVIDE: [{ $field: "users.age" }, 0] },
			};

			expect(() => {
				parseExpression(divisionByZero, testState);
			}).toThrow("Division by zero is not allowed");
		});
	});

	describe("JSON Path Edge Cases", () => {
		it("should handle deeply nested JSON paths", () => {
			const deepJsonPath = "users.metadata->level1->level2->level3->level4->level5";
			const condition: Condition = {
				[deepJsonPath]: { $eq: "deep_value" },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toContain("metadata");
			expect(result.sql).toContain("level1");
			expect(result.sql).toContain("level5");
		});

		it("should handle JSON paths with special characters", () => {
			const specialJsonPaths = [
				"users.metadata->'key-with-dashes'",
				"users.metadata->'key_with_underscores'",
				"users.metadata->'key.with.dots'",
				"users.metadata->'key with spaces'",
				"users.metadata->'key@with@symbols'",
				"users.metadata->'123numeric_key'",
			];

			for (const jsonPath of specialJsonPaths) {
				const condition: Condition = {
					[jsonPath]: { $eq: "test_value" },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual(["test_value"]);
			}
		});

		it("should reject empty JSON path segments", () => {
			const invalidJsonPaths = [
				"users.metadata->''",
				"users.metadata->''->valid",
				"users.metadata->valid->''",
				"users.metadata->->invalid",
			];

			for (const invalidPath of invalidJsonPaths) {
				const condition: Condition = {
					[invalidPath]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should reject JSON paths on non-object fields", () => {
			const invalidJsonOnString = "users.name->'invalid'";
			const condition: Condition = {
				[invalidJsonOnString]: { $eq: "test" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
		});
	});

	describe("Array Operations Edge Cases", () => {
		it("should handle empty arrays in IN/NOT IN", () => {
			const emptyArrayConditions = [{ "users.name": { $in: [] } }, { "users.name": { $nin: [] } }];

			for (const condition of emptyArrayConditions) {
				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should not handle arrays with mixed types", () => {
			const mixedArrays = [
				["string", 123, true],
				[true, false, "mixed"],
			];

			for (const mixedArray of mixedArrays) {
				const condition: Condition = {
					"users.name": { $in: mixedArray },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should handle very large arrays", () => {
			const largeArray = Array.from({ length: 1000 }, (_, i) => `value_${i}`);
			const condition: Condition = {
				"users.name": { $in: largeArray },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.params).toEqual(largeArray);
			expect(result.sql).toContain("IN");
		});

		it("should handle arrays with only primitive values", () => {
			const primitiveArrays = [
				["value1", "value2", "value3"],
				[1, 2, 3, 4, 5],
				[true, false, true],
			];

			for (const primitiveArray of primitiveArrays) {
				const condition: Condition = {
					"users.name": { $in: primitiveArray },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual(primitiveArray);
			}
		});
	});

	describe("Variable Resolution Edge Cases", () => {
		it("should handle undefined variables gracefully", () => {
			const undefinedVarExpr: AnyExpression = {
				$field: "undefined_variable",
			};

			// Should treat as field reference and potentially throw
			expect(() => parseExpression(undefinedVarExpr, testState)).toThrow();
		});

		it("should handle variables with special characters in names", () => {
			const specialConfig = {
				...testConfig,
				variables: {
					"auth.uid": 123,
					"system-version": "1.0",
					user_context: "admin",
					"data.nested.value": "test",
				},
			};

			const specialState = { ...testState, config: specialConfig };

			const variables = ["auth.uid", "system-version", "user_context", "data.nested.value"];

			for (const variable of variables) {
				const expr: AnyExpression = { $var: variable };
				const result = parseExpression(expr, specialState);
				expect(result).not.toContain("undefined");
			}
		});
	});

	describe("Operator Edge Cases", () => {
		it("should handle LIKE patterns with special SQL wildcards", () => {
			const likePatterns = [
				"%test%",
				"test%",
				"%test",
				"_test_",
				"test_",
				"_test",
				"[abc]test",
				"test[123]",
				"test\\%escaped",
				"test\\_escaped",
			];

			for (const pattern of likePatterns) {
				const condition: Condition = {
					"users.name": { $like: pattern },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([pattern]);
				expect(result.sql).toContain("LIKE");
			}
		});

		it("should handle REGEX patterns", () => {
			const regexPatterns = ["^test$", "test.*", ".*test.*", "[a-zA-Z]+", "\\d{3}-\\d{3}-\\d{4}", "(?i)case_insensitive"];

			for (const pattern of regexPatterns) {
				const condition: Condition = {
					"users.name": { $regex: pattern },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([pattern]);
				expect(result.sql).toContain("~");
			}
		});
	});

	describe("Complex Query Edge Cases", () => {
		it("should handle select queries with no selection fields", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "users",
						selection: {},
					},
					testConfig,
				);
			}).toThrow();
		});

		it("should handle aggregation queries with no fields", () => {
			expect(() => {
				parseAggregationQuery(
					{
						table: "users",
						groupBy: [],
						aggregatedFields: {},
					},
					testConfig,
				);
			}).toThrow();
		});

		it("should handle invalid table references", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "nonexistent_table",
						selection: { name: true },
					},
					testConfig,
				);
			}).toThrow();
		});

		it("should handle circular relationship references", () => {
			const circularConfig = {
				...testConfig,
				relationships: [
					{
						table: "users",
						field: "id",
						toTable: "posts",
						toField: "user_id",
						type: "one-to-many" as const,
					},
					{
						table: "posts",
						field: "user_id",
						toTable: "users",
						toField: "id",
						type: "many-to-one" as const,
					},
				],
			};

			// Should handle relationships without infinite loops
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						name: true,
						posts: {
							title: true,
						},
					},
				},
				circularConfig,
			);

			expect(query.select.length).toBeGreaterThan(0);
		});
	});
});
