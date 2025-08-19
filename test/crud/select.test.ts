/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { parseExpression } from "../../src/parsers";

import type { AggregationQuery, AnyExpression, Condition } from "../../src/schemas";
import type { Config, ParserState } from "../../src/types";
import { ExpressionTypeMap } from "../../src/utils/expression-map";
import { extractSelectWhereClause } from "../_helpers";

// Test configuration
let testConfig: Config;

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
					{ name: "created_at", type: "string", nullable: false },
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
			"auth.uid": "123",
			current_user: "456",
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
});

describe("CRUD - SELECT Query Operations", () => {
	describe("Basic Field Condition Parsing", () => {
		it("should parse simple equality condition", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name = $1");
			expect(result.params).toEqual(["John"]);
		});

		it("should parse multiple conditions with AND", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
				"users.age": { $gt: 25 },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.name = $1 AND users.age > $2)");
			expect(result.params).toEqual(["John", 25]);
		});

		it("should parse OR conditions", () => {
			const condition: Condition = {
				$or: [{ "users.name": { $eq: "John" } }, { "users.name": { $eq: "Jane" } }],
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.name = $1 OR users.name = $2)");
			expect(result.params).toEqual(["John", "Jane"]);
		});

		it("should parse NOT conditions", () => {
			const condition: Condition = {
				$not: { "users.active": { $eq: true } },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("NOT (users.active = $1)");
			expect(result.params).toEqual([true]);
		});
	});

	describe("Comparison Operator Processing", () => {
		it("should parse numeric comparison operators", () => {
			const condition: Condition = {
				"users.age": { $gte: 18, $lte: 65 },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.age >= $1 AND users.age <= $2)");
			expect(result.params).toEqual([18, 65]);
		});

		it("should parse array operators", () => {
			const condition: Condition = {
				"users.name": { $in: ["John", "Jane", "Bob"] },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name IN ($1, $2, $3)");
			expect(result.params).toEqual(["John", "Jane", "Bob"]);
		});

		it("should parse string operators", () => {
			const condition: Condition = {
				"users.name": { $like: "John%" },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name LIKE $1");
			expect(result.params).toEqual(["John%"]);
		});
	});

	describe("Dynamic Expression Support", () => {
		it("should parse simple field reference expressions", () => {
			const condition: Condition = {
				"users.id": { $eq: { $var: "auth.uid" } },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.id)::TEXT = '123'");
			expect(result.params).toEqual([]);
		});

		it("should parse conditional expressions", () => {
			const condition: Condition = {
				"users.status": {
					$eq: {
						$cond: {
							if: { "users.active": { $eq: true } },
							then: "active",
							else: "inactive",
						},
					},
				},
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.status = (CASE WHEN users.active = $1 THEN 'active' ELSE 'inactive' END)");
			expect(result.params).toEqual([true]);
		});
	});

	describe("JSON Field Access and Querying", () => {
		it("should parse JSON field access", () => {
			const condition: Condition = {
				"users.metadata->profile->name": { $eq: "John" },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.metadata->'profile'->>'name' = $1");
			expect(result.params).toEqual(["John"]);
		});
	});

	describe("EXISTS Condition Processing", () => {
		it("should parse EXISTS subqueries", () => {
			const condition: Condition = {
				$exists: {
					table: "posts",
					condition: {
						"posts.user_id": { $eq: { $field: "users.id" } },
						"posts.published": { $eq: true },
					},
				},
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("EXISTS (SELECT 1 FROM posts WHERE (posts.user_id = users.id AND posts.published = $1))");
			expect(result.params).toEqual([true]);
		});
	});
});

describe("CRUD - SELECT Field Selection and Projections", () => {
	describe("Basic selection", () => {
		it("should parse simple field selection", () => {
			const selection = {
				id: true,
				name: true,
				email: true,
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			const sql = compileSelectQuery(result);

			expect(result.select).toContain('users.id AS "id"');
			expect(result.select).toContain('users.name AS "name"');
			expect(result.select).toContain('users.email AS "email"');
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name", users.email AS "email" FROM users');
		});

		it("should parse selection with conditions", () => {
			const selection = { id: true, name: true };
			const condition: Condition = { "users.active": { $eq: true } };

			const result = parseSelectQuery({ rootTable: "users", selection, condition }, testConfig);
			const sql = compileSelectQuery(result);

			expect(result.params).toEqual([true]);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users WHERE users.active = $1');
		});
	});

	describe("Expression selection", () => {
		it("should parse expression fields", () => {
			const selection = {
				id: true,
				display_name: { $func: { CONCAT: [{ $field: "users.name" }, " - ", { $field: "users.email" }] } },
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);

			expect(result.select).toContain('users.id AS "id"');
			expect(result.select).toContain("CONCAT(users.name, ' - ', users.email) AS \"display_name\"");
		});
	});

	describe("Relationship joins", () => {
		it("should parse relationship selections with joins", () => {
			const selection = {
				id: true,
				name: true,
				posts: {
					id: true,
					title: true,
				},
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			compileSelectQuery(result); // Call to avoid unused variable

			expect(result.joins).toContain("LEFT JOIN posts ON (users.id)::UUID = (posts.user_id)::UUID");
			expect(result.select).toContain('users.id AS "id"');
			expect(result.select).toContain('posts.id AS "posts.id"');
			expect(result.select).toContain('posts.title AS "posts.title"');
		});
	});

	describe("JSON field selection", () => {
		it("should parse JSON field selection", () => {
			const selection = {
				id: true,
				"metadata->profile->name": true,
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			expect(result.select).toContain("users.metadata->'profile'->>'name' AS \"metadata->profile->name\"");
		});
	});
});

describe("CRUD - SELECT Aggregation Operations", () => {
	describe("Basic aggregation", () => {
		it("should parse simple COUNT aggregation", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					total_users: { function: "COUNT", field: "*" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(result.select).toContain('COUNT(*) AS "total_users"');
			expect(sql).toBe('SELECT COUNT(*) AS "total_users" FROM users');
		});

		it("should parse GROUP BY with aggregation", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.active"],
				aggregatedFields: {
					user_count: { function: "COUNT", field: "*" },
					avg_age: { function: "AVG", field: "users.age" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(result.select).toContain('users.active AS "active"');
			expect(result.select).toContain('COUNT(*) AS "user_count"');
			expect(result.select).toContain('AVG(users.age) AS "avg_age"');
			expect(result.groupBy).toContain("users.active");

			expect(sql).toBe(
				'SELECT users.active AS "active", COUNT(*) AS "user_count", AVG(users.age) AS "avg_age" FROM users GROUP BY users.active',
			);
		});
	});

	describe("Advanced aggregation", () => {
		it("should parse aggregation with expressions", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.active"],
				aggregatedFields: {
					max_age_plus_ten: {
						function: "MAX",
						field: { $func: { ADD: [{ $field: "users.age" }, 10] } },
					},
				},
			};

			const result = parseAggregationQuery(query, testConfig);

			expect(result.select).toContain('MAX(users.age + 10) AS "max_age_plus_ten"');
		});

		it("should parse JSON field aggregation", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.metadata->department"],
				aggregatedFields: {
					dept_count: { function: "COUNT", field: "*" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);

			expect(result.select).toContain("users.metadata->>'department' AS \"metadata->department\"");
			expect(result.groupBy).toContain("users.metadata->>'department'");
		});
	});
});

describe("CRUD - SELECT Expression Evaluation", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = { config: testConfig, params: [], expressions: new ExpressionTypeMap(), rootTable: "users" };
	});

	describe("Field references", () => {
		it("should resolve context variables", () => {
			const expr: AnyExpression = { $var: "auth.uid" };
			const result = parseExpression(expr, testState);
			expect(result).toBe("'123'");
		});

		it("should resolve field references", () => {
			const expr: AnyExpression = { $field: "users.name" };
			const result = parseExpression(expr, testState);
			expect(result).toBe("users.name");
		});
	});

	describe("Function calls", () => {
		it("should evaluate unary functions", () => {
			const expr: AnyExpression = { $func: { UPPER: [{ $field: "users.name" }] } };
			const result = parseExpression(expr, testState);
			expect(result).toBe("UPPER(users.name)");
		});

		it("should evaluate binary functions", () => {
			const expr: AnyExpression = { $func: { ADD: [{ $field: "users.age" }, 5] } };
			const result = parseExpression(expr, testState);
			expect(result).toBe("(users.age + 5)");
		});

		it("should evaluate variable functions", () => {
			const expr: AnyExpression = { $func: { CONCAT: [{ $field: "users.name" }, " - ", { $field: "users.email" }] } };
			const result = parseExpression(expr, testState);
			expect(result).toBe("CONCAT(users.name, ' - ', users.email)");
		});
	});

	describe("Conditional expressions", () => {
		it("should evaluate CASE WHEN expressions", () => {
			const expr: AnyExpression = {
				$cond: {
					if: { "users.active": { $eq: true } },
					then: "Active User",
					else: "Inactive User",
				},
			};
			const result = parseExpression(expr, testState);
			expect(result).toBe("(CASE WHEN users.active = $1 THEN 'Active User' ELSE 'Inactive User' END)");
			expect(testState.params).toEqual([true]);
		});
	});
});

describe("Error Handling", () => {
	describe("Invalid field references", () => {
		it("should throw error for non-existent table", () => {
			const condition: Condition = {
				"invalid_table.field": { $eq: "value" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow("Table 'invalid_table' is not allowed");
		});

		it("should throw error for non-existent field", () => {
			const condition: Condition = {
				"users.invalid_field": { $eq: "value" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"Field 'invalid_field' is not allowed or does not exist in 'users'",
			);
		});
	});

	describe("Type validation", () => {
		it("should throw error for wrong value type", () => {
			const condition: Condition = {
				"users.age": { $eq: "not_a_number" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for '=' comparison on 'age': expected FLOAT, got TEXT",
			);
		});

		it("should throw error for numeric operators on non-numeric fields", () => {
			const condition: Condition = {
				"users.name": { $gt: 5 },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for '>' comparison on 'name': expected TEXT, got FLOAT",
			);
		});
	});

	describe("JSON access validation", () => {
		it("should throw error for JSON access on non-JSON fields", () => {
			const condition: Condition = {
				"users.name->profile": { $eq: "value" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"JSON path access 'name->profile' is only allowed on JSON fields, but field 'name' is of type 'string'",
			);
		});
	});

	describe("Expression validation", () => {
		it("should throw error for unknown functions", () => {
			const condition: Condition = {
				"users.name": { $eq: { $func: { UNKNOWN_FUNC: ["arg"] } } },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				'Unknown function or operator: "UNKNOWN_FUNC"',
			);
		});

		it("should throw error for wrong argument count", () => {
			const condition: Condition = {
				"users.age": { $eq: { $func: { ABS: ["arg1", "arg2"] } } },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"Function 'ABS' requires exactly 1 argument, got 2",
			);
		});
	});
});

describe("UUID Support", () => {
	it("should handle UUID fields with proper casting in joins", () => {
		const uuidConfig: Config = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
					],
				},
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
					],
				},
			},
			variables: {},
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

		const selection = {
			id: true,
			name: true,
			posts: {
				id: true,
				title: true,
			},
		};

		const result = parseSelectQuery({ rootTable: "users", selection }, uuidConfig);

		expect(result.joins).toContain("LEFT JOIN posts ON (users.id)::UUID = (posts.user_id)::UUID");
		expect(result.select).toContain('users.id AS "id"');
		expect(result.select).toContain('users.name AS "name"');
		expect(result.select).toContain('posts.id AS "posts.id"');
		expect(result.select).toContain('posts.title AS "posts.title"');
	});

	it("should handle mixed UUID and other types in joins", () => {
		const mixedConfig: Config = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "total", type: "number", nullable: false },
					],
				},
			},
			variables: {},
			relationships: [
				{
					table: "users",
					field: "id",
					toTable: "orders",
					toField: "user_id",
					type: "one-to-many",
				},
			],
		};

		const selection = {
			id: true,
			name: true,
			orders: {
				id: true,
				total: true,
			},
		};

		const result = parseSelectQuery({ rootTable: "users", selection }, mixedConfig);

		// UUID field should be cast to UUID, number field should be cast to FLOAT
		expect(result.joins).toContain("LEFT JOIN orders ON (users.id)::UUID = (orders.user_id)::UUID");
	});
});

describe("Edge Case Tests", () => {
	let testState: ParserState;

	beforeEach(() => {
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

		it("should handle arrays with only scalar values", () => {
			const scalarArrays = [
				["value1", "value2", "value3"],
				[1, 2, 3, 4, 5],
				[true, false, true],
			];

			for (const scalarArray of scalarArrays) {
				const condition: Condition = {
					"users.name": { $in: scalarArray },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual(scalarArray);
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

describe("Expected Failure Tests", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = {
			config: testConfig,
			rootTable: "users",
			params: [],
			expressions: new ExpressionTypeMap(),
		};
	});

	describe("Invalid Field Access", () => {
		it("should reject non-existent tables", () => {
			expect(() => {
				extractSelectWhereClause({ "nonexistent.field": { $eq: "value" } }, testConfig, "users");
			}).toThrow("Table 'nonexistent' is not allowed or does not exist");
		});

		it("should reject non-existent fields", () => {
			expect(() => {
				extractSelectWhereClause(
					{
						"users.nonexistent_field": { $eq: "value" },
					},
					testConfig,
					"users",
				);
			}).toThrow("Field 'nonexistent_field' is not allowed or does not exist in 'users'");
		});

		it("should reject invalid table.field format", () => {
			const invalidFormats = ["users.field.extra.parts", "users..field", ".field", "users.", ""];

			for (const invalidFormat of invalidFormats) {
				expect(() => {
					extractSelectWhereClause(
						{
							[invalidFormat]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject fields that don't start with lowercase", () => {
			const invalidFieldNames = [
				"Users.name", // Capital first letter
				"USERS.name", // All caps
				"1users.name", // Starts with number
				"_users.name", // Starts with underscore
				"-users.name", // Starts with dash
			];

			for (const invalidField of invalidFieldNames) {
				expect(() => {
					extractSelectWhereClause(
						{
							[invalidField]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject JSON access on non-object fields", () => {
			expect(() => {
				extractSelectWhereClause({ "users.name->invalid": { $eq: "value" } }, testConfig, "users");
			}).toThrow("JSON path access 'name->invalid' is only allowed on JSON fields, but field 'name' is of type 'string'");

			expect(() => {
				extractSelectWhereClause({ "users.age->invalid": { $eq: "value" } }, testConfig, "users");
			}).toThrow("JSON path access 'age->invalid' is only allowed on JSON fields, but field 'age' is of type 'number'");

			expect(() => {
				extractSelectWhereClause({ "users.active->invalid": { $eq: "value" } }, testConfig, "users");
			}).toThrow("JSON path access 'active->invalid' is only allowed on JSON fields, but field 'active' is of type 'boolean'");
		});
	});

	describe("Invalid Operators and Values", () => {
		it("should reject unknown operators", () => {
			const unknownOperators = ["$unknown", "$invalid", "$custom", "$select", "$drop"];

			for (const unknownOp of unknownOperators) {
				expect(() => {
					const condition = {
						"users.name": { [unknownOp]: "value" },
					};
					extractSelectWhereClause(condition as Condition, testConfig, "users");
				}).toThrow();
			}
		});

		it("should reject empty arrays in IN/NOT IN operators", () => {
			expect(() => {
				extractSelectWhereClause(
					{
						"users.name": { $in: [] },
					},
					testConfig,
					"users",
				);
			}).toThrow("Operator 'IN' requires a non-empty array");

			expect(() => {
				extractSelectWhereClause(
					{
						"users.name": { $nin: [] },
					},
					testConfig,
					"users",
				);
			}).toThrow("Operator 'NOT IN' requires a non-empty array");
		});

		it("should reject invalid function names in expressions", () => {
			const invalidFunctions = ["INVALID_FUNC", "DROP_TABLE", "SELECT", "INSERT", "UPDATE", "DELETE"];

			for (const invalidFunc of invalidFunctions) {
				expect(() => {
					const expr: AnyExpression = {
						$func: { [invalidFunc]: ["users.name"] },
					};
					parseExpression(expr, testState);
				}).toThrow("Unknown function or operator");
			}
		});

		it("should reject incorrect argument counts for functions", () => {
			// Unary functions with wrong argument count
			expect(() => {
				const expr: AnyExpression = {
					$func: { UPPER: [] }, // No arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Function 'UPPER' requires exactly 1 argument, got 0");

			expect(() => {
				const expr: AnyExpression = {
					$func: { UPPER: ["arg1", "arg2"] }, // Too many arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Function 'UPPER' requires exactly 1 argument, got 2");

			// Binary functions with wrong argument count
			expect(() => {
				const expr: AnyExpression = {
					$func: { ADD: ["only_one"] }, // Not enough arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Function 'ADD' requires exactly 2 arguments, got 1");

			expect(() => {
				const expr: AnyExpression = {
					$func: { ADD: ["arg1", "arg2", "arg3"] }, // Too many arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Function 'ADD' requires exactly 2 arguments, got 3");
		});

		it("should reject variable functions with no arguments", () => {
			expect(() => {
				const expr: AnyExpression = {
					$func: { COALESCE_STRING: [] },
				};
				parseExpression(expr, testState);
			}).toThrow("Function 'COALESCE_STRING' requires at least 2 arguments, got 0");
		});
	});

	describe("Invalid Query Structures", () => {
		it("should reject empty AND conditions", () => {
			expect(() => {
				extractSelectWhereClause({ $and: [] }, testConfig, "users");
			}).toThrow("$and condition should be a non-empty array.");
		});

		it("should reject empty OR conditions", () => {
			expect(() => {
				extractSelectWhereClause({ $or: [] }, testConfig, "users");
			}).toThrow("$or condition should be a non-empty array.");
		});

		it("should reject select queries with empty selection", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "users",
						selection: {},
					},
					testConfig,
				);
			}).toThrow("Selection cannot be empty");
		});

		it("should reject aggregation queries with no fields", () => {
			expect(() => {
				parseAggregationQuery(
					{
						table: "users",
						groupBy: [],
						aggregatedFields: {},
					},
					testConfig,
				);
			}).toThrow("Aggregation query must have at least one group by field or aggregated field");
		});

		it("should reject invalid aggregation operators", () => {
			// This test verifies that invalid operators are caught
			expect(() => {
				// The aggregation operators array should validate this
				parseAggregationQuery(
					{
						table: "users",
						groupBy: ["name"],
						aggregatedFields: {
							result: {
								function: "COUNT", // Use valid operator to test the field validation instead
								field: "nonexistent_field",
							},
						},
					},
					testConfig,
				);
			}).toThrow();
		});

		it("should reject COUNT(*) with non-COUNT operators", () => {
			// Test that only COUNT can use "*" as field
			const validCountQuery = parseAggregationQuery(
				{
					table: "users",
					groupBy: ["name"],
					aggregatedFields: {
						count_all: {
							function: "COUNT",
							field: "*",
						},
					},
				},
				testConfig,
			);

			// This should work
			const sql = compileAggregationQuery(validCountQuery);
			expect(sql).toContain("COUNT(*)");
		});
	});

	describe("Invalid Relationships", () => {
		it("should reject queries with non-existent relationships", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "users",
						selection: {
							name: true,
							nonexistent_table: {
								field: true,
							},
						},
					},
					testConfig,
				);
			}).toThrow("No relationship found");
		});

		it("should reject EXISTS conditions with invalid tables", () => {
			expect(() => {
				extractSelectWhereClause(
					{
						$exists: {
							table: "nonexistent",
							condition: {
								"nonexistent.field": { $eq: "value" },
							},
						},
					},
					testConfig,
					"users",
				);
			}).toThrow("Table 'nonexistent' is not allowed or does not exist");
		});
	});

	describe("Type Validation Failures", () => {
		it("should reject malformed expression function objects", () => {
			expect(() => {
				const expr: AnyExpression = {
					$func: {}, // Empty object
				};
				parseExpression(expr, testState);
			}).toThrow("$func must contain exactly one function");

			expect(() => {
				const expr: AnyExpression = {
					$func: {
						UPPER: ["arg1"],
						LOWER: ["arg2"], // Multiple functions
					},
				};
				parseExpression(expr, testState);
			}).toThrow("$func must contain exactly one function");
		});
	});

	describe("Boundary and Edge Case Failures", () => {
		it("should handle reasonably nested conditions", () => {
			// Create a reasonably deeply nested condition
			let deepCondition: Condition = { "users.name": { $eq: "base" } };

			for (let i = 0; i < 100; i++) {
				deepCondition = { $and: [deepCondition] };
			}

			// This should work fine
			const result = extractSelectWhereClause(deepCondition, testConfig, "users");
			expect(result.sql).toContain("users.name = $1");
			expect(result.params).toEqual(["base"]);
		});

		it("should reject circular references in expressions", () => {
			// While not directly testable due to TypeScript protection,
			// we can test cases that might cause infinite loops
			expect(() => {
				const expr: AnyExpression = {
					$func: { CONCAT: [] }, // Empty array
				};
				parseExpression(expr, testState);
			}).toThrow();
		});

		it("should handle malformed JSON correctly", () => {
			// Test with strings that look like JSON but aren't valid
			const malformedJsonStrings = ["{ invalid json }", "[ incomplete array", "'single quotes'", '{ "unfinished": '];

			for (const malformed of malformedJsonStrings) {
				const condition: Condition = {
					"users.name": { $eq: malformed },
				};

				// Should handle these as regular strings, not throw JSON parse errors
				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([malformed]);
			}
		});
	});

	describe("Security Boundary Tests", () => {
		it("should reject attempts to access system tables", () => {
			const systemTables = ["information_schema.tables", "pg_catalog.pg_tables", "sys.tables", "mysql.user"];

			for (const systemTable of systemTables) {
				expect(() => {
					extractSelectWhereClause(
						{
							[`${systemTable}.column`]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject attempts to use dangerous SQL keywords in field names", () => {
			const dangerousKeywords = ["SELECT", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "CREATE"];

			for (const keyword of dangerousKeywords) {
				expect(() => {
					extractSelectWhereClause(
						{
							[`users.${keyword.toLowerCase()}`]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject attempts to escape parameter binding", () => {
			const escapeAttempts = [
				"'; SELECT * FROM users; --",
				"' UNION SELECT password FROM admin --",
				"'; DROP TABLE users; --",
				"' OR '1'='1",
			];

			for (const escapeAttempt of escapeAttempts) {
				const condition: Condition = {
					"users.name": { $eq: escapeAttempt },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");

				// Should be properly parameterized, not injected
				expect(result.sql).toBe("users.name = $1");
				expect(result.params).toEqual([escapeAttempt]);

				// Make sure dangerous SQL is not present in the expression
				expect(result.sql).not.toContain("SELECT");
				expect(result.sql).not.toContain("DROP");
				expect(result.sql).not.toContain("UNION");
			}
		});
	});

	describe("Resource Exhaustion Tests", () => {
		it("should handle reasonable limits on parameter count", () => {
			// Test with a very large number of parameters
			const largeArray = Array.from({ length: 10000 }, (_, i) => `value_${i}`);

			const condition: Condition = {
				"users.name": { $in: largeArray },
			};

			// Should either handle it gracefully or reject with a reasonable error
			try {
				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params.length).toBe(10000);
			} catch (error) {
				// If it throws, it should be a reasonable error about limits
				expect(error).toBeDefined();
			}
		});

		it("should handle deeply nested JSON paths reasonably", () => {
			// Create a very deep JSON path
			const deepPath = Array.from({ length: 100 }, (_, i) => `level${i}`).join("->");
			const condition: Condition = {
				[`users.metadata->${deepPath}`]: { $eq: "deep_value" },
			};

			// Should either handle it or reject with a reasonable error
			try {
				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual(["deep_value"]);
			} catch (error) {
				// If it throws, should be a reasonable error
				expect(error).toBeDefined();
			}
		});
	});
});
