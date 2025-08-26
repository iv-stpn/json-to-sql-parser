/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { parseExpression } from "../../src/parsers";

import type { AggregationQuery, AnyExpression, Condition } from "../../src/schemas";
import type { Config, ParserState } from "../../src/types";
import { quote } from "../../src/utils";
import { ExpressionTypeMap } from "../../src/utils/expression-map";
import { extractSelectWhereClause } from "../_helpers";

// Test configuration for SQLite
let testConfig: Config;

beforeEach(() => {
	testConfig = {
		dialect: "sqlite-3.44-extensions",
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

describe("CRUD - SELECT Query Operations (SQLite)", () => {
	describe("Basic Field Condition Parsing", () => {
		it("should parse simple equality condition", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.name = 'John'");
		});

		it("should parse multiple conditions with AND", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
				"users.age": { $gt: 25 },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.name = 'John' AND users.age > 25)");
		});

		it("should parse OR conditions", () => {
			const condition: Condition = {
				$or: [{ "users.name": { $eq: "John" } }, { "users.name": { $eq: "Jane" } }],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.name = 'John' OR users.name = 'Jane')");
		});

		it("should parse NOT conditions", () => {
			const condition: Condition = {
				$not: { "users.active": { $eq: true } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("NOT (users.active = TRUE)");
		});
	});

	describe("Comparison Operator Processing", () => {
		it("should parse numeric comparison operators", () => {
			const condition: Condition = {
				"users.age": { $gte: 18, $lte: 65 },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.age >= 18 AND users.age <= 65)");
		});

		it("should parse array operators", () => {
			const condition: Condition = {
				"users.name": { $in: ["John", "Jane", "Bob"] },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.name IN ('John', 'Jane', 'Bob')");
		});

		it("should parse string operators", () => {
			const condition: Condition = {
				"users.name": { $like: "John%" },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.name LIKE 'John%'");
		});
	});

	describe("Dynamic Expression Support", () => {
		it("should parse simple field reference expressions", () => {
			const condition: Condition = {
				"users.id": { $eq: { $var: "auth.uid" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("CAST(users.id AS TEXT) = '123'");
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.status = (CASE WHEN users.active = TRUE THEN 'active' ELSE 'inactive' END)");
		});
	});

	describe("JSON Field Access and Querying", () => {
		it("should parse JSON field access", () => {
			const condition: Condition = {
				"users.metadata->profile->name": { $eq: "John" },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.metadata->'profile'->>'name' = 'John'");
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("EXISTS (SELECT 1 FROM posts WHERE (posts.user_id = users.id AND posts.published = TRUE))");
		});
	});
});

describe("CRUD - SELECT Field Selection and Projections (SQLite)", () => {
	describe("Basic selection", () => {
		it("should parse simple field selection", () => {
			const selection = { id: true, name: true, email: true };
			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			const sql = compileSelectQuery(result);

			expect(sql).toContain('users.id AS "id"');
			expect(sql).toContain('users.name AS "name"');
			expect(sql).toContain('users.email AS "email"');
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name", users.email AS "email" FROM users');
		});

		it("should parse selection with conditions", () => {
			const selection = { id: true, name: true };
			const condition: Condition = { "users.active": { $eq: true } };

			const sql = compileSelectQuery(parseSelectQuery({ rootTable: "users", selection, condition }, testConfig));
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users WHERE users.active = TRUE');
		});
	});

	describe("Expression selection", () => {
		it("should parse expression fields", () => {
			const selection = {
				id: true,
				display_name: { $func: { CONCAT: [{ $field: "users.name" }, " - ", { $field: "users.email" }] } },
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			const sql = compileSelectQuery(result);

			expect(sql).toContain('users.id AS "id"');
			expect(sql).toContain("(users.name || ' - ' || users.email) AS \"display_name\"");
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
			const sql = compileSelectQuery(result);

			expect(result.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(sql).toContain('users.id AS "id"');
			expect(sql).toContain('posts.id AS "posts.id"');
			expect(sql).toContain('posts.title AS "posts.title"');
		});
	});

	describe("JSON field selection", () => {
		it("should parse JSON field selection", () => {
			const selection = {
				id: true,
				"metadata->profile->name": true,
			};

			const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
			const sql = compileSelectQuery(result);

			expect(sql).toContain("users.metadata->'profile'->>'name' AS \"metadata->profile->name\"");
		});
	});
});

describe("CRUD - SELECT Aggregation Operations (SQLite)", () => {
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

			expect(sql).toContain('COUNT(*) AS "total_users"');
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

			expect(sql).toContain('users.active AS "active"');
			expect(sql).toContain('COUNT(*) AS "user_count"');
			expect(sql).toContain('AVG(users.age) AS "avg_age"');
			expect(sql).toContain("GROUP BY users.active");

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
			const sql = compileAggregationQuery(result);

			expect(sql).toContain('MAX(users.age + 10) AS "max_age_plus_ten"');
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
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("users.metadata->>'department' AS \"metadata->department\"");
			expect(sql).toContain("GROUP BY users.metadata->>'department'");
		});
	});
});

describe("CRUD - SELECT Expression Evaluation (SQLite)", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = { config: testConfig, expressions: new ExpressionTypeMap(), rootTable: "users" };
	});

	describe("Field references", () => {
		it("should resolve context variables", () => {
			const expr: AnyExpression = { $var: "auth.uid" };
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("'123'");
		});

		it("should resolve field references", () => {
			const expr: AnyExpression = { $field: "users.name" };
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("users.name");
		});
	});

	describe("Function calls", () => {
		it("should evaluate unary functions", () => {
			const expr: AnyExpression = { $func: { UPPER: [{ $field: "users.name" }] } };
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("UPPER(users.name)");
		});

		it("should evaluate binary functions", () => {
			const expr: AnyExpression = { $func: { ADD: [{ $field: "users.age" }, 5] } };
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("(users.age + 5)");
		});

		it("should evaluate variable functions", () => {
			const expr: AnyExpression = { $func: { CONCAT: [{ $field: "users.name" }, " - ", { $field: "users.email" }] } };
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("(users.name || ' - ' || users.email)");
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
			const sql = parseExpression(expr, testState);
			expect(sql).toBe("(CASE WHEN users.active = TRUE THEN 'Active User' ELSE 'Inactive User' END)");
		});
	});
});

describe("Error Handling (SQLite)", () => {
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
				"Field type mismatch for '=' comparison on 'age': expected number, got string",
			);
		});

		it("should throw error for numeric operators on non-numeric fields", () => {
			const condition: Condition = {
				"users.name": { $gt: 5 },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for '>' comparison on 'name': expected string, got number",
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

describe("SQLite-specific Features", () => {
	it("should handle SQLite boolean values (1/0)", () => {
		const condition: Condition = {
			"users.active": { $eq: true },
		};

		const sql = extractSelectWhereClause(condition, testConfig, "users");
		expect(sql).toBe("users.active = TRUE");
	});

	it("should handle SQLite JSON extraction", () => {
		const selection = {
			id: true,
			dept: { $field: "users.metadata->department" },
		};

		const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);
		const sql = compileSelectQuery(result);

		expect(sql).toContain("users.metadata->>'department' AS \"dept\"");
	});

	it("should handle SQLite joins with UUID casting", () => {
		const selection = {
			id: true,
			name: true,
			posts: {
				id: true,
				title: true,
			},
		};

		const result = parseSelectQuery({ rootTable: "users", selection }, testConfig);

		expect(result.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
		expect(result.joins).not.toContain("::UUID"); // SQLite doesn't need UUID casting
	});

	it("should reject regex operator in SQLite", () => {
		const condition: Condition = {
			"users.name": { $regex: "^John.*" },
		};

		expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
			"Operator 'REGEXP' is not supported by default in SQLite",
		);
	});
});

describe("Edge Case Tests (SQLite)", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = {
			config: testConfig,
			rootTable: "users",
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toContain("AND");
			expect(sql).toContain("OR");
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toContain("NOT");
			expect(sql).toContain("AND");
		});
	});

	describe("SQLite-specific Value Testing", () => {
		it("should handle SQLite numeric values", () => {
			const largeNumbers = [
				Number.MAX_SAFE_INTEGER,
				9007199254740991, // MAX_SAFE_INTEGER
			];

			for (const largeNumber of largeNumbers) {
				const condition: Condition = {
					"users.age": { $eq: largeNumber },
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain(largeNumber.toString());
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

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain(quote(stringValue));
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

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain(quote(unicodeString));
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

			const sql = parseExpression(nestedExpression, testState);
			expect(sql).toBe("UPPER(LOWER('users.name' || ' - ' || 'users.status'))");
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toContain("users.metadata->'level1'->'level2'->'level3'->'level4'->>'level5'");
			expect(sql).toContain("deep_value");
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

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain("users.metadata->>");
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql.length).toBeGreaterThan(1000);
			expect(sql).toContain("IN");
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

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain("IN");
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
				const sql = parseExpression(expr, specialState);
				expect(sql).not.toContain("undefined");
			}
		});
	});

	describe("Operator Edge Cases", () => {
		it("should handle LIKE patterns with special SQL wildcards", () => {
			const likePatterns = ["%test%", "test%", "%test", "_test_", "test_", "_test", "test\\%escaped", "test\\_escaped"];

			for (const pattern of likePatterns) {
				const condition: Condition = {
					"users.name": { $like: pattern },
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toContain(`LIKE '${pattern}'`);
			}
		});

		it("should reject REGEX patterns in SQLite", () => {
			const regexPatterns = ["^test$", "test.*", ".*test.*", "[a-zA-Z]+", "\\d{3}-\\d{3}-\\d{4}"];

			for (const pattern of regexPatterns) {
				const condition: Condition = {
					"users.name": { $regex: pattern },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow(
					"Operator 'REGEXP' is not supported by default in SQLite",
				);
			}
		});
	});
});

describe("Expected Failure Tests (SQLite)", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = {
			config: testConfig,
			rootTable: "users",
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

	describe("SQLite Specific Error Cases", () => {
		it("should handle unsupported PostgreSQL functions gracefully", () => {
			// Test functions that exist in PostgreSQL but not in SQLite
			const postgresqlFunctions = ["EXTRACT_EPOCH"]; // Example of a PostgreSQL-specific function

			for (const func of postgresqlFunctions) {
				expect(() => {
					const expr: AnyExpression = {
						$func: { [func]: [{ $field: "users.created_at" }] },
					};
					parseExpression(expr, testState);
				}).toThrow("Type mismatch for 'EXTRACT_EPOCH': expected datetime, got string");
			}
		});

		it("should properly handle SQLite boolean conversion", () => {
			const condition: Condition = {
				"users.active": { $eq: false },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.active = FALSE");
		});
	});
});
