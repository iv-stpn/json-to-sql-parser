/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { parseExpression } from "../src/parsers";
import { type AggregationQuery, compileAggregationQuery, parseAggregationQuery } from "../src/parsers/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../src/parsers/select";
import { parseWhereClause } from "../src/parsers/where";
import type { AnyExpression, Condition } from "../src/schemas";
import type { Config, ParserState } from "../src/types";
import { ExpressionTypeMap } from "../src/utils/expression-map";

// Regex patterns for performance
const SELECT_REGEX = /^SELECT .* FROM users$/;
const WHERE_REGEX = /WHERE users\.active = \$1$/;
const GROUP_BY_REGEX = /GROUP BY users\.active$/;

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
					{ name: "content", type: "string", nullable: false },
					{ name: "user_id", type: "number", nullable: false },
					{ name: "published", type: "boolean", nullable: false },
					{ name: "tags", type: "object", nullable: true },
				],
			},
		},
		variables: {
			"auth.uid": 123,
			current_user: 456,
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

describe("Conditions Parser", () => {
	describe("Basic field conditions", () => {
		it("should parse simple equality condition", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name = $1");
			expect(result.params).toEqual(["John"]);
		});

		it("should parse multiple conditions with AND", () => {
			const condition: Condition = {
				"users.name": { $eq: "John" },
				"users.age": { $gt: 25 },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.name = $1 AND users.age > $2)");
			expect(result.params).toEqual(["John", 25]);
		});

		it("should parse OR conditions", () => {
			const condition: Condition = {
				$or: [{ "users.name": { $eq: "John" } }, { "users.name": { $eq: "Jane" } }],
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.name = $1 OR users.name = $2)");
			expect(result.params).toEqual(["John", "Jane"]);
		});

		it("should parse NOT conditions", () => {
			const condition: Condition = {
				$not: { "users.active": { $eq: true } },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("NOT (users.active = $1)");
			expect(result.params).toEqual([true]);
		});
	});

	describe("Comparison operators", () => {
		it("should parse numeric comparison operators", () => {
			const condition: Condition = {
				"users.age": { $gte: 18, $lte: 65 },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("(users.age >= $1 AND users.age <= $2)");
			expect(result.params).toEqual([18, 65]);
		});

		it("should parse array operators", () => {
			const condition: Condition = {
				"users.name": { $in: ["John", "Jane", "Bob"] },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name IN ($1, $2, $3)");
			expect(result.params).toEqual(["John", "Jane", "Bob"]);
		});

		it("should parse string operators", () => {
			const condition: Condition = {
				"users.name": { $like: "John%" },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name LIKE $1");
			expect(result.params).toEqual(["John%"]);
		});
	});

	describe("Expression support", () => {
		it("should parse simple field reference expressions", () => {
			const condition: Condition = {
				"users.id": { $eq: { $expr: "auth.uid" } },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.id = 123");
			expect(result.params).toEqual([]);
		});

		it("should parse function expressions", () => {
			const condition: Condition = {
				"users.age": { $gt: { $expr: { YEAR: [{ $expr: "users.created_at" }] } } },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.age > YEAR(users.created_at)");
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

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.status = (CASE WHEN users.active = $1 THEN 'active' ELSE 'inactive' END)");
			expect(result.params).toEqual([true]);
		});
	});

	describe("JSON field access", () => {
		it("should parse JSON field access", () => {
			const condition: Condition = {
				"users.metadata->profile->name": { $eq: "John" },
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.metadata->'profile'->>'name' = $1");
			expect(result.params).toEqual(["John"]);
		});
	});

	describe("EXISTS conditions", () => {
		it("should parse EXISTS subqueries", () => {
			const condition: Condition = {
				$exists: {
					table: "posts",
					conditions: {
						"posts.user_id": { $eq: { $expr: "users.id" } },
						"posts.published": { $eq: true },
					},
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("EXISTS (SELECT 1 FROM posts WHERE (posts.user_id = users.id AND posts.published = $1))");
			expect(result.params).toEqual([true]);
		});
	});
});

describe("Select Parser", () => {
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
			expect(sql).toMatch(SELECT_REGEX);
		});

		it("should parse selection with conditions", () => {
			const selection = { id: true, name: true };
			const condition: Condition = { "users.active": { $eq: true } };

			const result = parseSelectQuery({ rootTable: "users", selection, condition }, testConfig);
			const sql = compileSelectQuery(result);

			expect(sql).toMatch(WHERE_REGEX);
			expect(result.params).toEqual([true]);
		});
	});

	describe("Expression selection", () => {
		it("should parse expression fields", () => {
			const selection = {
				id: true,
				display_name: { $expr: { CONCAT: [{ $expr: "users.name" }, " - ", { $expr: "users.email" }] } },
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

			expect(result.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
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

describe("Aggregation Parser", () => {
	describe("Basic aggregation", () => {
		it("should parse simple COUNT aggregation", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					total_users: { operator: "COUNT", field: "*" },
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
					user_count: { operator: "COUNT", field: "*" },
					avg_age: { operator: "AVG", field: "users.age" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(result.select).toContain('users.active AS "active"');
			expect(result.select).toContain('COUNT(*) AS "user_count"');
			expect(result.select).toContain('AVG(users.age) AS "avg_age"');
			expect(result.groupBy).toContain("users.active");
			expect(sql).toMatch(GROUP_BY_REGEX);
		});
	});

	describe("Advanced aggregation", () => {
		it("should parse aggregation with expressions", () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.active"],
				aggregatedFields: {
					max_age_plus_ten: {
						operator: "MAX",
						field: { $expr: { ADD: [{ $expr: "users.age" }, 10] } },
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
					dept_count: { operator: "COUNT", field: "*" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);

			expect(result.select).toContain("users.metadata->>'department' AS \"metadata->department\"");
			expect(result.groupBy).toContain("users.metadata->>'department'");
		});
	});
});

describe("Expression Evaluation", () => {
	let testState: ParserState;

	beforeEach(() => {
		testState = { config: testConfig, params: [], expressions: new ExpressionTypeMap(), rootTable: "users" };
	});

	describe("Field references", () => {
		it("should resolve context variables", () => {
			const expr: AnyExpression = { $expr: "auth.uid" };
			const result = parseExpression(expr, testState);
			expect(result).toBe("123");
		});

		it("should resolve field references", () => {
			const expr: AnyExpression = { $expr: "users.name" };
			const result = parseExpression(expr, testState);
			expect(result).toBe("users.name");
		});
	});

	describe("Function calls", () => {
		it("should evaluate unary functions", () => {
			const expr: AnyExpression = { $expr: { UPPER: [{ $expr: "users.name" }] } };
			const result = parseExpression(expr, testState);
			expect(result).toBe("UPPER(users.name)");
		});

		it("should evaluate binary functions", () => {
			const expr: AnyExpression = { $expr: { ADD: [{ $expr: "users.age" }, 5] } };
			const result = parseExpression(expr, testState);
			expect(result).toBe("(users.age + 5)");
		});

		it("should evaluate variable functions", () => {
			const expr: AnyExpression = { $expr: { CONCAT: [{ $expr: "users.name" }, " - ", { $expr: "users.email" }] } };
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

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow("Table 'invalid_table' is not allowed");
		});

		it("should throw error for non-existent field", () => {
			const condition: Condition = {
				"users.invalid_field": { $eq: "value" },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Field 'invalid_field' is not allowed for table 'users'",
			);
		});
	});

	describe("Type validation", () => {
		it("should throw error for wrong value type", () => {
			const condition: Condition = {
				"users.age": { $eq: "not_a_number" },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for '=' comparison on 'age': expected number, got string",
			);
		});

		it("should throw error for string operators on non-string fields", () => {
			const condition: Condition = {
				"users.age": { $like: "25%" },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for LIKE operation on 'age': expected string, got number",
			);
		});

		it("should throw error for numeric operators on non-numeric fields", () => {
			const condition: Condition = {
				"users.name": { $gt: 5 },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Field type mismatch for '>' comparison on 'name': expected string, got number",
			);
		});
	});

	describe("JSON access validation", () => {
		it("should throw error for JSON access on non-JSON fields", () => {
			const condition: Condition = {
				"users.name->profile": { $eq: "value" },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"JSON path access 'profile' is only allowed on JSON fields",
			);
		});
	});

	describe("Expression validation", () => {
		it("should throw error for unknown functions", () => {
			const condition: Condition = {
				"users.name": { $eq: { $expr: { UNKNOWN_FUNC: ["arg"] } } },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow("Unknown function or operator: UNKNOWN_FUNC");
		});

		it("should throw error for wrong argument count", () => {
			const condition: Condition = {
				"users.age": { $eq: { $expr: { ABS: ["arg1", "arg2"] } } },
			};

			expect(() => parseWhereClause(condition, testConfig, "users")).toThrow(
				"Unary operator 'ABS' requires exactly 1 argument, got 2",
			);
		});
	});
});
