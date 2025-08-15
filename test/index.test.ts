/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../src/builders/select";
import { parseExpression } from "../src/parsers";

import type { AggregationQuery, AnyExpression, Condition } from "../src/schemas";
import type { Config, ParserState } from "../src/types";
import { ExpressionTypeMap } from "../src/utils/expression-map";
import { extractSelectWhereClause } from "./_helpers";

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
				],
			},
		},
		variables: {
			"auth.uid": "123",
			current_user: "456",
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

	describe("Comparison operators", () => {
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

	describe("Expression support", () => {
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

	describe("JSON field access", () => {
		it("should parse JSON field access", () => {
			const condition: Condition = {
				"users.metadata->profile->name": { $eq: "John" },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.metadata->'profile'->>'name' = $1");
			expect(result.params).toEqual(["John"]);
		});
	});

	describe("EXISTS conditions", () => {
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

describe("Aggregation Parser", () => {
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

describe("Expression Evaluation", () => {
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
				"Field 'invalid_field' is not allowed or does not exist for table 'users'",
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
