/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { Condition, OrderBy, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";

// Test configuration
let testConfig: Config;

beforeEach(() => {
	testConfig = {
		dialect: Dialect.POSTGRESQL,
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: true },
					{ name: "age", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "score", type: "number", nullable: true },
					{ name: "balance", type: "number", nullable: true },
					{ name: "description", type: "string", nullable: true },
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
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "rating", type: "number", nullable: true },
				],
			},
			orders: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "amount", type: "number", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "customer_id", type: "uuid", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "shipped_at", type: "datetime", nullable: true },
				],
			},
		},
		variables: {
			current_user_id: "123",
			max_page_size: 100,
			default_limit: 20,
		},
		relationships: [
			{
				table: "posts",
				field: "user_id",
				toTable: "users",
				toField: "id",
			},
			{
				table: "orders",
				field: "customer_id",
				toTable: "users",
				toField: "id",
			},
		],
	};
});

describe("CRUD - SELECT ORDER BY Operations", () => {
	describe("Basic ORDER BY", () => {
		it("should parse single field ORDER BY ASC", () => {
			const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.name ASC"]);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users ORDER BY users.name ASC');
		});

		it("should parse single field ORDER BY DESC", () => {
			const orderBy: OrderBy = [{ field: "users.created_at", direction: "DESC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.created_at DESC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.created_at AS "created_at" FROM users ORDER BY users.created_at DESC',
			);
		});

		it("should default to ASC when direction is not specified", () => {
			const orderBy: OrderBy = [{ field: "users.name" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.name ASC"]);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users ORDER BY users.name ASC');
		});

		it("should handle case-insensitive directions", () => {
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "asc" },
				{ field: "users.age", direction: "desc" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, age: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.name ASC", "users.age DESC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.age AS "age" FROM users ORDER BY users.name ASC, users.age DESC',
			);
		});
	});

	describe("Multiple fields ORDER BY", () => {
		it("should parse multiple fields with mixed directions", () => {
			const orderBy: OrderBy = [
				{ field: "users.status", direction: "ASC" },
				{ field: "users.created_at", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, status: true, created_at: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.status ASC", "users.created_at DESC", "users.name ASC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.status AS "status", users.created_at AS "created_at" FROM users ORDER BY users.status ASC, users.created_at DESC, users.name ASC',
			);
		});

		it("should handle ORDER BY with numeric fields", () => {
			const orderBy: OrderBy = [
				{ field: "users.age", direction: "DESC" },
				{ field: "users.score", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, age: true, score: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.age DESC", "users.score ASC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.age AS "age", users.score AS "score" FROM users ORDER BY users.age DESC, users.score ASC',
			);
		});

		it("should handle ORDER BY with boolean fields", () => {
			const orderBy: OrderBy = [
				{ field: "users.active", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, active: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.active DESC", "users.name ASC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.active AS "active" FROM users ORDER BY users.active DESC, users.name ASC',
			);
		});
	});

	describe("ORDER BY with Conditions", () => {
		it("should combine ORDER BY with WHERE conditions", () => {
			const condition: Condition = {
				"users.active": { $eq: true },
				"users.age": { $gte: 18 },
			};
			const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, age: true },
				condition,
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.name ASC"]);
			expect(parsed.where).toContain("users.active = TRUE");
			expect(parsed.where).toContain("users.age >= 18");
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.age AS "age" FROM users WHERE (users.active = TRUE AND users.age >= 18) ORDER BY users.name ASC',
			);
		});

		it("should combine ORDER BY with complex OR conditions", () => {
			const condition: Condition = {
				$or: [{ "users.status": { $eq: "premium" } }, { "users.score": { $gte: 100 } }],
			};
			const orderBy: OrderBy = [
				{ field: "users.score", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, status: true, score: true },
				condition,
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.score DESC", "users.name ASC"]);
			expect(parsed.where).toContain("users.status = 'premium'");
			expect(parsed.where).toContain("users.score >= 100");
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.status AS "status", users.score AS "score" FROM users WHERE (users.status = \'premium\' OR users.score >= 100) ORDER BY users.score DESC, users.name ASC',
			);
		});

		it("should combine ORDER BY with nested AND/OR conditions", () => {
			const condition: Condition = {
				$and: [
					{
						$or: [{ "users.status": { $eq: "active" } }, { "users.status": { $eq: "premium" } }],
					},
					{ "users.active": { $eq: true } },
				],
			};
			const orderBy: OrderBy = [
				{ field: "users.status", direction: "ASC" },
				{ field: "users.created_at", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, status: true, created_at: true },
				condition,
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.status ASC", "users.created_at DESC"]);
			expect(sql).toContain("ORDER BY users.status ASC, users.created_at DESC");
		});
	});

	describe("ORDER BY with Joins", () => {
		it("should combine ORDER BY with single table joins", () => {
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "ASC" },
				{ field: "posts.created_at", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
						created_at: true,
					},
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.orderBy).toEqual(["users.name ASC", "posts.created_at DESC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", posts.id AS "posts.id", posts.title AS "posts.title", posts.created_at AS "posts.created_at" FROM users LEFT JOIN posts ON users.id = posts.user_id ORDER BY users.name ASC, posts.created_at DESC',
			);
		});

		it("should combine ORDER BY with multiple table joins", () => {
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "ASC" },
				{ field: "posts.rating", direction: "DESC" },
				{ field: "orders.amount", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
						rating: true,
					},
					orders: {
						id: true,
						amount: true,
					},
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.joins).toContain("LEFT JOIN orders ON users.id = orders.customer_id");
			expect(parsed.orderBy).toEqual(["users.name ASC", "posts.rating DESC", "orders.amount DESC"]);
			expect(sql).toContain("ORDER BY users.name ASC, posts.rating DESC, orders.amount DESC");
		});

		it("should combine ORDER BY with joins and conditions", () => {
			const condition: Condition = {
				$and: [
					{ "users.active": { $eq: true } },
					{
						$exists: {
							table: "posts",
							condition: {
								$and: [{ "posts.user_id": { $eq: { $field: "users.id" } } }, { "posts.published": { $eq: true } }],
							},
						},
					},
				],
			};
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "ASC" },
				{ field: "posts.created_at", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
						created_at: true,
					},
				},
				condition,
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.where).toContain("EXISTS");
			expect(parsed.orderBy).toEqual(["users.name ASC", "posts.created_at DESC"]);
			expect(sql).toContain("ORDER BY users.name ASC, posts.created_at DESC");
		});
	});

	describe("ORDER BY with Pagination", () => {
		it("should combine ORDER BY with LIMIT", () => {
			const orderBy: OrderBy = [{ field: "users.created_at", direction: "DESC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				orderBy,
				pagination: { limit: 10 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.created_at DESC"]);
			expect(parsed.limit).toBe(10);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.created_at AS "created_at" FROM users ORDER BY users.created_at DESC LIMIT 10',
			);
		});

		it("should combine ORDER BY with LIMIT and OFFSET", () => {
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "ASC" },
				{ field: "users.created_at", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				orderBy,
				pagination: { limit: 20, offset: 40 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.name ASC", "users.created_at DESC"]);
			expect(parsed.limit).toBe(20);
			expect(parsed.offset).toBe(40);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.created_at AS "created_at" FROM users ORDER BY users.name ASC, users.created_at DESC LIMIT 20 OFFSET 40',
			);
		});

		it("should combine ORDER BY with OFFSET only", () => {
			const orderBy: OrderBy = [{ field: "users.score", direction: "DESC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, score: true },
				orderBy,
				pagination: { offset: 25 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.score DESC"]);
			expect(parsed.offset).toBe(25);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.score AS "score" FROM users ORDER BY users.score DESC OFFSET 25',
			);
		});
	});

	describe("Complete Query Integration", () => {
		it("should handle complex query with all clauses: SELECT, FROM, JOIN, WHERE, ORDER BY, LIMIT, OFFSET", () => {
			const condition: Condition = {
				$and: [
					{ "users.active": { $eq: true } },
					{
						$or: [{ "users.status": { $eq: "premium" } }, { "users.score": { $gte: 50 } }],
					},
				],
			};
			const orderBy: OrderBy = [
				{ field: "users.score", direction: "DESC" },
				{ field: "posts.created_at", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					status: true,
					score: true,
					posts: {
						id: true,
						title: true,
						created_at: true,
					},
				},
				condition,
				orderBy,
				pagination: { limit: 15, offset: 30 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			// Verify all parts are present
			expect(sql).toContain("SELECT");
			expect(sql).toContain("FROM users");
			expect(sql).toContain("LEFT JOIN posts");
			expect(sql).toContain("WHERE");
			expect(sql).toContain("ORDER BY users.score DESC, posts.created_at DESC, users.name ASC");
			expect(sql).toContain("LIMIT 15");
			expect(sql).toContain("OFFSET 30");

			// Verify correct clause order
			const selectIndex = sql.indexOf("SELECT");
			const fromIndex = sql.indexOf("FROM");
			const joinIndex = sql.indexOf("LEFT JOIN");
			const whereIndex = sql.indexOf("WHERE");
			const orderByIndex = sql.indexOf("ORDER BY");
			const limitIndex = sql.indexOf("LIMIT");
			const offsetIndex = sql.indexOf("OFFSET");

			expect(selectIndex).toBe(0);
			expect(fromIndex).toBeGreaterThan(selectIndex);
			expect(joinIndex).toBeGreaterThan(fromIndex);
			expect(whereIndex).toBeGreaterThan(joinIndex);
			expect(orderByIndex).toBeGreaterThan(whereIndex);
			expect(limitIndex).toBeGreaterThan(orderByIndex);
			expect(offsetIndex).toBeGreaterThan(limitIndex);
		});

		it("should handle ORDER BY with variables and expressions", () => {
			const condition: Condition = {
				"users.id": { $eq: { $var: "current_user_id" } },
			};
			const orderBy: OrderBy = [
				{ field: "users.score", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					score: true,
				},
				condition,
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.where).toContain("'123'"); // Variable value
			expect(parsed.orderBy).toEqual(["users.score DESC", "users.name ASC"]);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.score AS "score" FROM users WHERE (users.id)::TEXT = \'123\' ORDER BY users.score DESC, users.name ASC',
			);
		});
	});

	describe("JSON Field ORDER BY", () => {
		it("should handle ORDER BY on JSON fields", () => {
			const orderBy: OrderBy = [
				{ field: "users.metadata->profile->name", direction: "ASC" },
				{ field: "users.created_at", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					"metadata->profile->name": true,
					created_at: true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.metadata->'profile'->>'name' ASC", "users.created_at DESC"]);
			expect(sql).toContain("ORDER BY users.metadata->'profile'->>'name' ASC, users.created_at DESC");
		});

		it("should handle ORDER BY on nested JSON fields", () => {
			const orderBy: OrderBy = [
				{ field: "users.metadata->settings->theme", direction: "ASC" },
				{ field: "users.metadata->profile->age", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					"metadata->settings->theme": true,
					"metadata->profile->age": true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.metadata->'settings'->>'theme' ASC", "users.metadata->'profile'->>'age' DESC"]);
			expect(sql).toContain("ORDER BY users.metadata->'settings'->>'theme' ASC, users.metadata->'profile'->>'age' DESC");
		});
	});

	describe("Empty and undefined ORDER BY", () => {
		it("should handle undefined ORDER BY", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				// orderBy is undefined
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual([]);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users');
		});

		it("should handle empty ORDER BY array", () => {
			const orderBy: OrderBy = [];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual([]);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users');
		});
	});

	describe("Common ORDER BY Patterns", () => {
		it("should handle chronological ordering (newest first)", () => {
			const orderBy: OrderBy = [{ field: "users.created_at", direction: "DESC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.created_at AS "created_at" FROM users ORDER BY users.created_at DESC',
			);
		});

		it("should handle alphabetical ordering", () => {
			const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users ORDER BY users.name ASC');
		});

		it("should handle priority ordering (status then score)", () => {
			const orderBy: OrderBy = [
				{ field: "users.status", direction: "ASC" },
				{ field: "users.score", direction: "DESC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, status: true, score: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.status AS "status", users.score AS "score" FROM users ORDER BY users.status ASC, users.score DESC',
			);
		});

		it("should handle leaderboard ordering (score desc, then name asc for ties)", () => {
			const orderBy: OrderBy = [
				{ field: "users.score", direction: "DESC" },
				{ field: "users.name", direction: "ASC" },
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, score: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.score AS "score" FROM users ORDER BY users.score DESC, users.name ASC',
			);
		});
	});
});

describe("ORDER BY Error Handling", () => {
	describe("Invalid Field References", () => {
		it("should throw error for non-existent table in ORDER BY", () => {
			const orderBy: OrderBy = [{ field: "invalid_table.field", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				orderBy,
			};

			expect(() => parseSelectQuery(query, testConfig)).toThrow("Table 'invalid_table' is not allowed");
		});

		it("should throw error for non-existent field in ORDER BY", () => {
			const orderBy: OrderBy = [{ field: "users.invalid_field", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				orderBy,
			};

			expect(() => parseSelectQuery(query, testConfig)).toThrow(
				"Field 'invalid_field' is not allowed or does not exist in 'users'",
			);
		});

		it("should throw error for malformed field reference", () => {
			const orderBy: OrderBy = [{ field: "invalid.field.format", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				orderBy,
			};

			expect(() => parseSelectQuery(query, testConfig)).toThrow();
		});

		it("should throw error for empty field name", () => {
			const orderBy: OrderBy = [{ field: "", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				orderBy,
			};

			expect(() => parseSelectQuery(query, testConfig)).toThrow();
		});
	});

	describe("Field Type Considerations", () => {
		it("should handle ORDER BY on all supported field types", () => {
			const orderBy: OrderBy = [
				{ field: "users.id", direction: "ASC" }, // uuid
				{ field: "users.name", direction: "ASC" }, // string
				{ field: "users.age", direction: "DESC" }, // number
				{ field: "users.active", direction: "DESC" }, // boolean
				{ field: "users.created_at", direction: "DESC" }, // datetime
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					age: true,
					active: true,
					created_at: true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.orderBy).toEqual([
				"users.id ASC",
				"users.name ASC",
				"users.age DESC",
				"users.active DESC",
				"users.created_at DESC",
			]);
		});

		it("should handle ORDER BY on nullable fields", () => {
			const orderBy: OrderBy = [
				{ field: "users.email", direction: "ASC" }, // nullable string
				{ field: "users.age", direction: "DESC" }, // nullable number
				{ field: "users.description", direction: "ASC" }, // nullable string
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					email: true,
					age: true,
					description: true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.orderBy).toEqual(["users.email ASC", "users.age DESC", "users.description ASC"]);
		});

		it("should handle ORDER BY on JSON object fields", () => {
			const orderBy: OrderBy = [{ field: "users.metadata->key", direction: "ASC" }];
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					"metadata->key": true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.orderBy).toEqual(["users.metadata->>'key' ASC"]);
		});
	});
});

describe("ORDER BY Edge Cases", () => {
	describe("Large ORDER BY clauses", () => {
		it("should handle many ORDER BY fields", () => {
			const manyFields = [
				"users.name",
				"users.email",
				"users.age",
				"users.status",
				"users.score",
				"users.balance",
				"users.created_at",
			];
			const orderBy: OrderBy = manyFields.map((field, index) => ({
				field,
				direction: index % 2 === 0 ? ("ASC" as const) : ("DESC" as const),
			}));
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					age: true,
					status: true,
					score: true,
					balance: true,
					created_at: true,
				},
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.orderBy.length).toBe(7);
		});

		it("should handle duplicate field references", () => {
			const orderBy: OrderBy = [
				{ field: "users.name", direction: "ASC" },
				{ field: "users.name", direction: "DESC" }, // Duplicate but different direction
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				orderBy,
			};

			const parsed = parseSelectQuery(query, testConfig);
			// Should include both, even though they're the same field
			expect(parsed.orderBy).toEqual(["users.name ASC", "users.name DESC"]);
		});
	});

	describe("Performance Considerations", () => {
		it("should handle ORDER BY with deep pagination efficiently", () => {
			const orderBy: OrderBy = [
				{ field: "users.created_at", direction: "DESC" },
				{ field: "users.id", direction: "ASC" }, // Secondary sort for deterministic results
			];
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				orderBy,
				pagination: { limit: 20, offset: 10000 }, // Deep pagination
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.orderBy).toEqual(["users.created_at DESC", "users.id ASC"]);
			expect(sql).toContain("ORDER BY users.created_at DESC, users.id ASC");
			expect(sql).toContain("LIMIT 20 OFFSET 10000");
		});
	});
});
