/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { Condition, SelectQuery } from "../../src/schemas";
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
				],
			},
			orders: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "amount", type: "number", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "customer_id", type: "uuid", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
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

describe("CRUD - SELECT Pagination Operations", () => {
	describe("Basic Pagination", () => {
		it("should parse and compile LIMIT only", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 10 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBeUndefined();
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 10');
		});

		it("should parse and compile OFFSET only", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { offset: 20 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBeUndefined();
			expect(parsed.offset).toBe(20);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users OFFSET 20');
		});

		it("should parse and compile both LIMIT and OFFSET", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 10, offset: 20 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBe(20);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 10 OFFSET 20');
		});

		it("should handle pagination with zero values", () => {
			const queryWithZeroOffset: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 5, offset: 0 },
			};

			const parsed = parseSelectQuery(queryWithZeroOffset, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(5);
			expect(parsed.offset).toBe(0);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 5 OFFSET 0');
		});

		it("should handle large pagination values", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 1000, offset: 50000 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(1000);
			expect(parsed.offset).toBe(50000);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 1000 OFFSET 50000');
		});
	});

	describe("Pagination with Conditions", () => {
		it("should combine pagination with WHERE conditions", () => {
			const condition: Condition = {
				"users.active": { $eq: true },
				"users.age": { $gte: 18 },
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, age: true },
				condition,
				pagination: { limit: 15, offset: 30 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(15);
			expect(parsed.offset).toBe(30);
			expect(parsed.where).toContain("users.active = TRUE");
			expect(parsed.where).toContain("users.age >= 18");
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.age AS "age" FROM users WHERE (users.active = TRUE AND users.age >= 18) LIMIT 15 OFFSET 30',
			);
		});

		it("should combine pagination with complex OR conditions", () => {
			const condition: Condition = {
				$or: [{ "users.status": { $eq: "premium" } }, { "users.score": { $gte: 100 } }],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, status: true },
				condition,
				pagination: { limit: 25 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(25);
			expect(parsed.where).toContain("users.status = 'premium'");
			expect(parsed.where).toContain("users.score >= 100");
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.status AS "status" FROM users WHERE (users.status = \'premium\' OR users.score >= 100) LIMIT 25',
			);
		});

		it("should combine pagination with nested AND/OR conditions", () => {
			const condition: Condition = {
				$and: [
					{
						$or: [{ "users.status": { $eq: "active" } }, { "users.status": { $eq: "premium" } }],
					},
					{ "users.active": { $eq: true } },
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				condition,
				pagination: { limit: 50, offset: 100 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(50);
			expect(parsed.offset).toBe(100);
			expect(sql).toContain("LIMIT 50 OFFSET 100");
		});
	});

	describe("Pagination with Joins", () => {
		it("should combine pagination with single table joins", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
					},
				},
				pagination: { limit: 10, offset: 5 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBe(5);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", posts.id AS "posts.id", posts.title AS "posts.title" FROM users LEFT JOIN posts ON users.id = posts.user_id LIMIT 10 OFFSET 5',
			);
		});

		it("should combine pagination with multiple table joins", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
					},
					orders: {
						id: true,
						amount: true,
					},
				},
				pagination: { limit: 20 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.joins).toContain("LEFT JOIN orders ON users.id = orders.customer_id");
			expect(parsed.limit).toBe(20);
			expect(sql).toContain("LIMIT 20");
		});

		it("should combine pagination with joins and conditions", () => {
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

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
					},
				},
				condition,
				pagination: { limit: 5, offset: 10 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.where).toContain("EXISTS");
			expect(parsed.limit).toBe(5);
			expect(parsed.offset).toBe(10);
			expect(sql).toContain("LIMIT 5 OFFSET 10");
		});
	});

	describe("Pagination with Expressions", () => {
		it("should combine pagination with field expressions", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					display_name: {
						$func: {
							CONCAT: [{ $field: "users.name" }, " (", { $field: "users.status" }, ")"],
						},
					},
				},
				pagination: { limit: 12, offset: 24 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.select).toContain("(users.name || ' (' || users.status || ')') AS \"display_name\"");
			expect(parsed.limit).toBe(12);
			expect(parsed.offset).toBe(24);
			expect(sql).toContain("LIMIT 12 OFFSET 24");
		});

		it("should combine pagination with conditional expressions", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					user_type: {
						$cond: {
							if: { "users.status": { $eq: "premium" } },
							then: "Premium User",
							else: "Regular User",
						},
					},
				},
				pagination: { limit: 8 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.select.some((s) => s.includes("CASE WHEN"))).toBe(true);
			expect(parsed.limit).toBe(8);
			expect(sql).toContain("LIMIT 8");
		});

		it("should combine pagination with variable references", () => {
			const condition: Condition = {
				"users.id": { $eq: { $var: "current_user_id" } },
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					score: true,
				},
				condition,
				pagination: { limit: 1 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.where).toContain("'123'"); // Variable value
			expect(parsed.limit).toBe(1);
			expect(sql).toContain("LIMIT 1");
		});
	});

	describe("Common Pagination Patterns", () => {
		it("should handle first page (offset 0)", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 20, offset: 0 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(20);
			expect(parsed.offset).toBe(0);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 20 OFFSET 0');
		});

		it("should handle page-based calculations (page 1 = offset 0)", () => {
			const pageSize = 25;
			const page = 1; // First page
			const offset = (page - 1) * pageSize; // Should be 0

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: pageSize, offset },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(25);
			expect(parsed.offset).toBe(0);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 25 OFFSET 0');
		});

		it("should handle page-based calculations (page 3 = offset 50)", () => {
			const pageSize = 25;
			const page = 3; // Third page
			const offset = (page - 1) * pageSize; // Should be 50

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: pageSize, offset },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(25);
			expect(parsed.offset).toBe(50);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 25 OFFSET 50');
		});

		it("should handle cursor-based pagination pattern (limit only)", () => {
			// Common pattern: fetch next N records after a certain point
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				condition: {
					"users.created_at": { $gt: { $timestamp: "2024-01-01 00:00:00" } },
				},
				pagination: { limit: 10 }, // No offset needed for cursor-based
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBeUndefined();
			expect(parsed.where).toContain("created_at");
			expect(sql).toContain("LIMIT 10");
			expect(sql).not.toContain("OFFSET");
		});

		it("should handle infinite scroll pattern (increasing offsets)", () => {
			const scrollData = [
				{ limit: 20, offset: 0 }, // Initial load
				{ limit: 20, offset: 20 }, // First scroll
				{ limit: 20, offset: 40 }, // Second scroll
				{ limit: 20, offset: 60 }, // Third scroll
			];

			for (const [index, pagination] of scrollData.entries()) {
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true },
					pagination,
				};

				const parsed = parseSelectQuery(query, testConfig);
				const sql = compileSelectQuery(parsed, testConfig.dialect);

				expect(parsed.limit).toBe(20);
				expect(parsed.offset).toBe(index * 20);
				expect(sql).toContain(`LIMIT 20 OFFSET ${index * 20}`);
			}
		});
	});

	describe("Pagination Edge Cases", () => {
		it("should handle very large limit values", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 999999 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(999999);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 999999');
		});

		it("should handle very large offset values", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { offset: 1000000 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.offset).toBe(1000000);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users OFFSET 1000000');
		});

		it("should handle single record pagination (limit 1)", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 1, offset: 5 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(1);
			expect(parsed.offset).toBe(5);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 1 OFFSET 5');
		});

		it("should handle pagination without any conditions", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: 100 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(100);
			expect(parsed.where).toBeUndefined();
			expect(sql).toBe('SELECT users.id AS "id" FROM users LIMIT 100');
		});

		it("should maintain SQL clause order: SELECT FROM JOIN WHERE LIMIT OFFSET", () => {
			const condition: Condition = {
				"users.active": { $eq: true },
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						title: true,
					},
				},
				condition,
				pagination: { limit: 15, offset: 30 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			// Verify SQL clause order
			const selectIndex = sql.indexOf("SELECT");
			const fromIndex = sql.indexOf("FROM");
			const joinIndex = sql.indexOf("LEFT JOIN");
			const whereIndex = sql.indexOf("WHERE");
			const limitIndex = sql.indexOf("LIMIT");
			const offsetIndex = sql.indexOf("OFFSET");

			expect(selectIndex).toBe(0);
			expect(fromIndex).toBeGreaterThan(selectIndex);
			expect(joinIndex).toBeGreaterThan(fromIndex);
			expect(whereIndex).toBeGreaterThan(joinIndex);
			expect(limitIndex).toBeGreaterThan(whereIndex);
			expect(offsetIndex).toBeGreaterThan(limitIndex);
		});
	});

	describe("Performance Considerations", () => {
		it("should generate efficient queries for small page sizes", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 5 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			// Small limits should be straightforward
			expect(parsed.limit).toBe(5);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 5');
		});

		it("should handle medium page sizes efficiently", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: { limit: 100, offset: 200 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(100);
			expect(parsed.offset).toBe(200);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users LIMIT 100 OFFSET 200');
		});

		it("should suggest index-friendly patterns for deep pagination", () => {
			// Deep pagination often requires indexed columns for WHERE clauses
			const condition: Condition = {
				"users.created_at": { $gte: { $timestamp: "2024-01-01 00:00:00" } },
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true, created_at: true },
				condition,
				pagination: { limit: 50, offset: 10000 }, // Deep offset
			};

			const parsed = parseSelectQuery(query, testConfig);

			// Should include indexable conditions
			expect(parsed.where).toContain("created_at");
			expect(parsed.limit).toBe(50);
			expect(parsed.offset).toBe(10000);
		});
	});

	describe("Pagination Schema Validation", () => {
		it("should handle queries without pagination (undefined)", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				// pagination is optional and undefined
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBeUndefined();
			expect(parsed.offset).toBeUndefined();
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users');
		});

		it("should handle empty pagination object", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true, name: true },
				pagination: {}, // Empty pagination object
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBeUndefined();
			expect(parsed.offset).toBeUndefined();
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users');
		});
	});
});

describe("Pagination Error Handling", () => {
	describe("Invalid Pagination Values", () => {
		it("should handle negative limit values (type validation at schema level)", () => {
			// Note: Type validation would be handled at the schema validation level
			// The parseSelectQuery function doesn't validate input types
			const query = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: -1 }, // Invalid but allowed by parser
			} as SelectQuery;

			// Function should handle it without crashing, but behavior depends on DB
			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.limit).toBe(-1);
		});

		it("should handle negative offset values (type validation at schema level)", () => {
			const query = {
				rootTable: "users",
				selection: { id: true },
				pagination: { offset: -1 },
			} as SelectQuery;

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.offset).toBe(-1);
		});

		it("should handle zero limit values (type validation at schema level)", () => {
			const query = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: 0 },
			} as SelectQuery;

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.limit).toBe(0);
		});

		it("should accept zero offset values", () => {
			// Zero offset is valid (first page)
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				pagination: { offset: 0 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.offset).toBe(0);
		});

		it("should handle non-integer limit values (type validation at schema level)", () => {
			const query = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: 10.5 },
			} as SelectQuery;

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.limit).toBe(10.5);
		});

		it("should handle non-integer offset values (type validation at schema level)", () => {
			const query = {
				rootTable: "users",
				selection: { id: true },
				pagination: { offset: 20.7 },
			} as SelectQuery;

			const parsed = parseSelectQuery(query, testConfig);
			expect(parsed.offset).toBe(20.7);
		});
	});

	describe("Boundary Value Testing", () => {
		it("should handle maximum safe integer values", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: Number.MAX_SAFE_INTEGER, offset: Number.MAX_SAFE_INTEGER },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(Number.MAX_SAFE_INTEGER);
			expect(parsed.offset).toBe(Number.MAX_SAFE_INTEGER);
			expect(sql).toContain(`LIMIT ${Number.MAX_SAFE_INTEGER}`);
			expect(sql).toContain(`OFFSET ${Number.MAX_SAFE_INTEGER}`);
		});

		it("should handle minimum valid values", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: { id: true },
				pagination: { limit: 1, offset: 0 },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(1);
			expect(parsed.offset).toBe(0);
			expect(sql).toContain("LIMIT 1");
			expect(sql).toContain("OFFSET 0");
		});
	});
});

describe("Pagination Integration Patterns", () => {
	describe("Real-world Pagination Scenarios", () => {
		it("should handle user list pagination with search", () => {
			const condition: Condition = {
				$or: [{ "users.name": { $ilike: "%john%" } }, { "users.email": { $ilike: "%john%" } }],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					status: true,
				},
				condition,
				pagination: { limit: 20, offset: 40 }, // Page 3 with 20 items per page
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.where).toContain("ILIKE");
			expect(parsed.limit).toBe(20);
			expect(parsed.offset).toBe(40);
			expect(sql).toContain("LIMIT 20 OFFSET 40");
		});

		it("should handle admin dashboard pagination with filters", () => {
			const condition: Condition = {
				$and: [
					{ "users.active": { $eq: true } },
					{ "users.created_at": { $gte: { $timestamp: "2024-01-01 00:00:00" } } },
					{
						$or: [{ "users.status": { $eq: "premium" } }, { "users.status": { $eq: "active" } }],
					},
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					status: true,
					created_at: true,
				},
				condition,
				pagination: { limit: 50, offset: 100 },
			};

			const parsed = parseSelectQuery(query, testConfig);

			expect(parsed.where).toContain("active = TRUE");
			expect(parsed.where).toContain("created_at");
			expect(parsed.limit).toBe(50);
			expect(parsed.offset).toBe(100);
		});

		it("should handle blog post pagination with author information", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
						published: true,
						created_at: true,
					},
				},
				condition: {
					$exists: {
						table: "posts",
						condition: {
							$and: [{ "posts.user_id": { $eq: { $field: "users.id" } } }, { "posts.published": { $eq: true } }],
						},
					},
				},
				pagination: { limit: 10, offset: 20 },
			};

			const parsed = parseSelectQuery(query, testConfig);

			expect(parsed.joins).toContain("LEFT JOIN posts ON users.id = posts.user_id");
			expect(parsed.where).toContain("EXISTS");
			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBe(20);
		});

		it("should handle e-commerce order history pagination", () => {
			const condition: Condition = {
				$and: [
					{
						$exists: {
							table: "orders",
							condition: {
								"orders.customer_id": { $eq: { $field: "users.id" } },
							},
						},
					},
					{ "users.active": { $eq: true } },
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					orders: {
						id: true,
						amount: true,
						status: true,
						created_at: true,
					},
				},
				condition,
				pagination: { limit: 25, offset: 75 },
			};

			const parsed = parseSelectQuery(query, testConfig);

			expect(parsed.joins).toContain("LEFT JOIN orders ON users.id = orders.customer_id");
			expect(parsed.where).toContain("EXISTS");
			expect(parsed.limit).toBe(25);
			expect(parsed.offset).toBe(75);
		});
	});

	describe("Mobile App Pagination Patterns", () => {
		it("should handle mobile feed pagination (infinite scroll)", () => {
			// Mobile apps often use smaller page sizes for better UX
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					status: true,
				},
				condition: {
					"users.active": { $eq: true },
				},
				pagination: { limit: 15, offset: 30 }, // Small pages for mobile
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(15);
			expect(parsed.offset).toBe(30);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.status AS "status" FROM users WHERE users.active = TRUE LIMIT 15 OFFSET 30',
			);
		});

		it("should handle initial mobile app load (first page)", () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					status: true,
				},
				pagination: { limit: 20, offset: 0 }, // First load
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(20);
			expect(parsed.offset).toBe(0);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name", users.status AS "status" FROM users LIMIT 20 OFFSET 0');
		});
	});

	describe("API Response Patterns", () => {
		it("should generate pagination queries for REST API endpoints", () => {
			// Typical REST API pagination: GET /users?page=3&per_page=25
			const page = 3;
			const perPage = 25;
			const offset = (page - 1) * perPage; // 50

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					status: true,
				},
				pagination: { limit: perPage, offset },
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(25);
			expect(parsed.offset).toBe(50);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.email AS "email", users.status AS "status" FROM users LIMIT 25 OFFSET 50',
			);
		});

		it("should generate pagination for GraphQL resolvers", () => {
			// GraphQL often uses cursor-based pagination, but offset-based is also common
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
				},
				condition: {
					"users.id": { $gt: { $var: "current_user_id" } }, // After cursor simulation
				},
				pagination: { limit: 10 }, // No offset in cursor-based
			};

			const parsed = parseSelectQuery(query, testConfig);
			const sql = compileSelectQuery(parsed, testConfig.dialect);

			expect(parsed.limit).toBe(10);
			expect(parsed.offset).toBeUndefined();
			expect(parsed.where).toContain("'123'"); // Variable replacement
			expect(sql).toContain("LIMIT 10");
			expect(sql).not.toContain("OFFSET");
		});
	});
});
