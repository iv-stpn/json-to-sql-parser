/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { buildSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment } from "../_helpers";

describe("Integration - Complex Expression Processing and Type Casting", () => {
	let db: DatabaseHelper;

	const config: Config = {
		variables: {
			current_user_id: "550e8400-e29b-41d4-a716-446655440000",
			adminRole: "admin",
			testPattern: "test_%",
			maxResults: 100,
			score_threshold: 85.5,
		},
		dialect: Dialect.POSTGRESQL,
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: false },
					{ name: "age", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "updated_at", type: "datetime", nullable: true },
					{ name: "birth_date", type: "date", nullable: true },
					{ name: "metadata", type: "object", nullable: true },
				],
			},
			posts: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "user_id", type: "uuid", nullable: false },
					{ name: "title", type: "string", nullable: false },
					{ name: "content", type: "string", nullable: true },
					{ name: "published", type: "boolean", nullable: false },
					{ name: "tags", type: "object", nullable: true },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "published_at", type: "datetime", nullable: true },
				],
			},
			orders: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "customer_id", type: "uuid", nullable: false },
					{ name: "amount", type: "number", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "shipped_at", type: "datetime", nullable: true },
					{ name: "delivered_date", type: "date", nullable: true },
				],
			},
		},
		relationships: [
			{ table: "posts", field: "user_id", toTable: "users", toField: "id" },
			{ table: "orders", field: "customer_id", toTable: "users", toField: "id" },
		],
	};

	beforeAll(async () => {
		await setupTestEnvironment();
		db = new DatabaseHelper();
		await db.connect();
	});

	afterAll(async () => {
		await db.disconnect();
	});

	describe("Mathematical Expressions with Type Casting", () => {
		it("should handle complex mathematical operations with proper type casting", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					age: true,
					// Simple calculated field
					age_plus_ten: {
						$func: {
							ADD: [{ $field: "users.age" }, 10],
						},
					},
				},
				condition: {
					"users.active": { $eq: true },
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("+");
		});

		it("should handle string operations with proper type inference", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					// String length
					name_length: {
						$func: {
							LENGTH: [{ $field: "users.name" }],
						},
					},
					// String concatenation
					display_name: {
						$func: {
							CONCAT: [{ $field: "users.name" }, " (", { $field: "users.email" }, ")"],
						},
					},
				},
				condition: {
					"users.name": { $like: "%John%" },
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toBe(
				'SELECT users.id AS "id", users.name AS "name", users.email AS "email", LENGTH(users.name) AS "name_length", (users.name || \' (\' || users.email || \')\') AS "display_name" FROM users WHERE users.name LIKE \'%John%\'',
			);
		});
	});

	describe("JSON Operations and Type Casting", () => {
		it("should handle simple JSON field access", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					metadata: true,
				},
				condition: {
					"users.metadata": { $ne: null },
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("metadata");
		});

		it("should handle JSON path conditions with type inference", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.metadata->department": { $eq: "engineering" },
					},
					{
						"users.metadata->salary": { $gte: 50000 },
					},
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					name: true,
					metadata: true,
				},
				condition,
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(sql).toContain("->");
		});
	});

	describe("Complex Conditional Logic", () => {
		it("should handle nested conditional expressions", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					age: true,
					// Multi-level conditional
					tier: {
						$cond: {
							if: { "users.age": { $gte: 65 } },
							then: "Senior",
							else: {
								$cond: {
									if: { "users.age": { $gte: 30 } },
									then: "Adult",
									else: "Young",
								},
							},
						},
					},
					// Boolean to text conversion
					status_text: {
						$cond: {
							if: { "users.active": { $eq: true } },
							then: "ACTIVE",
							else: "INACTIVE",
						},
					},
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("Multi-table Type Inference", () => {
		it("should handle joins with proper type casting", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					posts: {
						id: true,
						title: true,
						published: true,
					},
				},
				condition: {
					$and: [
						{
							"users.active": { $eq: true },
						},
						{
							$exists: {
								table: "posts",
								condition: {
									"posts.published": { $eq: true },
								},
							},
						},
					],
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("LEFT JOIN");
		});
	});

	describe("Array and Set Operations", () => {
		it("should handle IN clauses with different data types", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.status": {
							$in: ["active", "premium", "vip"],
						},
					},
					{
						"users.age": {
							$in: [25, 30, 35, 40],
						},
					},
					{
						"users.age": {
							$nin: [25, 30, 35],
						},
					},
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					name: true,
					age: true,
					status: true,
				},
				condition,
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(sql).toContain("IN");
			expect(sql).toContain("NOT IN");
		});
	});

	describe("Null Handling and Type Coercion", () => {
		it("should handle null checks and type coercion properly", async () => {
			const condition: Condition = {
				$or: [{ $and: [{ "users.age": { $ne: null } }, { "users.age": { $gt: 18 } }] }, { "users.status": { $eq: "premium" } }],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					name: true,
					age: true,
					status: true,
				},
				condition,
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(sql).toContain("IS NOT NULL");
			expect(sql).toContain("OR");
			expect(sql).toContain("18");
			expect(sql).toContain("premium");
		});
	});

	describe("Date and Time Operations", () => {
		it("should handle date comparisons and extractions", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					created_at: true,
				},
				condition: {
					"users.created_at": {
						$gte: { $date: "2020-01-01" },
					},
				},
			};

			const sql = buildSelectQuery(query, config);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("created_at");
		});
	});

	describe("Complex Aggregation Performance", () => {
		it("should execute aggregations with complex field expressions", async () => {
			const query = parseAggregationQuery(
				{
					table: "users",
					groupBy: ["status", "active"],
					aggregatedFields: {
						total_users: { function: "COUNT", field: "*" },
						avg_age: { function: "AVG", field: "age" },
						min_age: { function: "MIN", field: "age" },
						max_age: { function: "MAX", field: "age" },
						name_lengths: {
							function: "AVG",
							field: { $func: { LENGTH: [{ $field: "name" }] } },
						},
						complex_calc: {
							function: "SUM",
							field: {
								$func: {
									ADD: [{ $func: { MULTIPLY: [{ $field: "age" }, 2] } }, { $func: { LENGTH: [{ $field: "name" }] } }],
								},
							},
						},
					},
				},
				config,
			);

			const sql = compileAggregationQuery(query);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);

			// Verify aggregation structure
			expect(sql).toContain("GROUP BY");
			expect(sql).toContain("COUNT(*)");
			expect(sql).toContain("AVG(");
			expect(sql).toContain("MIN(");
			expect(sql).toContain("MAX(");
			expect(sql).toContain("LENGTH");
			expect(sql).toContain("+"); // ADD becomes +
			expect(sql).toContain("*"); // MULTIPLY becomes *
		});
	});
});
