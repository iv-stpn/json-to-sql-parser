/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildSelectQuery } from "../../src";
import type { Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment } from "./_helpers";

describe("Integration Tests - Complex Expressions and Type Casting", () => {
	let db: DatabaseHelper;

	const config: Config = {
		variables: {},
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
			{ table: "users", field: "id", toTable: "posts", toField: "user_id", type: "one-to-many" },
			{ table: "users", field: "id", toTable: "orders", toField: "customer_id", type: "one-to-many" },
		],
	};

	beforeAll(async () => {
		await setupTestEnvironment();
		db = new DatabaseHelper();
		await db.connect();
	});

	afterAll(async () => {
		await db.disconnect();
		// await teardownTestEnvironment(); // Keep running for subsequent tests
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("+");
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("LENGTH");
			expect(result.sql).toContain("CONCAT");
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("metadata");
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

			const { sql, params } = buildSelectQuery(query, config);
			const rows = await db.query(sql, params);

			expect(rows).toBeDefined();
			expect(sql).toContain("->");
			expect(params).toContain("engineering");
			expect(params).toContain(50000);
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("CASE WHEN");
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
								conditions: {
									"posts.published": { $eq: true },
								},
							},
						},
					],
				},
			};

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("LEFT JOIN");
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

			const { sql, params } = buildSelectQuery(query, config);
			const rows = await db.query(sql, params);

			expect(rows).toBeDefined();
			expect(sql).toContain("IN");
			expect(sql).toContain("NOT IN");
			expect(params).toContain("active");
			expect(params).toContain(25);
			expect(params).toContain(25);
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

			const { sql, params } = buildSelectQuery(query, config);
			const rows = await db.query(sql, params);

			expect(rows).toBeDefined();
			expect(sql).toContain("IS NOT NULL");
			expect(sql).toContain("OR");
			expect(params).toContain(18);
			expect(params).toContain("premium");
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("created_at");
		});
	});
});
