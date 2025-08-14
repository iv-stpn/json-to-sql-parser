/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { generateSelectQuery } from "../../src";

import type { Condition, Selection } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment } from "./_helpers";
import { extractSelectWhereClause } from "../_helpers";

type SelectQuery = { rootTable: string; selection: Selection; condition?: Condition };

describe("Integration Tests - Edge Cases and Comprehensive Type Inference", () => {
	let db: DatabaseHelper;

	const config: Config = {
		variables: {},
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: false },
					{ name: "age", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "balance", type: "number", nullable: true },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "updated_at", type: "datetime", nullable: true },
					{ name: "status", type: "string", nullable: false },
					{ name: "metadata", type: "object", nullable: true },
					{ name: "preferences", type: "object", nullable: true },
					{ name: "profile_id", type: "uuid", nullable: true },
				],
			},
			posts: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "user_id", type: "number", nullable: false },
					{ name: "title", type: "string", nullable: false },
					{ name: "content", type: "string", nullable: true },
					{ name: "published", type: "boolean", nullable: false },
					{ name: "views", type: "number", nullable: false },
					{ name: "rating", type: "number", nullable: true },
					{ name: "tags", type: "object", nullable: true },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "updated_at", type: "datetime", nullable: true },
				],
			},
			orders: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "user_id", type: "number", nullable: false },
					{ name: "total", type: "number", nullable: false },
					{ name: "tax_amount", type: "number", nullable: true },
					{ name: "status", type: "string", nullable: false },
					{ name: "items", type: "object", nullable: true },
					{ name: "shipping_address", type: "object", nullable: true },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "order_uuid", type: "uuid", nullable: true },
				],
			},
		},
		relationships: [
			{ table: "users", field: "id", toTable: "posts", toField: "user_id", type: "one-to-many" },
			{ table: "users", field: "id", toTable: "orders", toField: "user_id", type: "one-to-many" },
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

	describe("UUID and Special Data Type Handling", () => {
		it("should handle UUID fields with proper type inference", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					profile_id: true,
					// UUID validation
					is_valid_uuid: {
						$cond: {
							if: { "users.profile_id": { $ne: null } },
							then: true,
							else: false,
						},
					},
				},
				condition: {
					"users.profile_id": { $ne: null },
				},
			};

			const result = generateSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("CASE WHEN");
		});

		it("should handle UUID comparisons and operations", async () => {
			const condition: Condition = {
				$and: [{ "users.profile_id": { $ne: null } }, { "orders.order_uuid": { $ne: null } }],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `SELECT COUNT(*) as count FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE ${result.sql}`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("IS NOT NULL");
		});
	});

	describe("Timestamp and Date Operations", () => {
		it("should handle complex date operations with type casting", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					created_at: true,
					updated_at: true,
					// Date formatting
					creation_date: {
						$expr: {
							DATE_FORMAT: [{ $expr: "users.created_at" }, "%Y-%m-%d"],
						},
					},
					// Date difference
					days_since_update: {
						$expr: {
							DATEDIFF: [{ $expr: "users.updated_at" }, { $expr: "users.created_at" }],
						},
					},
					// Extract parts
					creation_year: {
						$expr: {
							EXTRACT: ["year", { $expr: "users.created_at" }],
						},
					},
					creation_month: {
						$expr: {
							EXTRACT: ["month", { $expr: "users.created_at" }],
						},
					},
				},
				condition: {
					"users.created_at": {
						$gte: { $date: "2020-01-01" },
					},
				},
			};

			const result = generateSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("EXTRACT");
			expect(result.sql).toContain("year");
			expect(result.sql).toContain("month");
		});

		it("should handle timestamp comparisons and ranges", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.created_at": {
							$gte: { $timestamp: "2020-01-01T00:00:00Z" },
						},
					},
					{
						"users.updated_at": {
							$lte: { $timestamp: "2025-12-31T23:59:59Z" },
						},
					},
					{
						"posts.created_at": {
							$gte: { $timestamp: "2023-01-01T00:00:00Z" },
						},
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `SELECT COUNT(*) as count FROM users LEFT JOIN posts ON users.id = posts.user_id WHERE ${result.sql}`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.params.length).toBeGreaterThan(0);
		});
	});

	describe("Complex JSON Operations and Edge Cases", () => {
		it("should handle nested JSON path operations", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					metadata: true,
					// Deep nested JSON extraction
					user_department: {
						$expr: {
							JSON_EXTRACT: [{ $expr: "users.metadata" }, "$.profile.department"],
						},
					},
					// Array element access
					first_skill: {
						$expr: {
							JSON_EXTRACT: [{ $expr: "users.metadata" }, "$.skills[0]"],
						},
					},
					// JSON validity check
					has_valid_metadata: {
						$cond: {
							if: { "users.metadata": { $ne: null } },
							then: "Valid",
							else: "Invalid",
						},
					},
				},
				condition: {
					$and: [
						{
							"users.metadata": { $ne: null },
						},
						{
							"users.metadata->profile": { $ne: null },
						},
					],
				},
			};

			const result = generateSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("JSON_EXTRACT");
			expect(result.sql).toContain("->");
		});

		it("should handle JSON array operations", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.metadata->skills": { $ne: null },
					},
					{
						"posts.tags->categories": { $ne: null },
					},
					{
						"orders.items->products": { $ne: null },
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `
				SELECT COUNT(*) as count 
				FROM users 
				LEFT JOIN posts ON users.id = posts.user_id 
				LEFT JOIN orders ON users.id = orders.user_id 
				WHERE ${result.sql}
			`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("->");
		});
	});

	describe("Advanced String Pattern Matching", () => {
		it("should handle complex regex patterns with escaping", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.email": {
							$regex:
								"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$",
						},
					},
					{
						"users.name": {
							$regex: "^[A-Z][a-z]+(\\s[A-Z][a-z]+)*$",
						},
					},
					{
						"posts.title": {
							$regex: "\\b(JavaScript|TypeScript|Python|Java)\\b",
						},
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `
				SELECT COUNT(*) as count 
				FROM users 
				LEFT JOIN posts ON users.id = posts.user_id 
				WHERE ${result.sql}
			`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("~");
			expect(result.params.length).toBeGreaterThan(0);
		});

		it("should handle case-insensitive string operations", async () => {
			const condition: Condition = {
				$or: [
					{
						"users.name": { $ilike: "%john%" },
					},
					{
						"users.email": { $ilike: "%EXAMPLE.COM%" },
					},
					{
						"posts.title": { $ilike: "%javascript%" },
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `
				SELECT COUNT(*) as count 
				FROM users 
				LEFT JOIN posts ON users.id = posts.user_id 
				WHERE ${result.sql}
			`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("ILIKE");
		});
	});

	describe("Null Handling and Edge Cases", () => {
		it("should handle null checks across different data types", async () => {
			const condition: Condition = {
				$or: [
					{
						$and: [{ "users.balance": { $ne: null } }, { "users.balance": { $gt: 0 } }],
					},
					{
						$and: [{ "users.metadata": { $ne: null } }, { "users.metadata->vip": { $eq: true } }],
					},
					{
						$and: [{ "users.profile_id": { $ne: null } }, { "users.active": { $eq: true } }],
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `SELECT COUNT(*) as count FROM users WHERE ${result.sql}`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("IS NOT NULL");
			expect(result.sql).toContain("OR");
		});

		it("should handle mixed null and non-null comparisons", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					balance: true,
					metadata: true,
					// Null-aware calculations
					safe_balance: {
						$cond: {
							if: { "users.balance": { $ne: null } },
							then: { $expr: "users.balance" },
							else: 0,
						},
					},
					// Metadata availability
					has_metadata: {
						$cond: {
							if: { "users.metadata": { $ne: null } },
							then: "Yes",
							else: "No",
						},
					},
				},
				condition: {
					$or: [{ "users.balance": { $ne: null } }, { "users.metadata": { $ne: null } }],
				},
			};

			const result = generateSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("CASE WHEN");
			expect(result.sql).toContain("IS NOT NULL");
		});
	});

	describe("Complex Multi-table Scenarios", () => {
		it("should handle complex joins with multiple conditions and type inference", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					status: true,
					posts: {
						id: true,
						title: true,
						published: true,
						views: true,
						// Calculate engagement score
						engagement_score: {
							$expr: {
								MULTIPLY: [{ $expr: "posts.views" }, { $expr: "posts.rating" }],
							},
						},
					},
					orders: {
						id: true,
						total: true,
						status: true,
						// Calculate order value category
						value_category: {
							$cond: {
								if: { "orders.total": { $gte: 1000 } },
								then: "High",
								else: {
									$cond: {
										if: { "orders.total": { $gte: 100 } },
										then: "Medium",
										else: "Low",
									},
								},
							},
						},
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
									$and: [{ "posts.published": { $eq: true } }, { "posts.views": { $gte: 100 } }],
								},
							},
						},
						{
							$exists: {
								table: "orders",
								conditions: {
									$and: [{ "orders.status": { $eq: "completed" } }, { "orders.total": { $gte: 50 } }],
								},
							},
						},
					],
				},
			};

			const result = generateSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("LEFT JOIN");
			expect(result.sql).toContain("EXISTS");
			expect(result.sql).toContain("MULTIPLY");
			expect(result.sql).toContain("CASE WHEN");
		});
	});

	describe("Performance and Large Dataset Scenarios", () => {
		it("should handle queries designed for performance with proper indexing hints", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.id": { $in: [1, 2, 3, 4, 5] },
					},
					{
						"users.status": { $in: ["active", "premium"] },
					},
					{
						"users.created_at": {
							$gte: { $date: "2023-01-01" },
						},
					},
					{
						"users.balance": { $gte: 0 },
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `SELECT * FROM users WHERE ${result.sql} ORDER BY users.id LIMIT 10`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("IN");
			expect(result.params).toContain(1);
			expect(result.params).toContain("active");
		});

		it("should handle complex conditions with optimal parameter binding", async () => {
			const condition: Condition = {
				$or: [
					{
						$and: [{ "users.age": { $gte: 18 } }, { "users.age": { $lte: 65 } }, { "users.status": { $eq: "active" } }],
					},
					{
						$and: [{ "users.balance": { $gte: 1000 } }, { "users.metadata->vip": { $eq: true } }],
					},
					{
						$and: [{ "users.created_at": { $gte: { $date: "2024-01-01" } } }, { "users.active": { $eq: true } }],
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");
			const sql = `SELECT COUNT(*) as count FROM users WHERE ${result.sql}`;
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("<=");
			expect(result.sql).toContain(">=");
			expect(result.sql).toContain("OR");
			expect(result.params.length).toBeGreaterThan(5);
		});
	});
});
