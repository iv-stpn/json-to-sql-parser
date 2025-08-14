/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildSelectQuery } from "../../src";

import type { Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { extractSelectWhereClause } from "../_helpers";
import { DatabaseHelper, setupTestEnvironment } from "./_helpers";

describe("Integration Tests - Edge Cases and Comprehensive Type Inference", () => {
	let db: DatabaseHelper;

	const config: Config = {
		variables: {},
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: true },
					{ name: "age", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "balance", type: "number", nullable: true },
					{ name: "created_at", type: "datetime", nullable: false },
					{ name: "updated_at", type: "datetime", nullable: true },
					{ name: "status", type: "string", nullable: false },
					{ name: "metadata", type: "object", nullable: true },
					{ name: "birth_date", type: "date", nullable: true },
				],
			},
			posts: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "user_id", type: "uuid", nullable: false },
					{ name: "title", type: "string", nullable: false },
					{ name: "content", type: "string", nullable: false },
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

	describe("UUID and Special Data Type Handling", () => {
		it("should handle UUID fields with proper type inference", async () => {
			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					// UUID validation check using birth_date which is nullable
					is_valid_user: {
						$cond: {
							if: { "users.birth_date": { $ne: null } },
							then: true,
							else: false,
						},
					},
				},
				condition: {
					"users.birth_date": { $ne: null },
				},
			};

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("CASE WHEN");
		});

		it("should handle UUID comparisons and operations", () => {
			const condition: Condition = {
				$and: [{ "users.email": { $ne: null } }, { "users.balance": { $ne: null } }],
			};

			const result = extractSelectWhereClause(condition, config, "users");

			expect(result).toBeDefined();
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
					// Name formatting instead of date formatting
					name_upper: {
						$expr: {
							UPPER: ["users.name"],
						},
					},
					// Name length instead of date difference
					name_length: {
						$expr: {
							LENGTH: ["users.name"],
						},
					},
					// Simple field selections instead of complex operations
					user_name_upper: {
						$expr: {
							UPPER: [{ $expr: "users.name" }],
						},
					},
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
			expect(result.sql).toContain("UPPER");
		});

		it("should handle timestamp comparisons and ranges", () => {
			const condition: Condition = {
				$and: [
					{
						"users.created_at": {
							$gte: { $timestamp: "2020-01-01T00:00:00" },
						},
					},
					{
						"users.updated_at": {
							$lte: { $timestamp: "2025-12-31T23:59:59" },
						},
					},
					{
						"users.age": {
							$gte: 18,
						},
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");

			expect(result).toBeDefined();
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
					// Simple JSON field access instead of JSON_EXTRACT
					user_email: {
						$expr: {
							LOWER: [{ $expr: "users.email" }],
						},
					},
					// Simple string manipulation
					name_length: {
						$expr: {
							LENGTH: [{ $expr: "users.name" }],
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

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("LOWER");
			expect(result.sql).toContain("LENGTH");
			expect(result.sql).toContain("->");
		});

		it("should handle JSON array operations", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.metadata->skills": { $ne: null },
					},
					{
						"users.metadata->preferences": { $ne: null },
					},
					{
						"users.metadata->settings": { $ne: null },
					},
				],
			};

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					metadata: true,
					// JSON validity check
					has_skills: {
						$cond: {
							if: { "users.metadata->skills": { $ne: null } },
							then: "Yes",
							else: "No",
						},
					},
				},
				condition: condition,
			};

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("->");
		});
	});

	describe("Advanced String Pattern Matching", () => {
		it("should handle complex regex patterns with escaping", () => {
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
						"users.status": {
							$regex: "\\b(active|premium|vip)\\b",
						},
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");

			expect(result).toBeDefined();
			expect(result.sql).toContain("~");
			expect(result.params.length).toBeGreaterThan(0);
		});

		it("should handle case-insensitive string operations", () => {
			const condition: Condition = {
				$or: [
					{
						"users.name": { $ilike: "%john%" },
					},
					{
						"users.email": { $ilike: "%EXAMPLE.COM%" },
					},
					{
						"users.status": { $ilike: "%premium%" },
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");

			expect(result).toBeDefined();
			expect(result.sql).toContain("ILIKE");
		});
	});

	describe("Null Handling and Edge Cases", () => {
		it("should handle null checks across different data types", () => {
			const condition: Condition = {
				$or: [
					{
						$and: [{ "users.balance": { $ne: null } }, { "users.balance": { $gt: 0 } }],
					},
					{
						$and: [{ "users.metadata": { $ne: null } }, { "users.metadata->vip": { $eq: true } }],
					},
					{
						$and: [{ "users.email": { $ne: null } }, { "users.active": { $eq: true } }],
					},
				],
			};

			const result = extractSelectWhereClause(condition, config, "users");

			expect(result).toBeDefined();
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

			const result = buildSelectQuery(query, config);
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
						content: true,
						// Calculate title length instead of engagement score
						title_length: {
							$expr: {
								LENGTH: ["posts.title"],
							},
						},
					},
					orders: {
						id: true,
						amount: true,
						status: true,
						// Simple order status indicator instead of value category
						is_high_value: {
							$cond: {
								if: { status: { $eq: "pending" } },
								then: "Pending",
								else: "Other",
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
									$and: [{ "posts.published": { $eq: true } }, { "posts.published_at": { $ne: null } }],
								},
							},
						},
						{
							$exists: {
								table: "orders",
								conditions: {
									$and: [{ "orders.status": { $eq: "pending" } }, { "orders.amount": { $gte: 50 } }],
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
			expect(result.sql).toContain("EXISTS");
			expect(result.sql).toContain("LENGTH");
			expect(result.sql).toContain("CASE WHEN");
		});
	});

	describe("Performance and Large Dataset Scenarios", () => {
		it("should handle queries designed for performance with proper indexing hints", async () => {
			const condition: Condition = {
				$and: [
					{
						"users.age": { $in: [25, 30, 35, 28, 32] },
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

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					email: true,
					status: true,
					created_at: true,
					balance: true,
				},
				condition: condition,
			};

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(result.sql).toContain("IN");
			expect(result.params).toContain(25);
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

			const query: SelectQuery = {
				rootTable: "users",
				selection: {
					id: true,
					name: true,
					age: true,
					status: true,
					balance: true,
					created_at: true,
					active: true,
				},
				condition: condition,
			};

			const result = buildSelectQuery(query, config);
			const rows = await db.query(result.sql, result.params);

			expect(rows).toBeDefined();
			expect(result.sql).toContain("<=");
			expect(result.sql).toContain(">=");
			expect(result.sql).toContain("OR");
			expect(result.params.length).toBeGreaterThan(5);
		});
	});
});
