/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { generateAggregationQuery, generateSelectQuery } from "../../src";
import type { AggregationQuery, Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

describe("Integration Tests - Type Casting and Complex Expressions", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		await setupTestEnvironment();
		db = new DatabaseHelper();
		await db.connect();

		config = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "age", type: "number", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "birth_date", type: "date", nullable: true },
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
						{ name: "delivered_date", type: "date", nullable: true },
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
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "published_at", type: "datetime", nullable: true },
					],
				},
			},
			variables: {
				current_year: 2024,
				tax_rate: 0.085,
				shipping_threshold: 100,
				premium_multiplier: 1.5,
			},
			relationships: [
				{ table: "users", field: "id", toTable: "orders", toField: "customer_id", type: "one-to-many" },
				{ table: "users", field: "id", toTable: "posts", toField: "user_id", type: "one-to-many" },
			],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("Complex Type Casting in Conditions", () => {
		it("should handle mixed type conditions with proper casting", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						// Number comparison
						{
							"users.age": { $gte: 25 },
						},
						// Boolean comparison
						{
							"users.active": { $eq: true },
						},
						// String comparison
						{
							"users.status": { $eq: "premium" },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						active: true,
					},
					condition,
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify proper type casting in generated SQL
				expect(sql).toContain("users.age >= $");
				expect(sql).toContain("users.active = $");
				expect(sql).toContain("users.status = $");
			});
		});

		it("should handle numeric type casting and comparisons", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							"users.age": { $gt: 18 },
						},
						{
							"users.age": { $lt: 65 },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
					},
					condition,
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(params).toContain(18);
				expect(params).toContain(65);
			});
		});
	});

	describe("Complex Mathematical Expressions", () => {
		it("should handle arithmetic expressions with type casting", async () => {
			await db.executeInTransaction(async () => {
				const aggregation: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { operator: "SUM", field: "orders.amount" },
						count: { operator: "COUNT", field: "*" },
						avg_amount: { operator: "AVG", field: "orders.amount" },
					},
					condition: {
						"orders.amount": { $gt: 100 },
					},
				};

				const { sql, params } = generateAggregationQuery(aggregation, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify mathematical operations in SQL
				expect(sql).toContain("SUM");
				expect(sql).toContain("AVG");
				expect(params).toContain(100);
			});
		});
	});

	describe("String Manipulation with Type Casting", () => {
		it("should handle string concatenation and manipulation", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						name_with_age: {
							$expr: {
								CONCAT: [{ $expr: "users.name" }, " (Age: ", { $expr: "users.age" }, ")"],
							},
						},
					},
					condition: {
						"users.name": { $ne: null },
					},
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify string operations
				expect(sql).toContain("CONCAT");
			});
		});
	});

	describe("Conditional Expressions with Type Casting", () => {
		it("should handle case expressions with proper type casting", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						age_category: {
							$cond: {
								if: { "users.age": { $gte: 18 } },
								then: "Adult",
								else: "Minor",
							},
						},
					},
					condition: {
						"users.age": { $ne: null },
					},
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify conditional logic
				expect(sql).toContain("CASE");
				expect(sql).toContain("WHEN");
				expect(sql).toContain("THEN");
				expect(sql).toContain("ELSE");
			});
		});
	});

	describe("Type Validation and Error Handling", () => {
		it("should handle null values properly in type casting", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [
						{
							"users.age": { $eq: null },
						},
						{
							"users.age": { $gt: 18 },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
					},
					condition,
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify null handling
				expect(sql).toContain("IS NULL");
			});
		});
	});
});
