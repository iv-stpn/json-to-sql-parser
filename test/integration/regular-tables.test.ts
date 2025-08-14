import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type AggregationQuery, compileAggregationQuery, parseAggregationQuery } from "../../src/parsers/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/parsers/select";

import type { Condition } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";
import { extractSelectWhereClause } from "../_helpers";

describe("Integration Tests - Regular Tables", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		// Setup Docker environment and database
		await setupTestEnvironment();

		db = new DatabaseHelper();
		await db.connect();

		config = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "number", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "age", type: "number", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "status", type: "string", nullable: false },
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
				orders: {
					allowedFields: [
						{ name: "id", type: "number", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "number", nullable: false },
					],
				},
			},
			variables: {
				currentUserId: "1",
				adminRole: "admin",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
			],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("Condition Queries", () => {
		it("should execute simple equality condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = { "users.active": true };
				const result = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(4); // 4 active users
				expect(rows.every((row: unknown) => (row as Record<string, unknown>).active === true)).toBe(true);
			});
		});

		it("should execute complex AND condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [{ "users.active": true }, { "users.age": { $gte: 30 } }],
				};
				const result = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(2); // John (30) and Charlie (32)
				expect(
					rows.every((row: unknown) => {
						const r = row as Record<string, unknown>;
						return r.active === true && (r.age as number) >= 30;
					}),
				).toBe(true);
			});
		});

		it("should execute OR condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [{ "users.status": "premium" }, { "users.age": { $lt: 26 } }],
				};
				const result = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(3); // John & Alice (premium) + Jane (age 25)
			});
		});

		it("should execute JSON field access condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					"users.metadata->department": "engineering",
				};
				const result = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(2); // John and Alice
				expect(
					rows.every((row: unknown) => {
						const r = row as Record<string, unknown>;
						return (r.metadata as Record<string, unknown>).department === "engineering";
					}),
				).toBe(true);
			});
		});

		it("should handle null conditions correctly", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					"users.email": { $eq: null },
				};
				const result = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(1); // Charlie has null email
				expect((rows[0] as Record<string, unknown>).email).toBe(null);
			});
		});
	});

	describe("Select Queries", () => {
		it("should execute basic select query", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, email: true };
				const result = parseSelectQuery({ rootTable: "users", selection }, config);
				const sql = compileSelectQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(5);
				expect(rows[0]).toHaveProperty("id");
				expect(rows[0]).toHaveProperty("name");
				expect(rows[0]).toHaveProperty("email");
			});
		});

		it("should execute select with condition", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, status: true };
				const condition: Condition = { "users.status": "premium" };
				const result = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(2); // John and Alice
				expect(
					rows.every((row: unknown) => {
						const r = row as Record<string, unknown>;
						return r.status === "premium";
					}),
				).toBe(true);
			});
		});

		it("should execute select with JSON field", async () => {
			await db.executeInTransaction(async () => {
				const selection = {
					id: true,
					name: true,
					department: { $expr: "users.metadata->department" },
				};
				const result = parseSelectQuery({ rootTable: "users", selection }, config);
				const sql = compileSelectQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(5);
				expect(rows[0]).toHaveProperty("department");
				expect(typeof rows[0]).toBe("object");
			});
		});
	});

	describe("Aggregation Queries", () => {
		it("should execute simple aggregation", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { operator: "SUM", field: "orders.amount" },
						order_count: { operator: "COUNT", field: "orders.id" },
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("status");
				expect(rows[0]).toHaveProperty("total_amount");
				expect(rows[0]).toHaveProperty("order_count");

				// Check that completed orders have the expected total
				const completedOrders = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r.status === "completed";
				});
				expect(completedOrders).toBeDefined();
				const completedRecord = completedOrders as Record<string, unknown>;
				expect(Number(completedRecord.total_amount)).toBeCloseTo(899.97, 2); // 299.99 + 199.99 + 399.99
			});
		});

		it("should execute aggregation with JSON field grouping", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.metadata->department"],
					aggregatedFields: {
						avg_age: { operator: "AVG", field: "users.age" },
						user_count: { operator: "COUNT", field: "users.id" },
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("metadata->department");
				expect(rows[0]).toHaveProperty("avg_age");
				expect(rows[0]).toHaveProperty("user_count");

				// Check engineering department has 2 users
				const engineeringDept = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r["metadata->department"] === "engineering";
				});
				expect(engineeringDept).toBeDefined();
				const engineeringRecord = engineeringDept as Record<string, unknown>;
				expect(Number(engineeringRecord.user_count)).toBe(2);
			});
		});
	});

	describe("Complex Queries", () => {
		it("should execute multi-table aggregation with conditions", () => {
			// Get order statistics by user status
			const condition: Condition = {
				$and: [{ "orders.status": { $in: ["completed", "shipped"] } }],
			};

			const aggregationQuery: AggregationQuery = {
				table: "orders",
				groupBy: ["orders.status"],
				aggregatedFields: {
					total_revenue: { operator: "SUM", field: "orders.amount" },
					order_count: { operator: "COUNT", field: "orders.id" },
					avg_order_value: { operator: "AVG", field: "orders.amount" },
				},
			};

			const conditionResult = extractSelectWhereClause(condition, config, "orders");
			const aggregationResult = parseAggregationQuery(aggregationQuery, config);

			expect(conditionResult.sql).toContain("orders.status IN");
			expect(conditionResult.params).toEqual(["completed", "shipped"]);

			const sql = compileAggregationQuery(aggregationResult);
			expect(sql).toContain("GROUP BY");
			expect(sql).toContain("SUM(orders.amount)");
		});
	});
});
