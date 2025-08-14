/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/parsers/aggregate";
import type { AggregationQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment } from "./_helpers";

describe("Integration Tests - Advanced Aggregations with Type Inference", () => {
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
					{ name: "status", type: "string", nullable: false },
					{ name: "metadata", type: "object", nullable: true },
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
					{ name: "created_at", type: "datetime", nullable: false },
				],
			},
			orders: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "user_id", type: "number", nullable: false },
					{ name: "total", type: "number", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "created_at", type: "datetime", nullable: false },
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

	describe("Mathematical Aggregations with Type Casting", () => {
		it("should handle complex mathematical aggregations with conditional expressions", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["status"],
				aggregatedFields: {
					// Basic aggregations
					total_users: {
						operator: "COUNT",
						field: "users.id",
					},
					avg_age: {
						operator: "AVG",
						field: "users.age",
					},
					total_balance: {
						operator: "SUM",
						field: "users.balance",
					},
					// Conditional aggregation
					active_users_count: {
						operator: "SUM",
						field: {
							$cond: {
								if: { "users.active": { $eq: true } },
								then: 1,
								else: 0,
							},
						},
					},
					// Weighted calculation
					weighted_score: {
						operator: "SUM",
						field: {
							$expr: {
								MULTIPLY: [
									{ $expr: "users.age" },
									{
										$cond: {
											if: { "users.active": { $eq: true } },
											then: 1.5,
											else: 1.0,
										},
									},
								],
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("GROUP BY");
			expect(sql).toContain("COUNT");
			expect(sql).toContain("AVG");
			expect(sql).toContain("SUM");
			expect(sql).toContain("CASE WHEN");
		});

		it("should handle date-based aggregations", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					// Users created in recent period
					recent_users: {
						operator: "SUM",
						field: {
							$cond: {
								if: { "users.created_at": { $gte: { $date: "2023-01-01" } } },
								then: 1,
								else: 0,
							},
						},
					},
					// Average balance for recent users
					recent_users_balance: {
						operator: "AVG",
						field: {
							$cond: {
								if: { "users.created_at": { $gte: { $date: "2023-01-01" } } },
								then: { $expr: "users.balance" },
								else: null,
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("JSON Field Aggregations", () => {
		it("should handle JSON field aggregations with type casting", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					// Count users by department from JSON metadata
					engineering_count: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									"users.metadata->department": { $eq: "engineering" },
								},
								then: 1,
								else: 0,
							},
						},
					},
					// Count users with metadata
					users_with_metadata: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									"users.metadata": { $ne: null },
								},
								then: 1,
								else: 0,
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("String Aggregations", () => {
		it("should handle string-based aggregations", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["status"],
				aggregatedFields: {
					// Average name length by status
					avg_name_length: {
						operator: "AVG",
						field: {
							$expr: {
								LENGTH: [{ $expr: "users.name" }],
							},
						},
					},
					// Count users with long names
					long_name_count: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									"users.name": { $like: "%John%" },
								},
								then: 1,
								else: 0,
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("LENGTH");
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("Advanced Conditional Aggregations", () => {
		it("should handle tiered user classifications", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					// High-value active users
					gold_users: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									$and: [{ "users.balance": { $gte: 1000 } }, { "users.active": { $eq: true } }],
								},
								then: 1,
								else: 0,
							},
						},
					},
					// Medium-value active users
					silver_users: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									$and: [
										{ "users.balance": { $gte: 500 } },
										{ "users.balance": { $lt: 1000 } },
										{ "users.active": { $eq: true } },
									],
								},
								then: 1,
								else: 0,
							},
						},
					},
					// Low-value active users
					bronze_users: {
						operator: "SUM",
						field: {
							$cond: {
								if: {
									$and: [{ "users.balance": { $lt: 500 } }, { "users.active": { $eq: true } }],
								},
								then: 1,
								else: 0,
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("SUM");
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("Cross-table Aggregations", () => {
		it("should handle aggregations across related tables", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.status"],
				aggregatedFields: {
					// User count
					user_count: {
						operator: "COUNT",
						field: "users.id",
					},
					// Average post views
					avg_post_views: {
						operator: "AVG",
						field: "posts.views",
					},
					// Total order value per user group
					total_order_value: {
						operator: "SUM",
						field: "orders.total",
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("LEFT JOIN");
			expect(sql).toContain("GROUP BY");
		});
	});

	describe("Statistical Functions", () => {
		it("should handle statistical aggregations", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					min_age: {
						operator: "MIN",
						field: "users.age",
					},
					max_age: {
						operator: "MAX",
						field: "users.age",
					},
					avg_balance: {
						operator: "AVG",
						field: "users.balance",
					},
					total_balance: {
						operator: "SUM",
						field: "users.balance",
					},
					user_count: {
						operator: "COUNT",
						field: "users.id",
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("MIN");
			expect(sql).toContain("MAX");
			expect(sql).toContain("AVG");
			expect(sql).toContain("SUM");
			expect(sql).toContain("COUNT");
		});
	});

	describe("Aggregations with Complex Conditions", () => {
		it("should handle aggregations with filtered conditions", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.status"],
				aggregatedFields: {
					filtered_count: {
						operator: "COUNT",
						field: "users.id",
					},
					avg_filtered_balance: {
						operator: "AVG",
						field: "users.balance",
					},
					max_filtered_age: {
						operator: "MAX",
						field: "users.age",
					},
				},
			};

			const result = parseAggregationQuery(query, config);

			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql, result.params);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("GROUP BY");
		});
	});
});
