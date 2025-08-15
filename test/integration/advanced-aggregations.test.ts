/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import type { AggregationQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

describe("Integration Tests - Advanced Aggregations with Complex Type Casting", () => {
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
				min_order_threshold: 100,
				premium_tier_threshold: 250,
				"NOW()": "2024-01-01T00:00:00Z",
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

	describe("Complex Expression Aggregations with Type Inference", () => {
		it("should handle mathematical expressions with proper type casting", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { function: "SUM", field: "orders.amount" },
						avg_amount: { function: "AVG", field: "orders.amount" },
						// Complex mathematical expression with type inference
						revenue_score: {
							function: "SUM",
							field: {
								$func: {
									MULTIPLY: [
										{ $field: "orders.amount" },
										{
											$cond: {
												if: { "orders.amount": { $gte: { $var: "premium_tier_threshold" } } },
												then: 1.5, // Premium multiplier
												else: 1.0,
											},
										},
									],
								},
							},
						},
						// Total orders count
						total_orders: {
							function: "COUNT",
							field: "*",
						},
						order_count: { function: "COUNT", field: "*" },
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify proper type casting in SQL
				expect(sql).toContain("SUM(orders.amount)");
				expect(sql).toContain("AVG(orders.amount)");
				expect(sql).toContain("CASE WHEN");
				expect(sql).toContain("COUNT");
				expect(sql).toContain("GROUP BY");

				// Verify results have expected properties
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("status");
					expect(r).toHaveProperty("total_amount");
					expect(r).toHaveProperty("avg_amount");
					expect(r).toHaveProperty("revenue_score");
					expect(r).toHaveProperty("order_count");
				}
			});
		});

		it("should handle JSON path aggregations with type casting", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.metadata->department", "users.metadata->role"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "*" },
						avg_age: { function: "AVG", field: "users.age" },
						// Extract JSON boolean and count
						active_users: {
							function: "COUNT",
							field: {
								$cond: {
									if: { "users.active": { $eq: true } },
									then: 1,
									else: null,
								},
							},
						},
						// JSON extraction with mathematical operations
						settings_complexity: {
							function: "AVG",
							field: {
								$func: {
									ADD: [
										{
											$cond: {
												if: { "users.metadata->settings->notifications": { $eq: true } },
												then: 1,
												else: 0,
											},
										},
										{
											$func: {
												LENGTH: [{ $func: { COALESCE_STRING: [{ $field: "users.metadata->settings->>theme" }, "default"] } }],
											},
										},
									],
								},
							},
						},
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify JSON path access in SQL
				expect(sql).toContain("metadata");
				expect(sql).toContain("department");
				expect(sql).toContain("role");
				expect(sql).toContain("settings");
				expect(sql).toContain("COALESCE");
				expect(sql).toContain("LENGTH");

				// Check that we get engineering and marketing departments
				const departments = rows.map((row) => (row as Record<string, unknown>)["metadata->department"]);
				expect(departments).toContain("engineering");
				expect(departments).toContain("marketing");
			});
		});
	});

	describe("Cross-table Aggregations with Complex Joins", () => {
		it("should handle aggregations with EXISTS conditions", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.status"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "*" },
						avg_age: { function: "AVG", field: "users.age" },
						// Count users with orders above threshold
						high_value_customers: {
							function: "COUNT",
							field: {
								$cond: {
									if: {
										$exists: {
											table: "orders",
											condition: {
												$and: [
													{ "orders.customer_id": { $eq: { $field: "users.id" } } },
													{ "orders.amount": { $gte: { $var: "min_order_threshold" } } },
													{ "orders.status": { $eq: "completed" } },
												],
											},
										},
									},
									then: 1,
									else: null,
								},
							},
						},
						// Complex calculation involving multiple tables
						engagement_score: {
							function: "AVG",
							field: {
								$func: {
									ADD: [
										// Base score from age
										{
											$func: {
												DIVIDE: [{ $func: { COALESCE_NUMBER: [{ $field: "users.age" }, 25] } }, 10],
											},
										},
										// Bonus points for having posts
										{
											$cond: {
												if: {
													$exists: {
														table: "posts",
														condition: {
															$and: [{ "posts.user_id": { $eq: { $field: "users.id" } } }, { "posts.published": { $eq: true } }],
														},
													},
												},
												then: 5,
												else: 0,
											},
										},
									],
								},
							},
						},
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify EXISTS subqueries in aggregation
				expect(sql).toContain("EXISTS");
				expect(sql).toContain("SELECT 1 FROM orders");
				expect(sql).toContain("SELECT 1 FROM posts");
				expect(sql).toContain("CASE WHEN");
				expect(sql).toContain("COALESCE");
				expect(sql).toContain("/");

				// Verify results structure
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("status");
					expect(r).toHaveProperty("user_count");
					expect(r).toHaveProperty("engagement_score");
				}
			});
		});
	});

	describe("Date and Time Aggregations with Complex Casting", () => {
		it("should handle date extraction and time-based aggregations", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.created_at"],
					aggregatedFields: {
						order_count: { function: "COUNT", field: "*" },
						total_revenue: { function: "SUM", field: "orders.amount" },
						// Calculate average processing time for shipped orders (simplified)
						avg_processing_days: {
							function: "AVG",
							field: {
								$cond: {
									if: { "orders.shipped_at": { $ne: null } },
									then: {
										$func: {
											DIVIDE: [
												{
													$func: {
														SUBTRACT: [
															{ $func: { EXTRACT_EPOCH: [{ $field: "orders.shipped_at" }] } },
															{ $func: { EXTRACT_EPOCH: [{ $field: "orders.created_at" }] } },
														],
													},
												},
												86400, // Convert seconds to days
											],
										},
									},
									else: null,
								},
							},
						},
						// Simple order count
						avg_order_count: {
							function: "COUNT",
							field: "*",
						},
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify basic aggregation functions in SQL
				expect(sql).toContain("COUNT");
				expect(sql).toContain("AVG");
				expect(sql).toContain("/"); // Division operator

				// Verify results structure
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("order_count");
					expect(r).toHaveProperty("total_revenue");
				}
			});
		});
	});

	describe("String and Array Aggregations with Type Inference", () => {
		it("should handle string manipulation and array aggregations", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.email"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "*" },
						// Average name length with proper casting
						avg_name_length: {
							function: "AVG",
							field: {
								$func: {
									LENGTH: [{ $field: "users.name" }],
								},
							},
						},
						// Concatenated names with type preservation
						all_names: {
							function: "STRING_AGG",
							additionalArguments: [","],
							field: {
								$func: {
									CONCAT: [
										{ $func: { UPPER: [{ $field: "users.name" }] } },
										" (",
										{ $func: { COALESCE_STRING: [{ $field: "users.status" }, "unknown"] } },
										")",
									],
								},
							},
						},
						// Complex boolean aggregation
						has_premium_users: {
							function: "MAX",
							field: {
								$cond: {
									if: { "users.status": { $eq: "premium" } },
									then: 1,
									else: 0,
								},
							},
						},
						// Age statistics with null handling
						age_stats: {
							function: "STRING_AGG",
							additionalArguments: [","],
							field: {
								$func: {
									CONCAT: [{ $field: "users.name" }, ":", { $func: { COALESCE_STRING: [{ $field: "users.age" }, "unknown"] } }],
								},
							},
						},
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify string functions in SQL
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("UPPER");
				expect(sql).toContain("CONCAT");
				expect(sql).toContain("COALESCE");
				expect(sql).toContain("STRING_AGG");

				// Verify type casting for different operations
				expect(sql).toContain("MAX");
				expect(sql).toContain("AVG");
				expect(sql).toContain("CASE WHEN");
			});
		});
	});

	describe("Nested Expressions with Multiple Type Conversions", () => {
		it("should handle deeply nested expressions with proper type inference", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						order_count: { function: "COUNT", field: "*" },
						// Complex mathematical expression with multiple type conversions
						weighted_revenue: {
							function: "SUM",
							field: {
								$func: {
									MULTIPLY: [
										{ $field: "orders.amount" },
										{
											$func: {
												ADD: [
													1,
													{
														$func: {
															DIVIDE: [
																500, // Simple numeric constant
																2592000, // 30 days in seconds
															],
														},
													},
												],
											},
										},
									],
								},
							},
						},
						// String manipulation with mathematical results
						order_summary: {
							function: "STRING_AGG",
							additionalArguments: [","],
							field: {
								$func: {
									CONCAT: [
										"Order#",
										{ $func: { SUBSTRING: [{ $field: "orders.id" }, 1, 8] } },
										" ($",
										{
											$func: {
												MULTIPLY: [
													{ $field: "orders.amount" },
													{
														$cond: {
															if: { "orders.status": { $eq: "completed" } },
															then: 1.0,
															else: 0.8, // Potential value for non-completed orders
														},
													},
												],
											},
										},
										")",
									],
								},
							},
						},
						// Complex conditional aggregation
						efficiency_score: {
							function: "AVG",
							field: {
								$cond: {
									if: {
										$and: [{ "orders.shipped_at": { $ne: null } }, { "orders.status": { $in: ["shipped", "completed"] } }],
									},
									then: {
										$func: {
											DIVIDE: [
												{ $field: "orders.amount" },
												{
													$func: {
														ADD: [
															{
																$func: {
																	DIVIDE: [
																		{
																			$func: {
																				SUBTRACT: [
																					{ $func: { EXTRACT_EPOCH: [{ $field: "orders.shipped_at" }] } },
																					{ $func: { EXTRACT_EPOCH: [{ $field: "orders.created_at" }] } },
																				],
																			},
																		},
																		3600, // Convert to hours
																	],
																},
															},
															1, // Prevent division by zero
														],
													},
												},
											],
										},
									},
									else: 0,
								},
							},
						},
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex nested expressions in SQL
				expect(sql).toContain("CASE WHEN");
				expect(sql.split("CASE WHEN").length).toBeGreaterThanOrEqual(3); // Multiple CASE statements
				expect(sql).toContain("*");
				expect(sql).toContain("/");
				expect(sql).toContain("-");
				expect(sql).toContain("+");
				expect(sql).toContain("SUBSTRING");
				expect(sql).toContain("CONCAT");
				expect(sql).toContain("STRING_AGG");
			});
		});
	});
});
