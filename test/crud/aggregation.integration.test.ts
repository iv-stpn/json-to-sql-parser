/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import type { AggregationQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - Advanced Aggregations with Type Casting and Inference", () => {
	let db: DatabaseHelper;

	const config: Config = {
		dialect: "postgresql",
		variables: {},
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
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
					{ name: "id", type: "uuid", nullable: false },
					{ name: "user_id", type: "uuid", nullable: false },
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
					{ name: "id", type: "uuid", nullable: false },
					{ name: "user_id", type: "uuid", nullable: false },
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
	});

	describe("Mathematical Operations with Conditional Type Casting", () => {
		it("should handle complex mathematical aggregations with conditional expressions", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["status"],
				aggregatedFields: {
					// Basic aggregations
					total_users: {
						function: "COUNT",
						field: "users.id",
					},
					avg_age: {
						function: "AVG",
						field: "users.age",
					},
					total_balance: {
						function: "SUM",
						field: "users.balance",
					},
					// Conditional aggregation
					active_users_count: {
						function: "SUM",
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
						function: "SUM",
						field: {
							$func: {
								MULTIPLY: [
									{ $field: "users.age" },
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
			const rows = await db.query(sql);

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
						function: "SUM",
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
						function: "AVG",
						field: {
							$cond: {
								if: { "users.created_at": { $gte: { $date: "2023-01-01" } } },
								then: { $field: "users.balance" },
								else: null,
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("JSON Field Aggregation with Dynamic Type Inference", () => {
		it("should handle JSON field aggregations with type casting", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					// Count users by department from JSON metadata
					engineering_count: {
						function: "SUM",
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
						function: "SUM",
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
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("String Processing and Aggregation Operations", () => {
		it("should handle string-based aggregations", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["status"],
				aggregatedFields: {
					// Average name length by status
					avg_name_length: {
						function: "AVG",
						field: {
							$func: {
								LENGTH: [{ $field: "users.name" }],
							},
						},
					},
					// Count users with long names
					long_name_count: {
						function: "SUM",
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
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("LENGTH");
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("Advanced Conditional Logic in Aggregations", () => {
		it("should handle tiered user classifications", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					// High-value active users
					gold_users: {
						function: "SUM",
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
						function: "SUM",
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
						function: "SUM",
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
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("SUM");
			expect(sql).toContain("CASE WHEN");
		});
	});

	describe("Multi-Table Relationship Aggregations", () => {
		it("should handle aggregations across related tables", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.status"],
				aggregatedFields: {
					// User count
					user_count: {
						function: "COUNT",
						field: "users.id",
					},
					// Average user age
					avg_user_age: {
						function: "AVG",
						field: "users.age",
					},
					// Count related posts
					post_count: {
						function: "COUNT",
						field: "posts.id",
					},
					// Total user count per status
					total_users: {
						function: "COUNT",
						field: "*",
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("LEFT JOIN");
			expect(sql).toContain("GROUP BY");
		});
	});

	describe("Statistical Analysis Functions", () => {
		it("should handle statistical aggregations", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: [],
				aggregatedFields: {
					min_age: {
						function: "MIN",
						field: "users.age",
					},
					max_age: {
						function: "MAX",
						field: "users.age",
					},
					avg_balance: {
						function: "AVG",
						field: "users.balance",
					},
					total_balance: {
						function: "SUM",
						field: "users.balance",
					},
					user_count: {
						function: "COUNT",
						field: "users.id",
					},
				},
			};

			const result = parseAggregationQuery(query, config);
			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("MIN");
			expect(sql).toContain("MAX");
			expect(sql).toContain("AVG");
			expect(sql).toContain("SUM");
			expect(sql).toContain("COUNT");
		});
	});

	describe("Filtered Aggregations with Complex Conditions", () => {
		it("should handle aggregations with filtered conditions", async () => {
			const query: AggregationQuery = {
				table: "users",
				groupBy: ["users.status"],
				aggregatedFields: {
					filtered_count: {
						function: "COUNT",
						field: "users.id",
					},
					avg_filtered_balance: {
						function: "AVG",
						field: "users.balance",
					},
					max_filtered_age: {
						function: "MAX",
						field: "users.age",
					},
				},
			};

			const result = parseAggregationQuery(query, config);

			const sql = compileAggregationQuery(result);
			const rows = await db.query(sql);

			expect(rows).toBeDefined();
			expect(Array.isArray(rows)).toBe(true);
			expect(sql).toContain("GROUP BY");
		});
	});
});

describe("Integration - Complex Aggregations with Multi-Type Casting", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		await setupTestEnvironment();
		db = new DatabaseHelper();
		await db.connect();

		config = {
			dialect: "postgresql",
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
				const rows = await db.query(sql);

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
				const rows = await db.query(sql);

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
				const rows = await db.query(sql);

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
				const rows = await db.query(sql);

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
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify string functions in SQL
				expect(sql).toBe(
					"SELECT users.email AS \"email\", COUNT(*) AS \"user_count\", AVG(LENGTH(users.name)) AS \"avg_name_length\", STRING_AGG(UPPER(users.name) || ' (' || COALESCE(users.status, 'unknown') || ')', ',') AS \"all_names\", MAX(CASE WHEN users.status = 'premium' THEN 1 ELSE 0 END) AS \"has_premium_users\", STRING_AGG(users.name || ':' || COALESCE((users.age)::TEXT, 'unknown'), ',') AS \"age_stats\" FROM users GROUP BY users.email",
				);
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
										{ $func: { SUBSTR: [{ $field: "orders.id" }, 1, 8] } },
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
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex nested expressions in SQL
				expect(sql).toContain("CASE WHEN");
				expect(sql.split("CASE WHEN").length).toBeGreaterThanOrEqual(3); // Multiple CASE statements
				expect(sql).toBe(
					"SELECT orders.status AS \"status\", COUNT(*) AS \"order_count\", SUM(orders.amount * (1 + (500 / 2592000))) AS \"weighted_revenue\", STRING_AGG('Order#' || SUBSTR((orders.id)::TEXT, 1, 8) || ' ($' || (orders.amount * (CASE WHEN orders.status = 'completed' THEN 1 ELSE 0.8 END))::TEXT || ')', ',') AS \"order_summary\", AVG(CASE WHEN (orders.shipped_at IS NOT NULL AND orders.status IN ('shipped', 'completed')) THEN (orders.amount / (((EXTRACT(EPOCH FROM orders.shipped_at) - EXTRACT(EPOCH FROM orders.created_at)) / 3600) + 1)) ELSE 0 END) AS \"efficiency_score\" FROM orders GROUP BY orders.status",
				);
			});
		});
	});
});
