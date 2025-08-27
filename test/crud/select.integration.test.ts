/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { buildSelectQuery, compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { AggregationQuery, Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, extractSelectWhereClause, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - SELECT Multi-Table Operations and Complex Queries", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		// Setup Docker environment and database
		await setupTestEnvironment();

		db = new DatabaseHelper();
		await db.connect();

		config = {
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
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "birth_date", type: "date", nullable: true },
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
			},
			variables: {
				current_user_id: "1",
				adminRole: "admin",
				current_year: 2024,
				high_value_threshold: 200,
				premium_age_limit: 30,
				admin_role: "admin",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "users", field: "id", toTable: "posts", toField: "user_id", type: "one-to-many" },
				{ table: "users", field: "id", toTable: "orders", toField: "customer_id", type: "one-to-many" },
			],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("WHERE Condition Processing and Execution", () => {
		it("should execute simple equality condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = { "users.active": true };
				const whereSql = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${whereSql}`;
				const rows = await db.query(sql);

				expect(rows.length).toBe(4); // 4 active users
				expect(rows.every((row: unknown) => (row as Record<string, unknown>).active === true)).toBe(true);
			});
		});

		it("should execute complex AND condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [{ "users.active": true }, { "users.age": { $gte: 30 } }],
				};
				const whereSql = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${whereSql}`;
				const rows = await db.query(sql);

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
				const whereSql = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${whereSql}`;
				const rows = await db.query(sql);

				expect(rows.length).toBe(3); // John & Alice (premium) + Jane (age 25)
			});
		});

		it("should execute JSON field access condition", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					"users.metadata->department": "engineering",
				};
				const whereSql = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${whereSql}`;
				const rows = await db.query(sql);

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
				const whereSql = extractSelectWhereClause(condition, config, "users");

				const sql = `SELECT * FROM users WHERE ${whereSql}`;
				const rows = await db.query(sql);

				expect(rows.length).toBe(1); // Charlie has null email
				expect((rows[0] as Record<string, unknown>).email).toBe(null);
			});
		});
	});

	describe("Field Selection and Projection Queries", () => {
		it("should execute basic select query", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, email: true };
				const sql = compileSelectQuery(parseSelectQuery({ rootTable: "users", selection }, config));

				const rows = await db.query(sql);

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
				const sql = compileSelectQuery(parseSelectQuery({ rootTable: "users", selection, condition }, config));

				const rows = await db.query(sql);

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
					department: { $field: "users.metadata->department" },
				};
				const sql = compileSelectQuery(parseSelectQuery({ rootTable: "users", selection }, config));

				const rows = await db.query(sql);

				expect(rows.length).toBe(5);
				expect(rows[0]).toHaveProperty("department");
				expect(typeof rows[0]).toBe("object");
			});
		});
	});

	describe("Statistical Aggregation Operations", () => {
		it("should execute simple aggregation", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { function: "SUM", field: "orders.amount" },
						order_count: { function: "COUNT", field: "orders.id" },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));

				const rows = await db.query(sql);

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
						avg_age: { function: "AVG", field: "users.age" },
						user_count: { function: "COUNT", field: "users.id" },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));

				const rows = await db.query(sql);

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

	describe("Multi-Table Query Orchestration", () => {
		it("should execute multi-table aggregation with conditions", () => {
			// Get order statistics by user status
			const condition: Condition = {
				$and: [{ "orders.status": { $in: ["completed", "shipped"] } }],
			};

			const aggregationQuery: AggregationQuery = {
				table: "orders",
				groupBy: ["orders.status"],
				aggregatedFields: {
					total_revenue: { function: "SUM", field: "orders.amount" },
					order_count: { function: "COUNT", field: "orders.id" },
					avg_order_value: { function: "AVG", field: "orders.amount" },
				},
			};

			const conditionResult = extractSelectWhereClause(condition, config, "orders");
			const aggregationResult = parseAggregationQuery(aggregationQuery, config);

			expect(conditionResult).toContain("orders.status IN");

			const sql = compileAggregationQuery(aggregationResult);
			expect(sql).toContain("GROUP BY");
			expect(sql).toContain("SUM(orders.amount)");
		});
	});

	describe("Complex Multi-table Joins with Type Inference", () => {
		it("should handle complex nested selections with proper type casting", async () => {
			await db.executeInTransaction(async () => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						// Complex string expression with type inference
						display_name: {
							$func: {
								CONCAT: [
									{ $func: { UPPER: [{ $field: "users.name" }] } },
									" (",
									{ $func: { COALESCE_STRING: [{ $field: "users.status" }, "unknown"] } },
									")",
								],
							},
						},
						// Boolean expression with complex logic
						is_premium_eligible: {
							$cond: {
								if: {
									$and: [
										{ "users.age": { $gte: 18 } },
										{ "users.age": { $lte: { $var: "premium_age_limit" } } },
										{ "users.active": { $eq: true } },
										{
											$or: [{ "users.status": { $eq: "premium" } }, { "users.metadata->department": { $eq: "engineering" } }],
										},
									],
								},
								then: true,
								else: false,
							},
						},
						// Simplified age calculation using numeric constants
						calculated_age: {
							$func: {
								SUBTRACT: [
									30, // Average age as constant
									5, // Offset
								],
							},
						},
						// Related posts with complex expressions
						posts: {
							id: true,
							title: true,
							// Character count with type casting
							content_length: {
								$func: {
									LENGTH: [{ $field: "posts.content" }],
								},
							},
							// Complex conditional expression
							title_category: {
								$cond: {
									if: { "posts.title": { $like: "%PostgreSQL%" } },
									then: "database",
									else: {
										$cond: {
											if: { "posts.title": { $like: "%Marketing%" } },
											then: "marketing",
											else: "general",
										},
									},
								},
							},
						},
						// Related orders with mathematical operations
						orders: {
							id: true,
							amount: true,
							status: true,
							// Complex mathematical expression with type casting
							discounted_amount: {
								$func: {
									MULTIPLY: [
										{ $field: "orders.amount" },
										{
											$cond: {
												if: { "orders.amount": { $gte: { $var: "high_value_threshold" } } },
												then: 0.9, // 10% discount for high-value orders
												else: {
													$cond: {
														if: { "orders.status": { $eq: "completed" } },
														then: 0.95, // 5% discount for completed orders
														else: 1.0,
													},
												},
											},
										},
									],
								},
							},
							// Simple calculation using numeric constants
							days_since_order: {
								$func: {
									DIVIDE: [
										{
											$func: {
												SUBTRACT: [
													365, // Days in year as constant
													100, // Offset
												],
											},
										},
										1, // Year difference
									],
								},
							},
							// Delivery status with complex logic
							delivery_status: {
								$cond: {
									if: { "orders.delivered_date": { $ne: null } },
									then: "delivered",
									else: {
										$cond: {
											if: { "orders.shipped_at": { $ne: null } },
											then: "shipped",
											else: {
												$cond: {
													if: { "orders.status": { $eq: "cancelled" } },
													then: "cancelled",
													else: "pending",
												},
											},
										},
									},
								},
							},
						},
					},
					condition: {
						$and: [
							{
								$or: [{ "users.status": { $eq: "premium" } }, { "users.status": { $eq: "active" } }],
							},
							{ "users.active": { $eq: true } },
						],
					},
				};

				const sql = buildSelectQuery(selectQuery, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify complex SQL generation
				expect(sql).toBe(
					"SELECT users.id AS \"id\", users.name AS \"name\", (UPPER(users.name) || ' (' || COALESCE(users.status, 'unknown') || ')') AS \"display_name\", (CASE WHEN (users.age >= 18 AND users.age <= 30 AND users.active = TRUE AND (users.status = 'premium' OR users.metadata->>'department' = 'engineering')) THEN TRUE ELSE FALSE END) AS \"is_premium_eligible\", (30 - 5) AS \"calculated_age\", posts.id AS \"posts.id\", posts.title AS \"posts.title\", LENGTH(posts.content) AS \"posts.content_length\", (CASE WHEN posts.title LIKE '%PostgreSQL%' THEN 'database' ELSE (CASE WHEN posts.title LIKE '%Marketing%' THEN 'marketing' ELSE 'general' END) END) AS \"posts.title_category\", orders.id AS \"orders.id\", orders.amount AS \"orders.amount\", orders.status AS \"orders.status\", (orders.amount * (CASE WHEN orders.amount >= 200 THEN 0.9 ELSE (CASE WHEN orders.status = 'completed' THEN 0.95 ELSE 1 END) END)) AS \"orders.discounted_amount\", ((365 - 100) / 1) AS \"orders.days_since_order\", (CASE WHEN orders.delivered_date IS NOT NULL THEN 'delivered' ELSE (CASE WHEN orders.shipped_at IS NOT NULL THEN 'shipped' ELSE (CASE WHEN orders.status = 'cancelled' THEN 'cancelled' ELSE 'pending' END) END) END) AS \"orders.delivery_status\" FROM users LEFT JOIN posts ON users.id = posts.user_id LEFT JOIN orders ON users.id = orders.customer_id WHERE ((users.status = 'premium' OR users.status = 'active') AND users.active = TRUE)",
				);

				// Verify nested structure in results
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("id");
					expect(r).toHaveProperty("name");
					expect(r).toHaveProperty("display_name");
					expect(r).toHaveProperty("is_premium_eligible");
					expect(r).toHaveProperty(["posts.id"]);
					expect(r).toHaveProperty(["orders.id"]);

					// Verify posts structure
					if (r.posts && Array.isArray(r.posts)) {
						for (const post of r.posts as Record<string, unknown>[]) {
							expect(post).toHaveProperty("title_category");
							expect(post).toHaveProperty("content_length");
							expect(typeof post.content_length).toBe("number");
						}
					}

					// Verify orders structure
					if (r.orders && Array.isArray(r.orders)) {
						for (const order of r.orders as Record<string, unknown>[]) {
							expect(order).toHaveProperty("discounted_amount");
							expect(order).toHaveProperty("delivery_status");
							expect(typeof order.discounted_amount).toBe("number");
						}
					}
				}
			});
		});
	});

	describe("Advanced EXISTS and Subquery Operations", () => {
		it("should handle multiple nested EXISTS with complex type casting", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$exists: {
								table: "posts",
								condition: {
									$and: [
										{ "posts.user_id": { $eq: { $field: "users.id" } } },
										{ "posts.published": { $eq: true } },
										{
											$or: [
												{
													// String function with type casting
													"posts.title": {
														$like: {
															$func: {
																CONCAT: ["%", { $func: { UPPER: ["postgresql"] } }, "%"],
															},
														},
													},
												},
												{
													// JSON field access with type inference
													"posts.tags": { $like: '%"database"%' },
												},
												{
													// Simple content length condition
													"posts.content": { $like: "%PostgreSQL%" },
												},
											],
										},
									],
								},
							},
						},
						{
							$exists: {
								table: "orders",
								condition: {
									$and: [
										{ "orders.customer_id": { $eq: { $field: "users.id" } } },
										{ "orders.status": { $eq: "completed" } },
										{
											// Complex mathematical condition with variables
											"orders.amount": {
												$gte: {
													$func: {
														MULTIPLY: [
															{ $var: "high_value_threshold" },
															{
																$cond: {
																	if: { "users.status": { $eq: "premium" } },
																	then: 0.8, // Lower threshold for premium users
																	else: 1.0,
																},
															},
														],
													},
												},
											},
										},
									],
								},
							},
						},
						{
							$not: {
								$exists: {
									table: "orders",
									condition: {
										$and: [
											{ "orders.customer_id": { $eq: { $field: "users.id" } } },
											{ "orders.status": { $eq: "cancelled" } },
											{ "orders.created_at": { $gte: { $date: "2024-01-01" } } },
										],
									},
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
						email: true,
					},
					condition,
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex EXISTS conditions
				expect(sql).toBe(
					"SELECT users.id AS \"id\", users.name AS \"name\", users.email AS \"email\" FROM users WHERE (EXISTS (SELECT 1 FROM posts WHERE (posts.user_id = users.id AND posts.published = TRUE AND (posts.title LIKE ('%' || UPPER('postgresql') || '%')::TEXT OR (posts.tags)::TEXT LIKE '%\"database\"%' OR posts.content LIKE '%PostgreSQL%'))) AND EXISTS (SELECT 1 FROM orders WHERE (orders.customer_id = users.id AND orders.status = 'completed' AND orders.amount >= (200 * (CASE WHEN users.status = 'premium' THEN 0.8 ELSE 1 END)))) AND NOT (EXISTS (SELECT 1 FROM orders WHERE (orders.customer_id = users.id AND orders.status = 'cancelled' AND (orders.created_at)::DATE >= ('2024-01-01')::DATE))))",
				);
			});
		});
	});

	describe("Advanced JSON Operations with Type Casting", () => {
		it("should handle complex JSON path operations with proper type inference", async () => {
			await db.executeInTransaction(async () => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						// Direct JSON field access
						department: { $field: "users.metadata->department" },
						role: { $field: "users.metadata->role" },
						// Complex JSON manipulation
						profile_summary: {
							$func: {
								CONCAT: [
									{ $func: { COALESCE_STRING: [{ $field: "users.metadata->>department" }, "unknown"] } },
									" - ",
									{ $func: { UPPER: [{ $func: { COALESCE_STRING: [{ $field: "users.metadata->>role" }, "employee"] } }] } },
								],
							},
						},
						// JSON boolean extraction with type casting
						dark_theme_user: {
							$cond: {
								if: { "users.metadata->settings->theme": { $eq: "dark" } },
								then: true,
								else: false,
							},
						},
						// Complex JSON array operations
						settings_count: {
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
											LENGTH: [{ $func: { COALESCE_STRING: [{ $field: "users.metadata->settings->>theme" }, ""] } }],
										},
									},
								],
							},
						},
						// Nested posts with JSON operations
						posts: {
							id: true,
							title: true,
							// Extract specific tags
							has_database_tag: {
								$cond: {
									if: { "posts.tags": { $like: '%"database"%' } },
									then: true,
									else: false,
								},
							},
						},
					},
					condition: {
						$and: [
							{ "users.active": { $eq: true } },
							{
								$or: [
									{ "users.metadata->department": { $in: ["engineering", "marketing"] } },
									{ "users.metadata->role": { $in: ["senior", "manager"] } },
								],
							},
							{
								// Simple department length condition
								"users.metadata->department": { $in: ["engineering", "marketing"] },
							},
						],
					},
				};

				const sql = buildSelectQuery(selectQuery, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify JSON operations in SQL
				expect(sql).toContain("metadata");
				expect(sql).toContain("->");
				expect(sql).toContain("->>");
				expect(sql).toContain("department");
				expect(sql).toContain("settings");
				expect(sql).toContain("COALESCE");
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("CASE");

				// Verify results structure
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("department");
					expect(r).toHaveProperty("role");
					expect(r).toHaveProperty("profile_summary");
					expect(r).toHaveProperty("dark_theme_user");
					expect(typeof r.dark_theme_user).toBe("boolean");

					if (r.posts && Array.isArray(r.posts)) {
						for (const post of r.posts as Record<string, unknown>[]) {
							expect(post).toHaveProperty("has_database_tag");
							expect(typeof post.has_database_tag).toBe("boolean");
						}
					}
				}
			});
		});
	});

	describe("Cross-table Mathematical Operations", () => {
		it("should handle complex mathematical operations across multiple tables", async () => {
			await db.executeInTransaction(async () => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						age: true,
						// Calculate user value score based on multiple factors
						user_value_score: {
							$func: {
								ADD: [
									// Base score from age (normalized)
									{
										$func: {
											DIVIDE: [{ $func: { COALESCE_NUMBER: [{ $field: "users.age" }, 25] } }, 10],
										},
									},
									{
										$func: {
											ADD: [
												// Bonus for premium status
												{
													$cond: {
														if: { "users.status": { $eq: "premium" } },
														then: 5,
														else: 0,
													},
												},
												// Bonus for engineering department
												{
													$cond: {
														if: { "users.metadata->department": { $eq: "engineering" } },
														then: 3,
														else: 1,
													},
												},
											],
										},
									},
								],
							},
						},
						// Posts with calculated metrics
						posts: {
							id: true,
							title: true,
							// Content complexity score
							complexity_score: {
								$func: {
									MULTIPLY: [
										{
											$func: {
												DIVIDE: [{ $func: { LENGTH: [{ $field: "posts.content" }] } }, 100],
											},
										},
										{
											$cond: {
												if: { "posts.published": { $eq: true } },
												then: 1.5,
												else: 1.0,
											},
										},
									],
								},
							},
							// Estimated reading time (words per minute calculation)
							estimated_reading_minutes: {
								$func: {
									DIVIDE: [
										{
											$func: {
												DIVIDE: [{ $func: { LENGTH: [{ $field: "posts.content" }] } }, 5], // Approximate words (chars/5)
											},
										},
										200, // Average reading speed
									],
								},
							},
						},
						// Orders with financial calculations
						orders: {
							id: true,
							amount: true,
							status: true,
							// Tax calculation (fictional 8.5% tax)
							tax_amount: {
								$func: {
									MULTIPLY: [{ $field: "orders.amount" }, 0.085],
								},
							},
							// Total with tax
							total_with_tax: {
								$func: {
									ADD: [
										{ $field: "orders.amount" },
										{
											$func: {
												MULTIPLY: [{ $field: "orders.amount" }, 0.085],
											},
										},
									],
								},
							},
							// Shipping cost calculation based on amount
							shipping_cost: {
								$cond: {
									if: { "orders.amount": { $gte: 100 } },
									then: 0, // Free shipping
									else: {
										$func: {
											MULTIPLY: [
												{
													$func: {
														GREATEST_NUMBER: [5, { $func: { MULTIPLY: [{ $field: "orders.amount" }, 0.1] } }],
													},
												},
												1,
											],
										},
									},
								},
							},
							// Processing time (simplified - without EXTRACT)
							processing_days: {
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
												86400, // Convert to days
											],
										},
									},
									else: null,
								},
							},
						},
					},
					condition: {
						$and: [
							{ "users.active": { $eq: true } },
							{
								$or: [
									{
										$exists: {
											table: "posts",
											condition: {
												$and: [{ "posts.user_id": { $eq: { $field: "users.id" } } }, { "posts.published": { $eq: true } }],
											},
										},
									},
									{
										$exists: {
											table: "orders",
											condition: {
												$and: [
													{ "orders.customer_id": { $eq: { $field: "users.id" } } },
													{ "orders.status": { $eq: "completed" } },
													{ "orders.amount": { $gte: 50 } },
												],
											},
										},
									},
								],
							},
						],
					},
				};

				const sql = buildSelectQuery(selectQuery, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify mathematical operations in SQL
				expect(sql).toContain("*");
				expect(sql).toContain("/");
				expect(sql).toContain("+");
				expect(sql).toContain("-");
				expect(sql).toContain("GREATEST");
				expect(sql).toContain("COALESCE");

				// Verify complex calculations
				expect(sql).toContain("0.085"); // Tax rate
				expect(sql).toContain("86400"); // Seconds in day
				expect(sql).toContain("200"); // Reading speed

				// Verify results structure and types
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("user_value_score");
					expect(typeof r.user_value_score).toBe("number");

					if (r.orders && Array.isArray(r.orders)) {
						for (const order of r.orders as Record<string, unknown>[]) {
							expect(order).toHaveProperty("tax_amount");
							expect(order).toHaveProperty("total_with_tax");
							expect(order).toHaveProperty("shipping_cost");
							expect(typeof order.tax_amount).toBe("number");
							expect(typeof order.total_with_tax).toBe("number");

							// Verify calculated values are reasonable
							const amount = order.amount as number;
							const taxAmount = order.tax_amount as number;
							const totalWithTax = order.total_with_tax as number;

							expect(taxAmount).toBeCloseTo(amount * 0.085, 2);
							expect(totalWithTax).toBeCloseTo(amount + taxAmount, 2);
						}
					}
				}
			});
		});
	});
});
