/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { generateSelectQuery } from "../../src";
import type { Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

describe("Integration Tests - Multi-table Operations with Complex Type Casting", () => {
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
				current_year: 2024,
				high_value_threshold: 200,
				premium_age_limit: 30,
				admin_role: "admin",
			},
			relationships: [
				{ table: "users", field: "id", toTable: "posts", toField: "user_id", type: "one-to-many" },
				{ table: "users", field: "id", toTable: "orders", toField: "customer_id", type: "one-to-many" },
			],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
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
							$expr: {
								CONCAT: [
									{ $expr: { UPPER: [{ $expr: "users.name" }] } },
									" (",
									{ $expr: { COALESCE: [{ $expr: "users.status" }, "unknown"] } },
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
										{ "users.age": { $lte: { $expr: "premium_age_limit" } } },
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
						// Calculated age from birth date
						calculated_age: {
							$expr: {
								DIVIDE: [
									{
										$expr: {
											SUBTRACT: [
												{ $expr: { EXTRACT: ["epoch", { $expr: "NOW()" }] } },
												{ $expr: { EXTRACT: ["epoch", { $expr: "users.birth_date" }] } },
											],
										},
									},
									31557600, // Seconds in a year (365.25 days)
								],
							},
						},
						// Related posts with complex expressions
						posts: {
							id: true,
							title: true,
							// Character count with type casting
							content_length: {
								$expr: {
									LENGTH: [{ $expr: "posts.content" }],
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
							// JSON array manipulation
							tag_count: {
								$expr: {
									COALESCE: [
										{
											$expr: {
												LENGTH: [{ $expr: { COALESCE: [{ $expr: "posts.tags" }, "[]"] } }],
											},
										},
										0,
									],
								},
							},
							// Date formatting
							published_year: {
								$expr: {
									EXTRACT: ["year", { $expr: { COALESCE: [{ $expr: "posts.published_at" }, { $expr: "posts.created_at" }] } }],
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
								$expr: {
									MULTIPLY: [
										{ $expr: "orders.amount" },
										{
											$cond: {
												if: { "orders.amount": { $gte: { $expr: "high_value_threshold" } } },
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
							// Days since order with proper type handling
							days_since_order: {
								$expr: {
									DIVIDE: [
										{
											$expr: {
												SUBTRACT: [
													{ $expr: { EXTRACT: ["epoch", { $expr: "NOW()" }] } },
													{ $expr: { EXTRACT: ["epoch", { $expr: "orders.created_at" }] } },
												],
											},
										},
										86400, // Seconds in a day
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

				const { sql, params } = generateSelectQuery(selectQuery, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify complex SQL generation
				expect(sql).toContain("LEFT JOIN posts");
				expect(sql).toContain("LEFT JOIN orders");
				expect(sql).toContain("CASE WHEN");
				expect(sql).toContain("CONCAT");
				expect(sql).toContain("UPPER");
				expect(sql).toContain("COALESCE");
				expect(sql).toContain("EXTRACT");
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("MULTIPLY");
				expect(sql).toContain("DIVIDE");
				expect(sql).toContain("SUBTRACT");

				// Verify nested structure in results
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(r).toHaveProperty("id");
					expect(r).toHaveProperty("name");
					expect(r).toHaveProperty("display_name");
					expect(r).toHaveProperty("is_premium_eligible");
					expect(r).toHaveProperty("posts");
					expect(r).toHaveProperty("orders");

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
								conditions: {
									$and: [
										{ "posts.user_id": { $eq: { $expr: "users.id" } } },
										{ "posts.published": { $eq: true } },
										{
											$or: [
												{
													// String function with type casting
													"posts.title": {
														$like: {
															$expr: {
																CONCAT: ["%", { $expr: { UPPER: ["PostgreSQL"] } }, "%"],
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
								conditions: {
									$and: [
										{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
										{ "orders.status": { $eq: "completed" } },
										{
											// Complex mathematical condition with variables
											"orders.amount": {
												$gte: {
													$expr: {
														MULTIPLY: [
															{ $expr: "high_value_threshold" },
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
										{
											// Year comparison using simple condition
											"orders.created_at": { $like: "2024%" },
										},
									],
								},
							},
						},
						{
							$not: {
								$exists: {
									table: "orders",
									conditions: {
										$and: [
											{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
											{ "orders.status": { $eq: "cancelled" } },
											{
												// Simple time condition
												"orders.created_at": { $gte: "2024-01-01" },
											},
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
						matching_count: { $expr: { COUNT: ["*"] } },
					},
					condition,
				};

				const { sql, params } = generateSelectQuery(query, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex EXISTS conditions
				expect(sql).toContain("EXISTS");
				expect(sql.split("EXISTS").length - 1).toBe(3); // Three EXISTS clauses
				expect(sql).toContain("NOT (EXISTS");
				expect(sql).toContain("CONCAT");
				expect(sql).toContain("UPPER");
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("MULTIPLY");
				expect(sql).toContain("EXTRACT");
				expect(sql).toContain("SUBTRACT");

				// Verify parameter handling
				expect(params).toContain("PostgreSQL");
				expect(params).toContain(100);
				expect(params).toContain(0.8);
				expect(params).toContain(2592000);
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
						department: { $expr: "users.metadata->department" },
						role: { $expr: "users.metadata->role" },
						// Complex JSON manipulation
						profile_summary: {
							$expr: {
								CONCAT: [
									{ $expr: { COALESCE: [{ $expr: "users.metadata->department" }, "unknown"] } },
									" - ",
									{ $expr: { UPPER: [{ $expr: { COALESCE: [{ $expr: "users.metadata->role" }, "employee"] } }] } },
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
							$expr: {
								ADD: [
									{
										$cond: {
											if: { "users.metadata->settings->notifications": { $eq: true } },
											then: 1,
											else: 0,
										},
									},
									{
										$expr: {
											LENGTH: [{ $expr: { COALESCE: [{ $expr: "users.metadata->settings->theme" }, ""] } }],
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
							// Count tags
							tag_count: {
								$expr: {
									COALESCE: [
										{
											$cond: {
												if: { "posts.tags": { $ne: null } },
												then: {
													$expr: {
														SUBTRACT: [
															{
																$expr: {
																	LENGTH: [{ $expr: { COALESCE: [{ $expr: "posts.tags" }, "[]"] } }],
																},
															},
															{
																$expr: {
																	MULTIPLY: [
																		2, // Account for quotes and brackets
																		{
																			$expr: {
																				LENGTH: [{ $expr: { COALESCE: [{ $expr: "posts.tags" }, "[]"] } }],
																			},
																		},
																	],
																},
															},
														],
													},
												},
												else: 0,
											},
										},
										0,
									],
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

				const { sql, params } = generateSelectQuery(selectQuery, config);
				const rows = await db.query(sql, params);

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
							$expr: {
								ADD: [
									// Base score from age (normalized)
									{
										$expr: {
											DIVIDE: [{ $expr: { COALESCE: [{ $expr: "users.age" }, 25] } }, 10],
										},
									},
									{
										$expr: {
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
								$expr: {
									MULTIPLY: [
										{
											$expr: {
												DIVIDE: [{ $expr: { LENGTH: [{ $expr: "posts.content" }] } }, 100],
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
								$expr: {
									DIVIDE: [
										{
											$expr: {
												DIVIDE: [{ $expr: { LENGTH: [{ $expr: "posts.content" }] } }, 5], // Approximate words (chars/5)
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
								$expr: {
									MULTIPLY: [{ $expr: "orders.amount" }, 0.085],
								},
							},
							// Total with tax
							total_with_tax: {
								$expr: {
									ADD: [
										{ $expr: "orders.amount" },
										{
											$expr: {
												MULTIPLY: [{ $expr: "orders.amount" }, 0.085],
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
										$expr: {
											MULTIPLY: [
												{
													$expr: {
														GREATEST: [5, { $expr: { MULTIPLY: [{ $expr: "orders.amount" }, 0.1] } }],
													},
												},
												1,
											],
										},
									},
								},
							},
							// Processing time in business days
							processing_days: {
								$cond: {
									if: { "orders.shipped_at": { $ne: null } },
									then: {
										$expr: {
											DIVIDE: [
												{
													$expr: {
														SUBTRACT: [
															{ $expr: { EXTRACT: ["epoch", { $expr: "orders.shipped_at" }] } },
															{ $expr: { EXTRACT: ["epoch", { $expr: "orders.created_at" }] } },
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
											conditions: {
												$and: [{ "posts.user_id": { $eq: { $expr: "users.id" } } }, { "posts.published": { $eq: true } }],
											},
										},
									},
									{
										$exists: {
											table: "orders",
											conditions: {
												$and: [
													{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
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

				const { sql, params } = generateSelectQuery(selectQuery, config);
				const rows = await db.query(sql, params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify mathematical operations in SQL
				expect(sql).toContain("MULTIPLY");
				expect(sql).toContain("DIVIDE");
				expect(sql).toContain("ADD");
				expect(sql).toContain("SUBTRACT");
				expect(sql).toContain("GREATEST");
				expect(sql).toContain("EXTRACT");
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
