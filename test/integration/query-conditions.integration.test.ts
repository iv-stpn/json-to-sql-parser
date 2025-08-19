/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";

import type { Condition } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, extractSelectWhereClause, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - Complex Query Condition Processing", () => {
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
						{ name: "id", type: "uuid", nullable: false },
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
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
						{ name: "tags", type: "object", nullable: true },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: "550e8400-e29b-41d4-a716-446655440000",
				adminRole: "admin",
				minAge: 18,
				maxAge: 65,
				premiumThreshold: 1000,
				testPattern: "test_%",
				maxResults: 100,
				score_threshold: 85.5,
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

	describe("Deeply Nested Logical Conditions", () => {
		it("should execute 5-level deep nested AND/OR conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [
								{
									$and: [
										{ "users.active": true },
										{
											$or: [
												{ "users.age": { $gte: 25 } },
												{
													$and: [{ "users.status": "premium" }, { "users.metadata->department": "engineering" }],
												},
											],
										},
									],
								},
								{
									$and: [
										{ "users.status": "admin" },
										{
											$not: {
												$or: [{ "users.age": { $lt: 21 } }, { "users.email": { $eq: null } }],
											},
										},
									],
								},
							],
						},
						{
							$or: [{ "users.name": { $like: "A%" } }, { "users.name": { $like: "J%" } }],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				// Verify the SQL generates correctly without errors
				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex nesting structure
				expect(result.sql).toContain("AND");
				expect(result.sql).toContain("OR");
				expect(result.sql).toContain("NOT");
				expect(result.sql.split("(").length - 1).toBeGreaterThan(5); // Multiple nested levels
			});
		});

		it("should execute complex mixed operator conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [
						{
							$and: [
								{ "users.age": { $gte: 18, $lte: 65 } },
								{ "users.status": { $in: ["premium", "active"] } },
								{ "users.email": { $ne: null } },
								{ "users.name": { $like: "%a%" } },
							],
						},
						{
							$and: [
								{ "users.status": "admin" },
								{
									$not: {
										$and: [{ "users.age": { $lt: 25 } }, { "users.metadata->level": { $ne: "senior" } }],
									},
								},
							],
						},
						{
							$and: [
								{ "users.active": false },
								{ "users.status": { $nin: ["banned", "suspended"] } },
								{
									$or: [{ "users.email": { $like: "%@company.com" } }, { "users.metadata->verified": true }],
								},
							],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify all operators are present
				expect(result.sql).toContain(">=");
				expect(result.sql).toContain("<=");
				expect(result.sql).toContain("IN");
				expect(result.sql).toContain("NOT IN");
				expect(result.sql).toContain("LIKE");
				expect(result.sql).toContain("IS NOT NULL");
				expect(result.sql).toContain("NOT");
			});
		});

		it("should execute conditions with multiple EXISTS subqueries", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{ "users.active": true },
						{
							$exists: {
								table: "posts",
								condition: {
									$and: [
										{ "posts.user_id": { $eq: { $field: "users.id" } } },
										{ "posts.published": true },
										{
											$or: [{ "posts.title": { $like: "%tech%" } }, { "posts.tags->category": "technology" }],
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
										{ "orders.amount": { $gte: 100 } },
										{ "orders.status": { $in: ["completed", "shipped"] } },
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
											{ "orders.status": "cancelled" },
											{ "orders.amount": { $gt: 500 } },
										],
									},
								},
							},
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify EXISTS statements
				expect(result.sql.split("EXISTS").length - 1).toBe(3); // Three EXISTS clauses
				expect(result.sql).toContain("NOT (EXISTS");
			});
		});
	});

	describe("Complex JSON Path Conditions", () => {
		it("should execute deep JSON path conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [
						{
							$and: [
								{ "users.metadata->profile->settings->theme": "dark" },
								{ "users.metadata->profile->preferences->notifications": true },
								{ "users.metadata->account->type": "premium" },
							],
						},
						{
							$and: [
								{ "users.metadata->profile->settings->language": "en" },
								{ "users.metadata->access->level": { $gte: 5 } },
								{
									$or: [{ "users.metadata->permissions->admin": true }, { "users.metadata->permissions->moderator": true }],
								},
							],
						},
						{
							$and: [
								{ "users.metadata->subscription->plan": "enterprise" },
								{ "users.metadata->subscription->expires": { $gt: "2024-01-01" } },
								{
									$not: {
										"users.metadata->flags->suspended": true,
									},
								},
							],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify JSON path operations
				expect(result.sql).toContain("metadata");
				expect(result.sql).toContain("->");
				expect(result.sql).toContain("profile");
				expect(result.sql).toContain("settings");
			});
		});

		it("should execute mixed JSON and regular field conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [{ "users.status": "premium" }, { "users.metadata->account->tier": "gold" }],
						},
						{
							$and: [
								{ "users.age": { $gte: 25, $lte: 55 } },
								{ "users.active": true },
								{
									$or: [{ "users.email": { $like: "%@company.com" } }, { "users.metadata->work->company": "TechCorp" }],
								},
							],
						},
						{
							$not: {
								$or: [
									{ "users.name": { $like: "test%" } },
									{ "users.metadata->flags->demo": true },
									{ "users.metadata->trial->active": true },
								],
							},
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify mixed conditions
				expect(result.sql).toContain("users.status");
				expect(result.sql).toContain("metadata");
				expect(result.sql).toContain(">=");
			});
		});
	});

	describe("Complex Expression Conditions", () => {
		it("should execute conditions with nested expression functions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							"users.name": {
								$like: "%user%",
							},
						},
						{
							"users.age": {
								$gt: 20,
							},
						},
						{
							$or: [
								{
									"users.status": {
										$eq: "active",
									},
								},
								{
									"users.email": {
										$like: "%@example.com",
									},
								},
							],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify basic conditions work
				expect(result.sql).toContain("LIKE");
				expect(result.sql).toContain("AND");
				expect(result.sql).toContain("OR");
			});
		});

		it("should execute conditions with variable references and expressions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [
						{
							$and: [
								{ "users.id": { $eq: { $var: "current_user_id" } } },
								{
									"users.age": {
										$gte: { $var: "minAge" },
										$lte: { $var: "maxAge" },
									},
								},
								{
									"users.status": {
										$ne: {
											$func: {
												UPPER: [{ $var: "adminRole" }],
											},
										},
									},
								},
							],
						},
						{
							$and: [
								{
									"users.name": {
										$like: {
											$func: {
												CONCAT: [{ $var: "adminRole" }, "%"],
											},
										},
									},
								},
								{
									"users.metadata->balance": {
										$gte: { $var: "premiumThreshold" },
									},
								},
								{
									$exists: {
										table: "orders",
										condition: {
											$and: [
												{ "orders.customer_id": { $eq: { $field: "users.id" } } },
												{
													"orders.amount": {
														$gt: {
															$func: {
																DIVIDE: [{ $var: "premiumThreshold" }, 2],
															},
														},
													},
												},
											],
										},
									},
								},
							],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify variable substitution
				expect(result.sql).toContain("'550e8400-e29b-41d4-a716-446655440000'"); // current_user_id
				expect(result.sql).toContain("18"); // minAge
				expect(result.sql).toContain("65"); // maxAge
				expect(result.sql).toContain("1000"); // premiumThreshold
			});
		});
	});

	describe("Complex Select Queries with Deep Conditions", () => {
		it("should execute complex select with deeply nested conditions", async () => {
			await db.executeInTransaction(async () => {
				const query = parseSelectQuery(
					{
						rootTable: "users",
						selection: {
							id: true,
							name: true,
							email: true,
							computed_age_group: {
								$func: {
									CONCAT: ["age_", { $field: "users.age" }],
								},
							},
							posts: {
								id: true,
								title: true,
								content_preview: {
									$func: {
										CONCAT: [{ $func: { SUBSTRING: ["posts.content", 1, 50] } }, "..."],
									},
								},
							},
						},
						condition: {
							$and: [
								{
									$or: [
										{
											$and: [
												{ "users.active": true },
												{ "users.status": { $in: ["premium", "admin"] } },
												{
													$or: [{ "users.age": { $gte: 25, $lte: 50 } }, { "users.metadata->verified": true }],
												},
											],
										},
										{
											$and: [
												{ "users.status": "admin" },
												{ "users.metadata->permissions->super": true },
												{
													$not: {
														"users.metadata->restrictions->limited": true,
													},
												},
											],
										},
									],
								},
								{
									$exists: {
										table: "posts",
										condition: {
											$and: [
												{ "posts.user_id": { $eq: { $field: "users.id" } } },
												{ "posts.published": true },
												{
													$or: [{ "posts.title": { $like: "%important%" } }, { "posts.tags->priority": "high" }],
												},
											],
										},
									},
								},
							],
						},
					},
					config,
				);

				const sql = compileSelectQuery(query);
				const rows = await db.query(sql, query.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex SQL structure
				expect(sql).toContain("SELECT");
				expect(sql).toContain("LEFT JOIN");
				expect(sql).toContain("WHERE");
				expect(sql).toContain("AND");
				expect(sql).toContain("OR");
				expect(sql).toContain("EXISTS");
				expect(sql).toContain("SUBSTRING");
			});
		});

		it("should execute select with multiple relationship joins and complex conditions", async () => {
			await db.executeInTransaction(async () => {
				const query = parseSelectQuery(
					{
						rootTable: "users",
						selection: {
							id: true,
							name: true,
							status_display: {
								$func: {
									CONCAT: [{ $func: { UPPER: ["users.status"] } }, " - ACTIVE"],
								},
							},
							posts: {
								id: true,
								title: true,
								word_count: {
									$func: { LENGTH: ["posts.content"] },
								},
							},
							orders: {
								id: true,
								amount: true,
								status: true,
							},
						},
						condition: {
							$and: [
								{
									$or: [
										{
											$and: [
												{ "users.active": true },
												{ "users.age": { $gte: 21 } },
												{
													$or: [{ "users.status": "premium" }, { "users.metadata->tier": "gold" }],
												},
											],
										},
										{
											$and: [
												{ "users.status": "admin" },
												{
													$not: {
														$and: [{ "users.age": { $lt: 25 } }, { "users.metadata->experience": "junior" }],
													},
												},
											],
										},
									],
								},
								{
									$or: [
										{
											$exists: {
												table: "posts",
												condition: {
													$and: [
														{ "posts.user_id": { $eq: { $field: "users.id" } } },
														{ "posts.published": true },
														{
															$or: [{ "posts.title": { $like: "%featured%" } }, { "posts.tags->featured": true }],
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
														{ "orders.amount": { $gte: 500 } },
														{ "orders.status": "completed" },
													],
												},
											},
										},
									],
								},
							],
						},
					},
					config,
				);

				const sql = compileSelectQuery(query);
				const rows = await db.query(sql, query.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex joins and conditions
				expect(sql).toContain("LEFT JOIN posts");
				expect(sql).toContain("LEFT JOIN orders");
				expect(sql.split("EXISTS").length - 1).toBe(2); // Two EXISTS clauses
				expect(sql).toContain("UPPER");
				expect(sql).toContain("LENGTH");
			});
		});
	});

	describe("Performance and Edge Cases", () => {
		it("should handle conditions with large parameter lists", async () => {
			await db.executeInTransaction(async () => {
				const largeInArray = Array.from({ length: 100 }, (_, i) => `user_${i}`);
				const anotherLargeArray = Array.from(
					{ length: 50 },
					(_, i) => `550e840${i.toString().padStart(1, "0")}-e29b-41d4-a716-44665544000${(i % 10).toString()}`,
				);

				const condition: Condition = {
					$and: [
						{
							$or: [{ "users.name": { $in: largeInArray } }, { "users.id": { $in: anotherLargeArray } }],
						},
						{
							$and: [
								{ "users.active": true },
								{ "users.status": { $nin: ["banned", "suspended", "inactive"] } },
								{
									$or: [{ "users.age": { $gte: 18, $lte: 65 } }, { "users.metadata->verified": true }],
								},
							],
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);
				expect(result.params.length).toBeGreaterThan(150); // Should have lots of parameters
			});
		});

		it("should handle conditions with 200+ parameters efficiently", async () => {
			await db.executeInTransaction(async () => {
				const largeNameArray = Array.from({ length: 50 }, (_, i) => `user_${i}`);
				const largeIdArray = Array.from(
					{ length: 50 },
					(_, i) => `550e840${i.toString().padStart(1, "0")}-e29b-41d4-a716-44665544000${i.toString().padStart(1, "0")}`,
				);
				const largeStatusArray = Array.from({ length: 20 }, (_, i) => `status_${i}`);
				const largeAgeArray = Array.from({ length: 30 }, (_, i) => i + 18);

				const condition: Condition = {
					$and: [
						{
							$or: [
								{ "users.name": { $in: largeNameArray } },
								{ "users.id": { $in: largeIdArray } },
								{ "users.status": { $in: largeStatusArray } },
								{ "users.age": { $in: largeAgeArray } },
							],
						},
						{
							$and: [
								{ "users.active": true },
								{ "users.email": { $ne: null } },
								{
									$or: [
										{ "users.name": { $like: "test%" } },
										{ "users.email": { $like: "%@test.com" } },
										{ "users.metadata->category": { $in: ["premium", "gold", "platinum"] } },
									],
								},
							],
						},
						{
							$not: {
								$or: [
									{ "users.status": { $in: ["banned", "suspended", "deleted"] } },
									{ "users.metadata->flags->restricted": true },
								],
							},
						},
					],
				};

				const startTime = Date.now();
				const result = extractSelectWhereClause(condition, config, "users");
				const parseTime = Date.now() - startTime;

				const queryStartTime = Date.now();
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);
				const queryTime = Date.now() - queryStartTime;

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);
				expect(result.params.length).toBeGreaterThan(150); // Should have lots of parameters

				// Performance assertions
				expect(parseTime).toBeLessThan(1000); // Should parse in under 1 second
				expect(queryTime).toBeLessThan(5000); // Should execute in under 5 seconds
			});
		});

		it("should handle extreme nesting without stack overflow", async () => {
			await db.executeInTransaction(async () => {
				// Create a deeply nested condition (15 levels)
				let deepCondition: Condition = { "users.active": true };

				for (let i = 0; i < 15; i++) {
					deepCondition = {
						$and: [
							deepCondition,
							{
								$or: [{ "users.name": { $like: `%level${i}%` } }, { "users.age": { $gt: i * 2 } }],
							},
						],
					};
				}

				const result = extractSelectWhereClause(deepCondition, config, "users");
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);

				// Verify deep nesting in SQL
				expect(result.sql.split("(").length).toBeGreaterThan(20);
				expect(result.sql.split("AND").length).toBeGreaterThan(15);
			});
		});

		it("should handle deeply nested conditions with many branches", async () => {
			await db.executeInTransaction(async () => {
				// Create a condition with 20 levels of nesting
				const createNestedCondition = (depth: number): Condition => {
					if (depth === 0) {
						return {
							$or: [{ "users.active": true }, { "users.name": { $like: "%test%" } }, { "users.age": { $gte: 18 } }],
						};
					}

					return {
						$and: [
							createNestedCondition(depth - 1),
							{
								$or: [
									{ "users.status": `level_${depth}` },
									{ "users.metadata->level": depth },
									{ "users.age": { $gt: depth * 2 } },
								],
							},
						],
					};
				};

				const deepCondition = createNestedCondition(20);

				const startTime = Date.now();
				const result = extractSelectWhereClause(deepCondition, config, "users");
				const parseTime = Date.now() - startTime;

				const queryStartTime = Date.now();
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);
				const queryTime = Date.now() - queryStartTime;

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);

				// Verify deep nesting
				expect(result.sql.split("(").length).toBeGreaterThan(30);
				expect(result.sql.split("AND").length).toBeGreaterThan(20);

				// Performance assertions
				expect(parseTime).toBeLessThan(2000); // Should parse in under 2 seconds
				expect(queryTime).toBeLessThan(10000); // Should execute in under 10 seconds
			});
		});

		it("should handle multiple deep JSON path conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [
								{ "users.metadata->profile->personal->'firstName'": { $like: "John%" } },
								{ "users.metadata->profile->personal->'lastName'": { $like: "Smith%" } },
								{ "users.metadata->profile->contact->email->primary": { $like: "%@company.com" } },
								{ "users.metadata->profile->contact->phone->mobile": { $like: "+1%" } },
							],
						},
						{
							$and: [
								{ "users.metadata->settings->preferences->theme": "dark" },
								{ "users.metadata->settings->preferences->language": "en" },
								{ "users.metadata->settings->notifications->email": true },
								{ "users.metadata->settings->notifications->push": true },
							],
						},
						{
							$or: [
								{ "users.metadata->account->subscription->plan": "premium" },
								{ "users.metadata->account->subscription->plan": "enterprise" },
								{
									$and: [
										{ "users.metadata->account->billing->method": "credit_card" },
										{ "users.metadata->account->billing->auto_renew": true },
									],
								},
							],
						},
						{
							$not: {
								$or: [
									{ "users.metadata->flags->suspended": true },
									{ "users.metadata->flags->restricted": true },
									{ "users.metadata->flags->trial_expired": true },
								],
							},
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql} LIMIT 5`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify JSON path operations are present
				expect(result.sql).toContain("metadata");
				expect(result.sql).toContain("->");
				expect(result.sql).toContain("profile");
				expect(result.sql).toContain("settings");
				expect(result.sql).toContain("account");
			});
		});

		it("should handle JSON array operations with complex conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [
								{ "users.metadata->tags": { $like: '%"premium"%' } },
								{ "users.metadata->tags": { $like: '%"vip"%' } },
								{ "users.metadata->roles": { $like: '%"admin"%' } },
							],
						},
						{
							$and: [
								{ "users.metadata->scores->overall": { $gte: 85 } },
								{ "users.metadata->scores->recent": { $gte: 80 } },
								{
									$or: [
										{ "users.metadata->achievements": { $like: '%"expert"%' } },
										{ "users.metadata->certifications": { $like: '%"advanced"%' } },
									],
								},
							],
						},
						{
							$not: {
								$or: [
									{ "users.metadata->blacklist": { $like: '%"spam"%' } },
									{ "users.metadata->violations": { $like: '%"severe"%' } },
								],
							},
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql} LIMIT 3`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify JSON operations
				expect(result.sql).toContain("LIKE");
				expect(result.sql).toContain(">=");
				expect(result.sql).toContain("NOT");
			});
		});

		it("should handle multiple nested expression functions efficiently", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							"users.name": {
								$eq: {
									$func: {
										UPPER: [
											{
												$func: {
													CONCAT: [
														{
															$func: {
																SUBSTRING: [
																	{ $func: { LOWER: [{ $field: "users.email" }] } },
																	1,
																	{ $func: { LENGTH: [{ $field: "users.name" }] } },
																],
															},
														},
														"_",
														{ $func: { UPPER: ["users.status"] } },
													],
												},
											},
										],
									},
								},
							},
						},
						{
							"users.age": {
								$gt: {
									$func: {
										ADD: [
											{
												$func: {
													MULTIPLY: [{ $field: "users.age" }, 2],
												},
											},
											{
												$func: {
													DIVIDE: [{ $func: { LENGTH: [{ $field: "users.name" }] } }, 3],
												},
											},
										],
									},
								},
							},
						},
						{
							$or: [
								{
									"users.status": {
										$eq: {
											$cond: {
												if: {
													$and: [{ "users.age": { $gte: 65 } }, { "users.active": true }],
												},
												then: {
													$func: { UPPER: ["senior"] },
												},
												else: {
													$cond: {
														if: { "users.age": { $gte: 18 } },
														then: {
															$func: { CONCAT: ["adult_", { $field: "users.status" }] },
														},
														else: "minor",
													},
												},
											},
										},
									},
								},
								{
									"users.email": {
										$like: {
											$func: {
												CONCAT: [
													"%",
													{ $func: { LOWER: ["users.name"] } },
													"%@%",
													{ $func: { SUBSTRING: ["users.status", 1, 3] } },
													".com",
												],
											},
										},
									},
								},
							],
						},
					],
				};

				const startTime = Date.now();
				const result = extractSelectWhereClause(condition, config, "users");
				const parseTime = Date.now() - startTime;

				const queryStartTime = Date.now();
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);
				const queryTime = Date.now() - queryStartTime;

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex expressions are present
				expect(result.sql).toContain("UPPER");
				expect(result.sql).toContain("CONCAT");
				expect(result.sql).toContain("SUBSTRING");
				expect(result.sql).toContain("LOWER");
				expect(result.sql).toContain("LENGTH");
				expect(result.sql).toContain("+"); // ADD becomes +
				expect(result.sql).toContain("*"); // MULTIPLY becomes *
				expect(result.sql).toContain("/"); // DIVIDE becomes /
				expect(result.sql).toContain("CASE WHEN");

				// Performance assertions
				expect(parseTime).toBeLessThan(1500); // Should parse complex expressions quickly
				expect(queryTime).toBeLessThan(8000); // Should execute complex expressions reasonably fast
			});
		});

		it("should handle maximum complexity conditions without errors", async () => {
			await db.executeInTransaction(async () => {
				// Create the most complex condition possible
				const maxComplexityCondition: Condition = {
					$and: [
						{
							$or: [
								{
									"users.id": {
										$in: Array.from(
											{ length: 100 },
											(_, i) => `550e840${i.toString().padStart(1, "0")}-e29b-41d4-a716-44665544000${(i % 10).toString()}`,
										),
									},
								},
								{
									"users.name": {
										$in: Array.from({ length: 50 }, (_, i) => `user_${i}`),
									},
								},
							],
						},
						{
							$and: [
								{ "users.metadata->deep->level1->level2->level3->value": "test" },
								{ "users.metadata->complex->nested->item": "first" },
								{ "users.metadata->settings->advanced->features->enabled": true },
							],
						},
						{
							$or: [
								{
									"users.age": {
										$gt: {
											$func: {
												ADD: [
													{
														$func: {
															MULTIPLY: [{ $field: "users.age" }, { $func: { COALESCE_NUMBER: [{ $field: "users.age" }, 25] } }],
														},
													},
													{
														$func: {
															DIVIDE: [
																{ $func: { LENGTH: [{ $field: "users.name" }] } },
																{ $func: { GREATEST_NUMBER: [1, { $var: "score_threshold" }] } },
															],
														},
													},
												],
											},
										},
									},
								},
								{
									"users.email": {
										$like: {
											$func: {
												CONCAT: [
													{ $func: { LOWER: ["users.name"] } },
													"_",
													{ $func: { SUBSTRING: ["users.status", 1, 3] } },
													"@",
													{ $func: { UPPER: [{ $var: "adminRole" }] } },
													".com",
												],
											},
										},
									},
								},
							],
						},
						{
							$and: [
								{
									$exists: {
										table: "posts",
										condition: {
											$and: [
												{ "posts.user_id": { $eq: { $field: "users.id" } } },
												{ "posts.published": true },
												{
													$or: [
														{ "posts.title": { $like: "%important%" } },
														{ "posts.tags->priority": "high" },
														{ "posts.tags->featured": true },
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
												{ "orders.amount": { $gte: 100 } },
												{ "orders.status": { $in: ["completed", "shipped"] } },
											],
										},
									},
								},
							],
						},
						{
							$not: {
								$or: [
									{
										$and: [
											{ "users.status": { $in: ["banned", "suspended"] } },
											{ "users.metadata->violations->count": { $gte: 3 } },
										],
									},
									{
										$and: [{ "users.age": { $lt: 13 } }, { "users.metadata->parental_consent": { $ne: true } }],
									},
								],
							},
						},
					],
				};

				const startTime = Date.now();
				const result = extractSelectWhereClause(maxComplexityCondition, config, "users");
				const parseTime = Date.now() - startTime;

				const queryStartTime = Date.now();
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);
				const queryTime = Date.now() - queryStartTime;

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);

				// Should handle extreme complexity
				expect(result.params.length).toBeGreaterThan(150);
				expect(result.sql.length).toBeGreaterThan(1000);

				// Performance should still be reasonable
				expect(parseTime).toBeLessThan(3000); // 3 seconds max for parsing
				expect(queryTime).toBeLessThan(15000); // 15 seconds max for execution
			});
		});

		it("should handle concurrent complex queries", async () => {
			// Test multiple complex queries running concurrently
			const complexCondition: Condition = {
				$and: [
					{
						$or: [
							{ "users.status": { $in: ["premium", "gold", "platinum"] } },
							{ "users.metadata->tier": { $in: ["advanced", "pro"] } },
						],
					},
					{
						$and: [
							{ "users.age": { $gte: 25, $lte: 55 } },
							{ "users.active": true },
							{
								$exists: {
									table: "orders",
									condition: {
										$and: [{ "orders.customer_id": { $eq: { $field: "users.id" } } }, { "orders.amount": { $gte: 200 } }],
									},
								},
							},
						],
					},
				],
			};

			const queries = Array.from({ length: 5 }, async () => {
				return await db.executeInTransaction(async () => {
					const result = extractSelectWhereClause(complexCondition, config, "users");
					const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
					const rows = await db.query(sql, result.params);
					return rows;
				});
			});

			const startTime = Date.now();
			const results = await Promise.all(queries);
			const totalTime = Date.now() - startTime;

			// All queries should succeed
			for (const result of results) {
				expect(result).toBeDefined();
				expect(Array.isArray(result)).toBe(true);
			}

			// Concurrent execution should be reasonable
			expect(totalTime).toBeLessThan(10000); // 10 seconds for 5 concurrent queries
		});

		it("should handle mixed data types in complex conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [
								{
									"users.id": {
										$in: [
											"550e8400-e29b-41d4-a716-446655440000",
											"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
											"6ba7b811-9dad-11d1-80b4-00c04fd430c8",
											"7ba7b812-9dad-11d1-80b4-00c04fd430c9",
											"7ba7b812-9dad-11d1-80b4-00c04fd430ca",
										],
									},
								},
								{ "users.name": { $in: ["Alice", "Bob", "Charlie"] } },
								{ "users.active": { $in: [true, false] } },
							],
						},
						{
							$and: [
								{ "users.age": { $gte: 18, $lte: 99 } },
								{ "users.email": { $like: "%@%" } },
								{
									$or: [
										{ "users.metadata->score": { $gte: 85.5 } },
										{ "users.metadata->verified": true },
										{ "users.metadata->level": { $in: ["gold", "platinum"] } },
									],
								},
							],
						},
						{
							$not: {
								$and: [{ "users.status": { $in: ["banned", "suspended"] } }, { "users.metadata->restricted": true }],
							},
						},
					],
				};

				const result = extractSelectWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql} LIMIT 10`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify all data types are handled
				expect(result.params).toContain("550e8400-e29b-41d4-a716-446655440000"); // UUID
				expect(result.params).toContain("Alice"); // string
				expect(result.params).toContain(true); // boolean
				expect(result.params).toContain(85.5); // float
			});
		});
	});
});
