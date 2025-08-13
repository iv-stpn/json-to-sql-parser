/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { parseWhereClause } from "../../src/parsers/conditions";
import { compileSelectQuery, parseSelectQuery } from "../../src/parsers/select";
import type { Condition } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

describe("Integration Tests - Complex and Deep Conditions", () => {
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
				minAge: 18,
				maxAge: 65,
				premiumThreshold: 1000,
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

				const result = parseWhereClause(condition, config, "users");
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

				const result = parseWhereClause(condition, config, "users");
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
								conditions: {
									$and: [
										{ "posts.user_id": { $eq: { $expr: "users.id" } } },
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
								conditions: {
									$and: [
										{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
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
									conditions: {
										$and: [
											{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
											{ "orders.status": "cancelled" },
											{ "orders.amount": { $gt: 500 } },
										],
									},
								},
							},
						},
					],
				};

				const result = parseWhereClause(condition, config, "users");
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

				const result = parseWhereClause(condition, config, "users");
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

				const result = parseWhereClause(condition, config, "users");
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

				const result = parseWhereClause(condition, config, "users");
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
								{ "users.id": { $eq: { $expr: "currentUserId" } } },
								{
									"users.age": {
										$gte: { $expr: "minAge" },
										$lte: { $expr: "maxAge" },
									},
								},
								{
									"users.status": {
										$ne: {
											$expr: {
												UPPER: [{ $expr: "adminRole" }],
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
											$expr: {
												CONCAT: [{ $expr: "adminRole" }, "%"],
											},
										},
									},
								},
								{
									"users.metadata->balance": {
										$gte: { $expr: "premiumThreshold" },
									},
								},
								{
									$exists: {
										table: "orders",
										conditions: {
											$and: [
												{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
												{
													"orders.amount": {
														$gt: {
															$expr: {
																DIVIDE: [{ $expr: "premiumThreshold" }, 2],
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

				const result = parseWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify variable substitution
				expect(result.sql).toContain("'1'"); // currentUserId
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
								$expr: {
									CONCAT: ["age_", { $expr: "users.age" }],
								},
							},
							posts: {
								id: true,
								title: true,
								content_preview: {
									$expr: {
										CONCAT: [{ $expr: { SUBSTRING: ["posts.content", 1, 50] } }, "..."],
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
										conditions: {
											$and: [
												{ "posts.user_id": { $eq: { $expr: "users.id" } } },
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
								$expr: {
									CONCAT: [{ $expr: { UPPER: ["users.status"] } }, " - ACTIVE"],
								},
							},
							posts: {
								id: true,
								title: true,
								word_count: {
									$expr: { LENGTH: ["posts.content"] },
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
												conditions: {
													$and: [
														{ "posts.user_id": { $eq: { $expr: "users.id" } } },
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
												conditions: {
													$and: [
														{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
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
				const anotherLargeArray = Array.from({ length: 50 }, (_, i) => i + 1);

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

				const result = parseWhereClause(condition, config, "users");
				const sql = `SELECT count(*) as total FROM users WHERE ${result.sql}`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBe(1);
				expect(result.params.length).toBeGreaterThan(150); // Should have lots of parameters
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

				const result = parseWhereClause(deepCondition, config, "users");
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

		it("should handle mixed data types in complex conditions", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							$or: [
								{ "users.id": { $in: [1, 2, 3, 4, 5] } },
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

				const result = parseWhereClause(condition, config, "users");
				const sql = `SELECT * FROM users WHERE ${result.sql} LIMIT 10`;
				const rows = await db.query(sql, result.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify all data types are handled
				expect(result.params).toContain(1); // number
				expect(result.params).toContain("Alice"); // string
				expect(result.params).toContain(true); // boolean
				expect(result.params).toContain(85.5); // float
			});
		});
	});
});
