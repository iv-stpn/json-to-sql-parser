/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { parseSelectQuery } from "../../src/builders/select";
import type { Condition } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

// Helper function to extract WHERE clause from parsed select query
function extractSelectWhereClause(condition: Condition, config: Config, rootTable: string): { sql: string; params: unknown[] } {
	const query = {
		rootTable,
		selection: { [`${rootTable}.id`]: true }, // minimal selection
		condition,
	};
	const parsedQuery = parseSelectQuery(query, config);
	return { sql: parsedQuery.where || "", params: parsedQuery.params };
}

describe("Integration Tests - Complex Conditions Performance & Edge Cases", () => {
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
				currentUserId: "550e8400-e29b-41d4-a716-446655440000",
				adminRole: "admin",
				testPattern: "test_%",
				maxResults: 100,
				scoreThreshold: 85.5,
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

	describe("High Parameter Count Conditions", () => {
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

		it("should handle deeply nested conditions with many branches", async () => {
			await db.executeInTransaction(async () => {
				// Create a condition with 20 levels of nesting
				const createNestedCondition = (depth: number): Condition => {
					if (depth === 0) {
						return { "users.active": true };
					}

					return {
						$and: [
							createNestedCondition(depth - 1),
							{
								$or: [
									{ "users.name": { $like: `%level${depth}%` } },
									{ "users.age": { $gt: depth } },
									{ "users.status": `status_${depth}` },
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
	});

	describe("Complex JSON Path Conditions", () => {
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
	});

	describe("Complex Expression Performance", () => {
		it("should handle multiple nested expression functions efficiently", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							"users.name": {
								$eq: {
									$expr: {
										UPPER: [
											{
												$expr: {
													CONCAT: [
														{
															$expr: {
																SUBSTRING: [
																	{ $expr: { LOWER: [{ $expr: "users.email" }] } },
																	1,
																	{ $expr: { LENGTH: [{ $expr: "users.name" }] } },
																],
															},
														},
														"_",
														{ $expr: { UPPER: ["users.status"] } },
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
									$expr: {
										ADD: [
											{
												$expr: {
													MULTIPLY: [{ $expr: "users.age" }, 2],
												},
											},
											{
												$expr: {
													DIVIDE: [{ $expr: { LENGTH: [{ $expr: "users.name" }] } }, 3],
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
													$expr: { UPPER: ["senior"] },
												},
												else: {
													$cond: {
														if: { "users.age": { $gte: 18 } },
														then: {
															$expr: { CONCAT: ["adult_", { $expr: "users.status" }] },
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
											$expr: {
												CONCAT: [
													"%",
													{ $expr: { LOWER: ["users.name"] } },
													"%@%",
													{ $expr: { SUBSTRING: ["users.status", 1, 3] } },
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
	});

	describe("Complex Aggregation Performance", () => {
		it("should execute aggregations with complex field expressions", async () => {
			await db.executeInTransaction(async () => {
				const query = parseAggregationQuery(
					{
						table: "users",
						groupBy: ["status", "active"],
						aggregatedFields: {
							total_users: { operator: "COUNT", field: "*" },
							avg_age: { operator: "AVG", field: "age" },
							min_age: { operator: "MIN", field: "age" },
							max_age: { operator: "MAX", field: "age" },
							name_lengths: {
								operator: "AVG",
								field: { $expr: { LENGTH: [{ $expr: "name" }] } },
							},
							complex_calc: {
								operator: "SUM",
								field: {
									$expr: {
										ADD: [{ $expr: { MULTIPLY: [{ $expr: "age" }, 2] } }, { $expr: { LENGTH: [{ $expr: "name" }] } }],
									},
								},
							},
						},
					},
					config,
				);

				const sql = compileAggregationQuery(query);
				const rows = await db.query(sql, query.params);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify aggregation structure
				expect(sql).toContain("GROUP BY");
				expect(sql).toContain("COUNT(*)");
				expect(sql).toContain("AVG(");
				expect(sql).toContain("MIN(");
				expect(sql).toContain("MAX(");
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("+"); // ADD becomes +
				expect(sql).toContain("*"); // MULTIPLY becomes *
			});
		});
	});

	describe("Stress Test Scenarios", () => {
		it("should handle maximum complexity conditions without errors", async () => {
			await db.executeInTransaction(async () => {
				// Create the most complex condition possible
				const maxComplexityCondition: Condition = {
					$and: [
						// Large array operations
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
						// Deep JSON paths
						{
							$and: [
								{ "users.metadata->deep->level1->level2->level3->value": "test" },
								{ "users.metadata->complex->nested->item": "first" },
								{ "users.metadata->settings->advanced->features->enabled": true },
							],
						},
						// Complex expressions
						{
							$or: [
								{
									"users.age": {
										$gt: {
											$expr: {
												ADD: [
													{
														$expr: {
															MULTIPLY: [{ $expr: "users.age" }, { $expr: { COALESCE_NUMBER: [{ $expr: "users.age" }, 25] } }],
														},
													},
													{
														$expr: {
															DIVIDE: [
																{ $expr: { LENGTH: [{ $expr: "users.name" }] } },
																{ $expr: { GREATEST_NUMBER: [1, { $expr: "scoreThreshold" }] } },
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
											$expr: {
												CONCAT: [
													{ $expr: { LOWER: ["users.name"] } },
													"_",
													{ $expr: { SUBSTRING: ["users.status", 1, 3] } },
													"@",
													{ $expr: { UPPER: [{ $expr: "adminRole" }] } },
													".com",
												],
											},
										},
									},
								},
							],
						},
						// Multiple EXISTS conditions
						{
							$and: [
								{
									$exists: {
										table: "posts",
										conditions: {
											$and: [
												{ "posts.user_id": { $eq: { $expr: "users.id" } } },
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
										conditions: {
											$and: [
												{ "orders.customer_id": { $eq: { $expr: "users.id" } } },
												{ "orders.amount": { $gte: 100 } },
												{ "orders.status": { $in: ["completed", "shipped"] } },
											],
										},
									},
								},
							],
						},
						// Complex NOT conditions
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

				console.log(
					`Max complexity - Parse time: ${parseTime}ms, Query time: ${queryTime}ms, Params: ${result.params.length}, SQL length: ${result.sql.length}`,
				);
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
									conditions: {
										$and: [{ "orders.customer_id": { $eq: { $expr: "users.id" } } }, { "orders.amount": { $gte: 200 } }],
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
					return await db.query(sql, result.params);
				});
			});

			const startTime = Date.now();
			const results = await Promise.all(queries);
			const totalTime = Date.now() - startTime;

			// All queries should succeed
			for (const result of results) {
				expect(result).toBeDefined();
				expect(Array.isArray(result)).toBe(true);
				expect(result.length).toBe(1);
			}

			// Concurrent execution should be reasonable
			expect(totalTime).toBeLessThan(10000); // 10 seconds for 5 concurrent queries
		});
	});
});
