import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../src/parsers/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../src/parsers/select";
import { parseWhereClause } from "../src/parsers/where";
import type { Condition } from "../src/schemas";
import type { Config } from "../src/types";

describe("Comprehensive Integration Tests", () => {
	let testConfig: Config;

	beforeEach(() => {
		testConfig = {
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
						{ name: "balance", type: "number", nullable: true },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "updated_at", type: "string", nullable: true },
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
						{ name: "view_count", type: "number", nullable: true },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "category_id", type: "number", nullable: true },
					],
				},
				categories: {
					allowedFields: [
						{ name: "id", type: "number", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "description", type: "string", nullable: true },
						{ name: "parent_id", type: "number", nullable: true },
					],
				},
				comments: {
					allowedFields: [
						{ name: "id", type: "number", nullable: false },
						{ name: "post_id", type: "number", nullable: false },
						{ name: "user_id", type: "number", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "is_approved", type: "boolean", nullable: false },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "number", nullable: false },
						{ name: "user_id", type: "number", nullable: false },
						{ name: "total_amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "shipping_address", type: "object", nullable: true },
					],
				},
			},
			variables: {
				"auth.uid": 123,
				current_user: 456,
				admin_user: 1,
				max_age: 100,
				min_balance: 0,
				default_status: "active",
				app_version: "2.1.0",
			},
			relationships: [
				{
					table: "users",
					field: "id",
					toTable: "posts",
					toField: "user_id",
					type: "one-to-many",
				},
				{
					table: "posts",
					field: "id",
					toTable: "comments",
					toField: "post_id",
					type: "one-to-many",
				},
				{
					table: "users",
					field: "id",
					toTable: "comments",
					toField: "user_id",
					type: "one-to-many",
				},
				{
					table: "users",
					field: "id",
					toTable: "orders",
					toField: "user_id",
					type: "one-to-many",
				},
				{
					table: "categories",
					field: "id",
					toTable: "posts",
					toField: "category_id",
					type: "one-to-many",
				},
			],
		};
	});

	describe("Complex Select Queries", () => {
		it("should handle multi-table joins with complex conditions", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
						posts: {
							id: true,
							title: true,
							published: true,
							comments: {
								id: true,
								content: true,
								is_approved: true,
							},
						},
					},
					condition: {
						$and: [
							{ "users.active": { $eq: true } },
							{ "users.age": { $gte: 18 } },
							{
								$or: [{ "users.status": { $eq: "premium" } }, { "users.balance": { $gt: 100 } }],
							},
						],
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);

			expect(sql).toContain("SELECT");
			expect(sql).toContain("FROM users");
			expect(sql).toContain("LEFT JOIN");
			expect(sql).toContain("posts");
			expect(sql).toContain("comments");
			expect(sql).toContain("WHERE");
			expect(sql).toContain("AND");
			expect(sql).toContain("OR");

			expect(query.params).toEqual([true, 18, "premium", 100]);
		});

		it("should handle JSON path selections with complex filtering", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						"metadata->profile->avatar": true,
						"metadata->preferences->theme": true,
						posts: {
							title: true,
							"tags->categories": true,
							"tags->keywords": true,
						},
					},
					condition: {
						$and: [
							{ "users.metadata->profile->verified": { $eq: true } },
							{ "users.metadata->preferences->notifications": { $eq: "enabled" } },
							{
								$exists: {
									table: "posts",
									conditions: {
										"posts.published": { $eq: true },
										"posts.tags->featured": { $eq: true },
									},
								},
							},
						],
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);

			expect(sql).toContain("metadata");
			expect(sql).toContain("profile");
			expect(sql).toContain("avatar");
			expect(sql).toContain("preferences");
			expect(sql).toContain("theme");
			expect(sql).toContain("EXISTS");

			expect(query.params).toEqual([true, "enabled", true, true]);
		});

		it("should handle expressions in selections", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						full_name: { $expr: { CONCAT: ["users.name", " (", "users.email", ")"] } },
						age_group: { $expr: { CONCAT: ["age_", "users.age"] } },
						normalized_email: { $expr: { LOWER: ["users.email"] } },
						posts: {
							title: true,
							title_length: { $expr: { LENGTH: ["posts.title"] } },
							view_ratio: { $expr: { DIVIDE: ["posts.view_count", 100] } },
						},
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);

			expect(sql).toContain("CONCAT");
			expect(sql).toContain("LOWER");
			expect(sql).toContain("LENGTH");
			expect(sql).toContain("/");
		});

		it("should handle complex nested relationships", () => {
			const query = parseSelectQuery(
				{
					rootTable: "categories",
					selection: {
						id: true,
						name: true,
						posts: {
							id: true,
							title: true,
							user_id: true,
							comments: {
								id: true,
								content: true,
								user_id: true,
							},
						},
					},
					condition: {
						$and: [
							{ "categories.name": { $like: "Tech%" } },
							{
								$exists: {
									table: "posts",
									conditions: {
										$and: [
											{ "posts.published": { $eq: true } },
											{
												$exists: {
													table: "comments",
													conditions: {
														"comments.is_approved": { $eq: true },
													},
												},
											},
										],
									},
								},
							},
						],
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);

			expect(sql).toContain("categories");
			expect(sql).toContain("posts");
			expect(sql).toContain("comments");
			expect(sql).toContain("EXISTS");
			expect(sql).toContain("LIKE");

			expect(query.params).toEqual(["Tech%", true, true]);
		});
	});

	describe("Advanced Aggregation Queries", () => {
		it("should handle complex aggregation with multiple group by fields", () => {
			const query = parseAggregationQuery(
				{
					table: "posts",
					groupBy: ["user_id", "published", "category_id"],
					aggregatedFields: {
						total_posts: { operator: "COUNT", field: "*" },
						avg_views: { operator: "AVG", field: "view_count" },
						max_views: { operator: "MAX", field: "view_count" },
						min_views: { operator: "MIN", field: "view_count" },
						total_views: { operator: "SUM", field: "view_count" },
					},
				},
				testConfig,
			);

			const sql = compileAggregationQuery(query);

			expect(sql).toContain("COUNT(*)");
			expect(sql).toContain("AVG(posts.view_count)");
			expect(sql).toContain("MAX(posts.view_count)");
			expect(sql).toContain("MIN(posts.view_count)");
			expect(sql).toContain("SUM(posts.view_count)");
			expect(sql).toContain("GROUP BY");
			expect(sql).toContain("posts.user_id");
			expect(sql).toContain("posts.published");
			expect(sql).toContain("posts.category_id");
		});

		it("should handle aggregation with expression fields", () => {
			const query = parseAggregationQuery(
				{
					table: "users",
					groupBy: ["status"],
					aggregatedFields: {
						user_count: { operator: "COUNT", field: "*" },
						avg_name_length: {
							operator: "AVG",
							field: { $expr: { LENGTH: [{ $expr: "users.name" }] } },
						},
						total_balance: { operator: "SUM", field: "balance" },
						max_age: {
							operator: "MAX",
							field: { $expr: { COALESCE: [{ $expr: "users.age" }, 0] } },
						},
					},
				},
				testConfig,
			);

			const sql = compileAggregationQuery(query);

			expect(sql).toContain("COUNT(*)");
			expect(sql).toContain("AVG(LENGTH(users.name))");
			expect(sql).toContain("SUM(users.balance)");
			expect(sql).toContain("MAX(COALESCE(users.age, 0))");
		});

		it("should handle aggregation with JSON field access", () => {
			const query = parseAggregationQuery(
				{
					table: "orders",
					groupBy: ["status"],
					aggregatedFields: {
						order_count: { operator: "COUNT", field: "*" },
						avg_amount: { operator: "AVG", field: "total_amount" },
						unique_cities: {
							operator: "COUNT",
							field: "shipping_address->city",
						},
					},
				},
				testConfig,
			);

			const sql = compileAggregationQuery(query);

			expect(sql).toContain("COUNT(*)");
			expect(sql).toContain("AVG(orders.total_amount)");
			expect(sql).toContain("shipping_address");
			expect(sql).toContain("city");
		});
	});

	describe("Complex Condition Combinations", () => {
		it("should handle deeply nested logical operations", () => {
			const condition: Condition = {
				$or: [
					{
						$and: [
							{ "users.active": { $eq: true } },
							{ "users.age": { $gte: 21 } },
							{
								$or: [{ "users.status": { $in: ["premium", "vip"] } }, { "users.balance": { $gt: 1000 } }],
							},
						],
					},
					{
						$and: [
							{ "users.status": { $eq: "admin" } },
							{
								$not: {
									"users.metadata->restricted": { $eq: true },
								},
							},
						],
					},
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("OR");
			expect(result.sql).toContain("AND");
			expect(result.sql).toContain("NOT");
			expect(result.sql).toContain("IN");

			expect(result.params).toEqual([true, 21, "premium", "vip", 1000, "admin", true]);
		});

		it("should handle EXISTS conditions with complex subqueries", () => {
			const condition: Condition = {
				$and: [
					{ "users.active": { $eq: true } },
					{
						$exists: {
							table: "posts",
							conditions: {
								$and: [
									{ "posts.published": { $eq: true } },
									{ "posts.view_count": { $gt: 100 } },
									{
										$exists: {
											table: "comments",
											conditions: {
												$and: [{ "comments.is_approved": { $eq: true } }, { "comments.content": { $like: "%excellent%" } }],
											},
										},
									},
								],
							},
						},
					},
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("EXISTS");
			expect(result.sql).toContain("posts");
			expect(result.sql).toContain("comments");

			expect(result.params).toEqual([true, true, 100, true, "%excellent%"]);
		});

		it("should handle mixed comparison operators", () => {
			const condition: Condition = {
				$and: [
					{ "users.age": { $gte: 18, $lte: 65 } },
					{ "users.balance": { $gt: 0, $ne: 999.99 } },
					{ "users.email": { $like: "%@company.com", $regex: "^[a-z]+@" } },
					{ "users.status": { $in: ["active", "premium"], $nin: ["banned", "suspended"] } },
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain(">=");
			expect(result.sql).toContain("<=");
			expect(result.sql).toContain(">");
			expect(result.sql).toContain("!=");
			expect(result.sql).toContain("LIKE");
			expect(result.sql).toContain("~");
			expect(result.sql).toContain("IN");
			expect(result.sql).toContain("NOT IN");

			expect(result.params).toEqual([18, 65, 999.99, 0, "%@company.com", "^[a-z]+@", "active", "premium", "banned", "suspended"]);
		});

		it("should handle conditions with variable references", () => {
			const condition: Condition = {
				$and: [
					{ "users.id": { $eq: { $expr: "auth.uid" } } },
					{ "users.age": { $lt: { $expr: "max_age" } } },
					{ "users.balance": { $gte: { $expr: "min_balance" } } },
					{ "users.status": { $eq: { $expr: "default_status" } } },
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("users.id = 123");
			expect(result.sql).toContain("users.age < 100");
			expect(result.sql).toContain("users.balance >= 0");
			expect(result.sql).toContain("users.status = 'active'");
		});
	});

	describe("Expression Integration Tests", () => {
		it("should handle complex mathematical expressions", () => {
			const condition: Condition = {
				$and: [
					{
						"users.balance": {
							$gt: {
								$expr: {
									MULTIPLY: [{ $expr: { ADD: ["users.age", 10] } }, 100],
								},
							},
						},
					},
					{
						"users.name": {
							$eq: {
								$expr: {
									UPPER: [
										{
											$expr: {
												CONCAT: [{ $expr: { SUBSTRING: ["users.email", 1, 5] } }, "_USER"],
											},
										},
									],
								},
							},
						},
					},
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("*");
			expect(result.sql).toContain("+");
			expect(result.sql).toContain("UPPER");
			expect(result.sql).toContain("CONCAT");
		});

		it("should handle string manipulation functions", () => {
			const condition: Condition = {
				$and: [
					{
						"users.email": {
							$like: {
								$expr: {
									LOWER: [{ $expr: { CONCAT: ["%", "users.name", "@%"] } }],
								},
							},
						},
					},
					{
						"users.name": {
							$eq: {
								$expr: {
									UPPER: [{ $expr: { SUBSTRING: ["users.email", 1, 10] } }],
								},
							},
						},
					},
				],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("LOWER");
			expect(result.sql).toContain("UPPER");
			expect(result.sql).toContain("CONCAT");
			expect(result.sql).toContain("SUBSTRING");
		});

		it("should handle aggregation functions in expressions", () => {
			const condition: Condition = {
				"users.balance": {
					$gt: {
						$expr: {
							COALESCE: [{ $expr: "min_balance" }, { $expr: { MULTIPLY: ["users.age", 10] } }, 0],
						},
					},
				},
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.sql).toContain("COALESCE");
			expect(result.sql).toContain("*");
		});
	});

	describe("Performance and Scalability Tests", () => {
		it("should handle large numbers of parameters efficiently", () => {
			const largeInArray = Array.from({ length: 500 }, (_, i) => `user_${i}`);
			const condition: Condition = {
				$and: [{ "users.name": { $in: largeInArray } }, { "users.age": { $in: Array.from({ length: 100 }, (_, i) => i + 18) } }],
			};

			const result = parseWhereClause(condition, testConfig, "users");

			expect(result.params.length).toBe(600); // 500 + 100
			expect(result.sql).toContain("IN");
		});

		it("should handle deeply nested field structures", () => {
			const deepJsonCondition: Condition = {
				"users.metadata->level1->level2->level3->level4->level5": { $eq: "deep_value" },
			};

			const result = parseWhereClause(deepJsonCondition, testConfig, "users");

			expect(result.sql).toContain("metadata");
			expect(result.sql).toContain("level1");
			expect(result.sql).toContain("level5");
			expect(result.params).toEqual(["deep_value"]);
		});

		it("should handle complex query with all features combined", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						display_name: { $expr: { UPPER: ["users.name"] } },
						age_category: { $expr: { CONCAT: ["category_", "users.age"] } },
						"metadata->profile": true,
						posts: {
							id: true,
							title: true,
							word_count: { $expr: { LENGTH: ["posts.content"] } },
							"tags->primary": true,
							comments: {
								id: true,
								short_content: { $expr: { SUBSTRING: ["comments.content", 1, 50] } },
							},
						},
						orders: {
							id: true,
							total_amount: true,
							"shipping_address->city": true,
							"shipping_address->country": true,
						},
					},
					condition: {
						$and: [
							{ "users.active": { $eq: true } },
							{ "users.age": { $gte: 18, $lte: 80 } },
							{
								$or: [{ "users.status": { $in: ["premium", "vip"] } }, { "users.balance": { $gt: 1000 } }],
							},
							{
								$exists: {
									table: "posts",
									conditions: {
										$and: [{ "posts.published": { $eq: true } }, { "posts.view_count": { $gt: 100 } }],
									},
								},
							},
							{
								$not: {
									"users.metadata->flags->restricted": { $eq: true },
								},
							},
						],
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);

			// Verify all features are present
			expect(sql).toContain("SELECT");
			expect(sql).toContain("UPPER");
			expect(sql).toContain("LENGTH");
			expect(sql).toContain("SUBSTRING");
			expect(sql).toContain("LEFT JOIN");
			expect(sql).toContain("WHERE");
			expect(sql).toContain("AND");
			expect(sql).toContain("OR");
			expect(sql).toContain("EXISTS");
			expect(sql).toContain("NOT");
			expect(sql).toContain("IN");

			// Verify parameter count
			expect(query.params.length).toBeGreaterThan(0);
		});
	});
});
