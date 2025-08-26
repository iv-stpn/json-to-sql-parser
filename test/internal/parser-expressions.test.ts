/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { parseExpression } from "../../src/parsers";

import type { AnyExpression, Condition } from "../../src/schemas";
import type { Config, ParserState } from "../../src/types";
import { ExpressionTypeMap } from "../../src/utils/expression-map";
import { extractSelectWhereClause } from "../_helpers";

describe("Parser - Complex Queries and Expressions", () => {
	let testConfig: Config;

	beforeEach(() => {
		testConfig = {
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
						{ name: "balance", type: "number", nullable: true },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "updated_at", type: "string", nullable: true },
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
						{ name: "view_count", type: "number", nullable: true },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "category_id", type: "uuid", nullable: true },
					],
				},
				categories: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "description", type: "string", nullable: true },
						{ name: "parent_id", type: "uuid", nullable: true },
					],
				},
				comments: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "post_id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "is_approved", type: "boolean", nullable: false },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "total_amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "created_at", type: "string", nullable: false },
						{ name: "shipping_address", type: "object", nullable: true },
					],
				},
			},
			variables: {
				"auth.uid": "123",
				current_user: "456",
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

	describe("Select Queries - Multi-table Operations", () => {
		it("should parse and compile multi-table joins with complex conditions", () => {
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

			expect(sql).toContain("premium");
			expect(sql).toContain("18");
			expect(sql).toContain("100");
		});

		it("should parse and compile JSON path selections with complex filtering", () => {
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
									condition: {
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
		});

		it("should parse and compile expressions in field selections", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						full_name: { $func: { CONCAT: [{ $field: "users.name" }, " (", { $field: "users.email" }, ")"] } },
						age_group: { $func: { CONCAT: ["age_", { $field: "users.age" }] } },
						normalized_email: { $func: { LOWER: [{ $field: "users.email" }] } },
						posts: {
							title: true,
							title_length: { $func: { LENGTH: [{ $field: "posts.title" }] } },
							view_ratio: { $func: { DIVIDE: [{ $field: "posts.view_count" }, 100] } },
						},
					},
				},
				testConfig,
			);

			const sql = compileSelectQuery(query);
			expect(sql).toBe(
				'SELECT users.id AS "id", (users.name || \' (\' || users.email || \')\') AS "full_name", (\'age_\' || (users.age)::TEXT) AS "age_group", LOWER(users.email) AS "normalized_email", posts.title AS "posts.title", LENGTH(posts.title) AS "posts.title_length", (posts.view_count / 100) AS "posts.view_ratio" FROM users LEFT JOIN posts ON users.id = posts.user_id',
			);
		});

		it("should parse and compile complex nested relationship queries", () => {
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
									condition: {
										$and: [
											{ "posts.published": { $eq: true } },
											{
												$exists: {
													table: "comments",
													condition: {
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
		});
	});

	describe("Aggregation Queries - Advanced Operations", () => {
		it("should parse and compile complex aggregation with multiple group by fields", () => {
			const query = parseAggregationQuery(
				{
					table: "posts",
					groupBy: ["user_id", "published", "category_id"],
					aggregatedFields: {
						total_posts: { function: "COUNT", field: "*" },
						avg_views: { function: "AVG", field: "view_count" },
						max_views: { function: "MAX", field: "view_count" },
						min_views: { function: "MIN", field: "view_count" },
						total_views: { function: "SUM", field: "view_count" },
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

		it("should parse and compile aggregation with expression fields", () => {
			const query = parseAggregationQuery(
				{
					table: "users",
					groupBy: ["status"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "*" },
						avg_name_length: {
							function: "AVG",
							field: { $func: { LENGTH: [{ $field: "users.name" }] } },
						},
						total_balance: { function: "SUM", field: "balance" },
						max_age: {
							function: "MAX",
							field: { $func: { COALESCE_NUMBER: [{ $field: "users.age" }, 0] } },
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

		it("should parse and compile aggregation with JSON field access", () => {
			const query = parseAggregationQuery(
				{
					table: "orders",
					groupBy: ["status"],
					aggregatedFields: {
						order_count: { function: "COUNT", field: "*" },
						avg_amount: { function: "AVG", field: "total_amount" },
						unique_cities: {
							function: "COUNT",
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

	describe("Condition Parsing - Complex Combinations", () => {
		it("should parse deeply nested logical operations", () => {
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toContain("OR");
			expect(sql).toContain("AND");
			expect(sql).toContain("NOT");
			expect(sql).toContain("IN");
		});

		it("should parse EXISTS conditions with complex subqueries", () => {
			const condition: Condition = {
				$and: [
					{ "users.active": { $eq: true } },
					{
						$exists: {
							table: "posts",
							condition: {
								$and: [
									{ "posts.published": { $eq: true } },
									{ "posts.view_count": { $gt: 100 } },
									{
										$exists: {
											table: "comments",
											condition: {
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

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toContain("EXISTS");
			expect(sql).toContain("posts");
			expect(sql).toContain("comments");
		});

		it("should parse mixed comparison operators correctly", () => {
			const condition: Condition = {
				$and: [
					{ "users.age": { $gte: 18, $lte: 65 } },
					{ "users.balance": { $gt: 0, $ne: 999.99 } },
					{ "users.email": { $like: "%@company.com", $regex: "^[a-z]+@" } },
					{ "users.status": { $in: ["active", "premium"], $nin: ["banned", "suspended"] } },
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toContain(">=");
			expect(sql).toContain("<=");
			expect(sql).toContain(">");
			expect(sql).toContain("!=");
			expect(sql).toContain("LIKE");
			expect(sql).toContain("~");
			expect(sql).toContain("IN");
			expect(sql).toContain("NOT IN");
		});

		it("should parse conditions with variable references correctly", () => {
			const condition: Condition = {
				$and: [
					{ "users.id": { $eq: { $var: "auth.uid" } } },
					{ "users.age": { $lt: { $var: "max_age" } } },
					{ "users.balance": { $gte: { $var: "min_balance" } } },
					{ "users.status": { $eq: { $var: "default_status" } } },
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toContain("(users.id)::TEXT = '123'");
			expect(sql).toContain("users.age < 100");
			expect(sql).toContain("users.balance >= 0");
			expect(sql).toContain("users.status = 'active'");
		});
	});

	describe("Expression Parsing - Advanced Integration", () => {
		it("should parse complex mathematical expressions", () => {
			const condition: Condition = {
				$and: [
					{
						"users.balance": {
							$gt: {
								$func: {
									MULTIPLY: [{ $func: { ADD: [{ $field: "users.age" }, 10] } }, 100],
								},
							},
						},
					},
					{
						"users.name": {
							$eq: {
								$func: {
									UPPER: [
										{
											$func: {
												CONCAT: [{ $func: { SUBSTR: ["users.email", 1, 5] } }, "_USER"],
											},
										},
									],
								},
							},
						},
					},
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toBe(
				"(users.balance > ((users.age + 10) * 100) AND users.name = UPPER(SUBSTR('users.email', 1, 5) || '_USER'))",
			);
		});

		it("should parse string manipulation functions", () => {
			const condition: Condition = {
				$and: [
					{
						"users.email": {
							$like: {
								$func: {
									LOWER: [{ $func: { CONCAT: ["%", "users.name", "@%"] } }],
								},
							},
						},
					},
					{
						"users.name": {
							$eq: {
								$func: {
									UPPER: [{ $func: { SUBSTR: ["users.email", 1, 10] } }],
								},
							},
						},
					},
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe(
				"(users.email LIKE (LOWER('%' || 'users.name' || '@%'))::TEXT AND users.name = UPPER(SUBSTR('users.email', 1, 10)))",
			);
		});

		it("should parse aggregation functions in expressions", () => {
			const condition: Condition = {
				"users.balance": {
					$gt: {
						$func: {
							COALESCE_NUMBER: [{ $var: "min_balance" }, { $func: { MULTIPLY: [{ $field: "users.age" }, 10] } }, 0],
						},
					},
				},
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");

			expect(sql).toContain("COALESCE");
			expect(sql).toContain("*");
		});
	});

	describe("Performance Testing - Large Scale Operations", () => {
		it("should handle large arrays efficiently", () => {
			const largeInArray = Array.from({ length: 500 }, (_, i) => `user_${i}`);
			const condition: Condition = {
				$and: [{ "users.name": { $in: largeInArray } }, { "users.age": { $in: Array.from({ length: 100 }, (_, i) => i + 18) } }],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toContain("IN");
		});

		it("should handle deeply nested JSON field structures efficiently", () => {
			const deepJsonCondition: Condition = {
				"users.metadata->level1->level2->level3->level4->level5": { $eq: "deep_value" },
			};

			const sql = extractSelectWhereClause(deepJsonCondition, testConfig, "users");

			expect(sql).toContain("metadata");
			expect(sql).toContain("level1");
			expect(sql).toContain("level5");
		});

		it("should handle comprehensive query with all parser features combined", () => {
			const query = parseSelectQuery(
				{
					rootTable: "users",
					selection: {
						id: true,
						display_name: { $func: { UPPER: ["users.name"] } },
						age_category: { $func: { CONCAT: ["category_", "users.age"] } },
						"metadata->profile": true,
						posts: {
							id: true,
							title: true,
							word_count: { $func: { LENGTH: ["posts.content"] } },
							"tags->primary": true,
							comments: {
								id: true,
								short_content: { $func: { SUBSTR: ["comments.content", 1, 50] } },
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
									condition: {
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
			expect(sql).toBe(
				'SELECT users.id AS "id", UPPER(\'users.name\') AS "display_name", (\'category_\' || \'users.age\') AS "age_category", users.metadata->>\'profile\' AS "metadata->profile", posts.id AS "posts.id", posts.title AS "posts.title", LENGTH(\'posts.content\') AS "posts.word_count", posts.tags->>\'primary\' AS "posts.tags->primary", comments.id AS "comments.id", SUBSTR(\'comments.content\', 1, 50) AS "comments.short_content", orders.id AS "orders.id", orders.total_amount AS "orders.total_amount", orders.shipping_address->>\'city\' AS "orders.shipping_address->city", orders.shipping_address->>\'country\' AS "orders.shipping_address->country" FROM users LEFT JOIN posts ON users.id = posts.user_id LEFT JOIN comments ON posts.id = comments.post_id LEFT JOIN orders ON users.id = orders.user_id WHERE (users.active = TRUE AND (users.age >= 18 AND users.age <= 80) AND (users.status IN (\'premium\', \'vip\') OR users.balance > 1000) AND EXISTS (SELECT 1 FROM posts WHERE (posts.published = TRUE AND posts.view_count > 100)) AND NOT ((users.metadata->\'flags\'->>\'restricted\')::BOOLEAN = TRUE))',
			);
		});
	});

	describe("Expression Parser - Advanced Operations", () => {
		describe("Nested Expression Structures", () => {
			it("should parse deeply nested conditional expressions", () => {
				const condition: Condition = {
					"users.status": {
						$eq: {
							$cond: {
								if: {
									$and: [{ "users.active": { $eq: true } }, { "users.age": { $gte: 18 } }],
								},
								then: {
									$cond: {
										if: { "users.age": { $gte: 65 } },
										then: "senior",
										else: "adult",
									},
								},
								else: "inactive",
							},
						},
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe(
					"users.status = (CASE WHEN (users.active = TRUE AND users.age >= 18) THEN (CASE WHEN users.age >= 65 THEN 'senior' ELSE 'adult' END) ELSE 'inactive' END)",
				);
			});

			it("should parse expressions with mixed argument types", () => {
				const condition: Condition = {
					"users.name": {
						$eq: {
							$func: {
								CONCAT: [{ $field: "users.name" }, " (", { $var: "auth.uid" }, ")"],
							},
						},
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.name = (users.name || ' (' || '123' || ')')");
			});

			it("should parse function calls with nested expression arguments", () => {
				const condition: Condition = {
					"users.age": {
						$gt: {
							$func: {
								ADD: [{ $func: { LENGTH: [{ $field: "users.name" }] } }, 5],
							},
						},
					},
				};
				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.age > (LENGTH(users.name) + 5)");
			});
		});

		describe("Literal Value Processing", () => {
			it("should parse string literals in expressions correctly", () => {
				const condition: Condition = {
					"users.name": {
						$eq: { $func: { CONCAT: ["Hello", "World"] } },
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.name = ('Hello' || 'World')");
			});

			it("should parse numeric literals in expressions correctly", () => {
				const condition: Condition = {
					"users.age": {
						$eq: { $func: { ADD: [25, 5.5] } },
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.age = (25 + 5.5)");
			});
		});

		describe("Error Handling and Validation", () => {
			it("should reject invalid expression structure", () => {
				const condition: Condition = {
					"users.name": {
						$eq: { $func: {} },
					},
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow("$func must contain exactly one function");
			});

			it("should reject multiple functions in single $func", () => {
				const condition: Condition = {
					"users.name": {
						$eq: { $func: { UPPER: ["test"], LOWER: ["test"] } },
					},
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow("$func must contain exactly one function");
			});

			it("should reject empty function names", () => {
				const condition: Condition = {
					"users.name": {
						$eq: { $func: { "": ["test"] } },
					},
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow('Unknown function or operator: ""');
			});
		});

		describe("Direct Expression Evaluation", () => {
			let testState: ParserState;

			beforeEach(() => {
				testState = { config: testConfig, expressions: new ExpressionTypeMap(), rootTable: "users" };
			});

			it("should evaluate scalar values correctly", () => {
				expect(parseExpression("test", testState)).toBe("'test'");
				expect(parseExpression(42, testState)).toBe("42");
				expect(parseExpression(true, testState)).toBe("TRUE");
				expect(parseExpression(null, testState)).toBe("NULL");
			});

			it("should reject invalid expression types", () => {
				const invalidExpr = { $invalid: "test" } as unknown as AnyExpression;
				expect(() => parseExpression(invalidExpr, testState)).toThrow('Invalid expression object: {"$invalid":"test"}');
			});
		});
	});

	describe("Logical Condition Parsing - Complex Operations", () => {
		describe("Nested Logical Operators", () => {
			it("should parse complex AND/OR combinations", () => {
				const condition: Condition = {
					$or: [
						{
							$and: [{ "users.active": { $eq: true } }, { "users.age": { $gte: 18 } }],
						},
						{
							$and: [{ "users.name": { $like: "Admin%" } }, { "users.email": { $ne: null } }],
						},
					],
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("((users.active = TRUE AND users.age >= 18) OR (users.name LIKE 'Admin%' AND users.email IS NOT NULL))");
			});

			it("should parse nested NOT conditions correctly", () => {
				const condition: Condition = {
					$not: {
						$or: [{ "users.active": { $eq: false } }, { "users.email": { $eq: null } }],
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("NOT ((users.active = FALSE OR users.email IS NULL))");
			});
		});

		describe("Mixed Operator Combinations", () => {
			it("should parse multiple operators on same field", () => {
				const condition: Condition = {
					"users.age": { $gte: 18, $lte: 65, $ne: 30 },
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("(users.age != 30 AND users.age >= 18 AND users.age <= 65)");
			});

			it("should parse array operations with embedded expressions", () => {
				const condition: Condition = {
					"users.id": {
						$in: ["1", { $var: "auth.uid" }, { $var: "current_user" }],
					},
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("(users.id)::TEXT IN ('1', '123', '456')");
			});
		});
	});

	describe("Null Value Processing", () => {
		describe("Null Comparisons", () => {
			it("should convert null equality to IS NULL", () => {
				const condition: Condition = {
					"users.email": { $eq: null },
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.email IS NULL");
			});

			it("should convert null inequality to IS NOT NULL", () => {
				const condition: Condition = {
					"users.email": { $ne: null },
				};

				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBe("users.email IS NOT NULL");
			});
		});
	});
});
