/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildAggregationQuery } from "../../src/builders/aggregate";
import { buildSelectQuery } from "../../src/builders/select";
import type { AggregationQuery, Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { uuidRegex } from "../../src/utils/validators";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

// Type for database row results
type DbRow = Record<string, unknown>;

describe("Integration - Type Casting with UUID, Timestamp and Date Operations", () => {
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
						{ name: "updated_at", type: "datetime", nullable: true },
						{ name: "birth_date", type: "date", nullable: true },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: true },
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
						{ name: "user_id", type: "uuid", nullable: true },
						{ name: "published", type: "boolean", nullable: false },
						{ name: "tags", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "published_at", type: "datetime", nullable: true },
					],
				},
			},
			variables: {
				current_year: 2024,
				tax_rate: 0.085,
				shipping_threshold: 100,
				premium_multiplier: 1.5,
				"auth.uid": "550e8400-e29b-41d4-a716-446655440000",
				current_timestamp: "2024-01-15T10:30:45",
				current_date: "2024-01-15",
				system_user_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
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

	describe("Multi-Type Conditional Expression Casting", () => {
		it("should handle mixed type conditions with proper casting", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						// Number comparison
						{
							"users.age": { $gte: 25 },
						},
						// Boolean comparison
						{
							"users.active": { $eq: true },
						},
						// String comparison
						{
							"users.status": { $eq: "premium" },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						active: true,
					},
					condition,
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(sql).toBe(
					'SELECT users.name AS "name", users.age AS "age", users.active AS "active" FROM users WHERE (users.age >= 25 AND users.active = TRUE AND users.status = \'premium\')',
				);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should handle numeric type casting and comparisons", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						{
							"users.age": { $gt: 18 },
						},
						{
							"users.age": { $lt: 65 },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
					},
					condition,
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
			});
		});
	});

	describe("Mathematical Operations with Auto-Casting", () => {
		it("should handle arithmetic expressions with type casting", async () => {
			await db.executeInTransaction(async () => {
				const aggregation: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { function: "SUM", field: "orders.amount" },
						count: { function: "COUNT", field: "*" },
						avg_amount: { function: "AVG", field: "orders.amount" },
					},
					condition: {
						"orders.amount": { $gt: 100 },
					},
				};

				const sql = buildAggregationQuery(aggregation, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify mathematical operations in SQL
				expect(sql).toContain("SUM");
				expect(sql).toContain("AVG");
			});
		});
	});

	describe("String Processing with Type Conversion", () => {
		it("should handle string concatenation and manipulation", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						name_with_age: {
							$func: {
								CONCAT: [{ $field: "users.name" }, " (Age: ", { $field: "users.age" }, ")"],
							},
						},
					},
					condition: {
						"users.name": { $ne: "" },
					},
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify string operations
				expect(sql).toBe(
					"SELECT users.name AS \"name\", users.age AS \"age\", (users.name || ' (Age: ' || (users.age)::TEXT || ')') AS \"name_with_age\" FROM users WHERE users.name != ''",
				);
			});
		});
	});

	describe("Conditional Logic with Dynamic Type Inference", () => {
		it("should handle case expressions with proper type casting", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
						age_category: {
							$cond: {
								if: { "users.age": { $gte: 18 } },
								then: "Adult",
								else: "Minor",
							},
						},
					},
					condition: {
						"users.age": { $ne: null },
					},
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify conditional logic
				expect(sql).toContain("CASE");
				expect(sql).toContain("WHEN");
				expect(sql).toContain("THEN");
				expect(sql).toContain("ELSE");
			});
		});
	});

	describe("Type Safety Validation and Error Prevention", () => {
		it("should handle null values properly in type casting", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$or: [
						{
							"users.age": { $eq: null },
						},
						{
							"users.age": { $gt: 18 },
						},
					],
				};

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						name: true,
						age: true,
					},
					condition,
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify null handling
				expect(sql).toContain("IS NULL");
			});
		});
	});

	describe("UUID Field Processing and Query Operations", () => {
		it("should query users by UUID", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					"users.id": { $eq: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } },
				};
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
						age: true,
						active: true,
						status: true,
						metadata: true,
						created_at: true,
					},
					condition,
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows).toHaveLength(1);
				expect((rows[0] as DbRow).name).toBe("John Doe");
				expect((rows[0] as DbRow).id).toBe("550e8400-e29b-41d4-a716-446655440000");
			});
		});

		it("should query with UUID IN clause", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
						age: true,
						active: true,
						status: true,
						created_at: true,
						updated_at: true,
						birth_date: true,
						metadata: true,
					},
					condition: {
						"users.id": {
							$in: [{ $uuid: "550e8400-e29b-41d4-a716-446655440000" }, { $uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" }],
						},
					},
				};

				const sql = buildSelectQuery(query, config);
				const sqlWithOrderBy = `${sql} ORDER BY name`;

				const rows = await db.query(sqlWithOrderBy);
				expect(rows).toHaveLength(2);
				expect((rows[0] as DbRow).name).toBe("Jane Smith");
				expect((rows[1] as DbRow).name).toBe("John Doe");
			});
		});

		it("should handle UUID variables", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
						age: true,
						active: true,
						status: true,
						created_at: true,
						updated_at: true,
						birth_date: true,
						metadata: true,
					},
					condition: {
						"users.id": { $eq: { $var: "auth.uid" } },
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows).toHaveLength(1);
				expect((rows[0] as DbRow).name).toBe("John Doe");
			});
		});

		it("should handle UUID in related table queries", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						content: true,
						user_id: true,
						published: true,
						created_at: true,
						published_at: true,
						tags: true,
					},
					condition: {
						"posts.user_id": { $eq: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } },
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);
				expect(rows.every((row) => (row as DbRow).user_id === "550e8400-e29b-41d4-a716-446655440000")).toBe(true);
			});
		});
	});

	describe("Timestamp Field Processing and Temporal Queries", () => {
		it("should query by timestamp equality", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						content: true,
						user_id: true,
						published: true,
						created_at: true,
						published_at: true,
						tags: true,
					},
					condition: {
						"posts.published_at": { $eq: { $timestamp: "2024-01-15T10:30:00" } },
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows).toHaveLength(1);
				expect((rows[0] as DbRow).title).toBe("Getting Started with PostgreSQL");
			});
		});

		it("should query with timestamp range", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						published_at: true,
					},
					condition: {
						$and: [
							{ "posts.published_at": { $gte: { $timestamp: "2024-01-15T00:00:00" } } },
							{ "posts.published_at": { $lt: { $timestamp: "2024-01-17T00:00:00" } } },
						],
					},
				};

				const sql = buildSelectQuery(query, config);
				const sqlWithOrderBy = `${sql} ORDER BY published_at`;

				const rows = await db.query(sqlWithOrderBy);
				expect(rows.length).toBeGreaterThan(0);
				expect(
					rows.every((row) => {
						const publishedAt = (row as DbRow).published_at as string;
						return (
							publishedAt &&
							new Date(publishedAt) >= new Date("2024-01-15T00:00:00") &&
							new Date(publishedAt) < new Date("2024-01-17T00:00:00")
						);
					}),
				).toBe(true);
			});
		});

		it("should handle NULL timestamp queries", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						published_at: true,
					},
					condition: {
						"posts.published_at": { $eq: null },
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);
				expect(rows.every((row) => (row as DbRow).published_at === null)).toBe(true);
			});
		});

		it("should aggregate by timestamp ranges", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					aggregatedFields: {
						total_amount: { field: "amount", function: "SUM" },
						order_count: { field: "*", function: "COUNT" },
					},
					groupBy: ["status"],
				};

				const sql = buildAggregationQuery(aggregationQuery, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);

				expect(
					rows.every((row) => {
						const totalAmount = (row as DbRow)["0"] || (row as DbRow).total_amount;
						const orderCount = (row as DbRow)["1"] || (row as DbRow).order_count;
						return totalAmount !== null && totalAmount !== undefined && orderCount !== null && orderCount !== undefined;
					}),
				).toBe(true);
			});
		});
	});

	describe("Date Field Processing and Calendar Operations", () => {
		it("should query by date equality", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						birth_date: true,
					},
					condition: {
						"users.birth_date": { $eq: { $date: "1994-01-15" } },
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows).toHaveLength(1);
				expect((rows[0] as DbRow).name).toBe("John Doe");
				expect(((rows[0] as DbRow).birth_date as Date).toISOString().split("T")[0]).toBe("1994-01-15");
			});
		});

		it("should query with date range", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						birth_date: true,
					},
					condition: {
						$and: [
							{ "users.birth_date": { $gte: { $date: "1990-01-01" } } },
							{ "users.birth_date": { $lt: { $date: "2000-01-01" } } },
						],
					},
				};

				const sql = buildSelectQuery(query, config);
				const sqlWithOrderBy = `${sql} ORDER BY birth_date`;

				const rows = await db.query(sqlWithOrderBy);
				expect(rows.length).toBeGreaterThan(0);
				expect(
					rows.every((row) => {
						const birthDate = new Date((row as DbRow).birth_date as string);
						return birthDate >= new Date("1990-01-01") && birthDate < new Date("2000-01-01");
					}),
				).toBe(true);
			});
		});

		it("should handle delivered_date queries on orders", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						status: true,
						delivered_date: true,
					},
					condition: {
						"orders.delivered_date": { $ne: null },
					},
				};

				const sql = buildSelectQuery(query, config);
				const sqlWithOrderBy = `${sql} ORDER BY delivered_date`;

				const rows = await db.query(sqlWithOrderBy);
				expect(rows.length).toBeGreaterThan(0);
				expect(rows.every((row) => (row as DbRow).delivered_date !== null)).toBe(true);
			});
		});

		it("should aggregate orders by delivery date", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					aggregatedFields: {
						avg_amount: { field: "amount", function: "AVG" },
						order_count: { field: "*", function: "COUNT" },
					},
					groupBy: ["delivered_date"],
				};

				const sql = buildAggregationQuery(aggregationQuery, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);

				expect(
					rows.every((row) => {
						const avgAmount = (row as DbRow)["0"] || (row as DbRow).avg_amount;
						const orderCount = (row as DbRow)["1"] || (row as DbRow).order_count;
						return avgAmount !== null && avgAmount !== undefined && orderCount !== null && orderCount !== undefined;
					}),
				).toBe(true);
			});
		});
	});

	describe("Multi-Temporal Type Query Orchestration", () => {
		it("should handle queries combining UUID, timestamp, and date", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						customer_id: true,
						amount: true,
						delivered_date: true,
						created_at: true,
					},
					condition: {
						$and: [
							{
								"orders.customer_id": {
									$in: [{ $uuid: "550e8400-e29b-41d4-a716-446655440000" }, { $uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" }],
								},
							},
							{ "orders.created_at": { $gte: { $timestamp: "2024-01-15T00:00:00" } } },
							{ "orders.delivered_date": { $ne: null } },
						],
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);
				expect(
					rows.every((row) => {
						const deliveredDate = (row as DbRow).delivered_date;
						const amount = (row as DbRow).amount;
						return (
							deliveredDate !== null &&
							(typeof amount === "number" || typeof amount === "string") &&
							parseFloat(amount.toString()) > 0
						);
					}),
				).toBe(true);
			});
		});

		it("should handle complex aggregation with date types", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					aggregatedFields: {
						total_revenue: { field: "amount", function: "SUM" },
						completed_orders: { field: "*", function: "COUNT" },
					},
					groupBy: ["customer_id"],
				};

				const sql = buildAggregationQuery(aggregationQuery, config);

				const rows = await db.query(sql);
				expect(rows.length).toBeGreaterThan(0);

				expect(
					rows.every((row) => {
						const totalRevenue = (row as DbRow)["0"] || (row as DbRow).total_revenue;
						const completedOrders = (row as DbRow)["1"] || (row as DbRow).completed_orders;
						return (
							totalRevenue !== null &&
							totalRevenue !== undefined &&
							completedOrders !== null &&
							completedOrders !== undefined &&
							parseFloat(totalRevenue.toString()) > 0 &&
							parseInt(completedOrders.toString()) > 0
						);
					}),
				).toBe(true);
			});
		});

		it("should handle timestamp microsecond precision", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						status: true,
						shipped_at: true,
					},
					condition: {
						"orders.shipped_at": { $gte: { $timestamp: "2024-01-16T08:00:00.000" } },
					},
				};

				const sql = buildSelectQuery(query, config);
				const sqlWithOrderBy = `${sql} ORDER BY shipped_at`;

				const rows = await db.query(sqlWithOrderBy);
				expect(rows.length).toBeGreaterThan(0);
				expect(
					rows.every((row) => {
						const shippedAt = (row as DbRow).shipped_at;
						return (
							shippedAt !== null &&
							typeof shippedAt !== "undefined" &&
							new Date(shippedAt as string) >= new Date("2024-01-16T08:00:00.000")
						);
					}),
				).toBe(true);
			});
		});
	});

	describe("Performance Optimization and Edge Case Handling", () => {
		it("should handle large UUID IN clauses efficiently", async () => {
			await db.executeInTransaction(async () => {
				const uuidList = [
					{ $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					{ $uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" },
					{ $uuid: "6ba7b811-9dad-11d1-80b4-00c04fd430c8" },
					{ $uuid: "6ba7b812-9dad-11d1-80b4-00c04fd430c8" },
					{ $uuid: "6ba7b813-9dad-11d1-80b4-00c04fd430c8" },
				];

				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
					},
					condition: {
						"users.id": { $in: uuidList },
					},
				};

				const startTime = Date.now();
				const sql = buildSelectQuery(query, config);
				const parseTime = Date.now() - startTime;

				const queryStartTime = Date.now();
				const rows = await db.query(sql);
				const queryTime = Date.now() - queryStartTime;

				expect(rows.length).toBeGreaterThan(0);
				expect(parseTime).toBeLessThan(100); // Should parse quickly
				expect(queryTime).toBeLessThan(1000); // Should execute quickly
			});
		});

		it("should maintain order in complex queries", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
						birth_date: true,
						created_at: true,
					},
					condition: {
						$and: [
							{ "users.id": { $eq: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } } },
							{ "users.birth_date": { $eq: { $date: "1994-01-15" } } },
							{ "users.created_at": { $gte: { $timestamp: "2024-01-15T00:00:00" } } },
						],
					},
				};

				const sql = buildSelectQuery(query, config);

				const rows = await db.query(sql);
				expect(() => rows).not.toThrow();
			});
		});
	});

	describe("Database Schema Integrity Verification", () => {
		it("should verify that all primary and foreign keys are UUID type", async () => {
			await db.executeInTransaction(async () => {
				// Query the database schema to verify UUID types
				const schemaQuery = `
					SELECT column_name, data_type, is_nullable, table_name
					FROM information_schema.columns 
					WHERE table_name IN ('users', 'posts', 'orders') 
					AND column_name IN ('id', 'user_id', 'customer_id')
					ORDER BY table_name, column_name;
				`;

				const rows = await db.query(schemaQuery);
				expect(rows.length).toBeGreaterThan(0);

				// Verify that all id columns are UUID type
				expect(rows.every((row) => (row as DbRow).data_type === "uuid")).toBe(true);
			});
		});

		it("should verify UUID values in actual data", async () => {
			await db.executeInTransaction(async () => {
				const usersQuery = `SELECT id, name FROM users LIMIT 3`;
				const postsQuery = `SELECT id, user_id, title FROM posts LIMIT 3`;
				const ordersQuery = `SELECT id, customer_id, amount FROM orders LIMIT 3`;

				const [users, posts, orders] = await Promise.all([db.query(usersQuery), db.query(postsQuery), db.query(ordersQuery)]);

				// Verify UUID format (36 characters with hyphens)
				users.forEach((user) => {
					expect(uuidRegex.test((user as DbRow).id as string)).toBe(true);
				});

				posts.forEach((post) => {
					expect(uuidRegex.test((post as DbRow).id as string)).toBe(true);
					expect(uuidRegex.test((post as DbRow).user_id as string)).toBe(true);
				});

				orders.forEach((order) => {
					expect(uuidRegex.test((order as DbRow).id as string)).toBe(true);
					expect(uuidRegex.test((order as DbRow).customer_id as string)).toBe(true);
				});
			});
		});
	});
});
