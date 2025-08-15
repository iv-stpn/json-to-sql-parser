import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";

import type { AggregationQuery, Condition } from "../../src/schemas";
import type { Config } from "../../src/types";
import { extractSelectWhereClause } from "../_helpers";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "./_helpers";

describe("Integration Tests - Data Table Configuration", () => {
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
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
			],
			dataTable: {
				table: "data_storage",
				dataField: "data",
				tableField: "table_name",
				whereConditions: ["tenant_id = 'current_tenant'", "deleted_at IS NULL"],
			},
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("Condition Queries with Data Table", () => {
		it("should execute simple equality condition on data table", async () => {
			await db.executeInTransaction(async () => {
				const selection = {
					id: true,
					name: true,
					active: true,
				};
				const condition: Condition = { "users.active": true };

				const selectQuery = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(selectQuery);

				const rows = await db.query(sql, selectQuery.params);

				expect(rows.length).toBe(4); // 4 active users in current tenant
			});
		});

		it("should execute complex AND condition on data table", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, age: true, active: true };
				const condition: Condition = {
					$and: [{ "users.active": true }, { "users.age": { $gte: 30 } }],
				};

				const selectQuery = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(selectQuery);
				const rows = await db.query(sql, selectQuery.params);

				expect(rows.length).toBe(2); // John (30) and Charlie (32)
			});
		});

		it("should execute JSON field access condition on data table", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, metadata: true };
				const condition: Condition = {
					"users.metadata->department": "engineering",
				};

				const selectQuery = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(selectQuery);

				const rows = await db.query(sql, selectQuery.params);

				expect(rows.length).toBe(2); // John and Alice
			});
		});

		it("should filter out different tenant data", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, active: true };
				const condition: Condition = { "users.active": true };

				const selectQuery = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(selectQuery);
				const rows = await db.query(sql, selectQuery.params);

				// Should only return current_tenant users (4), not other_tenant users
				expect(rows.length).toBe(4);
			});
		});

		it("should filter out soft-deleted records", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, active: true };
				const condition: Condition = { "users.active": { $in: [true, false] } };

				const selectQuery = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(selectQuery);
				const rows = await db.query(sql, selectQuery.params);

				// Should return 5 users (excludes soft-deleted records due to dataTable config)
				expect(rows.length).toBe(5);
			});
		});
	});

	describe("Select Queries with Data Table", () => {
		it("should execute basic select query on data table", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, email: true };
				const result = parseSelectQuery({ rootTable: "users", selection }, config);
				const sql = compileSelectQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(5); // All users in current tenant (excluding deleted)
				expect(rows[0]).toHaveProperty("id");
				expect(rows[0]).toHaveProperty("name");
				expect(rows[0]).toHaveProperty("email");
			});
		});

		it("should execute select with condition on data table", async () => {
			await db.executeInTransaction(async () => {
				const selection = { id: true, name: true, status: true };
				const condition: Condition = { "users.status": "premium" };
				const result = parseSelectQuery({ rootTable: "users", selection, condition }, config);
				const sql = compileSelectQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBe(2); // John and Alice
			});
		});
	});

	describe("Aggregation Queries with Data Table", () => {
		it("should execute simple aggregation on data table", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { function: "SUM", field: "orders.amount" },
						order_count: { function: "COUNT", field: "orders.id" },
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("status");
				expect(rows[0]).toHaveProperty("total_amount");
				expect(rows[0]).toHaveProperty("order_count");
			});
		});

		it("should execute aggregation with JSON field grouping on data table", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.metadata->department"],
					aggregatedFields: {
						avg_age: { function: "AVG", field: "users.age" },
						user_count: { function: "COUNT", field: "users.id" },
					},
				};

				const result = parseAggregationQuery(aggregationQuery, config);
				const sql = compileAggregationQuery(result);

				const rows = await db.query(sql, result.params);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("metadata->department");
				expect(rows[0]).toHaveProperty("avg_age");
				expect(rows[0]).toHaveProperty("user_count");
			});
		});
	});

	describe("Data Table vs Regular Table Comparison", () => {
		it("should produce different SQL for same logical query", () => {
			const condition: Condition = { "users.active": true };

			// Regular table config (no dataTable)
			const regularConfig = { ...config };
			delete regularConfig.dataTable;

			const regularResult = extractSelectWhereClause(condition, regularConfig, "users");
			const dataTableResult = extractSelectWhereClause(condition, config, "users");

			// The expressions should be different due to data table transformation
			expect(regularResult.sql).toBe("users.active = $1");
			expect(dataTableResult.sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND (users.data->>'active')::BOOLEAN = $1)",
			);
			expect(regularResult.params).toEqual(dataTableResult.params);

			// But when building full queries, they differ significantly
			const regularSelection = { id: true, name: true };
			const regularSelectResult = parseSelectQuery({ rootTable: "users", selection: regularSelection }, regularConfig);
			const dataTableSelectResult = parseSelectQuery({ rootTable: "users", selection: regularSelection }, config);

			const regularSQL = compileSelectQuery(regularSelectResult);
			const dataTableSQL = compileSelectQuery(dataTableSelectResult);

			expect(regularSQL).toContain("FROM users");
			expect(dataTableSQL).toContain('FROM data_storage AS "users"');
			expect(dataTableSQL).toContain("users.table_name = 'users'");
			expect(dataTableSQL).toContain("users.tenant_id = 'current_tenant'");
		});
	});
});
