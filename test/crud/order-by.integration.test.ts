/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { OrderBy, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - ORDER BY Operations and Query Execution", () => {
	let testConfig: Config;
	let dbHelper: DatabaseHelper;

	beforeAll(async () => {
		// Setup Docker environment and database
		await setupTestEnvironment();

		dbHelper = new DatabaseHelper();
		await dbHelper.connect();

		testConfig = {
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
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "score", type: "number", nullable: true },
					],
				},
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
						{ name: "created_at", type: "datetime", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: "550e8400-e29b-41d4-a716-446655440001",
			},
			relationships: [
				{
					table: "posts",
					field: "user_id",
					toTable: "users",
					toField: "id",
				},
			],
		};
	});

	afterAll(async () => {
		await dbHelper.disconnect();
		await teardownTestEnvironment();
	});
	describe("Basic ORDER BY functionality", () => {
		it("should execute ORDER BY ASC correctly", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true },
					orderBy,
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains ORDER BY clause
				expect(sql).toContain("ORDER BY users.name ASC");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should execute ORDER BY DESC correctly", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [{ field: "users.created_at", direction: "DESC" }];
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true, created_at: true },
					orderBy,
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains ORDER BY clause
				expect(sql).toContain("ORDER BY users.created_at DESC");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should execute multiple field ORDER BY correctly", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [
					{ field: "users.status", direction: "ASC" },
					{ field: "users.name", direction: "ASC" },
				];
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true, status: true },
					orderBy,
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains ORDER BY clause
				expect(sql).toContain("ORDER BY users.status ASC, users.name ASC");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should execute ORDER BY with WHERE conditions", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true, active: true },
					condition: { "users.active": { $eq: true } },
					orderBy,
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains both WHERE and ORDER BY clauses
				expect(sql).toContain("WHERE users.active = TRUE");
				expect(sql).toContain("ORDER BY users.name ASC");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should execute ORDER BY with JOINS", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [
					{ field: "users.name", direction: "ASC" },
					{ field: "posts.created_at", direction: "DESC" },
				];
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						posts: {
							id: true,
							title: true,
							created_at: true,
						},
					},
					orderBy,
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains JOIN and ORDER BY clauses
				expect(sql).toContain("LEFT JOIN posts ON users.id = posts.user_id");
				expect(sql).toContain("ORDER BY users.name ASC, posts.created_at DESC");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should execute ORDER BY with PAGINATION", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [{ field: "users.created_at", direction: "DESC" }];
				const query: SelectQuery = {
					rootTable: "users",
					selection: { id: true, name: true, created_at: true },
					orderBy,
					pagination: { limit: 5, offset: 2 },
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify SQL contains ORDER BY, LIMIT, and OFFSET clauses
				expect(sql).toContain("ORDER BY users.created_at DESC");
				expect(sql).toContain("LIMIT 5");
				expect(sql).toContain("OFFSET 2");

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});

		it("should maintain correct SQL clause order", async () => {
			await dbHelper.executeInTransaction(async () => {
				const orderBy: OrderBy = [{ field: "users.name", direction: "ASC" }];
				const query: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						posts: {
							id: true,
							title: true,
						},
					},
					condition: { "users.active": { $eq: true } },
					orderBy,
					pagination: { limit: 10, offset: 5 },
				};

				const sql = buildSelectQuery(query, testConfig);

				// Verify correct SQL clause order: SELECT FROM JOIN WHERE ORDER BY LIMIT OFFSET
				const selectIndex = sql.indexOf("SELECT");
				const fromIndex = sql.indexOf("FROM");
				const joinIndex = sql.indexOf("LEFT JOIN");
				const whereIndex = sql.indexOf("WHERE");
				const orderByIndex = sql.indexOf("ORDER BY");
				const limitIndex = sql.indexOf("LIMIT");
				const offsetIndex = sql.indexOf("OFFSET");

				expect(selectIndex).toBe(0);
				expect(fromIndex).toBeGreaterThan(selectIndex);
				expect(joinIndex).toBeGreaterThan(fromIndex);
				expect(whereIndex).toBeGreaterThan(joinIndex);
				expect(orderByIndex).toBeGreaterThan(whereIndex);
				expect(limitIndex).toBeGreaterThan(orderByIndex);
				expect(offsetIndex).toBeGreaterThan(limitIndex);

				// Execute query to verify it works
				const rows = await dbHelper.query(sql);
				expect(Array.isArray(rows)).toBe(true);
			});
		});
	});
});
