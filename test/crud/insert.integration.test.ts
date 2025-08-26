import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildInsertQuery } from "../../src/builders/insert";
import type { InsertQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - INSERT Operations and Data Persistence", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		// Setup Docker environment and database
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
						{ name: "balance", type: "number", nullable: true },
						{ name: "birth_date", type: "date", nullable: true },
						{ name: "created_at", type: "datetime", nullable: true },
						{ name: "metadata", type: "object", nullable: true },
					],
				},
			},
			variables: {
				current_user_id: "550e8400-e29b-41d4-a716-446655440000",
				current_timestamp: "2024-08-19T10:00:00",
				admin_role: "admin",
			},
			relationships: [],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	it("should insert user with basic values", async () => {
		await db.executeInTransaction(async () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "01234567-89ab-4cde-a123-456789abcdef" },
					name: "Integration Test User",
					email: "test@example.com",
					active: true,
					status: "active",
				},
			};

			const sql = buildInsertQuery(insertQuery, config);
			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "email", "active", "status", "age", "balance", "birth_date", "created_at", "metadata") VALUES ('01234567-89ab-4cde-a123-456789abcdef'::UUID, 'Integration Test User', 'test@example.com', TRUE, 'active', NULL, NULL, NULL, NULL, NULL)`,
			);

			// Execute the insert
			await db.query(sql);

			// Verify the data was inserted
			const rows = await db.query(
				"SELECT id, name, email, active, status FROM users WHERE id = '01234567-89ab-4cde-a123-456789abcdef'",
			);

			expect(rows.length).toBe(1);

			const user = rows[0] as Record<string, unknown>;
			expect(user.id).toBe("01234567-89ab-4cde-a123-456789abcdef");
			expect(user.name).toBe("Integration Test User");
			expect(user.email).toBe("test@example.com");
			expect(user.active).toBe(true);
			expect(user.status).toBe("active");
		});
	});

	it("should insert user with scalar expressions", async () => {
		await db.executeInTransaction(async () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "12345678-9abc-4def-b456-789abcdef012" },
					name: "Date Test User",
					active: true,
					status: "active",
					birth_date: { $date: "1994-08-19" },
					created_at: { $timestamp: "2024-08-19T10:00:00" },
				},
			};

			const sql = buildInsertQuery(insertQuery, config);
			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "active", "status", "birth_date", "created_at", "email", "age", "balance", "metadata") VALUES ('12345678-9abc-4def-b456-789abcdef012'::UUID, 'Date Test User', TRUE, 'active', '1994-08-19'::DATE, '2024-08-19 10:00:00'::TIMESTAMP, NULL, NULL, NULL, NULL)`,
			);

			// Execute the insert
			await db.query(sql);

			// Verify the data was inserted
			const rows = await db.query(
				"SELECT id, name, birth_date, created_at FROM users WHERE id = '12345678-9abc-4def-b456-789abcdef012'",
			);

			expect(rows.length).toBe(1);
			const user = rows[0] as Record<string, unknown>;
			expect(user.id).toBe("12345678-9abc-4def-b456-789abcdef012");
			expect(user.name).toBe("Date Test User");
			expect(user.birth_date).toEqual(new Date("1994-08-19"));
			expect(user.created_at).toEqual(new Date("2024-08-19T10:00:00"));
		});
	});

	it("should fail with invalid UUID format", () => {
		const insertQuery: InsertQuery = {
			table: "users",
			newRow: {
				// biome-ignore lint/suspicious/noExplicitAny: Testing invalid UUID
				id: { $uuid: "invalid-uuid-format" } as any,
				name: "Invalid UUID User",
				active: true,
				status: "active",
			},
		};

		expect(() => buildInsertQuery(insertQuery, config)).toThrow("Invalid UUID format");
	});

	it("should handle null values correctly", async () => {
		await db.executeInTransaction(async () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "23456789-abcd-4ef4-8567-89abcdef0123" },
					name: "Null Test User",
					email: null,
					age: null,
					active: true,
					status: "active",
					balance: null,
				},
			};

			const sql = buildInsertQuery(insertQuery, config);

			// Execute the insert
			await db.query(sql);

			// Verify the data was inserted with null values
			const rows = await db.query(
				"SELECT name, email, age, balance FROM users WHERE id = '23456789-abcd-4ef4-8567-89abcdef0123'",
			);
			expect(rows.length).toBe(1);
			const user = rows[0] as Record<string, unknown>;
			expect(user.name).toBe("Null Test User");
			expect(user.email).toBe(null);
			expect(user.age).toBe(null);
			expect(user.balance).toBe(null);
		});
	});
});
