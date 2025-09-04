import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Config, DeleteQuery } from "../../src";
import { buildDeleteQuery } from "../../src/builders/delete";
import { Dialect } from "../../src/constants/dialects";
import { DatabaseHelper, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - DELETE Operations and Data Persistence", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		// Setup Docker environment and database
		await setupTestEnvironment();

		db = new DatabaseHelper();
		await db.connect();

		config = {
			dialect: Dialect.POSTGRESQL,
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "age", type: "number", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "balance", type: "number", nullable: true },
						{ name: "status", type: "string", nullable: false },
						{ name: "role", type: "string", nullable: false },
						{ name: "created_at", type: "datetime", nullable: true },
						{ name: "updated_at", type: "datetime", nullable: true },
						{ name: "birth_date", type: "date", nullable: true },
						{ name: "metadata", type: "object", nullable: true },
					],
				},
			},
			variables: {},
			relationships: [],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("Integration - DELETE Operations and Data Persistence", () => {
		test("should delete user with simple condition", async () => {
			await db.executeInTransaction(async () => {
				// First, insert a test user with a unique ID
				const testUserId = "aaaaaaaa-bbbb-cccc-dddd-111111111111";
				await db.query(`
				INSERT INTO users (id, name, email, active, age, status, role, birth_date, created_at, updated_at, balance, metadata) 
				VALUES ('${testUserId}', 'Delete Test User', 'deletetest@example.com', false, 25, 'inactive', 'user', NULL, NULL, NULL, NULL, NULL)
			`);

				// Build and execute delete query
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						name: "Delete Test User",
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);
				expect(sql).toBe("DELETE FROM users WHERE users.name = 'Delete Test User'");

				// Execute the delete
				await db.query(sql);

				// Verify the user was deleted
				const checkResult = await db.query(`SELECT * FROM users WHERE id = '${testUserId}'`);
				expect(checkResult).toHaveLength(0);
			});
		});

		test("should delete multiple users with complex condition", async () => {
			await db.executeInTransaction(async () => {
				// Insert test users with unique IDs
				const user1Id = "bbbbbbbb-cccc-dddd-eeee-222222222222";
				const user2Id = "cccccccc-dddd-eeee-ffff-333333333333";
				const user3Id = "dddddddd-eeee-ffff-aaaa-444444444444";

				await db.query(`
				INSERT INTO users (id, name, email, active, age, status, role, birth_date, created_at, updated_at, balance, metadata) VALUES 
				('${user1Id}', 'Delete User 1', 'deleteuser1@example.com', false, 20, 'inactive', 'user', NULL, NULL, NULL, NULL, NULL),
				('${user2Id}', 'Delete User 2', 'deleteuser2@example.com', false, 30, 'inactive', 'user', NULL, NULL, NULL, NULL, NULL),
				('${user3Id}', 'Delete User 3', 'deleteuser3@example.com', true, 25, 'active', 'user', NULL, NULL, NULL, NULL, NULL)
			`);

				// Build and execute delete query for inactive users aged 25 or older WITH specific names
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						$and: [
							{ active: false },
							{ age: { $gte: 25 } },
							{ name: { $like: "Delete User %" } }, // Only delete our test users
						],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);
				expect(sql).toBe(
					"DELETE FROM users WHERE (users.active = FALSE AND users.age >= 25 AND users.name LIKE 'Delete User %')",
				);

				// Execute the delete
				await db.query(sql);

				// Verify correct users remain (only check our test users)
				const remainingUsers = await db.query(`
				SELECT name FROM users 
				WHERE id IN ('${user1Id}', '${user2Id}', '${user3Id}')
				ORDER BY name
			`);
				expect(remainingUsers).toHaveLength(2);
				expect((remainingUsers[0] as { name: string }).name).toBe("Delete User 1"); // age 20, should remain
				expect((remainingUsers[1] as { name: string }).name).toBe("Delete User 3"); // active true, should remain
			});
		});

		test("should handle delete with no matching records", async () => {
			await db.executeInTransaction(async () => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						name: "Non-existent User",
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);
				// This should not throw an error, even though no records match
				await db.query(sql);
			});
		});
	});
});
