import { describe, expect, it, test } from "bun:test";
import type { Config, InsertQuery } from "../../src";
import { buildInsertQuery } from "../../src/builders/insert";
import { FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR } from "../../src/constants/errors";

const testConfig: Config = {
	tables: {
		users: {
			allowedFields: [
				{ name: "id", type: "uuid", nullable: false, default: { $func: { GEN_RANDOM_UUID: [] } } },
				{ name: "name", type: "string", nullable: false },
				{ name: "email", type: "string", nullable: true },
				{ name: "age", type: "number", nullable: true },
				{ name: "active", type: "boolean", nullable: false },
			],
		},
		posts: {
			allowedFields: [
				{ name: "id", type: "uuid", nullable: false },
				{ name: "title", type: "string", nullable: false },
				{ name: "content", type: "string", nullable: true },
				{ name: "user_id", type: "uuid", nullable: false },
			],
		},
	},
	variables: {
		current_user_id: "123e4567-e89b-12d3-a456-426614174000",
		admin_role: "admin",
	},
	relationships: [
		{
			table: "posts",
			field: "user_id",
			toTable: "users",
			toField: "id",
			type: "many-to-one",
		},
	],
};

describe("CRUD - INSERT Query Operations", () => {
	describe("Basic Row Insertion Operations", () => {
		test("should build insert query with scalar expressions", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "Jane Doe",
					email: "jane@example.com",
					active: true,
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "email", "active", "age") VALUES ('123e4567-e89b-12d3-a456-426614174000'::UUID, \'Jane Doe\', \'jane@example.com\', TRUE, NULL)`,
			);
		});

		test("should build insert query with timestamp and date values", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					name: "Test User",
					active: true,
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "active", "email", "age") VALUES ('550e8400-e29b-41d4-a716-446655440000'::UUID, 'Test User', TRUE, NULL, NULL)`,
			);
		});
	});

	describe("Data Validation and Schema Enforcement", () => {
		test("should validate table exists", () => {
			const insertQuery: InsertQuery = {
				table: "nonexistent",
				newRow: {
					name: "Test",
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow("Table 'nonexistent' is not allowed or does not exist");
		});

		test("should validate field exists in table", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					nonexistent_field: "value",
					active: true,
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow(
				"Field 'nonexistent_field' is not allowed for table 'users'",
			);
		});

		test("should insert default values", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					active: true,
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain(`INSERT INTO users ("name", "active", "email", "age", "id")`);
		});

		test("should validate required non-nullable fields", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					// Missing required 'active' fields
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow("Missing default value for non-nullable field 'active'");
		});

		test("should accept scalar expressions in newRow - uuid", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					name: "Test User",
					active: true,
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "active", "email", "age") VALUES ('550e8400-e29b-41d4-a716-446655440000'::UUID, 'Test User', TRUE, NULL, NULL)`,
			);
		});
	});

	describe("Conditional Insert with NEW_ROW Context", () => {
		test("should validate condition fields with NEW_ROW prefix", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					email: "john@example.com",
					active: true,
				},
				condition: {
					"NEW_ROW.email": { $ne: null },
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toContain(`INSERT INTO users ("name", "email", "active", "age", "id")`);
		});

		test("should reject condition fields without NEW_ROW prefix", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					active: true,
				},
				condition: {
					email: { $ne: null }, // Should be "NEW_ROW.email"
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow(FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR);
		});

		test("should validate NEW_ROW prefixed fields exist in table", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					name: "John Doe",
					active: true,
				},
				condition: {
					"NEW_ROW.nonexistent": { $ne: null },
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow(
				"Field 'nonexistent' is not allowed or does not exist in 'NEW_ROW'",
			);
		});

		test("should handle complex conditions with NEW_ROW prefix", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "1eb76aa8-f66b-4846-93df-587ff5749185",
					name: "John Doe",
					email: "john@example.com",
					age: 25,
					active: true,
				},
				condition: {
					$and: [{ "NEW_ROW.email": { $ne: null } }, { "NEW_ROW.age": { $gte: 18 } }],
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "email", "age", "active") VALUES ('1eb76aa8-f66b-4846-93df-587ff5749185'::UUID, 'John Doe', 'john@example.com', 25, TRUE)`,
			);
		});
	});

	describe("Insert Error Handling and Edge Cases", () => {
		test("should handle mixed types in newRow values", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John Doe",
					age: 30,
					active: true,
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "age", "active", "email") VALUES ('123e4567-e89b-12d3-a456-426614174000'::UUID, 'John Doe', 30, TRUE, NULL)`,
			);
		});

		test("should validate expressions within field conditions", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "012c9d27-a82c-49e8-be7a-0124cea33464",
					name: "Test User",
					active: true,
					email: "Test User", // Add email to make the condition true
				},
				condition: {
					"NEW_ROW.name": { $eq: { $field: "NEW_ROW.email" } },
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "active", "email", "age") VALUES ('012c9d27-a82c-49e8-be7a-0124cea33464'::UUID, 'Test User', TRUE, 'Test User', NULL)`,
			);
		});

		test("should reject invalid expressions with invalid fields", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "e308b0ae-5567-41a6-9a07-8648b226febf",
					name: "Test User",
					active: true,
				},
				condition: {
					"NEW_ROW.name": { $eq: { $field: "NEW_ROW.invalid_field" } },
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow(
				"Field 'invalid_field' is not allowed or does not exist in 'NEW_ROW'",
			);
		});

		test("should reject invalid expressions with non-NEW_ROW fields", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "e308b0ae-5567-41a6-9a07-8648b226febf",
					name: "Test User",
					active: true,
				},
				condition: {
					"NEW_ROW.name": { $eq: { $field: "email" } },
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow(FORBIDDEN_EXISTING_ROW_EVALUATION_ON_INSERT_ERROR);
		});

		test("should validate conditional expressions", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "34180af3-86b0-4538-a7ec-f192fb82b9f0",
					name: "no_email", // Change name to match what the conditional expression will return
					active: true,
				},
				condition: {
					"NEW_ROW.name": {
						$eq: {
							$cond: {
								if: { "NEW_ROW.email": { $ne: null } },
								// biome-ignore lint/suspicious/noThenProperty: Using 'then' in conditional expression
								then: "has_email",
								else: "no_email",
							},
						},
					},
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toBe(
				`INSERT INTO users ("id", "name", "active", "email", "age") VALUES ('34180af3-86b0-4538-a7ec-f192fb82b9f0'::UUID, 'no_email', TRUE, NULL, NULL)`,
			);
		});

		test("should reject invalid logical operator formats", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "09cab726-0624-47bd-970e-7911affae5f3",
					name: "Test User",
					active: true,
				},
				condition: {
					// biome-ignore lint/suspicious/noExplicitAny: Testing invalid input
					$and: "invalid_format" as any, // Should be array
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow("$and condition should be a non-empty array.");
		});

		test("should reject invalid $exists format", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: "7483d302-2da5-4af4-b873-63804de66ff6",
					name: "Test User",
					active: true,
				},
				condition: {
					// biome-ignore lint/suspicious/noExplicitAny: Testing invalid input
					$exists: { table: "posts" } as any, // Missing 'condition' property
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow('Invalid $exists expression: missing "condition" property');
		});
	});

	describe("JavaScript Condition Evaluation in Inserts", () => {
		it("should evaluate simple field conditions in JavaScript", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 25,
					active: true,
				},
				condition: {
					"NEW_ROW.age": { $gt: 18 },
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);

			expect(sql).toContain("INSERT INTO users");
			expect(sql).not.toContain("WHERE");
		});

		it("should throw error when condition evaluates to false", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 15,
					active: true,
				},
				condition: {
					"NEW_ROW.age": { $gt: 18 },
				},
			};

			expect(() => buildInsertQuery(insertQuery, testConfig)).toThrow("Insert condition not met");
		});

		it("should evaluate logical operators", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 25,
					active: true,
				},
				condition: {
					$and: [{ "NEW_ROW.age": { $gt: 18 } }, { "NEW_ROW.active": true }],
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain("INSERT INTO users");
		});

		it("should evaluate function expressions", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 25,
					active: true,
				},
				condition: {
					"NEW_ROW.age": {
						$eq: { $func: { ADD: [20, 5] } },
					},
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain("INSERT INTO users");
		});

		it("should evaluate variable references", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 25,
					active: true,
				},
				condition: {
					"NEW_ROW.name": {
						$ne: { $var: "admin_role" }, // Compare name against admin_role
					},
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain("INSERT INTO users");
		});

		it("should handle string operators like $like", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John Doe",
					active: true,
				},
				condition: {
					"NEW_ROW.name": { $like: "John%" },
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain("INSERT INTO users");
		});

		it("should handle array operators like $in", () => {
			const insertQuery: InsertQuery = {
				table: "users",
				newRow: {
					id: { $uuid: "123e4567-e89b-12d3-a456-426614174000" },
					name: "John",
					age: 25,
					active: true,
				},
				condition: {
					"NEW_ROW.age": { $in: [25, 30, 35] },
				},
			};

			const sql = buildInsertQuery(insertQuery, testConfig);
			expect(sql).toContain("INSERT INTO users");
		});
	});
});
