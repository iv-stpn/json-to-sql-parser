/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
/** biome-ignore-all lint/suspicious/noExplicitAny: using any for complex type testing scenarios */
import { beforeEach, describe, expect, it, test } from "bun:test";
import { buildUpdateQuery } from "../../src/builders/update";
import { parseNewRow, parseNewRowValue } from "../../src/parsers/mutations";
import type { UpdateQuery } from "../../src/schemas";
import type { Config, Field } from "../../src/types";

// Test configuration for mixed conditions tests
const mockConfig: Config = {
	dialect: "postgresql",
	tables: {
		users: {
			allowedFields: [
				{ name: "id", type: "number", nullable: false },
				{ name: "name", type: "string", nullable: false },
				{ name: "email", type: "string", nullable: true },
				{ name: "age", type: "number", nullable: true },
				{ name: "active", type: "boolean", nullable: false },
				{ name: "status", type: "string", nullable: true },
			],
		},
	},
	relationships: [],
	variables: {},
};

// Test configuration with comprehensive field types for complex expressions
let testConfig: Config;

beforeEach(() => {
	testConfig = {
		dialect: "postgresql",
		tables: {
			users: {
				allowedFields: [
					{ name: "id", type: "string", nullable: false },
					{ name: "name", type: "string", nullable: false },
					{ name: "email", type: "string", nullable: true },
					{ name: "age", type: "number", nullable: true },
					{ name: "score", type: "number", nullable: true },
					{ name: "active", type: "boolean", nullable: false },
					{ name: "status", type: "string", nullable: false },
					{ name: "role", type: "string", nullable: false },
					{ name: "balance", type: "number", nullable: true },
					{ name: "created_at", type: "string", nullable: false },
					{ name: "metadata", type: "object", nullable: true },
				],
			},
			posts: {
				allowedFields: [
					{ name: "id", type: "string", nullable: false },
					{ name: "title", type: "string", nullable: false },
					{ name: "content", type: "string", nullable: true },
					{ name: "user_id", type: "string", nullable: false },
					{ name: "published", type: "boolean", nullable: false },
					{ name: "priority", type: "number", nullable: true },
				],
			},
		},
		variables: {
			"auth.uid": "user123",
			current_user: "user456",
			admin_role: "admin",
			min_age: 18,
			max_score: 100,
		},
		relationships: [],
	};
});

describe("CRUD - UPDATE Mixed Row Condition Processing", () => {
	it("should generate WHERE clause when NEW_ROW conditions pass but old row conditions remain", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
				age: 25, // This will pass NEW_ROW.age >= 18
			},
			condition: {
				$and: [
					{ "NEW_ROW.age": { $gte: 18 } }, // This should be evaluated in JS and pass
					{ active: true }, // This should remain in WHERE clause
				],
			},
		};

		const sql = buildUpdateQuery(updateQuery, mockConfig);
		expect(sql).toBe('UPDATE users SET \"name\" = \'John Updated\', \"age\" = 25 WHERE users.active = TRUE');
	});

	it("should make NEW_ROW field conditions apply to existing values if those fields are not updated", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
			},
			condition: {
				$and: [
					{ "NEW_ROW.age": { $gte: 18 } }, // This should result in a condition on the existing value of age
					{ active: true }, // This should remain in WHERE clause
				],
			},
		};

		const sql = buildUpdateQuery(updateQuery, mockConfig);

		expect(sql).toBe("UPDATE users SET \"name\" = 'John Updated' WHERE (users.age >= 18 AND users.active = TRUE)");
	});

	it("should short-circuit when NEW_ROW condition in AND fails", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
				age: 15, // This will fail NEW_ROW.age >= 18
			},
			condition: {
				$and: [
					{ "NEW_ROW.age": { $gte: 18 } }, // This should fail
					{ active: true }, // This shouldn't matter
				],
			},
		};

		// Should throw error because condition fails
		expect(() => buildUpdateQuery(updateQuery, mockConfig)).toThrow("Update condition not met");
	});

	it("should handle OR conditions with NEW_ROW fields that pass", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
				age: 25,
			},
			condition: {
				$or: [
					{ "NEW_ROW.age": { $gte: 18 } }, // This will be true (25 >= 18)
					{ status: "premium" }, // This shouldn't matter
				],
			},
		};

		const sql = buildUpdateQuery(updateQuery, mockConfig);

		expect(sql).toBe('UPDATE users SET "name" = \'John Updated\', "age" = 25');
	});

	it("should handle pure NEW_ROW conditions", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
				age: 25,
			},
			condition: {
				"NEW_ROW.age": { $gte: 18 },
			},
		};

		const sql = buildUpdateQuery(updateQuery, mockConfig);

		expect(sql).toBe('UPDATE users SET "name" = \'John Updated\', "age" = 25');
	});

	it("should handle pure old row conditions", () => {
		const updateQuery: UpdateQuery = {
			table: "users",
			updates: {
				name: "John Updated",
				age: 25,
			},
			condition: {
				active: true,
				status: "premium",
			},
		};

		const sql = buildUpdateQuery(updateQuery, mockConfig);

		expect(sql).toBe(
			"UPDATE users SET \"name\" = 'John Updated', \"age\" = 25 WHERE (users.active = TRUE AND users.status = 'premium')",
		);
	});
});

describe("CRUD - UPDATE Query Complex Operations", () => {
	describe("Complex Nested Logical Operations", () => {
		test("should handle deeply nested $and/$or/$not combinations", () => {
			const updateQuery: UpdateQuery = {
				table: "users",
				updates: {
					status: "updated",
				},
				condition: {
					$and: [
						{
							$or: [
								{ "NEW_ROW.status": { $eq: "updated" } },
								{
									$and: [{ "NEW_ROW.status": { $ne: null } }, { "NEW_ROW.status": { $ne: "" } }],
								},
							],
						},
						{
							$not: {
								$or: [
									{ active: { $eq: false } },
									{
										$and: [{ age: { $lt: 18 } }, { role: { $ne: { $var: "admin_role" } } }],
									},
								],
							},
						},
					],
				},
			};

			const sql = buildUpdateQuery(updateQuery, testConfig);

			expect(sql).toContain("UPDATE users SET \"status\" = 'updated'");
			expect(sql).toContain("WHERE NOT");
		});

		test("should handle complex object field operations in conditions", () => {
			const updateQuery: UpdateQuery = {
				table: "users",
				updates: {
					metadata: { $jsonb: { updated: true, processed_at: "2024-01-01" } },
				},
				condition: {
					$and: [
						{
							$or: [
								{ role: { $eq: "admin" } },
								{
									role: {
										$eq: {
											$func: { UPPER: [{ $var: "admin_role" }] },
										},
									},
								},
							],
						},
					],
				},
			};

			const sql = buildUpdateQuery(updateQuery, testConfig);
			expect(sql).toBe(
				'UPDATE users SET "metadata" = (\'{"updated":true,"processed_at":"2024-01-01"}\')::JSONB WHERE (users.role = \'admin\' OR users.role = \'ADMIN\')',
			);
		});
	});

	describe("Edge Cases and Error Scenarios", () => {
		test("should handle null propagation in complex expression chains", () => {
			const updateQuery: UpdateQuery = {
				table: "users",
				updates: {
					score: null, // Directly set to null for simpler test
					metadata: { $jsonb: { info: "test" } },
				},
				condition: {
					$and: [
						{ "NEW_ROW.score": { $eq: null } }, // Will be true
						{ id: { $eq: { $var: "auth.uid" } } },
					],
				},
			};

			const sql = buildUpdateQuery(updateQuery, testConfig);

			expect(sql).toBe('UPDATE users SET "score" = NULL, "metadata" = (\'{"info":"test"}\')::JSONB WHERE users.id = \'user123\'');
		});

		test("should handle type coercion in complex conditional expressions", () => {
			const updateQuery: UpdateQuery = {
				table: "users",
				updates: {
					active: true,
					age: 25,
					name: "Test User",
				},
				condition: {
					$and: [
						{
							// String length comparison
							"NEW_ROW.name": {
								$ne: "",
							},
						},
						{
							// Boolean evaluation
							"NEW_ROW.active": {
								$eq: true,
							},
						},
						{
							// Old row condition
							score: { $gt: 50 },
						},
					],
				},
			};

			const sql = buildUpdateQuery(updateQuery, testConfig);

			// Should handle type coercion and generate appropriate WHERE clause
			expect(sql).toContain("UPDATE users SET");
			expect(sql).toContain("WHERE");
		});
	});

	describe("Performance Optimization Scenarios", () => {
		test("should optimize complex nested conditions", () => {
			const updateQuery: UpdateQuery = {
				table: "users",
				updates: {
					active: false,
				},
				condition: {
					$or: [
						{ "NEW_ROW.active": { $eq: false } }, // Will be true
						{
							$and: [
								{
									// Complex nested condition
									$or: [
										{
											role: {
												$eq: {
													$func: {
														UPPER: [{ $var: "admin_role" }],
													},
												},
											},
										},
										{
											role: { $eq: "premium" },
										},
									],
								},
								{
									age: {
										$gt: { $var: "min_age" },
									},
								},
							],
						},
					],
				},
			};

			const sql = buildUpdateQuery(updateQuery, testConfig);

			// Should short-circuit the $or since first condition is true
			expect(sql).toBe('UPDATE users SET "active" = FALSE');
		});
	});
});

describe("CRUD - UPDATE Evaluation Utilities", () => {
	const mockStringField: Field = { name: "name", type: "string", nullable: false };
	const mockUuidField: Field = { name: "id", type: "uuid", nullable: false };
	const mockNumberField: Field = { name: "age", type: "number", nullable: true };

	describe("autoConvertValue", () => {
		it("should auto-convert UUID strings for UUID fields", () => {
			const result = parseNewRowValue("123e4567-e89b-12d3-a456-426614174000", mockUuidField);
			expect(result).toEqual({ $uuid: "123e4567-e89b-12d3-a456-426614174000" });
		});

		it("should keep regular strings as strings", () => {
			const result = parseNewRowValue("regular string", mockStringField);
			expect(result).toBe("regular string");
		});
	});

	describe("validateFieldsExist", () => {
		const allowedFields: Field[] = [mockStringField, mockUuidField, mockNumberField];

		it("should validate that all fields exist", () => {
			const newRow = { name: "test", id: "123e4567-e89b-12d3-a456-426614174000" };
			expect(() => parseNewRow("users", newRow, allowedFields)).not.toThrow();
		});

		it("should throw for non-existent fields", () => {
			const newRow = { nonexistent: "test" };
			expect(() => parseNewRow("users", newRow, allowedFields)).toThrow("Field 'nonexistent' is not allowed for table 'users'");
		});
	});
});
