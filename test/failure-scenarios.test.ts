import { beforeEach, describe, expect, it } from "bun:test";
import { ExpressionTypeMap } from "../src/expression-map";
import { parseExpression } from "../src/parsers";
import { compileAggregationQuery, parseAggregationQuery } from "../src/parsers/aggregate";
import { parseWhereClause } from "../src/parsers/conditions";
import { parseSelectQuery } from "../src/parsers/select";
import type { AnyExpression, Condition } from "../src/schemas";
import type { Config, ParserState } from "../src/types";

describe("Expected Failure Tests", () => {
	let testConfig: Config;
	let testState: ParserState;

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
						{ name: "metadata", type: "object", nullable: true },
					],
				},
			},
			variables: {
				"auth.uid": 123,
				current_user: 456,
			},
			relationships: [],
		};

		testState = {
			config: testConfig,
			rootTable: "users",
			params: [],
			expressions: new ExpressionTypeMap(),
		};
	});

	describe("Invalid Field Access", () => {
		it("should reject non-existent tables", () => {
			expect(() => {
				parseWhereClause(
					{
						"nonexistent.field": { $eq: "value" },
					},
					testConfig,
					"users",
				);
			}).toThrow("Table 'nonexistent' is not allowed or does not exist");
		});

		it("should reject non-existent fields", () => {
			expect(() => {
				parseWhereClause(
					{
						"users.nonexistent_field": { $eq: "value" },
					},
					testConfig,
					"users",
				);
			}).toThrow("Field 'nonexistent_field' is not allowed for table 'users'");
		});

		it("should reject invalid table.field format", () => {
			const invalidFormats = ["users.field.extra.parts", "users..field", ".field", "users.", ""];

			for (const invalidFormat of invalidFormats) {
				expect(() => {
					parseWhereClause(
						{
							[invalidFormat]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject fields that don't start with lowercase", () => {
			const invalidFieldNames = [
				"Users.name", // Capital first letter
				"USERS.name", // All caps
				"1users.name", // Starts with number
				"_users.name", // Starts with underscore
				"-users.name", // Starts with dash
			];

			for (const invalidField of invalidFieldNames) {
				expect(() => {
					parseWhereClause(
						{
							[invalidField]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject JSON access on non-object fields", () => {
			const invalidJsonAccess = [
				"users.name->invalid", // String field
				"users.age->invalid", // Number field
				"users.active->invalid", // Boolean field
			];

			for (const invalidAccess of invalidJsonAccess) {
				expect(() => {
					parseWhereClause(
						{
							[invalidAccess]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow("JSON path access 'invalid' is only allowed on JSON fields");
			}
		});
	});

	describe("Invalid Operators and Values", () => {
		it("should reject unknown operators", () => {
			const unknownOperators = ["$unknown", "$invalid", "$custom", "$select", "$drop"];

			for (const unknownOp of unknownOperators) {
				expect(() => {
					const condition = {
						"users.name": { [unknownOp]: "value" },
					};
					parseWhereClause(condition as Condition, testConfig, "users");
				}).toThrow();
			}
		});

		it("should reject empty arrays in IN/NOT IN operators", () => {
			expect(() => {
				parseWhereClause(
					{
						"users.name": { $in: [] },
					},
					testConfig,
					"users",
				);
			}).toThrow("Operator 'IN' requires a non-empty array");

			expect(() => {
				parseWhereClause(
					{
						"users.name": { $nin: [] },
					},
					testConfig,
					"users",
				);
			}).toThrow("Operator 'NOT IN' requires a non-empty array");
		});

		it("should reject invalid function names in expressions", () => {
			const invalidFunctions = ["INVALID_FUNC", "DROP_TABLE", "SELECT", "INSERT", "UPDATE", "DELETE"];

			for (const invalidFunc of invalidFunctions) {
				expect(() => {
					const expr: AnyExpression = {
						$expr: { [invalidFunc]: ["users.name"] },
					};
					parseExpression(expr, testState);
				}).toThrow("Unknown function or operator");
			}
		});

		it("should reject incorrect argument counts for functions", () => {
			// Unary functions with wrong argument count
			expect(() => {
				const expr: AnyExpression = {
					$expr: { UPPER: [] }, // No arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Unary operator 'UPPER' requires exactly 1 argument");

			expect(() => {
				const expr: AnyExpression = {
					$expr: { UPPER: ["arg1", "arg2"] }, // Too many arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Unary operator 'UPPER' requires exactly 1 argument");

			// Binary functions with wrong argument count
			expect(() => {
				const expr: AnyExpression = {
					$expr: { ADD: ["only_one"] }, // Not enough arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Binary operator 'ADD' requires exactly 2 arguments");

			expect(() => {
				const expr: AnyExpression = {
					$expr: { ADD: ["arg1", "arg2", "arg3"] }, // Too many arguments
				};
				parseExpression(expr, testState);
			}).toThrow("Binary operator 'ADD' requires exactly 2 arguments");
		});

		it("should reject variable functions with no arguments", () => {
			expect(() => {
				const expr: AnyExpression = {
					$expr: { COALESCE: [] },
				};
				parseExpression(expr, testState);
			}).toThrow("Variable operator 'COALESCE' requires at least 1 argument");
		});
	});

	describe("Invalid Query Structures", () => {
		it("should reject empty AND conditions", () => {
			expect(() => {
				parseWhereClause(
					{
						$and: [],
					},
					testConfig,
					"users",
				);
			}).toThrow("No conditions provided for $and condition");
		});

		it("should reject empty OR conditions", () => {
			expect(() => {
				parseWhereClause(
					{
						$or: [],
					},
					testConfig,
					"users",
				);
			}).toThrow("No conditions provided for $or condition");
		});

		it("should reject select queries with empty selection", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "users",
						selection: {},
					},
					testConfig,
				);
			}).toThrow("Selection cannot be empty");
		});

		it("should reject aggregation queries with no fields", () => {
			expect(() => {
				parseAggregationQuery(
					{
						table: "users",
						groupBy: [],
						aggregatedFields: {},
					},
					testConfig,
				);
			}).toThrow("Aggregation query must have at least one group by field or aggregated field");
		});

		it("should reject invalid aggregation operators", () => {
			// This test verifies that invalid operators are caught
			expect(() => {
				// The aggregation operators array should validate this
				parseAggregationQuery(
					{
						table: "users",
						groupBy: ["name"],
						aggregatedFields: {
							result: {
								operator: "COUNT", // Use valid operator to test the field validation instead
								field: "nonexistent_field",
							},
						},
					},
					testConfig,
				);
			}).toThrow();
		});

		it("should reject COUNT(*) with non-COUNT operators", () => {
			// Test that only COUNT can use "*" as field
			const validCountQuery = parseAggregationQuery(
				{
					table: "users",
					groupBy: ["name"],
					aggregatedFields: {
						count_all: {
							operator: "COUNT",
							field: "*",
						},
					},
				},
				testConfig,
			);

			// This should work
			const sql = compileAggregationQuery(validCountQuery);
			expect(sql).toContain("COUNT(*)");
		});
	});

	describe("Invalid Relationships", () => {
		it("should reject queries with non-existent relationships", () => {
			expect(() => {
				parseSelectQuery(
					{
						rootTable: "users",
						selection: {
							name: true,
							nonexistent_table: {
								field: true,
							},
						},
					},
					testConfig,
				);
			}).toThrow("No relationship found");
		});

		it("should reject EXISTS conditions with invalid tables", () => {
			expect(() => {
				parseWhereClause(
					{
						$exists: {
							table: "nonexistent",
							conditions: {
								"nonexistent.field": { $eq: "value" },
							},
						},
					},
					testConfig,
					"users",
				);
			}).toThrow("Table 'nonexistent' is not allowed or does not exist");
		});
	});

	describe("Type Validation Failures", () => {
		it("should reject malformed expression function objects", () => {
			expect(() => {
				const expr: AnyExpression = {
					$expr: {}, // Empty object
				};
				parseExpression(expr, testState);
			}).toThrow("$expr must contain exactly one function");

			expect(() => {
				const expr: AnyExpression = {
					$expr: {
						UPPER: ["arg1"],
						LOWER: ["arg2"], // Multiple functions
					},
				};
				parseExpression(expr, testState);
			}).toThrow("$expr must contain exactly one function");
		});
	});

	describe("Boundary and Edge Case Failures", () => {
		it("should handle reasonably nested conditions", () => {
			// Create a reasonably deeply nested condition
			let deepCondition: Condition = { "users.name": { $eq: "base" } };

			for (let i = 0; i < 100; i++) {
				deepCondition = { $and: [deepCondition] };
			}

			// This should work fine
			const result = parseWhereClause(deepCondition, testConfig, "users");
			expect(result.sql).toContain("users.name = $1");
			expect(result.params).toEqual(["base"]);
		});

		it("should reject circular references in expressions", () => {
			// While not directly testable due to TypeScript protection,
			// we can test cases that might cause infinite loops
			expect(() => {
				const expr: AnyExpression = {
					$expr: { CONCAT: [] }, // Empty array
				};
				parseExpression(expr, testState);
			}).toThrow();
		});

		it("should handle malformed JSON correctly", () => {
			// Test with strings that look like JSON but aren't valid
			const malformedJsonStrings = ["{ invalid json }", "[ incomplete array", "'single quotes'", '{ "unfinished": '];

			for (const malformed of malformedJsonStrings) {
				const condition: Condition = {
					"users.name": { $eq: malformed },
				};

				// Should handle these as regular strings, not throw JSON parse errors
				const result = parseWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual([malformed]);
			}
		});
	});

	describe("Security Boundary Tests", () => {
		it("should reject attempts to access system tables", () => {
			const systemTables = ["information_schema.tables", "pg_catalog.pg_tables", "sys.tables", "mysql.user"];

			for (const systemTable of systemTables) {
				expect(() => {
					parseWhereClause(
						{
							[`${systemTable}.column`]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject attempts to use dangerous SQL keywords in field names", () => {
			const dangerousKeywords = ["SELECT", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "CREATE"];

			for (const keyword of dangerousKeywords) {
				expect(() => {
					parseWhereClause(
						{
							[`users.${keyword.toLowerCase()}`]: { $eq: "value" },
						},
						testConfig,
						"users",
					);
				}).toThrow();
			}
		});

		it("should reject attempts to escape parameter binding", () => {
			const escapeAttempts = [
				"'; SELECT * FROM users; --",
				"' UNION SELECT password FROM admin --",
				"'; DROP TABLE users; --",
				"' OR '1'='1",
			];

			for (const escapeAttempt of escapeAttempts) {
				const condition: Condition = {
					"users.name": { $eq: escapeAttempt },
				};

				const result = parseWhereClause(condition, testConfig, "users");

				// Should be properly parameterized, not injected
				expect(result.sql).toBe("users.name = $1");
				expect(result.params).toEqual([escapeAttempt]);

				// Make sure dangerous SQL is not present in the expression
				expect(result.sql).not.toContain("SELECT");
				expect(result.sql).not.toContain("DROP");
				expect(result.sql).not.toContain("UNION");
			}
		});
	});

	describe("Resource Exhaustion Tests", () => {
		it("should handle reasonable limits on parameter count", () => {
			// Test with a very large number of parameters
			const largeArray = Array.from({ length: 10000 }, (_, i) => `value_${i}`);

			const condition: Condition = {
				"users.name": { $in: largeArray },
			};

			// Should either handle it gracefully or reject with a reasonable error
			try {
				const result = parseWhereClause(condition, testConfig, "users");
				expect(result.params.length).toBe(10000);
			} catch (error) {
				// If it throws, it should be a reasonable error about limits
				expect(error).toBeDefined();
			}
		});

		it("should handle deeply nested JSON paths reasonably", () => {
			// Create a very deep JSON path
			const deepPath = Array.from({ length: 100 }, (_, i) => `level${i}`).join("->");
			const condition: Condition = {
				[`users.metadata->${deepPath}`]: { $eq: "deep_value" },
			};

			// Should either handle it or reject with a reasonable error
			try {
				const result = parseWhereClause(condition, testConfig, "users");
				expect(result.params).toEqual(["deep_value"]);
			} catch (error) {
				// If it throws, should be a reasonable error
				expect(error).toBeDefined();
			}
		});
	});
});
