import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import type { Condition } from "../../src/schemas";
import type { Config } from "../../src/types";

// Helper function to extract WHERE clause from parsed select query
function extractSelectWhereClause(condition: Condition, config: Config, rootTable: string): { sql: string; params: unknown[] } {
	const query = {
		rootTable,
		selection: { [`${rootTable}.id`]: true }, // minimal selection
		condition,
	};
	const parsedQuery = parseSelectQuery(query, config);
	return { sql: parsedQuery.where || "", params: parsedQuery.params };
}

describe("Security - SQL Injection Prevention and Input Validation", () => {
	let testConfig: Config;

	beforeEach(() => {
		testConfig = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
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
	});

	describe("Field Name Injection Attack Prevention", () => {
		it("should reject field names with SQL injection attempts", () => {
			const maliciousFieldNames = [
				"name'; DROP TABLE users; --",
				"name' OR '1'='1",
				"name'; SELECT * FROM users; --",
				"name UNION SELECT password FROM admin",
				"name/**/OR/**/1=1",
				'name"; DROP TABLE users; --',
				"name' AND SLEEP(5) --",
				"name' UNION ALL SELECT NULL,NULL,password FROM admin--",
				"name'||(SELECT password FROM admin WHERE id=1)||'",
				"name' AND (SELECT COUNT(*) FROM users) > 0 --",
			];

			for (const maliciousField of maliciousFieldNames) {
				const condition: Condition = {
					[`users.${maliciousField}`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should reject table names with SQL injection attempts", () => {
			const maliciousTableNames = [
				"users'; DROP TABLE admin; --",
				"users UNION SELECT * FROM admin",
				"users/**/OR/**/1=1",
				"users' OR '1'='1",
				"users; INSERT INTO admin (password) VALUES ('hacked')",
			];

			for (const maliciousTable of maliciousTableNames) {
				const condition: Condition = {
					[`${maliciousTable}.name`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should reject JSON path injection attempts", () => {
			const maliciousJsonPaths = [
				"metadata->''->test", // Empty JSON path segment
				"metadata->->test", // Invalid JSON path format
			];

			for (const maliciousPath of maliciousJsonPaths) {
				const condition: Condition = {
					[`users.${maliciousPath}`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});
	});

	describe("Value Injection Attack Prevention", () => {
		it("should properly escape string values with SQL injection attempts", () => {
			const maliciousValues = [
				"'; DROP TABLE users; --",
				"' OR '1'='1",
				"' UNION SELECT password FROM admin --",
				"'; INSERT INTO admin (password) VALUES ('hacked'); --",
				"' AND SLEEP(5) --",
				"'/**/OR/**/1=1/**/--",
				"' || (SELECT password FROM admin WHERE id=1) || '",
				"'; UPDATE users SET password='hacked' WHERE id=1; --",
				"\\'; DROP TABLE users; --",
				"''; DROP TABLE users; --''",
			];

			for (const maliciousValue of maliciousValues) {
				const condition: Condition = {
					"users.name": { $eq: maliciousValue },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");

				// Verify the malicious SQL is properly escaped and not injectable
				expect(result.sql).toBe("users.name = $1");
				expect(result.params).toEqual([maliciousValue]);

				// Ensure no raw SQL injection is possible
				expect(result.sql).not.toContain("DROP");
				expect(result.sql).not.toContain("INSERT");
				expect(result.sql).not.toContain("UPDATE");
				expect(result.sql).not.toContain("DELETE");
				expect(result.sql).not.toContain("UNION");
				expect(result.sql).not.toContain("SLEEP");
			}
		});

		it("should handle malicious number values", () => {
			const maliciousNumbers = [
				"123; DROP TABLE users; --",
				"123 OR 1=1",
				"123 UNION SELECT password FROM admin",
				"123/**/OR/**/1=1",
			];

			for (const maliciousNumber of maliciousNumbers) {
				const condition: Condition = {
					"users.age": { $eq: maliciousNumber as string },
				};

				// Should either reject non-numeric values or handle them safely
				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should handle malicious array values", () => {
			const maliciousArrays = [
				["test", "'; DROP TABLE users; --"],
				["test", "' OR '1'='1"],
				["test", "' UNION SELECT password FROM admin --"],
			];

			for (const maliciousArray of maliciousArrays) {
				const condition: Condition = {
					"users.name": { $in: maliciousArray },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");

				// Verify parameters are properly escaped
				expect(result.params).toEqual(maliciousArray);
				expect(result.sql).toContain("IN");
				expect(result.sql).not.toContain("DROP");
				expect(result.sql).not.toContain("UNION");
			}
		});
	});

	describe("Expression Injection Attack Prevention", () => {
		it("should reject malicious function names", () => {
			const maliciousFunctions = [
				"UPPER'; DROP TABLE users; --",
				"UPPER/**/OR/**/1=1",
				"UPPER' OR '1'='1",
				"pg_sleep",
				"load_file",
				"into_outfile",
				"exec",
				"system",
			];

			for (const maliciousFunction of maliciousFunctions) {
				const condition: Condition = {
					"users.name": {
						$eq: {
							$func: {
								[maliciousFunction]: ["users.name"],
							},
						},
					},
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should reject malicious variable names", () => {
			const maliciousVariables = ["undefined_variable'; DROP TABLE users; --", "nonexistent_var OR 1=1"];

			for (const maliciousVariable of maliciousVariables) {
				const condition: Condition = {
					"users.name": {
						$eq: { $field: maliciousVariable },
					},
				};

				// Should treat as unknown variable/field and throw error
				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});
	});

	describe("Select Query Injection Attack Prevention", () => {
		it("should prevent injection in select fields", () => {
			const maliciousSelections = {
				"name'; DROP TABLE users; --": true,
				"name' OR '1'='1": true,
				"name UNION SELECT password FROM admin": true,
				"name/**/OR/**/1=1": true,
			};

			expect(() => {
				const query = parseSelectQuery(
					{
						rootTable: "users",
						selection: maliciousSelections,
					},
					testConfig,
				);
				compileSelectQuery(query);
			}).toThrow();
		});

		it("should prevent injection in aliases", () => {
			const maliciousAliases = {
				user_name: {
					"malicious'; DROP TABLE users; --": { $field: "users.name" },
				},
			};

			expect(() => {
				const query = parseSelectQuery(
					{
						rootTable: "users",
						selection: maliciousAliases,
					},
					testConfig,
				);
				compileSelectQuery(query);
			}).toThrow();
		});
	});

	describe("Aggregation Query Injection Attack Prevention", () => {
		it("should prevent injection in group by fields", () => {
			const maliciousGroupBy = ["name'; DROP TABLE users; --", "name OR 1=1", "name UNION SELECT password FROM admin"];

			for (const maliciousField of maliciousGroupBy) {
				expect(() => {
					const query = parseAggregationQuery(
						{
							table: "users",
							groupBy: [maliciousField],
							aggregatedFields: {
								count: { function: "COUNT", field: "*" },
							},
						},
						testConfig,
					);
					compileAggregationQuery(query);
				}).toThrow();
			}
		});

		it("should prevent injection in aggregation fields", () => {
			const maliciousAggFields = {
				total: {
					function: "SUM" as const,
					field: "age'; DROP TABLE users; --",
				},
			};

			expect(() => {
				const query = parseAggregationQuery(
					{
						table: "users",
						groupBy: ["name"],
						aggregatedFields: maliciousAggFields,
					},
					testConfig,
				);
				compileAggregationQuery(query);
			}).toThrow();
		});

		it("should prevent injection in aggregation operators", () => {
			const maliciousOperators = ["COUNT'; DROP TABLE users; --", "SUM OR 1=1", "AVG UNION SELECT password FROM admin"];

			for (const maliciousOp of maliciousOperators) {
				expect(() => {
					const query = parseAggregationQuery(
						{
							table: "users",
							groupBy: ["name"],
							aggregatedFields: {
								result: {
									function: maliciousOp as "COUNT",
									field: "age",
								},
							},
						},
						testConfig,
					);
					compileAggregationQuery(query);
				}).toThrow();
			}
		});
	});

	describe("Advanced Injection Attack Edge Cases", () => {
		it("should handle unicode and encoded injection attempts", () => {
			const unicodeInjections = [
				"name\u0027; DROP TABLE users; --", // Unicode single quote
				"name\u002f\u002a\u002a\u002fOR\u002f\u002a\u002a\u002f1=1", // Unicode /* */ OR /* */ 1=1
				"name%27%20OR%20%271%27%3D%271", // URL encoded ' OR '1'='1
				"name&#39; OR &#39;1&#39;=&#39;1", // HTML entity encoded
			];

			for (const unicodeInjection of unicodeInjections) {
				const condition: Condition = {
					[`users.${unicodeInjection}`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should handle case variations of SQL keywords", () => {
			const caseVariations = [
				"name'; drop table users; --",
				"name'; DROP table users; --",
				"name'; Drop Table users; --",
				"name'; DrOp TaBlE users; --",
				"NAME'; DROP TABLE USERS; --",
			];

			for (const caseVariation of caseVariations) {
				const condition: Condition = {
					[`users.${caseVariation}`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should handle whitespace and comment variations", () => {
			const whitespaceVariations = [
				"name'/**/;/**/DROP/**/TABLE/**/users;/**/--",
				"name'\t;\tDROP\tTABLE\tusers;\t--",
				"name'\n;\nDROP\nTABLE\nusers;\n--",
				"name'\r\n;\r\nDROP\r\nTABLE\r\nusers;\r\n--",
				"name'  ;  DROP  TABLE  users;  --",
			];

			for (const whitespaceVariation of whitespaceVariations) {
				const condition: Condition = {
					[`users.${whitespaceVariation}`]: { $eq: "test" },
				};

				expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
			}
		});

		it("should prevent stacked queries", () => {
			const stackedQueries = [
				"'; DROP TABLE users; SELECT * FROM admin --",
				"'; UPDATE users SET password='hacked'; SELECT * FROM users --",
				"'; INSERT INTO admin VALUES ('hacker', 'password'); --",
				"'; DELETE FROM users; SELECT 'success' --",
			];

			for (const stackedQuery of stackedQueries) {
				const condition: Condition = {
					"users.name": { $eq: stackedQuery },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.sql).toBe("users.name = $1");
				expect(result.params).toEqual([stackedQuery]);
			}
		});
	});

	describe("Input Boundary Value Security Testing", () => {
		it("should handle extremely long field names", () => {
			const longFieldName = "a".repeat(10000);
			const condition: Condition = {
				[`users.${longFieldName}`]: { $eq: "test" },
			};

			expect(() => extractSelectWhereClause(condition, testConfig, "users")).toThrow();
		});

		it("should handle extremely long string values", () => {
			const longValue = "test".repeat(10000);
			const condition: Condition = {
				"users.name": { $eq: longValue },
			};

			const result = extractSelectWhereClause(condition, testConfig, "users");
			expect(result.sql).toBe("users.name = $1");
			expect(result.params).toEqual([longValue]);
		});

		it("should handle null bytes and special characters", () => {
			const specialValues = ["\0", "\x00", "\u0000", "test\0injection", "test\x00injection", "test\u0000injection"];

			for (const specialValue of specialValues) {
				const condition: Condition = {
					"users.name": { $eq: specialValue },
				};

				const result = extractSelectWhereClause(condition, testConfig, "users");
				expect(result.sql).toBe("users.name = $1");
				expect(result.params).toEqual([specialValue]);
			}
		});
	});
});
