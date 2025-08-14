import { beforeEach, describe, expect, it } from "bun:test";
import { parseFieldPath } from "../src/parsers";
import type { Config, ParserState } from "../src/types";
import { ExpressionTypeMap } from "../src/utils/expression-map";

// Regex patterns used in tests
const TABLE_NOT_ALLOWED_REGEX = /Table .* is not allowed or does not exist/;
const FIELD_NOT_ALLOWED_REGEX = /Field .* is not allowed for table/;
const INVALID_FIELD_PATH_REGEX = /Invalid field path.*must be of the form/;

describe("Field Path Parsing Tests", () => {
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
						{ name: "metadata", type: "object", nullable: true },
						{ name: "profile", type: "object", nullable: false },
						{ name: "settings", type: "object", nullable: true },
						{ name: "abc", type: "string", nullable: false },
						{ name: "def", type: "string", nullable: false },
						{ name: "foo", type: "object", nullable: false },
						{ name: "bar", type: "string", nullable: false },
						{ name: "special_field", type: "object", nullable: false },
						{ name: "field_with_underscores", type: "object", nullable: false },
					],
				},
			},
			variables: {},
			relationships: [],
		};

		testState = {
			config: testConfig,
			rootTable: "users",
			params: [],
			expressions: new ExpressionTypeMap(),
		};
	});

	describe("Valid Field Paths", () => {
		it("should parse simple field names", () => {
			const validSimpleFields = [
				"id",
				"name",
				"email",
				"metadata",
				"abc",
				"def",
				"foo",
				"bar",
				"special_field",
				"field_with_underscores",
			];

			for (const field of validSimpleFields) {
				const result = parseFieldPath({ field: `users.${field}`, state: testState });
				expect(result.table).toBe("users");
				expect(result.field).toBe(field);
				expect(result.jsonPathSegments).toEqual([]);
			}
		});

		it("should parse valid JSON paths without quotes", () => {
			const validJsonPaths = [
				"metadata->key",
				"metadata->level1->level2",
				"metadata->level1->level2->level3",
				"profile->settings->theme",
				"settings->ui->color",
			];

			for (const fieldPath of validJsonPaths) {
				const result = parseFieldPath({ field: `users.${fieldPath}`, state: testState });
				expect(result.table).toBe("users");
				expect(result.jsonPathSegments.length).toBeGreaterThan(0);
			}
		});

		it("should parse JSON paths with quoted segments", () => {
			const validQuotedPaths = [
				"metadata->'key'",
				"metadata->'level1'->'level2'",
				"metadata->level1->'level2'",
				"metadata->'level1'->level2",
				// Note: 'abc'->def won't work because 'abc' with quotes would be treated as a quoted field name
				// but the field is actually named 'abc' without quotes in our config
				"foo->'abc'", // This should work - unquoted field followed by quoted segment
			];

			for (const fieldPath of validQuotedPaths) {
				const result = parseFieldPath({ field: `users.${fieldPath}`, state: testState });
				expect(result.table).toBe("users");
				expect(result.jsonPathSegments.length).toBeGreaterThan(0);
			}
		});

		it("should parse alphanumeric and underscore field names", () => {
			const validFieldNames = [
				"field1",
				"field_2",
				"field_with_numbers123",
				"_field",
				"field_",
				"A",
				"Z",
				"a",
				"z",
				"Field123",
				"FIELD_NAME",
			];

			// Add these fields to config for testing
			const usersTable = testConfig.tables.users;
			if (!usersTable) throw new Error("Users table not found in config");

			const extendedConfig = {
				...testConfig,
				tables: {
					...testConfig.tables,
					users: {
						allowedFields: [
							...usersTable.allowedFields,
							...validFieldNames.map((name) => ({ name, type: "string" as const, nullable: false })),
						],
					},
				},
			};

			const extendedState = { ...testState, config: extendedConfig };

			for (const fieldName of validFieldNames) {
				const result = parseFieldPath({ field: `users.${fieldName}`, state: extendedState });
				expect(result.table).toBe("users");
				expect(result.field).toBe(fieldName);
			}
		});
	});

	describe("Invalid Field Paths - Should Fail", () => {
		it("should reject field paths with malformed JSON arrows", () => {
			const invalidArrowPaths = [
				"foo->'te->st'", // Arrow inside quotes
				"'->start'->foo", // Starting with arrow
				"foo->'bar->'", // Ending with arrow inside quotes
				"foo->bar->", // Ending with arrow
				"foo-->bar", // Double arrow
				"foo->->bar", // Empty segment between arrows
				"'->->->'", // Multiple arrows in quotes
				"foo->''", // Empty quoted segment
				"foo->''->bar", // Empty quoted segment in middle
			];

			for (const invalidPath of invalidArrowPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject empty and whitespace-only field names", () => {
			const emptyFieldNames = [
				"",
				" ",
				"  ",
				"\t",
				"\n",
				"users.", // Just table prefix
			];

			for (const emptyField of emptyFieldNames) {
				expect(() => {
					parseFieldPath({ field: emptyField, state: testState });
				}).toThrow();
			}
		});

		it("should reject field names starting with numbers", () => {
			const numericStartFields = ["1field", "2users", "123invalid", "9test", "0field"];

			for (const invalidField of numericStartFields) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidField}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject field names with special characters", () => {
			const specialCharFields = [
				"field-name", // Hyphen
				"field.name", // Dot
				"field@name", // At symbol
				"field#name", // Hash
				"field$name", // Dollar
				"field%name", // Percent
				"field^name", // Caret
				"field&name", // Ampersand
				"field*name", // Asterisk
				"field+name", // Plus
				"field=name", // Equals
				"field|name", // Pipe
				"field\\name", // Backslash
				"field/name", // Forward slash
				"field?name", // Question mark
				"field<name", // Less than
				"field>name", // Greater than
				"field,name", // Comma
				"field;name", // Semicolon
				"field:name", // Colon
				"field!name", // Exclamation
				"field~name", // Tilde
				"field`name", // Backtick
				"field name", // Space
			];

			for (const invalidField of specialCharFields) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidField}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject field names that are only underscores", () => {
			const underscoreOnlyFields = ["_", "__", "___", "____", "_____"];

			for (const invalidField of underscoreOnlyFields) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidField}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject malformed quoted segments", () => {
			const malformedQuotedPaths = [
				"metadata->'unclosed", // Unclosed quote
				"metadata->unclosed'", // Quote at end only
				"metadata->'middle'quote'", // Quote in middle
				"metadata->''", // Empty quoted segment
				"metadata->'key with -> arrow'", // Arrow inside quotes
				"metadata->'key'->'another -> arrow'", // Arrow inside second quoted segment
				"'field'->'segment'->'bad->arrow'", // Arrow inside final quoted segment
				"metadata->'nested''quote'", // Double quote
				"metadata->'\\'escaped'", // Backslash escape (not supported)
			];

			for (const invalidPath of malformedQuotedPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject JSON paths on non-object fields", () => {
			const invalidJsonOnNonObject = [
				"name->invalid", // String field with JSON path
				"id->invalid", // Number field with JSON path
				"email->invalid", // String field with JSON path
			];

			for (const invalidPath of invalidJsonOnNonObject) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject paths with consecutive arrows", () => {
			const consecutiveArrowPaths = [
				"metadata->->key",
				"metadata->key->->value",
				"metadata->->->key",
				"foo->->bar",
				"foo->bar->->baz",
			];

			for (const invalidPath of consecutiveArrowPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject paths starting or ending with arrows", () => {
			const arrowBoundaryPaths = ["->metadata", "metadata->", "->metadata->key", "metadata->key->", "->", "foo->bar->"];

			for (const invalidPath of arrowBoundaryPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});

		it("should reject completely invalid field path formats", () => {
			const completelyInvalidPaths = [
				".",
				"..",
				"...",
				"->",
				"<-",
				"<->",
				"==>",
				"|||",
				"###",
				"***",
				"???",
				"(((",
				")))",
				"[[[",
				"]]]",
				"{{{",
				"}}}",
			];

			for (const invalidPath of completelyInvalidPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow();
			}
		});
	});

	describe("Table Validation", () => {
		it("should reject unknown table references", () => {
			const unknownTables = ["unknown_table.field", "posts.title", "orders.amount", "admin.users"];

			for (const fieldPath of unknownTables) {
				expect(() => {
					parseFieldPath({ field: fieldPath, state: testState });
				}).toThrow(TABLE_NOT_ALLOWED_REGEX);
			}
		});

		it("should reject unknown field references on valid tables", () => {
			const unknownFields = ["users.unknown_field", "users.missing_column", "users.fake_field", "users.nonexistent"];

			for (const fieldPath of unknownFields) {
				expect(() => {
					parseFieldPath({ field: fieldPath, state: testState });
				}).toThrow(FIELD_NOT_ALLOWED_REGEX);
			}
		});

		it("should enforce target table constraints", () => {
			// When specifying a target table, field must reference that table
			expect(() => {
				parseFieldPath({
					field: "other_table.field",
					state: testState,
					targetTable: "users",
				});
			}).toThrow(TABLE_NOT_ALLOWED_REGEX);
		});
	});

	describe("JSON Path Segment Validation", () => {
		it("should handle deeply nested valid paths", () => {
			const deepPaths = [
				"metadata->a->b->c->d->e",
				"profile->user->contact->address->street->number",
				"settings->app->ui->theme->colors->primary->hex",
			];

			for (const fieldPath of deepPaths) {
				const result = parseFieldPath({ field: `users.${fieldPath}`, state: testState });
				expect(result.jsonPathSegments.length).toBeGreaterThan(4);
			}
		});

		it("should reject paths with empty JSON segments", () => {
			const emptySegmentPaths = ["metadata->''", "metadata->''->key", "metadata->key->''", "metadata->key->''->value"];

			for (const invalidPath of emptySegmentPaths) {
				expect(() => {
					parseFieldPath({ field: `users.${invalidPath}`, state: testState });
				}).toThrow(INVALID_FIELD_PATH_REGEX);
			}
		});

		it("should properly strip quotes from JSON path segments", () => {
			const quotedPath = "metadata->'level1'->'level2'->'level3'";
			const result = parseFieldPath({ field: `users.${quotedPath}`, state: testState });

			expect(result.jsonPathSegments).toEqual(["level1", "level2", "level3"]);
		});

		it("should handle mixed quoted and unquoted segments", () => {
			const mixedPath = "metadata->level1->'level2'->level3->'level4'";
			const result = parseFieldPath({ field: `users.${mixedPath}`, state: testState });

			expect(result.jsonPathSegments).toEqual(["level1", "level2", "level3", "level4"]);
		});
	});

	describe("Error Message Validation", () => {
		it("should provide clear error messages for regex failures", () => {
			const invalidPath = "users.123invalid";

			expect(() => {
				parseFieldPath({ field: invalidPath, state: testState });
			}).toThrow(INVALID_FIELD_PATH_REGEX);
		});

		it("should provide clear error messages for JSON access on wrong types", () => {
			const invalidPath = "users.name->invalid";

			expect(() => {
				parseFieldPath({ field: invalidPath, state: testState });
			}).toThrow();
		});

		it("should provide clear error messages for empty JSON segments", () => {
			const invalidPath = "users.metadata->''";

			expect(() => {
				parseFieldPath({ field: invalidPath, state: testState });
			}).toThrow(INVALID_FIELD_PATH_REGEX);
		});

		it("should provide clear error messages for unknown fields", () => {
			const invalidPath = "users.unknown_field";

			expect(() => {
				parseFieldPath({ field: invalidPath, state: testState });
			}).toThrow(FIELD_NOT_ALLOWED_REGEX);
		});

		it("should provide clear error messages for unknown tables", () => {
			const invalidPath = "unknown_table.field";

			expect(() => {
				parseFieldPath({ field: invalidPath, state: testState });
			}).toThrow(TABLE_NOT_ALLOWED_REGEX);
		});
	});

	describe("Edge Cases and Boundary Testing", () => {
		it("should handle extremely long field names", () => {
			const longFieldName = "a".repeat(1000);

			expect(() => {
				parseFieldPath({ field: `users.${longFieldName}`, state: testState });
			}).toThrow();
		});

		it("should handle extremely long JSON paths", () => {
			const longPath = `metadata->${Array.from({ length: 100 }, (_, i) => `level${i}`).join("->")}`;

			// Long paths should be allowed but might be impractical
			expect(() => {
				parseFieldPath({ field: `users.${longPath}`, state: testState });
			}).not.toThrow();
		});

		it("should handle unicode characters in field names", () => {
			const unicodeFields = [
				"field_中文",
				"field_العربية",
				"field_русский",
				"field_🚀",
				"field_\u0041", // Unicode A
			];

			for (const unicodeField of unicodeFields) {
				expect(() => {
					parseFieldPath({ field: `users.${unicodeField}`, state: testState });
				}).toThrow();
			}
		});

		it("should handle control characters and special unicode", () => {
			const controlCharFields = [
				"field\x00name", // Null byte
				"field\x01name", // Start of heading
				"field\x1fname", // Unit separator
				"field\x7fname", // Delete
				"field\u0080name", // High unicode
				"field\ufeffname", // BOM
			];

			for (const controlField of controlCharFields) {
				expect(() => {
					parseFieldPath({ field: `users.${controlField}`, state: testState });
				}).toThrow();
			}
		});
	});
});
