import { beforeEach, describe, expect, it } from "bun:test";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { parseExpression } from "../../src/parsers";
import type { Condition } from "../../src/schemas";
import type { Config, ParserState } from "../../src/types";
import { ExpressionTypeMap } from "../../src/utils/expression-map";
import { extractSelectWhereClause } from "../_helpers";

describe("Parser - Type Casting and Temporal Data Validation", () => {
	let testConfig: Config;
	let testState: ParserState;

	beforeEach(() => {
		testConfig = {
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "updated_at", type: "datetime", nullable: true },
						{ name: "birth_date", type: "date", nullable: true },
						{ name: "profile_id", type: "uuid", nullable: true },
						{ name: "last_login", type: "datetime", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
					],
				},
				events: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "event_type", type: "string", nullable: false },
						{ name: "occurred_at", type: "datetime", nullable: false },
						{ name: "scheduled_date", type: "date", nullable: true },
						{ name: "session_id", type: "uuid", nullable: true },
					],
				},
			},
			variables: {
				"auth.uid": "550e8400-e29b-41d4-a716-446655440000",
				current_timestamp: "2024-01-15T10:30:45",
				current_date: "2024-01-15",
				system_user_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			},
			relationships: [
				{
					table: "users",
					field: "id",
					toTable: "events",
					toField: "user_id",
					type: "one-to-many",
				},
			],
		};

		testState = {
			config: testConfig,
			rootTable: "users",
			expressions: new ExpressionTypeMap(),
		};
	});

	describe("UUID Type Validation and Casting", () => {
		it("should parse and cast UUID literals in WHERE conditions", () => {
			const condition: Condition = {
				"users.id": { $eq: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.id = '550e8400-e29b-41d4-a716-446655440000'::UUID");
		});

		it("should handle UUID values with comparison operators", () => {
			const conditions: Condition[] = [
				{ "users.id": { $ne: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } } },
				{
					"users.profile_id": {
						$in: [{ $uuid: "550e8400-e29b-41d4-a716-446655440000" }, { $uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" }],
					},
				},
			];

			for (const condition of conditions) {
				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBeTruthy();
				expect(sql).toContain("550e8400-e29b-41d4-a716-446655440000");
			}
		});

		it("should resolve and cast UUID variables", () => {
			const condition: Condition = {
				"users.id": { $eq: { $var: "auth.uid" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.id)::TEXT = '550e8400-e29b-41d4-a716-446655440000'");
		});

		it("should reject invalid UUID formats", () => {
			const invalidUuids = [
				"550e8400-e29b-41d4-a716", // too short
				"550e8400-e29b-41d4-a716-446655440000-extra", // too long
				"550e8400-e29b-41d4-a716-44665544000g", // invalid character
				"550e8400e29b41d4a716446655440000", // missing hyphens
				"550E8400-E29B-41D4-A716-446655440000", // uppercase (should pass with case insensitive)
			];

			const validUppercase = {
				$uuid: "550E8400-E29B-41D4-A716-446655440000",
			};
			expect(() => parseExpression(validUppercase, testState)).not.toThrow();

			const invalidCases = invalidUuids.slice(0, -1); // exclude uppercase test
			for (const invalidUuid of invalidCases) {
				const uuidExpression = { $uuid: invalidUuid };
				expect(() => parseExpression(uuidExpression, testState)).toThrow("Invalid UUID format");
			}
		});

		it("should parse UUID in SELECT queries", () => {
			const query = {
				rootTable: "users",
				selection: {
					"users.id": true,
					"users.name": true,
				},
				condition: {
					"users.profile_id": { $eq: { $uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" } },
				},
			};

			const sql = compileSelectQuery(parseSelectQuery(query, testConfig));
			expect(sql).toContain("users.profile_id = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'");
		});
	});

	describe("DateTime/Timestamp Type Validation and Casting", () => {
		it("should parse and cast timestamp literals in WHERE conditions", () => {
			const condition: Condition = {
				"users.created_at": { $gte: { $timestamp: "2024-01-01T00:00:00" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.created_at >= '2024-01-01 00:00:00'::TIMESTAMP");
		});

		it("should handle timestamp values with comparison operators", () => {
			const conditions = [
				{ "users.created_at": { $gt: { $timestamp: "2024-01-01T00:00:00" } } } as Condition,
				{ "users.updated_at": { $lt: { $timestamp: "2024-12-31T23:59:59" } } } as Condition,
				{ "users.last_login": { $gte: { $timestamp: "2024-06-15T12:00:00" } } } as Condition,
				{ "users.last_login": { $lte: { $timestamp: "2024-06-15T18:00:00" } } } as Condition,
			];

			for (const condition of conditions) {
				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBeTruthy();
				expect(sql).toContain("::TIMESTAMP");
			}
		});

		it("should resolve and cast timestamp variables", () => {
			const condition: Condition = {
				"users.created_at": { $lt: { $var: "current_timestamp" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.created_at)::TEXT < '2024-01-15T10:30:45'");
		});

		it("should reject invalid timestamp formats", () => {
			const invalidTimestamps = [
				"2024-01-15", // date only
				"10:30:45", // time only
				"2024-13-15T10:30:45", // invalid month
				"2024-01-32T10:30:45", // invalid day
				"2024-01-15T25:30:45", // invalid hour
				"2024-01-15T10:60:45", // invalid minute
				"2024-01-15T10:30:60", // invalid second
				"2024-1-15T10:30:45", // single digit month
				"24-01-15T10:30:45", // 2-digit year
				"2024/01/15T10:30:45", // wrong date separator
			];

			for (const invalidTimestamp of invalidTimestamps) {
				const timestampExpression = { $timestamp: invalidTimestamp };
				expect(() => parseExpression(timestampExpression, testState)).toThrow("Invalid timestamp format");
			}
		});

		it("should parse timestamp ranges", () => {
			const condition: Condition = {
				$and: [
					{ "events.occurred_at": { $gte: { $timestamp: "2024-01-01T00:00:00" } } },
					{ "events.occurred_at": { $lt: { $timestamp: "2024-02-01T00:00:00" } } },
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "events");
			expect(sql).toBe(
				"(events.occurred_at >= '2024-01-01 00:00:00'::TIMESTAMP AND events.occurred_at < '2024-02-01 00:00:00'::TIMESTAMP)",
			);
		});
	});

	describe("Date Type Validation and Casting", () => {
		it("should parse and cast date literals in WHERE conditions", () => {
			const condition: Condition = {
				"users.birth_date": { $eq: { $date: "1990-05-15" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("users.birth_date = '1990-05-15'::DATE");
		});

		it("should handle date values with comparison operators", () => {
			const conditions = [
				{ "users.birth_date": { $gt: { $date: "1990-01-01" } } },
				{ "users.birth_date": { $lt: { $date: "2000-12-31" } } },
				{ "users.birth_date": { $gte: { $date: "1985-01-01" } } },
				{ "users.birth_date": { $lte: { $date: "1995-12-31" } } },
			];

			for (const condition of conditions) {
				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBeTruthy();
				expect(sql).toContain("::DATE");
			}
		});

		it("should resolve and cast date variables", () => {
			const condition: Condition = {
				"events.scheduled_date": { $eq: { $var: "current_date" } },
			};

			const sql = extractSelectWhereClause(condition, testConfig, "events");
			expect(sql).toBe("(events.scheduled_date)::TEXT = '2024-01-15'");
		});

		it("should reject invalid date formats", () => {
			const invalidDates = [
				"2024-13-15", // invalid month
				"2024-01-32", // invalid day
				"2024-02-30", // invalid day for February
				"2023-02-29", // invalid day for non-leap year
				"2024-1-15", // single digit month
				"2024-01-1", // single digit day
				"24-01-15", // 2-digit year
				"2024/01/15", // wrong separator
				"01-15-2024", // wrong order
				"2024-01", // incomplete
				"2024-01-15T10:30:45", // timestamp format
			];

			for (const invalidDate of invalidDates) {
				const dateExpression = { $date: invalidDate };
				expect(() => parseExpression(dateExpression, testState)).toThrow("Invalid date format");
			}
		});

		it("should parse date ranges", () => {
			const condition: Condition = {
				$and: [
					{ "users.birth_date": { $gte: { $date: "1980-01-01" } } },
					{ "users.birth_date": { $lt: { $date: "2000-01-01" } } },
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "users");
			expect(sql).toBe("(users.birth_date >= '1980-01-01'::DATE AND users.birth_date < '2000-01-01'::DATE)");
		});
	});

	describe("Multi-Type Temporal Query Scenarios", () => {
		it("should handle queries with UUID, timestamp, and date together", () => {
			const condition: Condition = {
				$and: [
					{ "events.user_id": { $eq: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } } },
					{ "events.occurred_at": { $gte: { $timestamp: "2024-01-01T00:00:00" } } },
					{ "events.scheduled_date": { $eq: { $date: "2024-01-15" } } },
				],
			};

			const sql = extractSelectWhereClause(condition, testConfig, "events");
			expect(sql).toBe(
				"(events.user_id = '550e8400-e29b-41d4-a716-446655440000'::UUID AND events.occurred_at >= '2024-01-01 00:00:00'::TIMESTAMP AND events.scheduled_date = '2024-01-15'::DATE)",
			);
		});

		it("should handle NULL comparisons with date types", () => {
			const conditions = [
				{ "users.birth_date": { $eq: null } } as Condition,
				{ "users.last_login": { $ne: null } } as Condition,
				{ "users.profile_id": { $eq: null } } as Condition,
			];

			for (const condition of conditions) {
				const sql = extractSelectWhereClause(condition, testConfig, "users");
				expect(sql).toBeTruthy();
			}
		});

		it("should handle IN operations with date types", () => {
			const condition: Condition = {
				"events.scheduled_date": {
					$in: [{ $date: "2024-01-15" }, { $date: "2024-01-16" }, { $date: "2024-01-17" }],
				},
			};

			const sql = extractSelectWhereClause(condition, testConfig, "events");
			expect(sql).toBe("events.scheduled_date IN ('2024-01-15'::DATE, '2024-01-16'::DATE, '2024-01-17'::DATE)");
		});

		it("should parse SELECT query with date type fields", () => {
			const query = {
				rootTable: "events",
				selection: {
					"events.id": true,
					"events.user_id": true,
					"events.occurred_at": true,
					"events.scheduled_date": true,
				},
				condition: {
					$and: [
						{ "events.user_id": { $eq: { $var: "auth.uid" } } },
						{ "events.occurred_at": { $gte: { $timestamp: "2024-01-01T00:00:00" } } },
					],
				},
			};

			const sql = compileSelectQuery(parseSelectQuery(query, testConfig));
			expect(sql).toContain("events.user_id");
			expect(sql).toContain("(events.user_id)::TEXT = '550e8400-e29b-41d4-a716-446655440000'");
			expect(sql).toContain("events.occurred_at >= '2024-01-01 00:00:00'::TIMESTAMP");
		});
	});

	describe("Type Casting Edge Cases and Error Validation", () => {
		it("should handle empty string values", () => {
			const expressions = [{ $uuid: "" }, { $timestamp: "" }, { $date: "" }];

			for (const expression of expressions) {
				expect(() => parseExpression(expression, testState)).toThrow();
			}
		});

		it("should handle special date values", () => {
			// Test leap year edge cases
			const validLeapYear = { $date: "2024-02-29" }; // valid leap year
			const invalidNonLeapYear = { $date: "2023-02-29" }; // invalid non-leap year

			expect(() => parseExpression(validLeapYear, testState)).not.toThrow();
			expect(() => parseExpression(invalidNonLeapYear, testState)).toThrow("Invalid date format");
		});

		it("should handle timestamp microseconds precision", () => {
			const timestampsWithMicroseconds = [
				{ $timestamp: "2024-01-15T10:30:45.1" },
				{ $timestamp: "2024-01-15T10:30:45.12" },
				{ $timestamp: "2024-01-15T10:30:45.123" },
				{ $timestamp: "2024-01-15T10:30:45.1234" },
				{ $timestamp: "2024-01-15T10:30:45.12345" },
				{ $timestamp: "2024-01-15T10:30:45.123456" },
				{ $timestamp: "2024-01-15T10:30:45.1234567" }, // too many digits
			];

			// Valid microsecond precisions (1-6 digits)
			for (let i = 0; i < 6; i++) {
				const timestamp = timestampsWithMicroseconds[i];
				if (timestamp) {
					expect(() => parseExpression(timestamp, testState)).not.toThrow();
				}
			}

			// Invalid microsecond precision (7 digits)
			const invalidTimestamp = timestampsWithMicroseconds[6];
			if (invalidTimestamp) {
				expect(() => parseExpression(invalidTimestamp, testState)).toThrow("Invalid timestamp format");
			}
		});
	});
});
