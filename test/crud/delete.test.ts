import { describe, expect, test } from "bun:test";
import type { Config, DeleteQuery } from "../../src";
import { buildDeleteQuery, compileDeleteQuery, parseDeleteQuery } from "../../src";
import { Dialect } from "../../src/constants/dialects";

const config: Config = {
	dialect: Dialect.POSTGRESQL,
	tables: {
		users: {
			allowedFields: [
				{ name: "id", type: "uuid", nullable: false },
				{ name: "name", type: "string", nullable: false },
				{ name: "email", type: "string", nullable: true },
				{ name: "active", type: "boolean", nullable: false },
				{ name: "age", type: "number", nullable: true },
			],
		},
		posts: {
			allowedFields: [
				{ name: "id", type: "uuid", nullable: false },
				{ name: "title", type: "string", nullable: false },
				{ name: "content", type: "string", nullable: true },
				{ name: "user_id", type: "uuid", nullable: false },
				{ name: "published", type: "boolean", nullable: false },
			],
		},
	},
	variables: {},
	relationships: [],
};

describe("Delete Query Builder", () => {
	describe("parseDeleteQuery", () => {
		test("should parse delete query without condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
			};

			const result = parseDeleteQuery(deleteQuery, config);

			expect(result).toEqual({
				table: "users",
			});
		});

		test("should parse delete query with simple condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: {
					active: false,
				},
			};

			const result = parseDeleteQuery(deleteQuery, config);

			expect(result).toEqual({
				table: "users",
				where: "users.active = FALSE",
			});
		});

		test("should parse delete query with complex condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: {
					$and: [{ active: false }, { age: { $lt: 18 } }],
				},
			};

			const result = parseDeleteQuery(deleteQuery, config);

			expect(result).toEqual({
				table: "users",
				where: "(users.active = FALSE AND users.age < 18)",
			});
		});

		test("should throw error for non-existent table", () => {
			const deleteQuery: DeleteQuery = {
				table: "non_existent",
			};

			expect(() => parseDeleteQuery(deleteQuery, config)).toThrow("Table 'non_existent' is not allowed or does not exist");
		});

		test("should throw error when condition evaluates to false", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: false,
			};

			expect(() => parseDeleteQuery(deleteQuery, config)).toThrow("Delete condition not met.");
		});
	});

	describe("compileDeleteQuery", () => {
		test("should compile delete query without WHERE clause", () => {
			const parsedQuery = {
				table: "users",
			};

			const result = compileDeleteQuery(parsedQuery);

			expect(result).toBe("DELETE FROM users");
		});

		test("should compile delete query with WHERE clause", () => {
			const parsedQuery = {
				table: "users",
				where: "users.active = FALSE",
			};

			const result = compileDeleteQuery(parsedQuery);

			expect(result).toBe("DELETE FROM users WHERE users.active = FALSE");
		});

		test("should compile delete query with complex WHERE clause", () => {
			const parsedQuery = {
				table: "users",
				where: "(users.active = FALSE AND users.age < 18)",
			};

			const result = compileDeleteQuery(parsedQuery);

			expect(result).toBe("DELETE FROM users WHERE (users.active = FALSE AND users.age < 18)");
		});
	});

	describe("buildDeleteQuery", () => {
		test("should build complete delete query without condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
			};

			const result = buildDeleteQuery(deleteQuery, config);

			expect(result).toBe("DELETE FROM users");
		});

		test("should build complete delete query with condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: {
					active: false,
				},
			};

			const result = buildDeleteQuery(deleteQuery, config);

			expect(result).toBe("DELETE FROM users WHERE users.active = FALSE");
		});

		test("should build delete query with OR condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "posts",
				condition: {
					$or: [{ published: false }, { title: { $like: "draft%" } }],
				},
			};

			const result = buildDeleteQuery(deleteQuery, config);

			expect(result).toBe("DELETE FROM posts WHERE (posts.published = FALSE OR posts.title LIKE 'draft%')");
		});

		test("should build delete query with field comparison", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: {
					age: { $gte: 65 },
				},
			};

			const result = buildDeleteQuery(deleteQuery, config);

			expect(result).toBe("DELETE FROM users WHERE users.age >= 65");
		});

		test("should build delete query with IN condition", () => {
			const deleteQuery: DeleteQuery = {
				table: "users",
				condition: {
					age: { $in: [25, 30, 35] },
				},
			};

			const result = buildDeleteQuery(deleteQuery, config);

			expect(result).toBe("DELETE FROM users WHERE users.age IN (25, 30, 35)");
		});
	});
});
