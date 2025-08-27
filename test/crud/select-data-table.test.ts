import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { compileSelectQuery, parseSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import { parseExpression } from "../../src/parsers";
import type { AggregationQuery, Condition } from "../../src/schemas";
import type { Config, ParserState } from "../../src/types";
import { ExpressionTypeMap } from "../../src/utils/expression-map";
import { extractSelectWhereClause } from "../_helpers";

describe("CRUD - SELECT Data Table Configuration and Schema-less Storage", () => {
	let regularConfig: Config;
	let dataTableConfig: Config;

	beforeEach(() => {
		// Configuration without data table (regular tables)
		regularConfig = {
			dialect: Dialect.POSTGRESQL,
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
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: 123,
				adminRole: "admin",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
			],
		};

		// Configuration with data table (JSON-based storage)
		dataTableConfig = {
			dialect: Dialect.POSTGRESQL,
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
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: 123,
				adminRole: "admin",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
			],
			dataTable: {
				table: "data_storage",
				dataField: "data",
				tableField: "table_name",
				whereConditions: ["tenant_id = 'current_tenant'", "deleted_at IS NULL"],
			},
		};
	});

	describe("Basic Field Conditions - Regular vs Data Table Architecture", () => {
		const condition: Condition = { "users.active": true };

		it("should parse simple condition without data table", () => {
			const sql = extractSelectWhereClause(condition, regularConfig, "users");
			expect(sql).toBe("users.active = TRUE");
		});

		it("should parse simple condition with data table", () => {
			const sql = extractSelectWhereClause(condition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND (users.data->>'active')::BOOLEAN = TRUE)",
			);
		});
	});

	describe("Complex AND/OR Conditions - Regular vs Data Table Architecture", () => {
		const complexCondition: Condition = {
			$and: [{ "users.active": true }, { "users.age": { $gte: 18 } }, { "users.email": { $ne: null } }],
		};

		it("should parse complex AND condition without data table", () => {
			const sql = extractSelectWhereClause(complexCondition, regularConfig, "users");
			expect(sql).toBe("(users.active = TRUE AND users.age >= 18 AND users.email IS NOT NULL)");
		});

		it("should parse complex AND condition with data table", () => {
			const sql = extractSelectWhereClause(complexCondition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND ((users.data->>'active')::BOOLEAN = TRUE AND (users.data->>'age')::FLOAT >= 18 AND users.data->>'email' IS NOT NULL))",
			);
		});
	});

	describe("OR Logic Conditions - Regular vs Data Table Architecture", () => {
		const orCondition: Condition = {
			$or: [{ "users.active": true }, { "users.name": { $like: "Admin%" } }],
		};

		it("should parse OR condition without data table", () => {
			const sql = extractSelectWhereClause(orCondition, regularConfig, "users");
			expect(sql).toBe("(users.active = TRUE OR users.name LIKE 'Admin%')");
		});

		it("should parse OR condition with data table", () => {
			const sql = extractSelectWhereClause(orCondition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND ((users.data->>'active')::BOOLEAN = TRUE OR users.data->>'name' LIKE 'Admin%'))",
			);
		});
	});

	describe("JSON Field Access - Regular vs Data Table Architecture", () => {
		const jsonCondition: Condition = {
			"users.metadata->settings->theme": "dark",
		};

		it("should parse JSON field access without data table", () => {
			const sql = extractSelectWhereClause(jsonCondition, regularConfig, "users");
			expect(sql).toBe("users.metadata->'settings'->>'theme' = 'dark'");
		});

		it("should parse JSON field access with data table", () => {
			const sql = extractSelectWhereClause(jsonCondition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND users.data->'metadata'->'settings'->>'theme' = 'dark')",
			);
		});
	});

	describe("Select Queries - Regular vs Data Table", () => {
		const selection = {
			id: true,
			name: true,
			email: true,
		};

		it("should build select query without data table", () => {
			const result = parseSelectQuery({ rootTable: "users", selection }, regularConfig);
			const sql = compileSelectQuery(result);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name", users.email AS "email" FROM users');
		});

		it("should build select query with data table", () => {
			const result = parseSelectQuery({ rootTable: "users", selection }, dataTableConfig);
			const sql = compileSelectQuery(result);
			expect(sql).toBe(
				"SELECT (users.data->>'id')::UUID AS \"id\", users.data->>'name' AS \"name\", users.data->>'email' AS \"email\" FROM data_storage AS \"users\" WHERE (users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL)",
			);
		});
	});

	describe("Select with Conditions - Regular vs Data Table", () => {
		const selection = { id: true, name: true };
		const condition: Condition = { "users.active": true };

		it("should build select with condition without data table", () => {
			const result = parseSelectQuery({ rootTable: "users", selection, condition }, regularConfig);
			const sql = compileSelectQuery(result);
			expect(sql).toBe('SELECT users.id AS "id", users.name AS "name" FROM users WHERE users.active = TRUE');
		});

		it("should build select with condition with data table", () => {
			const result = parseSelectQuery({ rootTable: "users", selection, condition }, dataTableConfig);
			const sql = compileSelectQuery(result);
			expect(sql).toBe(
				"SELECT (users.data->>'id')::UUID AS \"id\", users.data->>'name' AS \"name\" FROM data_storage AS \"users\" WHERE (users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND (users.data->>'active')::BOOLEAN = TRUE)",
			);
		});
	});

	describe("Aggregation Queries - Regular vs Data Table", () => {
		const aggregationQuery: AggregationQuery = {
			table: "orders",
			groupBy: ["orders.status"],
			aggregatedFields: {
				total_amount: { function: "SUM", field: "orders.amount" },
				order_count: { function: "COUNT", field: "orders.id" },
			},
		};

		it("should build aggregation query without data table", () => {
			const result = parseAggregationQuery(aggregationQuery, regularConfig);
			const sql = compileAggregationQuery(result);
			expect(sql).toBe(
				'SELECT orders.status AS "status", SUM(orders.amount) AS "total_amount", COUNT(orders.id) AS "order_count" FROM orders GROUP BY orders.status',
			);
		});

		it("should build aggregation query with data table", () => {
			const result = parseAggregationQuery(aggregationQuery, dataTableConfig);
			const sql = compileAggregationQuery(result);
			expect(sql).toBe(
				"SELECT orders.data->>'status' AS \"status\", SUM((orders.data->>'amount')::FLOAT) AS \"total_amount\", COUNT(orders.data->>'id') AS \"order_count\" FROM data_storage AS \"orders\" WHERE (orders.table_name = 'orders' AND orders.tenant_id = 'current_tenant' AND orders.deleted_at IS NULL) GROUP BY orders.data->>'status'",
			);
		});
	});

	describe("JSON Path in Data Table - Advanced Cases", () => {
		const jsonAggregationQuery: AggregationQuery = {
			table: "users",
			groupBy: ["users.metadata->department"],
			aggregatedFields: {
				avg_age: { function: "AVG", field: "users.age" },
				user_count: { function: "COUNT", field: "*" },
			},
		};

		it("should handle JSON path aggregation without data table", () => {
			const result = parseAggregationQuery(jsonAggregationQuery, regularConfig);
			const sql = compileAggregationQuery(result);
			expect(sql).toBe(
				'SELECT users.metadata->>\'department\' AS "metadata->department", AVG(users.age) AS "avg_age", COUNT(*) AS "user_count" FROM users GROUP BY users.metadata->>\'department\'',
			);
		});

		it("should handle JSON path aggregation with data table", () => {
			const result = parseAggregationQuery(jsonAggregationQuery, dataTableConfig);
			const sql = compileAggregationQuery(result);
			expect(sql).toBe(
				"SELECT users.data->'metadata'->>'department' AS \"metadata->department\", AVG((users.data->>'age')::FLOAT) AS \"avg_age\", COUNT(*) AS \"user_count\" FROM data_storage AS \"users\" WHERE (users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL) GROUP BY users.data->'metadata'->>'department'",
			);
		});
	});

	describe("Expression Evaluation - Regular vs Data Table", () => {
		let regularState: ParserState;
		let dataTableState: ParserState;

		beforeEach(() => {
			regularState = { config: regularConfig, expressions: new ExpressionTypeMap(), rootTable: "users" };
			dataTableState = { config: dataTableConfig, expressions: new ExpressionTypeMap(), rootTable: "users" };
		});

		it("should evaluate field reference without data table", () => {
			const result = parseExpression({ $field: "users.name" }, regularState);
			expect(result).toBe("users.name");
		});

		it("should evaluate field reference with data table", () => {
			const result = parseExpression({ $field: "users.name" }, dataTableState);
			expect(result).toBe("users.data->>'name'");
		});

		it("should evaluate function with context variable without data table", () => {
			const result = parseExpression({ $func: { CONCAT: [{ $var: "current_user_id" }, " user"] } }, regularState);
			expect(result).toBe("((123)::TEXT || ' user')");
		});

		it("should evaluate function with context variable with data table", () => {
			const result = parseExpression({ $func: { CONCAT: [{ $var: "current_user_id" }, " user"] } }, dataTableState);
			expect(result).toBe("((123)::TEXT || ' user')");
		});
	});

	describe("Array Operations - Regular vs Data Table", () => {
		const arrayCondition: Condition = {
			"users.age": { $in: [18, 25, 30, 35] },
		};

		it("should parse array condition without data table", () => {
			const sql = extractSelectWhereClause(arrayCondition, regularConfig, "users");
			expect(sql).toBe("users.age IN (18, 25, 30, 35)");
		});

		it("should parse array condition with data table", () => {
			const sql = extractSelectWhereClause(arrayCondition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND (users.data->>'age')::FLOAT IN (18, 25, 30, 35))",
			);
		});
	});

	describe("Nested Conditions - Regular vs Data Table", () => {
		const nestedCondition: Condition = {
			$or: [
				{
					$and: [{ "users.active": true }, { "users.age": { $gte: 18 } }],
				},
				{
					$and: [{ "users.name": { $like: "Admin%" } }, { "users.email": { $ne: null } }],
				},
			],
		};

		it("should parse nested condition without data table", () => {
			const sql = extractSelectWhereClause(nestedCondition, regularConfig, "users");
			expect(sql).toBe("((users.active = TRUE AND users.age >= 18) OR (users.name LIKE 'Admin%' AND users.email IS NOT NULL))");
		});

		it("should parse nested condition with data table", () => {
			const sql = extractSelectWhereClause(nestedCondition, dataTableConfig, "users");
			expect(sql).toBe(
				"(users.table_name = 'users' AND users.tenant_id = 'current_tenant' AND users.deleted_at IS NULL AND (((users.data->>'active')::BOOLEAN = TRUE AND (users.data->>'age')::FLOAT >= 18) OR (users.data->>'name' LIKE 'Admin%' AND users.data->>'email' IS NOT NULL)))",
			);
		});
	});
});
