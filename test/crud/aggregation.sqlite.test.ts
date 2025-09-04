/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { Dialect } from "../../src/constants/dialects";
import type { AggregationQuery } from "../../src/schemas";
import type { Config } from "../../src/types";

let testConfig: Config;

beforeEach(() => {
	testConfig = {
		dialect: Dialect.SQLITE_EXTENSIONS,
		tables: {
			sales: {
				allowedFields: [
					{ name: "id", type: "uuid", nullable: false },
					{ name: "amount", type: "number", nullable: false },
					{ name: "region", type: "string", nullable: false },
					{ name: "date", type: "string", nullable: false },
					{ name: "customer_id", type: "uuid", nullable: false },
					{ name: "product_data", type: "object", nullable: true },
				],
			},
		},
		variables: {},
		relationships: [],
		dataTable: {
			table: "raw_data",
			dataField: "data",
			tableField: "table_name",
		},
	};
});

describe("CRUD - AGGREGATION Query Operations (SQLite)", () => {
	describe("Schema-less Data Table Aggregation", () => {
		it("should handle aggregation on schema-less data table", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					total_sales: { function: "SUM", field: "sales.amount" },
					count: { function: "COUNT", field: "*" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("sales.data->>'region' AS \"region\"");
			expect(sql).toContain("SUM(CAST(sales.data->>'amount' AS REAL)) AS \"total_sales\"");
			expect(sql).toContain('COUNT(*) AS "count"');
			expect(sql).toContain("GROUP BY sales.data->>'region'");
			expect(sql).toContain('FROM raw_data AS "sales"');
		});

		it("should handle JSON path aggregation in schema-less table", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.product_data->'category'"],
				aggregatedFields: {
					avg_amount: { function: "AVG", field: "sales.amount" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("sales.data->'product_data'->>'category' AS \"product_data->category\"");
			expect(sql).toContain("AVG(CAST(sales.data->>'amount' AS REAL)) AS \"avg_amount\"");
			expect(sql).toContain("GROUP BY sales.data->'product_data'->>'category'");
		});
	});

	describe("Complex aggregation expressions", () => {
		it("should handle aggregation with complex expressions", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					adjusted_total: {
						function: "SUM",
						field: {
							$func: {
								MULTIPLY: [
									{ $field: "sales.amount" },
									{ $cond: { if: { "sales.region": { $eq: "premium" } }, then: 1.2, else: 1.0 } },
								],
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain(
				"SUM(CAST(sales.data->>'amount' AS REAL) * (CASE WHEN sales.data->>'region' = 'premium' THEN 1.2 ELSE 1 END)) AS \"adjusted_total\"",
			);
		});

		it("should handle multiple aggregation operators", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					total: { function: "SUM", field: "sales.amount" },
					average: { function: "AVG", field: "sales.amount" },
					maximum: { function: "MAX", field: "sales.amount" },
					minimum: { function: "MIN", field: "sales.amount" },
					count: { function: "COUNT", field: "*" },
					unique_customers: { function: "COUNT_DISTINCT", field: "sales.customer_id" },
					regions_list: { function: "STRING_AGG", field: "sales.region", additionalArguments: [","] },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toBe(
				"SELECT sales.data->>'region' AS \"region\", SUM(CAST(sales.data->>'amount' AS REAL)) AS \"total\", AVG(CAST(sales.data->>'amount' AS REAL)) AS \"average\", MAX(CAST(sales.data->>'amount' AS REAL)) AS \"maximum\", MIN(CAST(sales.data->>'amount' AS REAL)) AS \"minimum\", COUNT(*) AS \"count\", COUNT(DISTINCT sales.data->>'customer_id') AS \"unique_customers\", GROUP_CONCAT(sales.data->>'region', ',') AS \"regions_list\" FROM raw_data AS \"sales\" WHERE sales.table_name = 'sales' GROUP BY sales.data->>'region'",
			);
		});
	});

	describe("SQLite-specific aggregation features", () => {
		it("should use GROUP_CONCAT instead of STRING_AGG", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					regions_list: { function: "STRING_AGG", field: "sales.region", additionalArguments: [","] },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toBe(
				"SELECT sales.data->>'region' AS \"region\", GROUP_CONCAT(sales.data->>'region', ',') AS \"regions_list\" FROM raw_data AS \"sales\" WHERE sales.table_name = 'sales' GROUP BY sales.data->>'region'",
			);
		});

		it("should handle SQLite numeric casting", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { function: "SUM", field: "sales.amount" },
					average: { function: "AVG", field: "sales.amount" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("SUM(CAST(sales.data->>'amount' AS REAL))");
			expect(sql).toContain("AVG(CAST(sales.data->>'amount' AS REAL))");
		});

		it("should handle boolean fields without casting", () => {
			const booleanConfig: Config = {
				...testConfig,
				tables: {
					sales: {
						allowedFields: [
							{ name: "id", type: "uuid", nullable: false },
							{ name: "is_active", type: "boolean", nullable: false },
							{ name: "region", type: "string", nullable: false },
						],
					},
				},
			};

			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					active_count: {
						function: "SUM",
						field: { $cond: { if: { "sales.is_active": { $eq: true } }, then: 1, else: 0 } },
					},
				},
			};

			const result = parseAggregationQuery(query, booleanConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("CASE WHEN sales.data->>'is_active' = TRUE THEN 1 ELSE 0 END");
		});
	});

	describe("Validation", () => {
		it("should validate COUNT(*) usage", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { function: "SUM", field: "*" },
				},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow(
				"Aggregation function 'SUM' cannot be used with '*'. Only COUNT(*) is supported.",
			);
		});

		it("should validate table exists", () => {
			const query: AggregationQuery = {
				table: "invalid_table",
				groupBy: [],
				aggregatedFields: {
					count: { function: "COUNT", field: "*" },
				},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Table 'invalid_table' is not allowed");
		});

		it("should validate field exists for group by", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.invalid_field"],
				aggregatedFields: {},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow(
				"Field 'invalid_field' is not allowed or does not exist in 'sales'",
			);
		});

		it("should validate field exists for aggregation", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { function: "SUM", field: "sales.invalid_field" },
				},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow(
				"Field 'invalid_field' is not allowed or does not exist in 'sales'",
			);
		});

		it("should validate table reference in field paths", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["wrong_table.region"],
				aggregatedFields: {},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Table 'wrong_table' is not allowed or does not exist");
		});

		it("should validate unknown aggregation operator", () => {
			const query = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { function: "UNKNOWN", field: "sales.amount" },
				},
			} as unknown as AggregationQuery;

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Invalid aggregation operator: UNKNOWN");
		});

		it("should validate empty aggregation query", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow(
				"Aggregation query must have at least one group by field or aggregated field",
			);
		});
	});
});

describe("Regular table aggregation (SQLite)", () => {
	let regularConfig: Config;

	beforeEach(() => {
		regularConfig = {
			dialect: Dialect.SQLITE_EXTENSIONS,
			tables: {
				sales: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "region", type: "string", nullable: false },
						{ name: "date", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
						{ name: "product_data", type: "object", nullable: true },
					],
				},
			},
			variables: {},
			relationships: [],
			// No dataTable config - this is a regular table
		};
	});

	it("should handle aggregation on regular table without dataTable config", () => {
		const query: AggregationQuery = {
			table: "sales",
			groupBy: ["sales.region"],
			aggregatedFields: {
				total_sales: { function: "SUM", field: "sales.amount" },
			},
		};

		const result = parseAggregationQuery(query, regularConfig);
		const sql = compileAggregationQuery(result);

		expect(sql).toContain('sales.region AS "region"');
		expect(sql).toContain('SUM(sales.amount) AS "total_sales"');
		expect(sql).toContain("GROUP BY sales.region");
		expect(sql).toContain("FROM sales");
		expect(sql).not.toContain("AS sales"); // No alias for regular tables
	});

	it("should handle JSON field access in regular table", () => {
		const query: AggregationQuery = {
			table: "sales",
			groupBy: ["sales.product_data->'category'"],
			aggregatedFields: { count: { function: "COUNT", field: "*" } },
		};

		const result = parseAggregationQuery(query, regularConfig);
		const sql = compileAggregationQuery(result);

		expect(sql).toContain("sales.product_data->>'category' AS \"product_data->category\"");
		expect(sql).toContain("GROUP BY sales.product_data->>'category'");
	});

	it("should handle regular table with SQLite functions", () => {
		const query: AggregationQuery = {
			table: "sales",
			groupBy: ["sales.region"],
			aggregatedFields: {
				total: { function: "SUM", field: "sales.amount" },
				average: { function: "AVG", field: "sales.amount" },
				count: { function: "COUNT", field: "*" },
				unique_customers: { function: "COUNT_DISTINCT", field: "sales.customer_id" },
				regions_list: { function: "STRING_AGG", field: "sales.region", additionalArguments: [","] },
			},
		};

		const result = parseAggregationQuery(query, regularConfig);
		const sql = compileAggregationQuery(result);

		expect(sql).toBe(
			'SELECT sales.region AS "region", SUM(sales.amount) AS "total", AVG(sales.amount) AS "average", COUNT(*) AS "count", COUNT(DISTINCT sales.customer_id) AS "unique_customers", GROUP_CONCAT(sales.region, \',\') AS "regions_list" FROM sales GROUP BY sales.region',
		);
	});
});

describe("SQLite minimal dialect", () => {
	let minimalConfig: Config;

	beforeEach(() => {
		minimalConfig = {
			dialect: Dialect.SQLITE_MINIMAL,
			tables: {
				sales: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "region", type: "string", nullable: false },
						{ name: "date", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
						{ name: "product_data", type: "object", nullable: true },
					],
				},
			},
			variables: {},
			relationships: [],
		};
	});

	it("should handle minimal SQLite dialect without JSON extensions", () => {
		const query: AggregationQuery = {
			table: "sales",
			groupBy: ["sales.region"],
			aggregatedFields: {
				total_sales: { function: "SUM", field: "sales.amount" },
				count: { function: "COUNT", field: "*" },
			},
		};

		const result = parseAggregationQuery(query, minimalConfig);
		const sql = compileAggregationQuery(result);

		expect(sql).toContain('sales.region AS "region"');
		expect(sql).toContain('SUM(sales.amount) AS "total_sales"');
		expect(sql).toContain('COUNT(*) AS "count"');
		expect(sql).toContain("GROUP BY sales.region");
		expect(sql).toContain("FROM sales");
	});

	it("should use basic SQLite functions in minimal dialect", () => {
		const query: AggregationQuery = {
			table: "sales",
			groupBy: ["sales.region"],
			aggregatedFields: {
				total: { function: "SUM", field: "sales.amount" },
				average: { function: "AVG", field: "sales.amount" },
				maximum: { function: "MAX", field: "sales.amount" },
				minimum: { function: "MIN", field: "sales.amount" },
				count: { function: "COUNT", field: "*" },
				unique_customers: { function: "COUNT_DISTINCT", field: "sales.customer_id" },
				regions_list: { function: "STRING_AGG", field: "sales.region", additionalArguments: [","] },
			},
		};

		const result = parseAggregationQuery(query, minimalConfig);
		const sql = compileAggregationQuery(result);

		expect(sql).toBe(
			'SELECT sales.region AS "region", SUM(sales.amount) AS "total", AVG(sales.amount) AS "average", MAX(sales.amount) AS "maximum", MIN(sales.amount) AS "minimum", COUNT(*) AS "count", COUNT(DISTINCT sales.customer_id) AS "unique_customers", GROUP_CONCAT(sales.region, \',\') AS "regions_list" FROM sales GROUP BY sales.region',
		);
	});
});

describe("SQLite Edge Cases", () => {
	describe("Complex mathematical operations", () => {
		it("should handle complex mathematical aggregations with SQLite casting", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					weighted_total: {
						function: "SUM",
						field: {
							$func: {
								MULTIPLY: [
									{ $field: "sales.amount" },
									{
										$cond: {
											if: { "sales.region": { $eq: "premium" } },
											then: 1.5,
											else: 1.0,
										},
									},
								],
							},
						},
					},
					avg_calculation: {
						function: "AVG",
						field: {
							$func: {
								DIVIDE: [
									{ $field: "sales.amount" },
									{
										$func: {
											ADD: [1, 0.1],
										},
									},
								],
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toBe(
				"SELECT sales.data->>'region' AS \"region\", SUM(CAST(sales.data->>'amount' AS REAL) * (CASE WHEN sales.data->>'region' = 'premium' THEN 1.5 ELSE 1 END)) AS \"weighted_total\", AVG(CAST(sales.data->>'amount' AS REAL) / (1 + 0.1)) AS \"avg_calculation\" FROM raw_data AS \"sales\" WHERE sales.table_name = 'sales' GROUP BY sales.data->>'region'",
			);
		});
	});

	describe("String operations", () => {
		it("should handle string concatenation in aggregations", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					concatenated_ids: {
						function: "STRING_AGG",
						field: {
							$func: {
								CONCAT: ["ID:", { $field: "sales.id" }],
							},
						},
						additionalArguments: [","],
					},
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toBe(
				"SELECT sales.data->>'region' AS \"region\", GROUP_CONCAT('ID:' || CAST(sales.data->>'id' AS TEXT), ',') AS \"concatenated_ids\" FROM raw_data AS \"sales\" WHERE sales.table_name = 'sales' GROUP BY sales.data->>'region'",
			);
		});
	});

	describe("Null handling", () => {
		it("should handle null values in aggregations", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					non_null_count: {
						function: "COUNT",
						field: "sales.amount",
					},
					total_including_null: {
						function: "SUM",
						field: {
							$func: {
								COALESCE_NUMBER: [{ $field: "sales.amount" }, 0],
							},
						},
					},
				},
			};

			const result = parseAggregationQuery(query, testConfig);
			const sql = compileAggregationQuery(result);

			expect(sql).toContain("COUNT");
			expect(sql).toContain("SUM");
			expect(sql).toContain("COALESCE");
		});
	});
});
