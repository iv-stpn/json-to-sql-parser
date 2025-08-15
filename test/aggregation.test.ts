/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import type { AggregationQuery } from "../src";
import { compileAggregationQuery, parseAggregationQuery } from "../src/builders/aggregate";
import type { Config } from "../src/types";

let testConfig: Config;

beforeEach(() => {
	testConfig = {
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

describe("Aggregation Edge Cases", () => {
	describe("Schema-less data table aggregation", () => {
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

			expect(result.select).toContain("sales.data->>'region' AS \"region\"");
			expect(result.select).toContain("SUM((sales.data->>'amount')::FLOAT) AS \"total_sales\"");
			expect(result.select).toContain('COUNT(*) AS "count"');
			expect(result.groupBy).toContain("sales.data->>'region'");
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

			expect(result.select).toContain("sales.data->'product_data'->>'category' AS \"product_data->category\"");
			expect(result.select).toContain("AVG((sales.data->>'amount')::FLOAT) AS \"avg_amount\"");
			expect(result.groupBy).toContain("sales.data->'product_data'->>'category'");
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

			expect(result.select).toContain(
				"SUM((sales.data->>'amount')::FLOAT * (CASE WHEN sales.data->>'region' = $1 THEN 1.2 ELSE 1 END)) AS \"adjusted_total\"",
			);
			expect(result.params).toEqual(["premium"]);
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
					std_dev: { function: "STDDEV", field: "sales.amount" },
					variance: { function: "VARIANCE", field: "sales.amount" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);

			expect(result.select).toContain("SUM((sales.data->>'amount')::FLOAT) AS \"total\"");
			expect(result.select).toContain("AVG((sales.data->>'amount')::FLOAT) AS \"average\"");
			expect(result.select).toContain("MAX((sales.data->>'amount')::FLOAT) AS \"maximum\"");
			expect(result.select).toContain("MIN((sales.data->>'amount')::FLOAT) AS \"minimum\"");
			expect(result.select).toContain('COUNT(*) AS "count"');
			expect(result.select).toContain("COUNT(DISTINCT (sales.data->>'customer_id')::UUID) AS \"unique_customers\"");
			expect(result.select).toContain("STRING_AGG(sales.data->>'region', ',') AS \"regions_list\"");
			expect(result.select).toContain("STDDEV((sales.data->>'amount')::FLOAT) AS \"std_dev\"");
			expect(result.select).toContain("VARIANCE((sales.data->>'amount')::FLOAT) AS \"variance\"");
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
				"Field 'invalid_field' is not allowed or does not exist for table 'sales'",
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
				"Field 'invalid_field' is not allowed or does not exist for table 'sales'",
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

describe("Regular table aggregation", () => {
	let regularConfig: Config;

	beforeEach(() => {
		regularConfig = {
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

		expect(result.select).toContain('sales.region AS "region"');
		expect(result.select).toContain('SUM(sales.amount) AS "total_sales"');
		expect(result.groupBy).toContain("sales.region");
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

		expect(result.select).toContain("sales.product_data->>'category' AS \"product_data->category\"");
		expect(result.groupBy).toContain("sales.product_data->>'category'");
	});
});
