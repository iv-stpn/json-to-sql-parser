/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { beforeEach, describe, expect, it } from "bun:test";
import { MISSING_AGGREGATION_FIELD } from "../src/constants/errors";
import type { AggregationQuery } from "../src/parsers/aggregate";
import { compileAggregationQuery, parseAggregationQuery } from "../src/parsers/aggregate";
import type { Config } from "../src/types";

let testConfig: Config;

export function validateGroupByInSelect(query: AggregationQuery): void {
	const { groupBy, aggregatedFields } = query;
	if (groupBy.length === 0 && Object.keys(aggregatedFields).length === 0) throw new Error(MISSING_AGGREGATION_FIELD);

	// Pure aggregation without grouping is allowed (e.g., COUNT(*))
	// For queries with GROUP BY, all non-aggregated fields in SELECT must be in GROUP BY
	// This is automatically ensured by our design since groupBy are automatically added to SELECT
}

beforeEach(() => {
	testConfig = {
		tables: {
			sales: {
				allowedFields: [
					{ name: "id", type: "number", nullable: false },
					{ name: "amount", type: "number", nullable: false },
					{ name: "region", type: "string", nullable: false },
					{ name: "date", type: "string", nullable: false },
					{ name: "customer_id", type: "number", nullable: false },
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
					total_sales: { operator: "SUM", field: "sales.amount" },
					count: { operator: "COUNT", field: "*" },
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
					avg_amount: { operator: "AVG", field: "sales.amount" },
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
						operator: "SUM",
						field: {
							$expr: {
								MULTIPLY: [
									{ $expr: "sales.amount" },
									{
										$cond: {
											if: { "sales.region": { $eq: "premium" } },
											then: 1.2,
											else: 1.0,
										},
									},
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
					total: { operator: "SUM", field: "sales.amount" },
					average: { operator: "AVG", field: "sales.amount" },
					maximum: { operator: "MAX", field: "sales.amount" },
					minimum: { operator: "MIN", field: "sales.amount" },
					count: { operator: "COUNT", field: "*" },
					unique_customers: { operator: "COUNT_DISTINCT", field: "sales.customer_id" },
					regions_list: { operator: "STRING_AGG", field: "sales.region" },
					std_dev: { operator: "STDDEV", field: "sales.amount" },
					variance: { operator: "VARIANCE", field: "sales.amount" },
				},
			};

			const result = parseAggregationQuery(query, testConfig);

			expect(result.select).toContain("SUM((sales.data->>'amount')::FLOAT) AS \"total\"");
			expect(result.select).toContain("AVG((sales.data->>'amount')::FLOAT) AS \"average\"");
			expect(result.select).toContain("MAX((sales.data->>'amount')::FLOAT) AS \"maximum\"");
			expect(result.select).toContain("MIN((sales.data->>'amount')::FLOAT) AS \"minimum\"");
			expect(result.select).toContain('COUNT(*) AS "count"');
			expect(result.select).toContain("COUNT(DISTINCT (sales.data->>'customer_id')::FLOAT) AS \"unique_customers\"");
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
					total: { operator: "SUM", field: "*" },
				},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Operator 'SUM' cannot be used with '*'");
		});

		it("should validate table exists", () => {
			const query: AggregationQuery = {
				table: "invalid_table",
				groupBy: [],
				aggregatedFields: {
					count: { operator: "COUNT", field: "*" },
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

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Field 'invalid_field' is not allowed for table 'sales'");
		});

		it("should validate field exists for aggregation", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { operator: "SUM", field: "sales.invalid_field" },
				},
			};

			expect(() => parseAggregationQuery(query, testConfig)).toThrow("Field 'invalid_field' is not allowed for table 'sales'");
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
					total: { operator: "UNKNOWN", field: "sales.amount" },
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

	describe("Group By validation", () => {
		it("should pass validation for valid query", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: ["sales.region"],
				aggregatedFields: {
					total: { operator: "SUM", field: "sales.amount" },
				},
			};

			expect(() => validateGroupByInSelect(query)).not.toThrow();
		});

		it("should pass validation for pure aggregation", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {
					total: { operator: "COUNT", field: "*" },
				},
			};

			expect(() => validateGroupByInSelect(query)).not.toThrow();
		});

		it("should throw error for empty query", () => {
			const query: AggregationQuery = {
				table: "sales",
				groupBy: [],
				aggregatedFields: {},
			};

			expect(() => validateGroupByInSelect(query)).toThrow(
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
						{ name: "id", type: "number", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "region", type: "string", nullable: false },
						{ name: "date", type: "string", nullable: false },
						{ name: "customer_id", type: "number", nullable: false },
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
				total_sales: { operator: "SUM", field: "sales.amount" },
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
			aggregatedFields: { count: { operator: "COUNT", field: "*" } },
		};

		const result = parseAggregationQuery(query, regularConfig);

		expect(result.select).toContain("sales.product_data->>'category' AS \"product_data->category\"");
		expect(result.groupBy).toContain("sales.product_data->>'category'");
	});
});
