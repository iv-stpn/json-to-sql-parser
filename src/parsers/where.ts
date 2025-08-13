import { conditionSchema } from "../schemas";
import type { Config, Primitive } from "../types";
import { ExpressionTypeMap } from "../utils/expression-map";
import { parseCondition } from ".";

export function parseWhereClause(condition: unknown, config: Config, rootTable: string) {
	const params: Primitive[] = [];
	const expressions = new ExpressionTypeMap();
	const sql = parseCondition(conditionSchema.parse(condition), { config, params, expressions, rootTable });
	return { sql, params };
}
