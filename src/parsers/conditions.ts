import { ExpressionTypeMap } from "../expression-map";
import { conditionSchema } from "../schemas";
import type { Config, Primitive } from "../types";
import { parseCondition } from ".";

export function parseWhereClause(condition: unknown, config: Config, rootTable: string) {
	const params: Primitive[] = [];
	const expressions = new ExpressionTypeMap();
	const sql = parseCondition(conditionSchema.parse(condition), { config, params, expressions, rootTable });
	return { sql, params };
}
