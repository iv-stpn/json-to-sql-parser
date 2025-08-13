import type { CastType } from "./operators";
import type { ExpressionObject } from "./schemas";
import { objectKeys } from "./utils";

export class ExpressionTypeMap {
	private map: Map<string, CastType>;

	constructor() {
		this.map = new Map<string, CastType>();
	}

	private _deepSort(obj: unknown): unknown {
		if (Array.isArray(obj)) {
			return obj.map((item) => this._deepSort(item));
		}

		if (typeof obj === "object" && obj !== null && obj.constructor === Object) {
			const sorted: Record<string, unknown> = {};
			const keys = objectKeys(obj).sort();

			for (const key of keys) sorted[key] = this._deepSort((obj as Record<string, unknown>)[key]);
			return sorted;
		}

		return obj;
	}

	private _serialize(key: ExpressionObject): string {
		return JSON.stringify(this._deepSort(key));
	}

	add(key: ExpressionObject, value: CastType): boolean {
		const serializedKey = this._serialize(key);
		if (!this.map.has(serializedKey)) {
			this.map.set(serializedKey, value);
			return true;
		}
		return false;
	}

	get(key: ExpressionObject): CastType {
		const type = this.map.get(this._serialize(key));
		if (type === undefined) throw new Error(`Expression type not found for key: ${JSON.stringify(key)}`);
		return type;
	}
}
