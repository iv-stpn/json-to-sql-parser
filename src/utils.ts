export function isNotNull<T>(value: T | null): value is T {
	return value !== null;
}

export function isInArray<_T, U extends readonly unknown[]>(array: U, value: unknown): value is U[number] {
	return array.includes(value as U[number]);
}

export function objectKeys<T extends object>(obj: T): (keyof T)[] {
	return Object.keys(obj) as (keyof T)[];
}

export function objectEntries<T extends Record<string, unknown>>(obj: T): [keyof T, T[keyof T]][] {
	return Object.entries(obj) as [keyof T, T[keyof T]][];
}

function removeAllWrappingParens(expression: string): string {
	expression = expression.trim();

	while (expression.startsWith("(") && expression.endsWith(")")) {
		let depth = 0;
		let wraps = true;

		for (let i = 0; i < expression.length; i++) {
			if (expression[i] === "(") depth++;
			if (expression[i] === ")") depth--;
			if (depth === 0 && i < expression.length - 1) {
				wraps = false;
				break;
			}
		}

		if (wraps) expression = expression.slice(1, -1).trim();
		else break;
	}

	return expression;
}

export const quote = (str: string): string => `'${str.replaceAll(/'/g, "''")}'`;
export const isEmpty = (str: string) => str.trim() === "";

export function applyFunction(functionName: string, args: string[]): string {
	if (args.length === 0) throw new Error(`Function '${functionName}' requires at least one argument`);
	return `${functionName}(${args.map((arg) => removeAllWrappingParens(arg)).join(", ")})`;
}
