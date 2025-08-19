export function isNotNull<T>(value: T | null): value is T {
	return value !== null;
}

export function isNonEmptyArray<T>(value: unknown): value is [T, ...T[]] {
	return Array.isArray(value) && value.length > 0;
}

export function objectSize<T extends object>(obj: T): number {
	return Object.keys(obj).length;
}

export function objectKeys<T extends object>(obj: T): (keyof T)[] {
	return Object.keys(obj) as (keyof T)[];
}

export function objectEntries<T extends Record<string, unknown>>(obj: T): [keyof T, T[keyof T]][] {
	return Object.entries(obj) as [keyof T, T[keyof T]][];
}

export const quote = (str: string): string => `'${str.replaceAll(/'/g, "''")}'`;
export const doubleQuote = (str: string): string => `"${str.replaceAll(/"/g, '""')}"`;

export const getErrorMessage = (error: unknown): string => {
	if (error instanceof AggregateError) return error.errors.map(getErrorMessage).join(", ");
	if (error instanceof Error) return `${error.name}: ${error.message}`;
	if (typeof error === "string") return error;
	return String(error);
};
