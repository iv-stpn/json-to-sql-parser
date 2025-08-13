export const isEmpty = (str: string) => str.trim() === "";

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

export const quote = (str: string): string => `'${str.replaceAll(/'/g, "''")}'`;

export const getErrorMessage = (error: unknown): string => {
	if (error instanceof AggregateError) return error.errors.map(getErrorMessage).join(", ");
	if (error instanceof Error) return `${error.name}: ${error.message}`;
	if (typeof error === "string") return error;
	return String(error);
};
