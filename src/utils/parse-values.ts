export function parseTimestamp(value: string): Date {
	const date = new Date(value.replace(" ", "T"));
	if (Number.isNaN(date.getTime())) throw new Error("Invalid timestamp format");
	return date;
}

export function parseDate(value: string): Date {
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) throw new Error("Invalid date format");
	return date;
}
