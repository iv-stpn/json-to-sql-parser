const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
const timestampRegex = /^\d{4}-\d{2}-\d{2}(?:T| )\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$/;

export function isValidDate(value: string): boolean {
	if (!dateRegex.test(value)) return false;

	const [year, month, day] = value.split("-").map(Number);
	if (year === undefined || month === undefined || day === undefined) return false;

	const date = new Date(year, month - 1, day);
	return date.getFullYear() === year && date.getMonth() + 1 === month && date.getDate() === day;
}

const timeSeparatorRegex = /[:.]/;
export function isValidTimestamp(value: string): boolean {
	if (!timestampRegex.test(value)) return false;

	const datePart = value.slice(0, 10);
	if (!isValidDate(datePart)) return false;

	const timePart = value.slice(11);
	const [hours, minutes, seconds] = timePart.split(timeSeparatorRegex).map(Number);
	if (hours === undefined || minutes === undefined) return false;
	if (hours < 0 || hours >= 24 || minutes < 0 || minutes >= 60) return false;
	if (seconds !== undefined && (seconds < 0 || seconds >= 60)) return false;
	return true;
}

export const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
