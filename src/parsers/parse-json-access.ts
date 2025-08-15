import { INVALID_JSON_ACCESS_ERROR } from "../constants/errors";

const segmentCharacter = "[0-9a-z_]";
const segmentRegex = `(?:(?:'[^']+'|${segmentCharacter}+))`;
export const fieldPathRegex = `->(?:>${segmentRegex}|${segmentRegex}(?:->${segmentRegex})*(?:->>${segmentRegex})?)`;

const jsonAccessRegex = new RegExp(`^${fieldPathRegex}$`);
const segmentCharacterRegex = new RegExp(segmentCharacter);

export function parseJsonAccess(jsonAccessPath: string): { jsonAccess: string[]; jsonExtractText?: true } {
	// Validate the input matches the expected pattern
	if (!jsonAccessRegex.test(jsonAccessPath)) throw new Error(INVALID_JSON_ACCESS_ERROR(jsonAccessPath, "format"));

	const segments: string[] = [];

	let i = 2; // Start after the initial '->'
	let currentSegment = "";
	let jsonExtractText = false;

	while (i < jsonAccessPath.length) {
		const char = jsonAccessPath[i];

		// Check if we're at the start of an arrow sequence
		if (char === "-" && i + 1 < jsonAccessPath.length && jsonAccessPath[i + 1] === ">") {
			// We found an arrow, so the current segment is complete
			if (currentSegment) {
				segments.push(currentSegment);
				currentSegment = "";
			}

			// Skip the arrow
			i += 2;

			// Check if the next character is '>' (indicating jsonExtractText = true)
			// This should only happen on the last segment
			if (i < jsonAccessPath.length && jsonAccessPath[i] === ">") {
				jsonExtractText = true;
				i++;
			}

			continue;
		}

		// At the start, check if we have ->> pattern
		if (i === 2 && char === ">") {
			// This is the ->> pattern at the beginning, which means jsonExtractText = true
			// and this will be the only segment
			jsonExtractText = true;
			i++;
			continue;
		}

		// Handle quoted segments
		if (char === "'") {
			i++; // Skip opening quote
			let quotedContent = "";

			// Read until closing quote
			while (i < jsonAccessPath.length && jsonAccessPath[i] !== "'") {
				quotedContent += jsonAccessPath[i];
				i++;
			}

			if (i >= jsonAccessPath.length) throw new Error(INVALID_JSON_ACCESS_ERROR(jsonAccessPath, "quote"));

			// Skip closing quote
			i++;
			currentSegment = quotedContent;
		} else {
			let character = jsonAccessPath[i];
			while (i < jsonAccessPath.length && character && segmentCharacterRegex.test(character)) {
				currentSegment += character;
				i++;
				character = jsonAccessPath[i];
			}
		}
	}

	// Add the final segment if there is one
	if (currentSegment) segments.push(currentSegment);
	return { jsonAccess: segments, jsonExtractText: jsonExtractText || undefined };
}
