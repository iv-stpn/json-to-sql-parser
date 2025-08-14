import { INVALID_JSON_ACCESS_ERROR } from "../constants/errors";

const segmentCharacter = "[0-9a-z_]";
const segmentRegex = `(?:(?:'[^']+'|${segmentCharacter}+))`;

const fieldPathRegex = new RegExp(`^->(?:>${segmentRegex}|${segmentRegex}(?:->${segmentRegex})*(?:->>${segmentRegex})?)$`);
const segmentCharacterRegex = new RegExp(segmentCharacter);

export function parseJsonAccess(jsonAccess: string): { jsonPathSegments: string[]; jsonExtractText: boolean } {
	// Validate the input matches the expected pattern
	if (!fieldPathRegex.test(jsonAccess)) throw new Error(INVALID_JSON_ACCESS_ERROR(jsonAccess, "format"));

	const segments: string[] = [];

	let i = 2; // Start after the initial '->'
	let currentSegment = "";
	let jsonExtractText = false;

	while (i < jsonAccess.length) {
		const char = jsonAccess[i];

		// Check if we're at the start of an arrow sequence
		if (char === "-" && i + 1 < jsonAccess.length && jsonAccess[i + 1] === ">") {
			// We found an arrow, so the current segment is complete
			if (currentSegment) {
				segments.push(currentSegment);
				currentSegment = "";
			}

			// Skip the arrow
			i += 2;

			// Check if the next character is '>' (indicating jsonExtractText = true)
			// This should only happen on the last segment
			if (i < jsonAccess.length && jsonAccess[i] === ">") {
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
			while (i < jsonAccess.length && jsonAccess[i] !== "'") {
				quotedContent += jsonAccess[i];
				i++;
			}

			if (i >= jsonAccess.length) throw new Error(INVALID_JSON_ACCESS_ERROR(jsonAccess, "quote"));

			// Skip closing quote
			i++;
			currentSegment = quotedContent;
		} else {
			let character = jsonAccess[i];
			while (i < jsonAccess.length && character && segmentCharacterRegex.test(character)) {
				currentSegment += character;
				i++;
				character = jsonAccess[i];
			}
		}
	}

	// Add the final segment if there is one
	if (currentSegment) segments.push(currentSegment);
	return { jsonPathSegments: segments, jsonExtractText };
}
