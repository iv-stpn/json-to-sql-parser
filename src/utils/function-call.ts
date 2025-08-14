export function removeAllWrappingParens(expression: string): string {
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

export function applyFunction(functionName: string, args: string[]): string {
	if (args.length === 0) throw new Error(`Function '${functionName}' requires at least one argument`);
	return `${functionName}(${args.map((arg) => removeAllWrappingParens(arg)).join(", ")})`;
}
