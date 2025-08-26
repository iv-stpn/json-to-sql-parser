export const availableDialects = ["postgresql", "sqlite-minimal", "sqlite-extensions"] as const;
export type Dialect = (typeof availableDialects)[number];
