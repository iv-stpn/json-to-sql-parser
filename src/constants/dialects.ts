export const availableDialects = ["postgresql", "sqlite-3.44-minimal", "sqlite-3.44-extensions"] as const;
export type Dialect = (typeof availableDialects)[number];
