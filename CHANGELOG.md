# json-to-sql-parser

## 4.0.2

### Patch Changes

- [`5022db0`](https://github.com/iv-stpn/json-to-sql-parser/commit/5022db0716176ff229ce3ac3e28e01f05a1859f8) Thanks [@iv-stpn](https://github.com/iv-stpn)! - expose more internal apis

## 4.0.1

### Patch Changes

- [`1a428d5`](https://github.com/iv-stpn/json-to-sql-parser/commit/1a428d58bc807f086c3d7563477f30e0299a8c59) Thanks [@iv-stpn](https://github.com/iv-stpn)! - minor type fixes

## 4.0.0

### Major Changes

- [`b8293ff`](https://github.com/iv-stpn/json-to-sql-parser/commit/b8293ff864d8ad2996e1336bdc4e36dcce3998bb) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Major refactor: add delete parser; add order by to select; allow expressions in mutation fields; multiple fixes

## 3.2.2

### Patch Changes

- [`e7bff75`](https://github.com/iv-stpn/json-to-sql-parser/commit/e7bff75623984eed05be86bd0ffc474d38dfd93b) Thanks [@iv-stpn](https://github.com/iv-stpn)! - allow inline foreign keys in config, remove cardinality from relationships

## 3.2.1

### Patch Changes

- [`765ac99`](https://github.com/iv-stpn/json-to-sql-parser/commit/765ac991338fd75b597d82498f6b2d51a77b9882) Thanks [@iv-stpn](https://github.com/iv-stpn)! - add pagination to select builder

## 3.2.0

### Minor Changes

- [`ef74d11`](https://github.com/iv-stpn/json-to-sql-parser/commit/ef74d1165ab9d8acff4d678519a615df9aff2f0b) Thanks [@iv-stpn](https://github.com/iv-stpn)! - fix relationships: only limit to one-to-one and many-to-one to avoid redundancy; remove wrong "many-to-many" relation type

### Patch Changes

- [`8d3ff1b`](https://github.com/iv-stpn/json-to-sql-parser/commit/8d3ff1b6b0a300d8fdcc20abf3bc9dafd7956f92) Thanks [@iv-stpn](https://github.com/iv-stpn)! - expose more internal apis

## 3.1.1

### Patch Changes

- [`036e385`](https://github.com/iv-stpn/json-to-sql-parser/commit/036e3859cdadf595f7397674fe09ca145792a8cc) Thanks [@iv-stpn](https://github.com/iv-stpn)! - expose fieldTypes in index, rename cast-types -> field-types

- [`78865b0`](https://github.com/iv-stpn/json-to-sql-parser/commit/78865b0f3246b7755996e168624476643b220847) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Simplify Dialect as enum

## 3.1.0

### Minor Changes

- [`6b984d4`](https://github.com/iv-stpn/json-to-sql-parser/commit/6b984d4c735dd6a0681c98a240bcc2e2917149f0) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Add SQLite dialect support, simplify type casting, remove auth functions and enforce safe joins

## 3.0.1

### Patch Changes

- [`52bed57`](https://github.com/iv-stpn/json-to-sql-parser/commit/52bed57e0cc4e44fcd646c750e73416539124205) Thanks [@iv-stpn](https://github.com/iv-stpn)! - use Record instead of index signature in $func

## 3.0.0

### Major Changes

- [`81efe95`](https://github.com/iv-stpn/json-to-sql-parser/commit/81efe957c0017041bd81c740e3664e9292eda6de) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Major refactors: remove parametrized query (only escape values), add INSERT and UPDATE operations, add in-JS evaluation logic for mutations, refactor parsers, tests and exposed APIs

## 2.1.0

### Minor Changes

- [`b2112c3`](https://github.com/iv-stpn/json-to-sql-parser/commit/b2112c3edfb5c258cd4e6550db40a82609844fb3) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Improve aggregation functions spec and minor refactors

## 2.0.0

### Major Changes

- [`4e83271`](https://github.com/iv-stpn/json-to-sql-parser/commit/4e8327128e61ab5e851fc5da6da0e03c04282fda) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Major spec refactor

## 1.0.2

### Patch Changes

- [`81eae11`](https://github.com/iv-stpn/json-to-sql-parser/commit/81eae11fc2bea2266a55ff39e08fa0d7039e762e) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Add UUID typecasting and typecasting in relationship fields

## 1.0.1

### Patch Changes

- [`188c026`](https://github.com/iv-stpn/json-to-sql-parser/commit/188c02645660d686565afe28c7481bebe392c614) Thanks [@iv-stpn](https://github.com/iv-stpn)! - Update README, reorganize code structure
