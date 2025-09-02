# JSON to SQL Parser

A TypeScript library that converts JSON-based query specifications into safe SQL
queries. This parser provides a declarative way to build complex SQL queries
with strong type safety, field validation, and built-in protection against SQL
injection.

## Features

- 📝 **JSON Schema Validation**: Validates query structure using Zod
- 🛡️ **Field Whitelisting**: Only allow querying predefined fields
- 🔗 **Relationship Support**: Automatic JOIN generation based on relationships
- 📊 **Aggregation Queries**: Support for GROUP BY and aggregation functions
- 🎯 **Expression System**: Complex expressions with functions and operators
- 📱 **JSON Field Support**: Query nested JSON/JSONB fields with path syntax
- 🔍 **Field Type Casting Inference**: Casting based on field definitions and
  inferred expression types
- 🏢 **Universal Data Table Support**: Data table configuration for schema-less
  storage

## Installation

```bash
npm install json-to-sql-parser
# or
bun add json-to-sql-parser
```

## Quick Start

```typescript
import { buildSelectQuery } from "json-to-sql-parser";

// Define your database schema
const config = {
  tables: {
    users: {
      allowedFields: [
        { name: "id", type: "uuid", nullable: false },
        { name: "name", type: "string", nullable: false },
        { name: "email", type: "string", nullable: true },
        { name: "active", type: "boolean", nullable: false },
        { name: "metadata", type: "object", nullable: true },
      ],
    },
  },
  variables: {
    "auth.uid": 123,
  },
  relationships: [],
};

// Create a query
const query = {
  rootTable: "users",
  selection: {
    id: true,
    name: true,
    email: true,
  },
  condition: {
    "users.active": { $eq: true },
  },
};

// Generate SQL directly
const sql = buildSelectQuery(query, config);

console.log(sql);
// SELECT users.id AS "id", users.name AS "name", users.email AS "email" FROM users WHERE users.active = TRUE
```

## Configuration

### Database Schema Definition

The configuration object defines your database structure:

```typescript
interface Config {
  tables: {
    [tableName: string]: {
      allowedFields: Field[];
    };
  };
  variables: { [varName: string]: ScalarPrimitive };
  relationships: Relationship[];
  dataTable?: DataTableConfig; // For schema-less storage
}

interface Field {
  name: string;
  type: "string" | "number" | "boolean" | "object";
  nullable: boolean;
}

interface Relationship {
  table: string;
  field: string;
  toTable: string;
  toField: string;
  type: "one-to-one" | "many-to-one";
}
```

### Variables

Variables allow you to inject context values into queries:

```typescript
const config = {
  // ... tables
  variables: {
    "auth.uid": 123,
    "current_tenant": "tenant_abc",
    "max_age": 100,
  },
};

// Use in conditions
const condition = {
  "users.id": { $eq: { $var: "auth.uid" } },
};
```

## Query Types

### Select Queries

Select queries return data from one or more tables:

```typescript
// Basic selection
const query = {
  rootTable: "users",
  selection: {
    id: true,
    name: true,
    email: true,
  },
};

// With relationships
const query = {
  rootTable: "users",
  selection: {
    id: true,
    name: true,
    posts: {
      id: true,
      title: true,
      content: true,
    },
  },
};

// With expressions
const query = {
  rootTable: "users",
  selection: {
    id: true,
    display_name: {
      $func: {
        CONCAT: [
          { $field: "users.name" },
          " (",
          { $field: "users.email" },
          ")",
        ],
      },
    },
  },
};
```

### Where Conditions

Conditions support various operators and logical combinations:

```typescript
// Basic operators
const condition = {
  "users.age": { $gte: 18 },
  "users.active": { $eq: true },
  "users.name": { $like: "John%" },
};

// Logical operators
const condition = {
  $and: [
    { "users.age": { $gte: 18 } },
    { "users.active": { $eq: true } },
  ],
};

const condition = {
  $or: [
    { "users.role": { $eq: "admin" } },
    { "users.role": { $eq: "moderator" } },
  ],
};

// NOT operator
const condition = {
  $not: {
    "users.status": { $eq: "banned" },
  },
};

// EXISTS subqueries
const condition = {
  $exists: {
    table: "posts",
    conditions: {
      "posts.user_id": { $field: "users.id" },
      "posts.published": { $eq: true },
    },
  },
};

// Array operators
const condition = {
  "users.status": { $in: ["active", "pending"] },
  "users.role": { $nin: ["banned", "suspended"] },
};
```

### Aggregation Queries

Perform GROUP BY operations and aggregations:

```typescript
import { buildAggregationQuery } from "json-to-sql-parser";

const aggregationQuery = {
  table: "orders",
  groupBy: ["orders.status", "orders.region"],
  aggregatedFields: {
    total_amount: { function: "SUM", field: "orders.amount" },
    order_count: { function: "COUNT", field: "orders.id" },
    avg_amount: { function: "AVG", field: "orders.amount" },
    max_amount: { function: "MAX", field: "orders.amount" },
    regions: {
      function: "STRING_AGG",
      field: "orders.region",
      additionalArguments: [","],
    },
  },
};

const sql = buildAggregationQuery(aggregationQuery, config);
```

## Expression System

The expression system supports various functions and operations:

### Field References

```typescript
// Simple field reference
{
  $field: "users.name";
}

// Cross-table reference (requires relationship)
{
  $field: "posts.title";
}

// Variable reference
{
  $var: "auth.uid";
}
```

### Functions

```typescript
// String functions
{
  $func: {
    UPPER: [{ $field: "users.name" }];
  }
}
{
  $func: {
    LOWER: [{ $field: "users.email" }];
  }
}
{
  $func: {
    LENGTH: [{ $field: "users.name" }];
  }
}
{
  $func: {
    CONCAT: ["Hello, ", { $field: "users.name" }];
  }
}

// Math functions
{
  $func: {
    ABS: [{ $field: "users.balance" }];
  }
}
{
  $func: {
    SQRT: [{ $field: "users.score" }];
  }
}
{
  $func: {
    ADD: [{ $field: "users.score" }, 10];
  }
}
{
  $func: {
    MULTIPLY: [{ $field: "users.hourly_rate" }, 8];
  }
}

// Date functions
{
  $func: {
    YEAR: [{ $field: "users.created_at" }];
  }
}

// Utility functions
{
  $func: {
    COALESCE_STRING: [{ $field: "users.nickname" }, { $field: "users.name" }];
  }
}
{
  $func: {
    GREATEST_NUMBER: [{ $field: "users.score1" }, { $field: "users.score2" }];
  }
}
```

### Conditional Expressions

```typescript
const expression = {
  $cond: {
    if: { "users.age": { $gte: 18 } },
    then: "Adult",
    else: "Minor",
  },
};
```

## JSON Field Queries

Query nested JSON/JSONB fields using arrow syntax:

```typescript
// Simple JSON field access
const selection = {
  id: true,
  "metadata->profile->name": true,
  "settings->preferences->theme": true,
};

// In conditions
const condition = {
  "metadata->profile->active": { $eq: true },
  "settings->notifications->email": { $eq: false },
};

// Complex JSON paths
const selection = {
  "data->user->contact->emails->0": true, // Array access
  "metadata->'complex key'->value": true, // Quoted keys
};
```

## Data Table Configuration

For schema-less storage where multiple entity types are stored in a single
table:

```typescript
const config = {
  // ... regular configuration
  dataTable: {
    table: "data_storage", // Physical table name
    dataField: "data", // Column containing JSON data
    tableField: "table_name", // Column indicating entity type
    whereConditions: [ // Additional filter conditions
      "tenant_id = 'current_tenant'",
      "deleted_at IS NULL",
    ],
  },
};

// Queries automatically handle the data table structure
const query = {
  rootTable: "users", // Logical table name
  selection: { id: true, name: true },
};

// Generates SQL like:
// SELECT (data->>'id')::FLOAT AS "id", (data->>'name')::TEXT AS "name"
// FROM data_storage
// WHERE table_name = 'users' AND tenant_id = 'current_tenant' AND deleted_at IS NULL
```

## Security Features

### SQL Injection Protection

All user values are automatically quoted and escaped:

```typescript
const condition = {
  "users.name": { $eq: "Robert'; DROP TABLE users; --" },
};

// Generates safe SQL:
// WHERE users.name = 'Robert''; DROP TABLE users; --'
```

### Field Whitelisting

Only fields defined in the configuration can be queried:

```typescript
// This will throw an error if 'secret_field' is not in allowedFields
const selection = {
  secret_field: true, // Error: Field 'secret_field' is not allowed
};
```

### Type Validation

All queries are validated against Zod schemas:

```typescript
// Invalid query structure throws validation error
const invalid = {
  rootTable: "users",
  selection: "invalid", // Error: Expected object, received string
};
```

## API Reference

### Core Functions

#### `buildSelectQuery(selectQuery, config)`

Generate the SQL for a select query based on the select query provided.

#### `buildAggregationQuery(aggregationQuery, config)`

Generate the SQL for an aggregation query based on the aggregation query
provided.

### Query Schemas

All query inputs are validated using Zod schemas:

- `conditionSchema`: Validates where conditions
- `aggregationQuerySchema`: Validates aggregation queries
- `selectQuerySchema`: Validates select queries

### Types

Key TypeScript types:

- `Config`: Database configuration
- `Condition`: Where clause conditions
- `Selection`: Field selection specification
- `AggregationQuery`: Aggregation query specification

## Examples

### Complex Multi-table Query

```typescript
const query = {
  rootTable: "users",
  selection: {
    id: true,
    name: true,
    total_posts: {
      $func: {
        COALESCE_NUMBER: [
          { $field: "post_count.count" },
          0,
        ],
      },
    },
    posts: {
      id: true,
      title: true,
      comments: {
        id: true,
        content: true,
        author: {
          name: true,
        },
      },
    },
  },
  condition: {
    $and: [
      { "users.active": { $eq: true } },
      {
        $exists: {
          table: "posts",
          conditions: {
            "posts.user_id": { $field: "users.id" },
            "posts.published": { $eq: true },
          },
        },
      },
    ],
  },
};
```

### Advanced Aggregation

```typescript
const salesReport = {
  table: "orders",
  groupBy: ["orders.region", "orders.product_category"],
  aggregatedFields: {
    total_revenue: { function: "SUM", field: "orders.amount" },
    order_count: { function: "COUNT", field: "orders.id" },
    avg_order_value: { function: "AVG", field: "orders.amount" },
    unique_customers: {
      operator: "COUNT_DISTINCT",
      field: "orders.customer_id",
    },
    top_sale: { function: "MAX", field: "orders.amount" },
  },
};
```

## Error Handling

The parser provides detailed error messages for common issues:

- **Invalid field references**: When querying non-existent or non-allowed fields
- **Type mismatches**: When operators don't match field types
- **Invalid relationships**: When trying to join tables without defined
  relationships
- **Schema validation**: When query structure doesn't match expected format

```typescript
try {
  const sql = buildSelectQuery(query, config);
} catch (error) {
  console.error("Query parsing failed:", error.message);
}
```

## Contributing

This library is built with:

- **TypeScript**: For type safety
- **Zod**: For schema validation
- **Bun**: For testing and building

Run tests with:

```bash
bun test
```

Build the library:

```bash
bun run build
```

## License

MIT License - see LICENSE file for details.
