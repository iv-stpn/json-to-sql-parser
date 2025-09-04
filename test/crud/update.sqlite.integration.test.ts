/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildUpdateQuery } from "../../src/builders/update";
import { Dialect } from "../../src/constants/dialects";
import type { UpdateQuery } from "../../src/schemas";
import type { Config } from "../../src/types";

// SQLite database helper class
class SQLiteDatabaseHelper {
	private db: Database;

	constructor(filename: string = ":memory:") {
		this.db = new Database(filename);
	}

	connect(): void {
		// Enable foreign keys
		this.db.run("PRAGMA foreign_keys = ON");
		// Enable JSON1 extension if available
		try {
			this.db.run("SELECT json('{}')");
		} catch {
			console.warn("JSON1 extension not available");
		}
	}

	disconnect(): void {
		this.db.close();
	}

	query(sql: string): unknown[] {
		try {
			const stmt = this.db.prepare(sql);
			return stmt.all();
		} catch (error) {
			console.error("SQLite Query Error:", error);
			console.error("SQL:", sql);
			throw error;
		}
	}

	run(sql: string): void {
		this.db.run(sql);
	}

	beginTransaction(): void {
		this.db.run("BEGIN TRANSACTION");
	}

	rollback(): void {
		this.db.run("ROLLBACK");
	}

	commit(): void {
		this.db.run("COMMIT");
	}

	/**
	 * Execute a query within a transaction that automatically rolls back
	 */
	executeInTransaction<T>(fn: (helper: SQLiteDatabaseHelper) => T): T {
		this.beginTransaction();
		try {
			const result = fn(this);
			return result;
		} finally {
			this.rollback();
		}
	}
}

// Setup SQLite test database
function setupSQLiteDatabase(db: SQLiteDatabaseHelper): void {
	// Create tables
	db.run(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			age INTEGER,
			active INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'active',
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
			metadata TEXT,
			score REAL,
			balance REAL,
			description TEXT
		)
	`);

	db.run(`
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			user_id TEXT,
			published INTEGER NOT NULL DEFAULT 0,
			tags TEXT,
			rating REAL,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
			published_at TEXT,
			view_count INTEGER DEFAULT 0,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)
	`);

	db.run(`
		CREATE TABLE orders (
			id TEXT PRIMARY KEY,
			amount REAL NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			customer_id TEXT,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
			shipped_at TEXT,
			delivered_date TEXT,
			notes TEXT,
			FOREIGN KEY (customer_id) REFERENCES users(id)
		)
	`);

	db.run(`
		CREATE TABLE products (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL NOT NULL,
			category TEXT NOT NULL,
			stock_quantity INTEGER DEFAULT 0,
			metadata TEXT,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP
		)
	`);

	// Insert test data
	db.run(`
		INSERT INTO users (id, name, email, age, active, status, metadata, balance) VALUES
		('550e8400-e29b-41d4-a716-446655440000', 'John Doe', 'john@example.com', 30, 1, 'premium', '{"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}', 1000.00),
		('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Jane Smith', 'jane@example.com', 25, 1, 'active', '{"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}', 1500.00),
		('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Bob Johnson', 'bob@example.com', 35, 0, 'inactive', '{"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}', 800.00),
		('6ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Alice Brown', 'alice@example.com', 28, 1, 'premium', '{"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}', 1200.00),
		('6ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Charlie Wilson', NULL, 32, 1, 'active', '{"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}', 900.00),
		('6ba7b814-9dad-11d1-80b4-00c04fd430c8', 'Diana Prince', 'diana@example.com', 29, 1, 'premium', '{"department": "engineering", "role": "senior", "settings": {"theme": "light", "notifications": true}}', 2000.00)
	`);

	db.run(`
		INSERT INTO posts (id, title, content, user_id, published, published_at, tags, rating, view_count) VALUES
		('7ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Getting Started with SQLite', 'This is a comprehensive guide to SQLite...', '550e8400-e29b-41d4-a716-446655440000', 1, '2024-01-15 10:30:00', '["database", "sqlite", "tutorial"]', 4.5, 150),
		('7ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Advanced SQL Queries', 'Learn advanced SQL techniques...', '550e8400-e29b-41d4-a716-446655440000', 1, '2024-01-16 14:45:00', '["sql", "advanced", "database"]', 4.8, 200),
		('7ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Marketing Strategies 2024', 'The latest marketing trends...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-17 09:15:00', '["marketing", "trends", "2024"]', 4.2, 120),
		('7ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Team Building Activities', 'Effective team building exercises...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["teamwork", "management", "hr"]', 3.9, 80),
		('7ba7b814-9dad-11d1-80b4-00c04fd430c8', 'Sales Techniques', 'How to close more deals...', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["sales", "techniques", "business"]', 4.1, 90),
		('7ba7b815-9dad-11d1-80b4-00c04fd430c8', 'Engineering Best Practices', 'Code quality and architecture...', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-18 11:20:00', '["engineering", "best-practices", "code"]', 4.7, 180)
	`);

	db.run(`
		INSERT INTO orders (id, amount, status, customer_id, shipped_at, delivered_date, notes) VALUES
		('8ba7b810-9dad-11d1-80b4-00c04fd430c8', 299.99, 'completed', '550e8400-e29b-41d4-a716-446655440000', '2024-01-16 08:00:00', '2024-01-18', 'Fast delivery requested'),
		('8ba7b811-9dad-11d1-80b4-00c04fd430c8', 149.50, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-17 12:30:00', NULL, 'Standard shipping'),
		('8ba7b812-9dad-11d1-80b4-00c04fd430c8', 89.99, 'pending', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Awaiting payment confirmation'),
		('8ba7b813-9dad-11d1-80b4-00c04fd430c8', 199.99, 'completed', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '2024-01-18 15:45:00', '2024-01-20', 'Express delivery'),
		('8ba7b814-9dad-11d1-80b4-00c04fd430c8', 59.99, 'cancelled', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Customer cancelled'),
		('8ba7b815-9dad-11d1-80b4-00c04fd430c8', 399.99, 'completed', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '2024-01-19 11:20:00', '2024-01-22', 'Fragile items'),
		('8ba7b816-9dad-11d1-80b4-00c04fd430c8', 79.99, 'pending', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Processing order')
	`);

	db.run(`
		INSERT INTO products (id, name, price, category, stock_quantity, metadata) VALUES
		('prd001', 'SQLite Database Book', 49.99, 'books', 100, '{"isbn": "978-1234567890", "pages": 320}'),
		('prd002', 'Advanced SQL Course', 199.99, 'courses', 50, '{"duration_hours": 40, "level": "advanced"}'),
		('prd003', 'Programming T-Shirt', 24.99, 'apparel', 200, '{"size": ["S", "M", "L", "XL"], "color": "black"}'),
		('prd004', 'Database Design Tool', 99.99, 'software', 25, '{"license_type": "single", "version": "2024.1"}'),
		('prd005', 'SQL Cheat Sheet Poster', 15.99, 'accessories', 300, '{"dimensions": "24x36", "material": "glossy paper"}')
	`);
}

describe("Integration - UPDATE Operations with SQLite", () => {
	let db: SQLiteDatabaseHelper;
	let config: Config;

	beforeAll(() => {
		db = new SQLiteDatabaseHelper();
		db.connect();
		setupSQLiteDatabase(db);

		config = {
			dialect: Dialect.SQLITE_EXTENSIONS,
			tables: {
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "age", type: "number", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "updated_at", type: "datetime", nullable: false },
						{ name: "balance", type: "number", nullable: true },
						{ name: "score", type: "number", nullable: true },
						{ name: "description", type: "string", nullable: true },
					],
				},
				posts: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "content", type: "string", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "published", type: "boolean", nullable: false },
						{ name: "tags", type: "object", nullable: true },
						{ name: "rating", type: "number", nullable: true },
						{ name: "view_count", type: "number", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "updated_at", type: "datetime", nullable: false },
						{ name: "published_at", type: "datetime", nullable: true },
					],
				},
				orders: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "amount", type: "number", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "customer_id", type: "uuid", nullable: false },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "updated_at", type: "datetime", nullable: false },
						{ name: "shipped_at", type: "datetime", nullable: true },
						{ name: "delivered_date", type: "date", nullable: true },
						{ name: "notes", type: "string", nullable: true },
					],
				},
				products: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "price", type: "number", nullable: false },
						{ name: "category", type: "string", nullable: false },
						{ name: "stock_quantity", type: "number", nullable: true },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "updated_at", type: "datetime", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
				admin_role: "admin",
				current_timestamp: "2024-01-20 12:00:00",
				min_age: 18,
				max_score: 100,
				high_value_threshold: 200,
				premium_status: "premium",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id" },
			],
		};
	});

	afterAll(() => {
		db.disconnect();
	});

	describe("Basic UPDATE Operations", () => {
		it("should execute simple field updates", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						name: "John Doe Updated",
						age: 31,
					},
					condition: {
						id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// Verify generated SQL
				expect(sql).toBe(
					"UPDATE users SET \"name\" = 'John Doe Updated', \"age\" = 31 WHERE users.id = '550e8400-e29b-41d4-a716-446655440000'",
				);

				// Execute the update
				db.run(sql);

				// Verify the update was applied
				const result = db.query("SELECT name, age FROM users WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
				expect(result.length).toBe(1);
				const user = result[0] as Record<string, unknown>;
				expect(user.name).toBe("John Doe Updated");
				expect(user.age).toBe(31);
			});
		});

		it("should execute updates with boolean fields", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						active: false,
						status: "inactive",
					},
					condition: {
						id: { $uuid: "6ba7b811-9dad-11d1-80b4-00c04fd430c8" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// Verify SQLite boolean handling
				expect(sql).toBe(
					"UPDATE users SET \"active\" = FALSE, \"status\" = 'inactive' WHERE users.id = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'",
				);

				// Execute the update
				db.run(sql);

				// Verify the update was applied (SQLite stores booleans as 0/1)
				const result = db.query("SELECT active, status FROM users WHERE id = '6ba7b811-9dad-11d1-80b4-00c04fd430c8'");
				expect(result.length).toBe(1);
				const user = result[0] as Record<string, unknown>;
				expect(user.active).toBe(0); // SQLite FALSE becomes 0
				expect(user.status).toBe("inactive");
			});
		});

		it("should execute updates with NULL values", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						email: null,
						description: null,
					},
					condition: {
						id: { $uuid: "6ba7b812-9dad-11d1-80b4-00c04fd430c8" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					'UPDATE users SET "email" = NULL, "description" = NULL WHERE users.id = \'6ba7b812-9dad-11d1-80b4-00c04fd430c8\'',
				);

				// Execute the update
				db.run(sql);

				// Verify the update was applied
				const result = db.query("SELECT email, description FROM users WHERE id = '6ba7b812-9dad-11d1-80b4-00c04fd430c8'");
				expect(result.length).toBe(1);
				const user = result[0] as Record<string, unknown>;
				expect(user.email).toBe(null);
				expect(user.description).toBe(null);
			});
		});

		it("should execute updates with numeric fields", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "orders",
					updates: {
						amount: 259.99,
						notes: "Updated order amount",
					},
					condition: {
						id: { $uuid: "8ba7b810-9dad-11d1-80b4-00c04fd430c8" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE orders SET \"amount\" = 259.99, \"notes\" = 'Updated order amount' WHERE orders.id = '8ba7b810-9dad-11d1-80b4-00c04fd430c8'",
				);

				// Execute the update
				db.run(sql);

				// Verify the update was applied
				const result = db.query("SELECT amount, notes FROM orders WHERE id = '8ba7b810-9dad-11d1-80b4-00c04fd430c8'");
				expect(result.length).toBe(1);
				const order = result[0] as Record<string, unknown>;
				expect(order.amount).toBe(259.99);
				expect(order.notes).toBe("Updated order amount");
			});
		});
	});

	describe("UPDATE with Complex Conditions", () => {
		it("should execute updates with AND conditions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						balance: 2323232.0,
						status: "premium",
					},
					condition: {
						$and: [{ active: true }, { age: { $gte: 25 } }, { status: "active" }],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE users SET \"balance\" = 2323232, \"status\" = 'premium' WHERE (users.active = TRUE AND users.age >= 25 AND users.status = 'active')",
				);

				// Execute the update
				db.run(sql);

				// Verify multiple users were updated
				const result = db.query("SELECT id, balance, status FROM users WHERE active = 1 AND age >= 25 AND balance = 2323232");
				expect(result.length).toBeGreaterThan(0);

				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(u.status).toBe("premium");
				}
			});
		});

		it("should execute updates with OR conditions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "posts",
					updates: {
						rating: 4.0,
						view_count: 100,
					},
					condition: {
						$or: [{ published: false }, { rating: { $lt: 4.0 } }],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe('UPDATE posts SET "rating" = 4, "view_count" = 100 WHERE (posts.published = FALSE OR posts.rating < 4)');

				// Execute the update
				db.run(sql);

				// Verify updates were applied to unpublished posts or low-rated posts
				const result = db.query("SELECT id, rating, view_count FROM posts WHERE published = 0 OR rating <= 4.0");
				for (const post of result) {
					const p = post as Record<string, unknown>;
					expect(p.rating).toBe(4.0);
					expect(p.view_count).toBe(100);
				}
			});
		});

		it("should execute updates with nested logical conditions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "orders",
					updates: {
						status: "processing",
						notes: "Order being processed",
					},
					condition: {
						$and: [
							{ status: "pending" },
							{
								$or: [{ amount: { $gte: 50 } }, { customer_id: { $var: "current_user_id" } }],
							},
						],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE orders SET \"status\" = 'processing', \"notes\" = 'Order being processed' WHERE (orders.status = 'pending' AND (orders.amount >= 50 OR orders.customer_id = '550e8400-e29b-41d4-a716-446655440000'))",
				);

				// Execute the update
				db.run(sql);

				// Verify updates were applied correctly
				const result = db.query(
					"SELECT id, status, notes FROM orders WHERE status = 'processing' AND notes = 'Order being processed'",
				);
				expect(result.length).toBeGreaterThan(0);
			});
		});

		it("should handle IN conditions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						score: 85.5,
					},
					condition: {
						status: { $in: ["premium", "active"] },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe("UPDATE users SET \"score\" = 85.5 WHERE users.status IN ('premium', 'active')");

				// Execute the update
				db.run(sql);

				// Verify updates were applied to users with premium or active status
				const result = db.query("SELECT id, score, status FROM users WHERE status IN ('premium', 'active')");
				expect(result.length).toBeGreaterThan(0);

				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(u.score).toBe(85.5);
				}
			});
		});
	});

	describe("UPDATE with JSON Operations", () => {
		it("should execute updates with JSON field operations", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						metadata: { $jsonb: { department: "engineering", role: "lead", updated: true } },
					},
					condition: {
						id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// SQLite handles JSON differently than PostgreSQL
				expect(sql).toBe(
					'UPDATE users SET "metadata" = \'{"department":"engineering","role":"lead","updated":true}\' WHERE users.id = \'550e8400-e29b-41d4-a716-446655440000\'',
				);

				// Execute the update
				db.run(sql);

				// Verify the JSON was updated
				const result = db.query("SELECT metadata FROM users WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
				expect(result.length).toBe(1);
				const user = result[0] as Record<string, unknown>;
				const metadata = JSON.parse(user.metadata as string);
				expect(metadata.department).toBe("engineering");
				expect(metadata.role).toBe("lead");
				expect(metadata.updated).toBe(true);
			});
		});

		it("should execute updates with JSON field conditions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						status: "senior_engineer",
						balance: 1500.0,
					},
					condition: {
						$and: [{ "metadata->department": "engineering" }, { "metadata->role": "senior" }],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE users SET \"status\" = 'senior_engineer', \"balance\" = 1500 WHERE (users.metadata->>'department' = 'engineering' AND users.metadata->>'role' = 'senior')",
				);

				// Execute the update
				db.run(sql);

				// Verify updates were applied to users in engineering with senior role
				const result = db.query(`
					SELECT id, status, balance, metadata 
					FROM users 
					WHERE JSON_EXTRACT(metadata, '$.department') = 'engineering' 
					AND JSON_EXTRACT(metadata, '$.role') = 'senior'
				`);

				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(u.status).toBe("senior_engineer");
					expect(u.balance).toBe(1500);
				}
			});
		});
	});

	describe("UPDATE with Expressions and Functions", () => {
		it("should execute updates with calculated values", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						balance: {
							$func: {
								MULTIPLY: [{ $field: "users.balance" }, 1.1], // 10% increase
							},
						},
						description: {
							$func: {
								CONCAT: ["Balance updated on ", { $var: "current_timestamp" }],
							},
						},
					},
					condition: {
						active: true,
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE users SET \"balance\" = (users.balance * 1.1), \"description\" = ('Balance updated on ' || '2024-01-20 12:00:00') WHERE users.active = TRUE",
				);

				// Execute the update
				db.run(sql);

				// Verify calculated updates
				const result = db.query("SELECT id, balance, description FROM users WHERE active = 1");
				expect(result.length).toBeGreaterThan(0);

				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(typeof u.balance).toBe("number");
					expect(Number(u.balance)).toBeGreaterThan(0);
					expect(u.description as string).toContain("Balance updated on 2024-01-20 12:00:00");
				}
			});
		});

		it("should execute updates with conditional expressions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "orders",
					updates: {
						status: {
							$cond: {
								if: { "orders.amount": { $gte: { $var: "high_value_threshold" } } },
								then: "priority",
								else: "standard",
							},
						},
						notes: {
							$func: {
								CONCAT: [
									"Status: ",
									{
										$cond: {
											if: { "orders.amount": { $gte: 200 } },
											then: "High Value Order",
											else: "Standard Order",
										},
									},
								],
							},
						},
					},
					condition: {
						status: "pending",
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE orders SET \"status\" = (CASE WHEN orders.amount >= 200 THEN 'priority' ELSE 'standard' END), \"notes\" = ('Status: ' || (CASE WHEN orders.amount >= 200 THEN 'High Value Order' ELSE 'Standard Order' END)) WHERE orders.status = 'pending'",
				);

				// Execute the update
				db.run(sql);

				// Verify conditional updates
				const result = db.query("SELECT id, amount, status, notes FROM orders WHERE status IN ('priority', 'standard')");

				for (const order of result) {
					const o = order as Record<string, unknown>;
					const amount = Number(o.amount);
					if (amount >= 200) {
						expect(o.status).toBe("priority");
						expect(o.notes as string).toContain("High Value Order");
					} else {
						expect(o.status).toBe("standard");
						expect(o.notes as string).toContain("Standard Order");
					}
				}
			});
		});

		it("should execute updates with string functions", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "posts",
					updates: {
						title: {
							$func: {
								CONCAT: ["[UPDATED] ", { $func: { UPPER: [{ $field: "posts.title" }] } }],
							},
						},
						content: {
							$func: {
								CONCAT: [{ $field: "posts.content" }, "\\n\\n--- Updated on ", { $var: "current_timestamp" }, " ---"],
							},
						},
					},
					condition: {
						published: false,
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					"UPDATE posts SET \"title\" = ('[UPDATED] ' || UPPER(posts.title)), \"content\" = (posts.content || '\\n\\n--- Updated on ' || '2024-01-20 12:00:00' || ' ---') WHERE posts.published = FALSE",
				);

				// Execute the update
				db.run(sql);

				// Verify string function updates
				const result = db.query("SELECT id, title, content FROM posts WHERE published = 0");

				for (const post of result) {
					const p = post as Record<string, unknown>;
					expect(p.title as string).toStartWith("[UPDATED] ");
					expect(p.content as string).toContain("--- Updated on 2024-01-20 12:00:00 ---");
				}
			});
		});
	});

	describe("UPDATE with NEW_ROW Conditions", () => {
		it("should execute updates with NEW_ROW field validation", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						age: 26,
						status: "verified",
					},
					condition: {
						$and: [
							{ "NEW_ROW.age": { $gte: { $var: "min_age" } } }, // Age must be >= 18
							{ "NEW_ROW.status": { $eq: "verified" } }, // New status must be verified
							{ active: true }, // Existing condition
						],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// NEW_ROW conditions should be evaluated and removed from WHERE clause
				expect(sql).toBe('UPDATE users SET "age" = 26, "status" = \'verified\' WHERE users.active = TRUE');

				// Execute the update
				db.run(sql);

				// Verify updates were applied to active users
				const result = db.query("SELECT id, age, status FROM users WHERE active = 1 AND age = 26 AND status = 'verified'");
				expect(result.length).toBeGreaterThan(0);
			});
		});

		it("should handle NEW_ROW conditions that reference non-updated fields", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						status: "senior",
					},
					condition: {
						$and: [
							{ "NEW_ROW.age": { $gte: 30 } }, // Check existing age since age is not updated
							{ "NEW_ROW.status": { $eq: "senior" } }, // Check new status
							{ active: true },
						],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// NEW_ROW.age should become a regular condition since age is not being updated
				expect(sql).toBe("UPDATE users SET \"status\" = 'senior' WHERE (users.age >= 30 AND users.active = TRUE)");

				// Execute the update
				db.run(sql);

				// Verify updates were applied correctly
				const result = db.query("SELECT id, age, status FROM users WHERE active = 1 AND age >= 30 AND status = 'senior'");
				expect(result.length).toBeGreaterThan(0);

				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(Number(u.age)).toBeGreaterThanOrEqual(30);
					expect(u.status).toBe("senior");
				}
			});
		});

		it("should throw error when NEW_ROW conditions fail", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						age: 15, // This violates the min_age condition
						status: "junior",
					},
					condition: {
						$and: [
							{ "NEW_ROW.age": { $gte: { $var: "min_age" } } }, // Age must be >= 18, but we're setting it to 15
							{ id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } },
						],
					},
				};

				// This should throw an error because NEW_ROW.age (15) < min_age (18)
				expect(() => buildUpdateQuery(updateQuery, config)).toThrow("Update condition not met");
			});
		});
	});

	describe("Bulk UPDATE Operations", () => {
		it("should execute bulk updates with single condition", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "posts",
					updates: {
						rating: 4.5,
						view_count: 200,
					},
					condition: {
						published: true,
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe('UPDATE posts SET "rating" = 4.5, "view_count" = 200 WHERE posts.published = TRUE');

				// Count how many records should be updated
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE published = 1");
				const expectedCount = (beforeCount[0] as Record<string, unknown>).count as number;

				// Execute the update
				db.run(sql);

				// Verify all published posts were updated
				const result = db.query("SELECT id, rating, view_count FROM posts WHERE published = 1");
				expect(result.length).toBe(expectedCount);

				for (const post of result) {
					const p = post as Record<string, unknown>;
					expect(p.rating).toBe(4.5);
					expect(p.view_count).toBe(200);
				}
			});
		});

		it("should execute bulk updates across multiple tables conceptually", () => {
			db.executeInTransaction(() => {
				// Update all active users' balances
				const userUpdateQuery: UpdateQuery = {
					table: "users",
					updates: {
						balance: {
							$func: {
								MULTIPLY: [{ $field: "users.balance" }, 1.05], // 5% bonus
							},
						},
						description: "Bulk balance update applied",
					},
					condition: {
						active: true,
					},
				};

				const userSql = buildUpdateQuery(userUpdateQuery, config);
				db.run(userSql);

				// Update all pending orders
				const orderUpdateQuery: UpdateQuery = {
					table: "orders",
					updates: {
						status: "reviewed",
						notes: "Bulk review completed",
					},
					condition: {
						status: "pending",
					},
				};

				const orderSql = buildUpdateQuery(orderUpdateQuery, config);
				db.run(orderSql);

				// Verify bulk updates
				const userResults = db.query("SELECT id, balance, description FROM users WHERE active = 1");
				const orderResults = db.query("SELECT id, status, notes FROM orders WHERE status = 'reviewed'");

				expect(userResults.length).toBeGreaterThan(0);
				expect(orderResults.length).toBeGreaterThan(0);

				for (const user of userResults) {
					const u = user as Record<string, unknown>;
					expect(u.description).toBe("Bulk balance update applied");
					expect(typeof u.balance).toBe("number");
				}

				for (const order of orderResults) {
					const o = order as Record<string, unknown>;
					expect(o.status).toBe("reviewed");
					expect(o.notes).toBe("Bulk review completed");
				}
			});
		});
	});

	describe("UPDATE Error Handling and Edge Cases", () => {
		it("should handle updates with special characters in strings", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "posts",
					updates: {
						title: "SQL Injection Test: '; DROP TABLE users; --",
						content: "Content with 'single quotes' and \"double quotes\" and \\backslashes\\",
					},
					condition: {
						id: { $uuid: "7ba7b810-9dad-11d1-80b4-00c04fd430c8" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// Verify proper escaping
				expect(sql).toBe(
					"UPDATE posts SET \"title\" = 'SQL Injection Test: ''; DROP TABLE users; --', \"content\" = 'Content with ''single quotes'' and \"double quotes\" and \\backslashes\\' WHERE posts.id = '7ba7b810-9dad-11d1-80b4-00c04fd430c8'",
				);

				// Execute the update (should be safe due to proper escaping)
				db.run(sql);

				// Verify the update was applied safely
				const result = db.query("SELECT title, content FROM posts WHERE id = '7ba7b810-9dad-11d1-80b4-00c04fd430c8'");
				expect(result.length).toBe(1);
				const post = result[0] as Record<string, unknown>;
				expect(post.title as string).toContain("SQL Injection Test");
				expect(post.content as string).toContain("single quotes");
			});
		});

		it("should handle updates with large numeric values", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						balance: 999999999.99,
						score: -123.456,
					},
					condition: {
						id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" },
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				expect(sql).toBe(
					'UPDATE users SET "balance" = 999999999.99, "score" = -123.456 WHERE users.id = \'550e8400-e29b-41d4-a716-446655440000\'',
				);

				// Execute the update
				db.run(sql);

				// Verify large numbers are handled correctly
				const result = db.query("SELECT balance, score FROM users WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
				expect(result.length).toBe(1);
				const user = result[0] as Record<string, unknown>;
				expect(user.balance).toBe(999999999.99);
				expect(user.score).toBe(-123.456);
			});
		});
	});

	describe("Performance and Optimization", () => {
		it("should generate efficient SQL for simple updates", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: { status: "updated" },
					condition: { id: { $uuid: "550e8400-e29b-41d4-a716-446655440000" } },
				};

				// Should be a simple, efficient UPDATE statement
				const sql = buildUpdateQuery(updateQuery, config);
				expect(sql).toBe("UPDATE users SET \"status\" = 'updated' WHERE users.id = '550e8400-e29b-41d4-a716-446655440000'");
			});
		});

		it("should handle updates with indexed field conditions efficiently", () => {
			db.executeInTransaction(() => {
				const updateQuery: UpdateQuery = {
					table: "users",
					updates: {
						balance: {
							$func: {
								ADD: [{ $field: "users.balance" }, 100],
							},
						},
					},
					condition: {
						$and: [
							{ id: { $in: ["550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"] } },
							{ active: true },
						],
					},
				};

				const sql = buildUpdateQuery(updateQuery, config);

				// Should use efficient IN clause for primary key lookups
				expect(sql).toBe(
					"UPDATE users SET \"balance\" = (users.balance + 100) WHERE (CAST(users.id AS TEXT) IN ('550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8') AND users.active = TRUE)",
				);

				// Execute and verify
				db.run(sql);

				const result = db.query(`
					SELECT id, balance 
					FROM users 
					WHERE id IN ('550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8') 
					AND active = 1
				`);

				expect(result.length).toBeGreaterThan(0);
				for (const user of result) {
					const u = user as Record<string, unknown>;
					expect(typeof u.balance).toBe("number");
					expect(Number(u.balance)).toBeGreaterThan(100); // Should have increased
				}
			});
		});
	});
});
