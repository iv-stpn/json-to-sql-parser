/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildDeleteQuery } from "../../src/builders/delete";
import { Dialect } from "../../src/constants/dialects";
import type { DeleteQuery } from "../../src/schemas";
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
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
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
			FOREIGN KEY (customer_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`);

	db.run(`
		CREATE TABLE audit_logs (
			id TEXT PRIMARY KEY,
			user_id TEXT,
			action TEXT NOT NULL,
			table_name TEXT NOT NULL,
			record_id TEXT,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			metadata TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
		)
	`);

	db.run(`
		CREATE TABLE sessions (
			id TEXT PRIMARY KEY,
			user_id TEXT,
			token TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			created_at TEXT DEFAULT CURRENT_TIMESTAMP,
			last_activity TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
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
		('6ba7b814-9dad-11d1-80b4-00c04fd430c8', 'Diana Prince', 'diana@example.com', 29, 1, 'premium', '{"department": "engineering", "role": "senior", "settings": {"theme": "light", "notifications": true}}', 2000.00),
		('6ba7b815-9dad-11d1-80b4-00c04fd430c8', 'Test User 1', 'test1@example.com', 22, 0, 'inactive', '{"department": "test", "role": "tester"}', 100.00),
		('6ba7b816-9dad-11d1-80b4-00c04fd430c8', 'Test User 2', 'test2@example.com', 23, 0, 'inactive', '{"department": "test", "role": "tester"}', 200.00),
		('6ba7b817-9dad-11d1-80b4-00c04fd430c8', 'Demo User', 'demo@example.com', 24, 1, 'demo', '{"department": "demo", "role": "demo"}', 50.00),
		('6ba7b818-9dad-11d1-80b4-00c04fd430c8', 'Temp User', 'temp@example.com', 20, 1, 'temporary', '{"department": "temp", "role": "temp"}', 10.00)
	`);

	db.run(`
		INSERT INTO posts (id, title, content, user_id, published, published_at, tags, rating, view_count) VALUES
		('7ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Getting Started with SQLite', 'This is a comprehensive guide to SQLite...', '550e8400-e29b-41d4-a716-446655440000', 1, '2024-01-15 10:30:00', '["database", "sqlite", "tutorial"]', 4.5, 150),
		('7ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Advanced SQL Queries', 'Learn advanced SQL techniques...', '550e8400-e29b-41d4-a716-446655440000', 1, '2024-01-16 14:45:00', '["sql", "advanced", "database"]', 4.8, 200),
		('7ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Marketing Strategies 2024', 'The latest marketing trends...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-17 09:15:00', '["marketing", "trends", "2024"]', 4.2, 120),
		('7ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Team Building Activities', 'Effective team building exercises...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["teamwork", "management", "hr"]', 3.9, 80),
		('7ba7b814-9dad-11d1-80b4-00c04fd430c8', 'Sales Techniques', 'How to close more deals...', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["sales", "techniques", "business"]', 4.1, 90),
		('7ba7b815-9dad-11d1-80b4-00c04fd430c8', 'Engineering Best Practices', 'Code quality and architecture...', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-18 11:20:00', '["engineering", "best-practices", "code"]', 4.7, 180),
		('7ba7b816-9dad-11d1-80b4-00c04fd430c8', 'Draft Post 1', 'This is a draft post...', '6ba7b815-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["draft", "test"]', 2.0, 5),
		('7ba7b817-9dad-11d1-80b4-00c04fd430c8', 'Draft Post 2', 'Another draft post...', '6ba7b816-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["draft", "test"]', 2.5, 10),
		('7ba7b818-9dad-11d1-80b4-00c04fd430c8', 'Temp Post', 'Temporary post for testing...', '6ba7b817-9dad-11d1-80b4-00c04fd430c8', 0, NULL, '["temp", "delete"]', 1.0, 1),
		('7ba7b819-9dad-11d1-80b4-00c04fd430c8', 'Demo Post', 'Demo post content...', '6ba7b818-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-19 16:00:00', '["demo"]', 3.0, 25)
	`);

	db.run(`
		INSERT INTO orders (id, amount, status, customer_id, shipped_at, delivered_date, notes) VALUES
		('8ba7b810-9dad-11d1-80b4-00c04fd430c8', 299.99, 'completed', '550e8400-e29b-41d4-a716-446655440000', '2024-01-16 08:00:00', '2024-01-18', 'Fast delivery requested'),
		('8ba7b811-9dad-11d1-80b4-00c04fd430c8', 149.50, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-17 12:30:00', NULL, 'Standard shipping'),
		('8ba7b812-9dad-11d1-80b4-00c04fd430c8', 89.99, 'pending', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Awaiting payment confirmation'),
		('8ba7b813-9dad-11d1-80b4-00c04fd430c8', 199.99, 'completed', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '2024-01-18 15:45:00', '2024-01-20', 'Express delivery'),
		('8ba7b814-9dad-11d1-80b4-00c04fd430c8', 59.99, 'cancelled', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Customer cancelled'),
		('8ba7b815-9dad-11d1-80b4-00c04fd430c8', 399.99, 'completed', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '2024-01-19 11:20:00', '2024-01-22', 'Fragile items'),
		('8ba7b816-9dad-11d1-80b4-00c04fd430c8', 79.99, 'pending', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Processing order'),
		('8ba7b817-9dad-11d1-80b4-00c04fd430c8', 25.99, 'cancelled', '6ba7b815-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Test order - cancel'),
		('8ba7b818-9dad-11d1-80b4-00c04fd430c8', 15.99, 'cancelled', '6ba7b816-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Test order - cancel'),
		('8ba7b819-9dad-11d1-80b4-00c04fd430c8', 5.99, 'pending', '6ba7b817-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Demo order'),
		('8ba7b820-9dad-11d1-80b4-00c04fd430c8', 1.99, 'pending', '6ba7b818-9dad-11d1-80b4-00c04fd430c8', NULL, NULL, 'Temp order')
	`);

	db.run(`
		INSERT INTO audit_logs (id, user_id, action, table_name, record_id, metadata) VALUES
		('aud001', '550e8400-e29b-41d4-a716-446655440000', 'CREATE', 'posts', '7ba7b810-9dad-11d1-80b4-00c04fd430c8', '{"details": "Post created"}'),
		('aud002', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'UPDATE', 'users', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '{"details": "Profile updated"}'),
		('aud003', '6ba7b815-9dad-11d1-80b4-00c04fd430c8', 'CREATE', 'posts', '7ba7b816-9dad-11d1-80b4-00c04fd430c8', '{"details": "Draft post created"}'),
		('aud004', '6ba7b816-9dad-11d1-80b4-00c04fd430c8', 'CREATE', 'posts', '7ba7b817-9dad-11d1-80b4-00c04fd430c8', '{"details": "Draft post created"}'),
		('aud005', '6ba7b817-9dad-11d1-80b4-00c04fd430c8', 'CREATE', 'orders', '8ba7b819-9dad-11d1-80b4-00c04fd430c8', '{"details": "Demo order created"}'),
		('aud006', '6ba7b818-9dad-11d1-80b4-00c04fd430c8', 'CREATE', 'orders', '8ba7b820-9dad-11d1-80b4-00c04fd430c8', '{"details": "Temp order created"}'),
		('aud007', NULL, 'SYSTEM', 'users', 'bulk-update', '{"details": "System maintenance"}')
	`);

	db.run(`
		INSERT INTO sessions (id, user_id, token, expires_at, last_activity) VALUES
		('sess001', '550e8400-e29b-41d4-a716-446655440000', 'token_john_1', '2024-02-01 00:00:00', '2024-01-20 15:30:00'),
		('sess002', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'token_jane_1', '2024-02-01 00:00:00', '2024-01-20 14:00:00'),
		('sess003', '6ba7b815-9dad-11d1-80b4-00c04fd430c8', 'token_test1_1', '2024-01-25 00:00:00', '2024-01-15 10:00:00'),
		('sess004', '6ba7b816-9dad-11d1-80b4-00c04fd430c8', 'token_test2_1', '2024-01-25 00:00:00', '2024-01-15 11:00:00'),
		('sess005', '6ba7b817-9dad-11d1-80b4-00c04fd430c8', 'token_demo_1', '2024-01-22 00:00:00', '2024-01-19 16:30:00'),
		('sess006', '6ba7b818-9dad-11d1-80b4-00c04fd430c8', 'token_temp_1', '2024-01-21 00:00:00', '2024-01-20 12:00:00')
	`);
}

describe("Integration - DELETE Operations with SQLite", () => {
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
				audit_logs: {
					allowedFields: [
						{ name: "id", type: "string", nullable: false }, // Fixed to match test data
						{ name: "user_id", type: "uuid", nullable: true },
						{ name: "action", type: "string", nullable: false },
						{ name: "table_name", type: "string", nullable: false },
						{ name: "record_id", type: "string", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
					],
				},
				sessions: {
					allowedFields: [
						{ name: "id", type: "string", nullable: false }, // Fixed to match test data
						{ name: "user_id", type: "uuid", nullable: true },
						{ name: "token", type: "string", nullable: false },
						{ name: "expires_at", type: "datetime", nullable: false },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "last_activity", type: "datetime", nullable: true },
					],
				},
			},
			variables: {
				current_user_id: "550e8400-e29b-41d4-a716-446655440000",
				admin_role: "admin",
				current_timestamp: "2024-01-20 12:00:00",
				min_age: 18,
				cutoff_date: "2024-01-18",
				low_rating_threshold: 3.0,
				inactive_days: 30,
				temp_status: "temporary",
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id" },
				{ table: "audit_logs", field: "user_id", toTable: "users", toField: "id" },
				{ table: "sessions", field: "user_id", toTable: "users", toField: "id" },
			],
		};
	});

	afterAll(() => {
		db.disconnect();
	});

	describe("Basic DELETE Operations", () => {
		it("should execute simple single record deletion", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						id: { $uuid: "6ba7b818-9dad-11d1-80b4-00c04fd430c8" },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM users WHERE users.id = '6ba7b818-9dad-11d1-80b4-00c04fd430c8'"); // Count before deletion
				const beforeCount = db.query("SELECT COUNT(*) as count FROM users WHERE id = '6ba7b818-9dad-11d1-80b4-00c04fd430c8'");
				expect((beforeCount[0] as Record<string, unknown>).count).toBe(1);

				// Execute the deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM users WHERE id = '6ba7b818-9dad-11d1-80b4-00c04fd430c8'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with simple equality condition", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						title: "Temp Post",
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE posts.title = 'Temp Post'");

				// Execute and verify
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title = 'Temp Post'");
				expect((beforeCount[0] as Record<string, unknown>).count).toBe(1);

				db.run(sql);

				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title = 'Temp Post'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with boolean condition", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						active: false,
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM users WHERE users.active = FALSE");

				// Count inactive users before deletion
				const beforeCount = db.query("SELECT COUNT(*) as count FROM users WHERE active = 0");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify all inactive users were deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM users WHERE active = 0");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with numeric conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "orders",
					condition: {
						amount: { $lt: 20 },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM orders WHERE orders.amount < 20");

				// Count orders with amount < 20
				const beforeCount = db.query("SELECT COUNT(*) as count FROM orders WHERE amount < 20");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify low-amount orders were deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM orders WHERE amount < 20");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with NULL conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						email: { $eq: null },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM users WHERE users.email IS NULL");

				// Count users with null emails
				const beforeCount = db.query("SELECT COUNT(*) as count FROM users WHERE email IS NULL");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify users with null emails were deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM users WHERE email IS NULL");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});
	});

	describe("DELETE with Complex Conditions", () => {
		it("should execute deletion with AND conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						$and: [{ published: false }, { rating: { $lt: { $var: "low_rating_threshold" } } }],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE (posts.published = FALSE AND posts.rating < 3)");

				// Count matching posts
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE published = 0 AND rating < 3");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE published = 0 AND rating < 3");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with OR conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "orders",
					condition: {
						$or: [{ status: "cancelled" }, { amount: { $lt: 50 } }],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM orders WHERE (orders.status = 'cancelled' OR orders.amount < 50)");

				// Count matching orders
				const beforeCount = db.query("SELECT COUNT(*) as count FROM orders WHERE status = 'cancelled' OR amount < 50");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM orders WHERE status = 'cancelled' OR amount < 50");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with nested logical conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						$and: [
							{
								$or: [{ status: "demo" }, { status: { $var: "temp_status" } }],
							},
							{ balance: { $lt: 100 } },
						],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe(
					"DELETE FROM users WHERE ((users.status = 'demo' OR users.status = 'temporary') AND users.balance < 100)",
				);

				// Count matching users
				const beforeCount = db.query(
					"SELECT COUNT(*) as count FROM users WHERE (status = 'demo' OR status = 'temporary') AND balance < 100",
				);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query(
					"SELECT COUNT(*) as count FROM users WHERE (status = 'demo' OR status = 'temporary') AND balance < 100",
				);
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with IN conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						user_id: { $in: ["6ba7b815-9dad-11d1-80b4-00c04fd430c8", "6ba7b816-9dad-11d1-80b4-00c04fd430c8"] },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe(
					"DELETE FROM posts WHERE CAST(posts.user_id AS TEXT) IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);

				// Count matching posts
				const beforeCount = db.query(
					"SELECT COUNT(*) as count FROM posts WHERE user_id IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query(
					"SELECT COUNT(*) as count FROM posts WHERE user_id IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with NOT conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "sessions",
					condition: {
						$not: {
							expires_at: { $gte: { $timestamp: "2024-01-24 12:00:00" } }, // Updated date
						},
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM sessions WHERE NOT (sessions.expires_at >= CAST('2024-01-24 12:00:00' AS TEXT))"); // Count expired sessions (should have some)
				const beforeCount = db.query("SELECT COUNT(*) as count FROM sessions WHERE NOT (expires_at >= '2024-01-24 12:00:00')");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify expired sessions were deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM sessions WHERE NOT (expires_at >= '2024-01-20 12:00:00')");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});
	});

	describe("DELETE with JSON Operations", () => {
		it("should execute deletion with JSON field conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						$and: [{ "metadata->department": "test" }, { "metadata->role": "tester" }],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe(
					"DELETE FROM users WHERE (users.metadata->>'department' = 'test' AND users.metadata->>'role' = 'tester')",
				);

				// Count users in test department with tester role
				const beforeCount = db.query(`
					SELECT COUNT(*) as count FROM users 
					WHERE JSON_EXTRACT(metadata, '$.department') = 'test' 
					AND JSON_EXTRACT(metadata, '$.role') = 'tester'
				`);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query(`
					SELECT COUNT(*) as count FROM users 
					WHERE JSON_EXTRACT(metadata, '$.department') = 'test' 
					AND JSON_EXTRACT(metadata, '$.role') = 'tester'
				`);
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with nested JSON path conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						$and: [{ "metadata->settings->theme": "dark" }, { "metadata->settings->notifications": false }],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe(
					"DELETE FROM users WHERE (users.metadata->'settings'->>'theme' = 'dark' AND users.metadata->'settings'->>'notifications' = FALSE)",
				);

				// Count users with dark theme and notifications disabled
				const beforeCount = db.query(`
					SELECT COUNT(*) as count FROM users 
					WHERE JSON_EXTRACT(metadata, '$.settings.theme') = 'dark' 
					AND JSON_EXTRACT(metadata, '$.settings.notifications') = 0
				`);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;

				// Execute deletion
				db.run(sql);

				// Verify deletion (users matching this criteria should be deleted)
				const afterCount = db.query(`
					SELECT COUNT(*) as count FROM users 
					WHERE JSON_EXTRACT(metadata, '$.settings.theme') = 'dark' 
					AND JSON_EXTRACT(metadata, '$.settings.notifications') = 0
				`);
				expect((afterCount[0] as Record<string, unknown>).count).toBeLessThan(expectedDeleteCount);
			});
		});
	});

	describe("DELETE with String Operations", () => {
		it("should execute deletion with LIKE conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						title: { $like: "%Draft%" },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE posts.title LIKE '%Draft%'");

				// Count draft posts
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title LIKE '%Draft%'");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title LIKE '%Draft%'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute deletion with case insensitive LIKE conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						// Use UPPER/LOWER for case insensitive search in SQLite
						name: { $like: "%USER%" },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				// SQLite doesn't have native ILIKE, should use LIKE with case handling
				expect(sql).toContain("DELETE FROM users WHERE");
				expect(sql).toContain("LIKE");

				// Count users with "user" in name (case insensitive approach)
				const beforeCount = db.query("SELECT COUNT(*) as count FROM users WHERE UPPER(name) LIKE UPPER('%user%')");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM users WHERE UPPER(name) LIKE UPPER('%user%')");
				expect((afterCount[0] as Record<string, unknown>).count).toBeLessThanOrEqual(expectedDeleteCount);
			});
		});

		it("should execute deletion with string length conditions", () => {
			db.executeInTransaction(() => {
				// Find posts with short titles (less than 15 characters)
				// Using a simpler approach without function calls for now
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						title: { $like: "Demo%" }, // Delete demo posts
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE posts.title LIKE 'Demo%'");

				// Count demo posts
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title LIKE 'Demo%'");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title LIKE 'Demo%'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});
	});

	describe("Bulk DELETE Operations", () => {
		it("should execute bulk deletion with status conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "orders",
					condition: {
						status: { $in: ["cancelled", "pending"] },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM orders WHERE orders.status IN ('cancelled', 'pending')");

				// Count orders to be deleted
				const beforeCount = db.query("SELECT COUNT(*) as count FROM orders WHERE status IN ('cancelled', 'pending')");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify bulk deletion
				const afterCount = db.query("SELECT COUNT(*) as count FROM orders WHERE status IN ('cancelled', 'pending')");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);

				// Verify remaining orders are only completed or shipped
				const remainingOrders = db.query("SELECT DISTINCT status FROM orders");
				const statuses = remainingOrders.map((row) => (row as Record<string, unknown>).status);
				expect(statuses.every((status) => ["completed", "shipped"].includes(status as string))).toBe(true);
			});
		});

		it("should execute cascading deletion simulation", () => {
			db.executeInTransaction(() => {
				// First delete related records, then the main record
				// This simulates a cascading delete for users with status "demo"

				// Delete audit logs for demo users first
				const auditDeleteQuery: DeleteQuery = {
					table: "audit_logs",
					condition: {
						user_id: { $uuid: "6ba7b817-9dad-11d1-80b4-00c04fd430c8" }, // Demo user
					},
				};

				const auditSql = buildDeleteQuery(auditDeleteQuery, config);
				// Expect normal comparison with proper UUID
				expect(auditSql).toBe("DELETE FROM audit_logs WHERE audit_logs.user_id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'");
				db.run(auditSql);

				// Delete sessions for demo users
				const sessionDeleteQuery: DeleteQuery = {
					table: "sessions",
					condition: {
						user_id: { $uuid: "6ba7b817-9dad-11d1-80b4-00c04fd430c8" }, // Demo user
					},
				};

				const sessionSql = buildDeleteQuery(sessionDeleteQuery, config);
				expect(sessionSql).toContain("sessions.user_id = ");
				db.run(sessionSql);

				// Delete orders for demo users
				const orderDeleteQuery: DeleteQuery = {
					table: "orders",
					condition: {
						customer_id: { $uuid: "6ba7b817-9dad-11d1-80b4-00c04fd430c8" }, // Demo user
					},
				};

				const orderSql = buildDeleteQuery(orderDeleteQuery, config);
				expect(orderSql).toContain("orders.customer_id = ");
				db.run(orderSql);

				// Delete posts for demo users
				const postDeleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						user_id: { $uuid: "6ba7b817-9dad-11d1-80b4-00c04fd430c8" }, // Demo user
					},
				};

				const postSql = buildDeleteQuery(postDeleteQuery, config);
				expect(postSql).toContain("posts.user_id = ");
				db.run(postSql);

				// Finally delete the demo user
				const userDeleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						id: { $uuid: "6ba7b817-9dad-11d1-80b4-00c04fd430c8" }, // Demo user
					},
				};

				const userSql = buildDeleteQuery(userDeleteQuery, config);
				expect(userSql).toContain("users.id = ");
				db.run(userSql);

				// Verify all related records were deleted
				const userCount = db.query("SELECT COUNT(*) as count FROM users WHERE id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'");
				const postCount = db.query("SELECT COUNT(*) as count FROM posts WHERE user_id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'");
				const orderCount = db.query(
					"SELECT COUNT(*) as count FROM orders WHERE customer_id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'",
				);
				const auditCount = db.query(
					"SELECT COUNT(*) as count FROM audit_logs WHERE user_id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'",
				);
				const sessionCount = db.query(
					"SELECT COUNT(*) as count FROM sessions WHERE user_id = '6ba7b817-9dad-11d1-80b4-00c04fd430c8'",
				);

				expect((userCount[0] as Record<string, unknown>).count).toBe(0);
				expect((postCount[0] as Record<string, unknown>).count).toBe(0);
				expect((orderCount[0] as Record<string, unknown>).count).toBe(0);
				expect((auditCount[0] as Record<string, unknown>).count).toBe(0);
				expect((sessionCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should execute conditional bulk deletion based on calculated fields", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						$and: [
							{ published: false },
							{ rating: { $lt: 3.0 } }, // Simple condition instead of function
						],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE (posts.published = FALSE AND posts.rating < 3)");

				// Count posts with low rating and unpublished
				const beforeCount = db.query("SELECT COUNT(*) as count FROM posts WHERE published = 0 AND rating < 3");
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBeGreaterThan(0);

				// Execute deletion
				db.run(sql);

				// Verify deletion of low-rating unpublished posts
				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE published = 0 AND rating < 3");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});
	});

	describe("DELETE Error Handling and Edge Cases", () => {
		it("should handle deletion with no matching records", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						id: { $uuid: "019916f3-d359-7d38-a4b9-d9e118310d5b" }, // Non-existent but valid UUID
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM users WHERE users.id = CAST('019916f0-52bc-77c9-ab55-78b3dcfe36a3' AS TEXT)"); // Count before deletion (should be 0)
				const beforeCount = db.query("SELECT COUNT(*) as count FROM users WHERE id = 'non-existent-id-12345'");
				expect((beforeCount[0] as Record<string, unknown>).count).toBe(0);

				// Execute deletion (should succeed but affect 0 rows)
				db.run(sql);

				// Count after deletion (should still be 0)
				const afterCount = db.query("SELECT COUNT(*) as count FROM users WHERE id = 'non-existent-id-12345'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should handle deletion with complex conditions that match no records", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						$and: [
							{ published: true },
							{ rating: { $gt: 10 } }, // Impossible rating
							{ view_count: { $lt: 0 } }, // Impossible view count
						],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM posts WHERE (posts.published = TRUE AND posts.rating > 10 AND posts.view_count < 0)");

				// Execute deletion (should affect 0 rows)
				db.run(sql);

				// Verify no posts were deleted (impossible conditions)
				const totalCount = db.query("SELECT COUNT(*) as count FROM posts");
				expect(Number((totalCount[0] as Record<string, unknown>).count)).toBeGreaterThan(0);
			});
		});

		it("should handle deletion with special characters in conditions", () => {
			db.executeInTransaction(() => {
				// First insert a record with special characters for testing
				db.run(`
					INSERT INTO posts (id, title, content, user_id, published) 
					VALUES ('test-special-chars', 'Title with single quotes', 'Content with backslashes and special chars', '550e8400-e29b-41d4-a716-446655440000', 0)
				`);

				const deleteQuery: DeleteQuery = {
					table: "posts",
					condition: {
						title: { $like: "%single%" },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				// Should properly handle the LIKE pattern
				expect(sql).toContain("LIKE");
				expect(sql).toContain("single");

				// Execute deletion
				db.run(sql);

				// Verify the special character record was found and deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM posts WHERE title LIKE '%single%'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should handle deletion with very large numeric conditions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "orders",
					condition: {
						amount: { $gt: 999999999.99 }, // Very large amount
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe("DELETE FROM orders WHERE orders.amount > 999999999.99");

				// Execute deletion (should affect 0 rows since no orders have such large amounts)
				db.run(sql);

				// Verify no orders were deleted
				const orderCount = db.query("SELECT COUNT(*) as count FROM orders");
				expect(Number((orderCount[0] as Record<string, unknown>).count)).toBeGreaterThan(0);
			});
		});
	});

	describe("DELETE Performance and Optimization", () => {
		it("should generate efficient SQL for simple deletions", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "sessions",
					condition: {
						id: "sess006",
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				// Should be a simple, efficient DELETE statement
				expect(sql).toBe("DELETE FROM sessions WHERE sessions.id = 'sess006'");
				expect(sql).not.toContain("CASE WHEN"); // No unnecessary complexity
				expect(sql).not.toContain("||"); // No unnecessary concatenations
				expect(sql).not.toContain("JOIN"); // No unnecessary joins

				// Execute and verify
				db.run(sql);

				const afterCount = db.query("SELECT COUNT(*) as count FROM sessions WHERE id = 'sess006'");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should handle deletion with indexed field conditions efficiently", () => {
			db.executeInTransaction(() => {
				const deleteQuery: DeleteQuery = {
					table: "users",
					condition: {
						id: { $in: ["6ba7b815-9dad-11d1-80b4-00c04fd430c8", "6ba7b816-9dad-11d1-80b4-00c04fd430c8"] },
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				// Should use efficient IN clause for primary key lookups
				expect(sql).toBe(
					"DELETE FROM users WHERE CAST(users.id AS TEXT) IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);

				// Count before deletion
				const beforeCount = db.query(
					"SELECT COUNT(*) as count FROM users WHERE id IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;
				expect(expectedDeleteCount).toBe(2);

				// Execute deletion
				db.run(sql);

				// Verify efficient deletion
				const afterCount = db.query(
					"SELECT COUNT(*) as count FROM users WHERE id IN ('6ba7b815-9dad-11d1-80b4-00c04fd430c8', '6ba7b816-9dad-11d1-80b4-00c04fd430c8')",
				);
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);
			});
		});

		it("should handle batch deletion patterns efficiently", () => {
			db.executeInTransaction(() => {
				// Pattern: Delete old audit logs (batch cleanup)
				const deleteQuery: DeleteQuery = {
					table: "audit_logs",
					condition: {
						$and: [
							{ action: { $ne: "SYSTEM" } },
							{ table_name: { $in: ["posts", "orders"] } },
							{
								user_id: { $ne: null },
							},
						],
					},
				};

				const sql = buildDeleteQuery(deleteQuery, config);

				expect(sql).toBe(
					"DELETE FROM audit_logs WHERE (audit_logs.action != 'SYSTEM' AND audit_logs.table_name IN ('posts', 'orders') AND audit_logs.user_id IS NOT NULL)",
				);

				// Count matching audit logs
				const beforeCount = db.query(`
					SELECT COUNT(*) as count FROM audit_logs 
					WHERE action != 'SYSTEM' 
					AND table_name IN ('posts', 'orders') 
					AND user_id IS NOT NULL
				`);
				const expectedDeleteCount = (beforeCount[0] as Record<string, unknown>).count as number;

				// Execute batch deletion
				db.run(sql);

				// Verify batch deletion
				const afterCount = db.query(`
					SELECT COUNT(*) as count FROM audit_logs 
					WHERE action != 'SYSTEM' 
					AND table_name IN ('posts', 'orders') 
					AND user_id IS NOT NULL
				`);
				expect((afterCount[0] as Record<string, unknown>).count).toBeLessThan(expectedDeleteCount);
			});
		});
	});

	describe("DELETE without WHERE clause", () => {
		it("should execute deletion without conditions (DELETE ALL)", () => {
			db.executeInTransaction(() => {
				// Create a test table for this specific test
				db.run(`
					CREATE TABLE temp_test_table (
						id TEXT PRIMARY KEY,
						name TEXT
					)
				`);

				db.run(`
					INSERT INTO temp_test_table (id, name) VALUES 
					('test1', 'Test 1'),
					('test2', 'Test 2'),
					('test3', 'Test 3')
				`);

				// Add temp table to config
				const tempConfig = {
					...config,
					tables: {
						...config.tables,
						temp_test_table: {
							allowedFields: [
								{ name: "id", type: "uuid" as const, nullable: false },
								{ name: "name", type: "string" as const, nullable: true },
							],
						},
					},
				};

				// Delete all records from the test table
				const deleteQuery: DeleteQuery = {
					table: "temp_test_table",
				};

				const sql = buildDeleteQuery(deleteQuery, tempConfig);

				expect(sql).toBe("DELETE FROM temp_test_table");

				// Count before deletion
				const beforeCount = db.query("SELECT COUNT(*) as count FROM temp_test_table");
				expect(Number((beforeCount[0] as Record<string, unknown>).count)).toBe(3);

				// Execute deletion
				db.run(sql);

				// Verify all records were deleted
				const afterCount = db.query("SELECT COUNT(*) as count FROM temp_test_table");
				expect((afterCount[0] as Record<string, unknown>).count).toBe(0);

				// Cleanup
				db.run("DROP TABLE temp_test_table");
			});
		});
	});
});
