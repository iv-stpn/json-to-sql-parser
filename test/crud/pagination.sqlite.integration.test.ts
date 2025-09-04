/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildSelectQuery } from "../../src/builders/select";
import { Dialect } from "../../src/constants/dialects";
import type { SelectQuery } from "../../src/schemas";
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

	// Insert test data for pagination testing
	db.run(`
		INSERT INTO users (id, name, email, age, active, status, metadata, balance) VALUES
		('user-001', 'Alice Johnson', 'alice@example.com', 28, 1, 'premium', '{"department": "engineering", "role": "senior"}', 2000.00),
		('user-002', 'Bob Smith', 'bob@example.com', 32, 1, 'active', '{"department": "marketing", "role": "manager"}', 1500.00),
		('user-003', 'Carol Brown', 'carol@example.com', 26, 1, 'active', '{"department": "sales", "role": "rep"}', 1200.00),
		('user-004', 'David Wilson', 'david@example.com', 30, 1, 'premium', '{"department": "engineering", "role": "junior"}', 1800.00),
		('user-005', 'Emma Davis', 'emma@example.com', 27, 1, 'active', '{"department": "hr", "role": "specialist"}', 1300.00),
		('user-006', 'Frank Miller', 'frank@example.com', 35, 1, 'premium', '{"department": "engineering", "role": "senior"}', 2200.00),
		('user-007', 'Grace Taylor', 'grace@example.com', 29, 1, 'active', '{"department": "marketing", "role": "specialist"}', 1400.00),
		('user-008', 'Henry Anderson', 'henry@example.com', 31, 1, 'active', '{"department": "sales", "role": "manager"}', 1600.00),
		('user-009', 'Ivy Thomas', 'ivy@example.com', 24, 1, 'active', '{"department": "support", "role": "specialist"}', 1100.00),
		('user-010', 'Jack White', 'jack@example.com', 33, 1, 'premium', '{"department": "engineering", "role": "senior"}', 2400.00),
		('user-011', 'Karen Harris', 'karen@example.com', 25, 1, 'active', '{"department": "marketing", "role": "coordinator"}', 1250.00),
		('user-012', 'Leo Clark', 'leo@example.com', 36, 1, 'premium', '{"department": "engineering", "role": "lead"}', 2600.00),
		('user-013', 'Mia Lewis', 'mia@example.com', 23, 1, 'active', '{"department": "support", "role": "agent"}', 1000.00),
		('user-014', 'Noah Young', 'noah@example.com', 34, 1, 'active', '{"department": "sales", "role": "rep"}', 1350.00),
		('user-015', 'Olivia King', 'olivia@example.com', 28, 1, 'premium', '{"department": "engineering", "role": "senior"}', 2100.00)
	`);

	db.run(`
		INSERT INTO posts (id, title, content, user_id, published, published_at, tags, rating, view_count) VALUES
		('post-001', 'Introduction to SQLite', 'SQLite is a lightweight database...', 'user-001', 1, '2024-01-01 10:00:00', '["database", "sqlite"]', 4.5, 150),
		('post-002', 'Advanced SQL Queries', 'Learn complex SQL patterns...', 'user-001', 1, '2024-01-02 14:30:00', '["sql", "advanced"]', 4.8, 220),
		('post-003', 'Marketing Best Practices', 'Effective marketing strategies...', 'user-002', 1, '2024-01-03 09:15:00', '["marketing", "strategy"]', 4.2, 180),
		('post-004', 'Sales Techniques 101', 'Fundamentals of successful selling...', 'user-003', 1, '2024-01-04 11:45:00', '["sales", "basics"]', 4.0, 160),
		('post-005', 'Engineering Culture', 'Building great engineering teams...', 'user-004', 1, '2024-01-05 16:20:00', '["engineering", "culture"]', 4.6, 190),
		('post-006', 'HR Policies Overview', 'Understanding company policies...', 'user-005', 1, '2024-01-06 08:30:00', '["hr", "policies"]', 3.8, 120),
		('post-007', 'Database Optimization', 'Performance tuning techniques...', 'user-006', 1, '2024-01-07 13:00:00', '["database", "performance"]', 4.7, 250),
		('post-008', 'Digital Marketing Trends', 'Latest marketing technologies...', 'user-007', 1, '2024-01-08 15:45:00', '["marketing", "digital"]', 4.3, 170),
		('post-009', 'Customer Support Excellence', 'Delivering exceptional support...', 'user-009', 1, '2024-01-09 10:30:00', '["support", "customer"]', 4.1, 140),
		('post-010', 'Code Review Best Practices', 'Effective code review processes...', 'user-010', 1, '2024-01-10 12:15:00', '["engineering", "code-review"]', 4.4, 200),
		('post-011', 'Content Marketing Strategy', 'Creating engaging content...', 'user-011', 0, NULL, '["marketing", "content"]', 3.9, 110),
		('post-012', 'System Architecture Design', 'Scalable system design principles...', 'user-012', 1, '2024-01-11 14:00:00', '["engineering", "architecture"]', 4.9, 300),
		('post-013', 'Support Automation', 'Automating customer support...', 'user-013', 0, NULL, '["support", "automation"]', 3.7, 95),
		('post-014', 'Lead Generation Tactics', 'Effective lead generation...', 'user-014', 1, '2024-01-12 09:45:00', '["sales", "leads"]', 4.2, 175),
		('post-015', 'Performance Monitoring', 'Monitoring application performance...', 'user-015', 1, '2024-01-13 11:30:00', '["engineering", "monitoring"]', 4.5, 185),
		('post-016', 'Social Media Marketing', 'Leveraging social platforms...', 'user-002', 0, NULL, '["marketing", "social"]', 3.6, 85),
		('post-017', 'Technical Debt Management', 'Managing and reducing technical debt...', 'user-006', 1, '2024-01-14 16:45:00', '["engineering", "debt"]', 4.3, 165),
		('post-018', 'Sales Pipeline Management', 'Optimizing sales processes...', 'user-008', 1, '2024-01-15 13:20:00', '["sales", "pipeline"]', 4.1, 155),
		('post-019', 'Remote Work Guidelines', 'Best practices for remote work...', 'user-005', 0, NULL, '["hr", "remote"]', 3.8, 130),
		('post-020', 'API Design Principles', 'Creating robust APIs...', 'user-012', 1, '2024-01-16 10:10:00', '["engineering", "api"]', 4.6, 210)
	`);

	db.run(`
		INSERT INTO orders (id, amount, status, customer_id, shipped_at, delivered_date, notes) VALUES
		('order-001', 299.99, 'completed', 'user-001', '2024-01-10 08:00:00', '2024-01-12', 'Express delivery'),
		('order-002', 149.50, 'completed', 'user-002', '2024-01-11 10:30:00', '2024-01-14', 'Standard shipping'),
		('order-003', 89.99, 'shipped', 'user-003', '2024-01-15 14:20:00', NULL, 'In transit'),
		('order-004', 199.99, 'completed', 'user-004', '2024-01-12 09:15:00', '2024-01-15', 'Fragile handling'),
		('order-005', 59.99, 'pending', 'user-005', NULL, NULL, 'Payment processing'),
		('order-006', 399.99, 'completed', 'user-006', '2024-01-13 16:45:00', '2024-01-16', 'Premium packaging'),
		('order-007', 79.99, 'shipped', 'user-007', '2024-01-16 11:30:00', NULL, 'Standard delivery'),
		('order-008', 119.99, 'completed', 'user-008', '2024-01-14 13:00:00', '2024-01-17', 'Gift wrapping'),
		('order-009', 25.99, 'pending', 'user-009', NULL, NULL, 'Awaiting stock'),
		('order-010', 349.99, 'completed', 'user-010', '2024-01-15 15:20:00', '2024-01-18', 'Express delivery'),
		('order-011', 45.99, 'cancelled', 'user-011', NULL, NULL, 'Customer cancelled'),
		('order-012', 529.99, 'completed', 'user-012', '2024-01-16 12:45:00', '2024-01-19', 'Large item delivery'),
		('order-013', 15.99, 'pending', 'user-013', NULL, NULL, 'Small order'),
		('order-014', 189.99, 'shipped', 'user-014', '2024-01-17 14:10:00', NULL, 'Priority shipping'),
		('order-015', 249.99, 'completed', 'user-015', '2024-01-18 09:30:00', '2024-01-20', 'Standard delivery'),
		('order-016', 99.99, 'pending', 'user-001', NULL, NULL, 'Second order'),
		('order-017', 179.99, 'completed', 'user-003', '2024-01-19 11:15:00', '2024-01-22', 'Customer pickup'),
		('order-018', 69.99, 'cancelled', 'user-007', NULL, NULL, 'Out of stock'),
		('order-019', 429.99, 'shipped', 'user-010', '2024-01-20 13:40:00', NULL, 'Large order'),
		('order-020', 35.99, 'pending', 'user-013', NULL, NULL, 'Low priority')
	`);
}

describe("Integration - Pagination with SQLite", () => {
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
			},
			variables: {
				current_user_id: "user-001",
				admin_role: "admin",
				current_timestamp: "2024-01-20 12:00:00",
				min_age: 18,
				page_size: 5,
				max_results: 100,
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

	describe("Basic Pagination Operations", () => {
		it("should execute simple LIMIT pagination", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						email: true,
					},
					pagination: {
						limit: 5,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 5");
				expect(sql).not.toContain("OFFSET");

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(5);

				// Verify we got the first 5 users
				const userIds = (results as Array<Record<string, unknown>>).map((row) => row.id);
				expect(userIds).toContain("user-001");
				expect(userIds).toContain("user-002");
				expect(userIds).toContain("user-003");
				expect(userIds).toContain("user-004");
				expect(userIds).toContain("user-005");
			});
		});

		it("should execute LIMIT with OFFSET pagination", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						status: true,
					},
					pagination: {
						limit: 3,
						offset: 5,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 3");
				expect(sql).toContain("OFFSET 5");

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(3);

				// Verify we got users 6, 7, 8 (0-indexed offset 5 means starting from 6th record)
				const userIds = (results as Array<Record<string, unknown>>).map((row) => row.id);
				expect(userIds).toContain("user-006");
				expect(userIds).toContain("user-007");
				expect(userIds).toContain("user-008");
			});
		});

		it("should execute OFFSET without LIMIT", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						offset: 10,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("OFFSET 10");
				// For SQLite, LIMIT -1 should be added when offset is provided without limit
				expect(sql).toContain("LIMIT -1");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBe(5); // Total 15 users, offset 10 = 5 remaining

				// Verify we got the last 5 users
				const userIds = (results as Array<Record<string, unknown>>).map((row) => row.id);
				expect(userIds).toContain("user-011");
				expect(userIds).toContain("user-015");
			});
		});

		it("should handle pagination with zero offset", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						published: true,
					},
					pagination: {
						limit: 4,
						offset: 0,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 4");
				expect(sql).toContain("OFFSET 0");

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(4);

				// Should be equivalent to just LIMIT 4
				const limitOnlyQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						published: true,
					},
					pagination: {
						limit: 4,
					},
				};

				const limitOnlySql = buildSelectQuery(limitOnlyQuery, config);
				const limitOnlyResults = db.query(limitOnlySql);

				// Results should be the same
				expect((results as Array<Record<string, unknown>>).map((r) => r.id)).toEqual(
					(limitOnlyResults as Array<Record<string, unknown>>).map((r) => r.id),
				);
			});
		});
	});

	describe("Pagination with Filtering", () => {
		it("should execute pagination with WHERE conditions", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						rating: true,
						published: true,
					},
					condition: {
						published: true,
					},
					pagination: {
						limit: 3,
						offset: 2,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("WHERE posts.published = TRUE");
				expect(sql).toContain("LIMIT 3");
				expect(sql).toContain("OFFSET 2");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(3);

				// Verify all results are published posts
				for (const result of results as Array<Record<string, unknown>>) {
					expect(result.published).toBeTruthy();
				}
			});
		});

		it("should execute pagination with complex WHERE conditions", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						age: true,
						status: true,
					},
					condition: {
						$and: [{ status: "premium" }, { age: { $gte: 28 } }],
					},
					pagination: {
						limit: 2,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("WHERE (users.status = 'premium' AND users.age >= 28)");
				expect(sql).toContain("LIMIT 2");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(2);

				// Verify all results match the conditions
				for (const result of results as Array<Record<string, unknown>>) {
					expect(result.status).toBe("premium");
					expect(Number(result.age)).toBeGreaterThanOrEqual(28);
				}
			});
		});

		it("should execute pagination with JSON field conditions", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						"metadata->department": true,
						"metadata->role": true,
					},
					condition: {
						"metadata->department": "engineering",
					},
					pagination: {
						limit: 3,
						offset: 1,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("users.metadata->>'department' = 'engineering'");
				expect(sql).toContain("LIMIT 3");
				expect(sql).toContain("OFFSET 1");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(3);

				// Verify all results are from engineering department
				for (const result of results as Array<Record<string, unknown>>) {
					expect(result["metadata->department"]).toBe("engineering");
				}
			});
		});
	});

	describe("Pagination with Sorting", () => {
		it("should execute pagination with ORDER BY (implicit ordering)", () => {
			db.executeInTransaction(() => {
				// Test consistent ordering by verifying multiple pages return different results
				const page1Query: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						amount: true,
						status: true,
					},
					pagination: {
						limit: 5,
						offset: 0,
					},
				};

				const page2Query: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						amount: true,
						status: true,
					},
					pagination: {
						limit: 5,
						offset: 5,
					},
				};

				const page1Sql = buildSelectQuery(page1Query, config);
				const page2Sql = buildSelectQuery(page2Query, config);

				const page1Results = db.query(page1Sql);
				const page2Results = db.query(page2Sql);

				expect(page1Results).toHaveLength(5);
				expect(page2Results).toHaveLength(5);

				// Verify pages return different results
				const page1Ids = (page1Results as Array<Record<string, unknown>>).map((r) => r.id);
				const page2Ids = (page2Results as Array<Record<string, unknown>>).map((r) => r.id);

				// No overlap between pages
				const overlap = page1Ids.filter((id) => page2Ids.includes(id));
				expect(overlap).toHaveLength(0);
			});
		});

		it("should handle pagination edge cases with small datasets", () => {
			db.executeInTransaction(() => {
				// Create a temporary table with only 2 records
				db.run(`
					CREATE TABLE temp_small (
						id TEXT PRIMARY KEY,
						name TEXT
					)
				`);

				db.run(`
					INSERT INTO temp_small (id, name) VALUES 
					('small-1', 'First'),
					('small-2', 'Second')
				`);

				// Test pagination larger than dataset
				const selectQuery: SelectQuery = {
					rootTable: "temp_small",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 10, // More than available
						offset: 0,
					},
				};

				const sql = buildSelectQuery(selectQuery, {
					tables: {
						temp_small: {
							allowedFields: [
								{ name: "id", type: "string", nullable: false },
								{ name: "name", type: "string", nullable: false },
							],
						},
					},
					variables: {},
					dialect: Dialect.SQLITE_EXTENSIONS,
					relationships: [],
				});

				const results = db.query(sql);
				expect(results).toHaveLength(2); // Only 2 records available

				// Test offset beyond dataset
				const beyondQuery: SelectQuery = {
					rootTable: "temp_small",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 5,
						offset: 10, // Beyond available records
					},
				};

				const beyondSql = buildSelectQuery(beyondQuery, {
					tables: {
						temp_small: {
							allowedFields: [
								{ name: "id", type: "string", nullable: false },
								{ name: "name", type: "string", nullable: false },
							],
						},
					},
					variables: {},
					dialect: Dialect.SQLITE_EXTENSIONS,
					relationships: [],
				});
				const beyondResults = db.query(beyondSql);
				expect(beyondResults).toHaveLength(0); // No records

				// Cleanup
				db.run("DROP TABLE temp_small");
			});
		});
	});

	describe("Pagination Performance and Edge Cases", () => {
		it("should handle large offset values efficiently", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
					},
					pagination: {
						limit: 2,
						offset: 15, // Near the end of our 20 posts
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 2");
				expect(sql).toContain("OFFSET 15");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(2);

				// Should get the last few posts
				if (results.length > 0) {
					const postIds = (results as Array<Record<string, unknown>>).map((r) => r.id);
					// These should be among the last posts
					expect(postIds.some((id) => String(id).includes("post-0"))).toBe(true);
				}
			});
		});

		it("should handle pagination with exact boundary conditions", () => {
			db.executeInTransaction(() => {
				// First, count total users to know exact boundaries
				const totalUsers = db.query("SELECT COUNT(*) as count FROM users");
				const totalCount = (totalUsers[0] as Record<string, unknown>).count as number;

				// Test pagination at exact boundary (last page)
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 5,
						offset: totalCount - 2, // Should get last 2 records
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				const results = db.query(sql);
				expect(results.length).toBe(2); // Should get exactly 2 records

				// Test pagination exactly at total count
				const exactBoundaryQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 5,
						offset: totalCount, // Should get 0 records
					},
				};

				const exactBoundarySql = buildSelectQuery(exactBoundaryQuery, config);
				const exactBoundaryResults = db.query(exactBoundarySql);
				expect(exactBoundaryResults).toHaveLength(0);
			});
		});

		it("should handle pagination with very large limit values", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						amount: true,
						status: true,
					},
					pagination: {
						limit: 1000000, // Very large limit
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 1000000");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBe(20); // Should return all available orders (20)
			});
		});

		it("should generate efficient SQL for simple pagination", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 10,
						offset: 5,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				// Should be a clean, simple SELECT with LIMIT OFFSET
				expect(sql).toContain("SELECT");
				expect(sql).toContain("FROM users");
				expect(sql).toContain("LIMIT 10");
				expect(sql).toContain("OFFSET 5");
				expect(sql).not.toContain("CASE WHEN"); // No unnecessary complexity
				expect(sql).not.toContain("UNION"); // No unnecessary unions
				expect(sql).not.toContain("SUBQUERY"); // No unnecessary subqueries
			});
		});
	});

	describe("Pagination with Relationships", () => {
		it("should execute pagination with JOIN operations", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						users: {
							name: true,
							status: true,
						},
					},
					condition: {
						published: true,
					},
					pagination: {
						limit: 3,
						offset: 1,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("JOIN users");
				expect(sql).toContain("LIMIT 3");
				expect(sql).toContain("OFFSET 1");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(3);

				// Verify JOIN data is present
				for (const result of results as Array<Record<string, unknown>>) {
					expect(result["users.name"]).toBeDefined();
					expect(result["users.status"]).toBeDefined();
				}
			});
		});

		it("should handle pagination with multiple JOIN relationships", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "orders",
					selection: {
						id: true,
						amount: true,
						status: true,
						users: {
							name: true,
							email: true,
						},
					},
					condition: {
						status: { $in: ["completed", "shipped"] },
					},
					pagination: {
						limit: 5,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("JOIN users");
				expect(sql).toContain("LIMIT 5");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(5);

				// Verify all results have customer data
				for (const result of results as Array<Record<string, unknown>>) {
					expect(result["users.name"]).toBeDefined();
					expect(["completed", "shipped"]).toContain(result.status);
				}
			});
		});
	});

	describe("Pagination Variable Usage", () => {
		it("should handle consistent pagination values", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
						age: true,
					},
					condition: {
						active: true,
					},
					pagination: {
						limit: 5, // Using the same value as page_size variable
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 5"); // Consistent with page_size variable

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(5);
			});
		});

		it("should handle pagination with computed offset values", () => {
			db.executeInTransaction(() => {
				// Create a manual pagination calculation
				const pageNumber = 2; // 0-indexed, so this is page 3
				const pageSize = 4;
				const offset = pageNumber * pageSize; // 2 * 4 = 8

				const selectQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						rating: true,
					},
					pagination: {
						limit: pageSize,
						offset: offset,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 4");
				expect(sql).toContain("OFFSET 8");

				// Execute and verify
				const results = db.query(sql);
				expect(results.length).toBeLessThanOrEqual(4);

				// Compare with getting all posts to verify we got the right page
				const allPostsQuery: SelectQuery = {
					rootTable: "posts",
					selection: {
						id: true,
						title: true,
						rating: true,
					},
				};

				const allPosts = db.query(buildSelectQuery(allPostsQuery, config));
				const expectedPagePosts = (allPosts as Array<Record<string, unknown>>).slice(offset, offset + pageSize);

				expect(results).toHaveLength(expectedPagePosts.length);
			});
		});
	});

	describe("Pagination Error Handling", () => {
		it("should handle invalid pagination parameters gracefully", () => {
			db.executeInTransaction(() => {
				// Test with limit 0 (should be handled by validation, but if it gets through)
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					pagination: {
						limit: 1, // Minimum valid limit
						offset: 0,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 1");

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(1);
			});
		});

		it("should handle pagination without any matching records", () => {
			db.executeInTransaction(() => {
				const selectQuery: SelectQuery = {
					rootTable: "users",
					selection: {
						id: true,
						name: true,
					},
					condition: {
						status: "non-existent-status", // No matching records
					},
					pagination: {
						limit: 10,
						offset: 0,
					},
				};

				const sql = buildSelectQuery(selectQuery, config);

				expect(sql).toContain("LIMIT 10");
				expect(sql).toContain("OFFSET 0");

				// Execute and verify
				const results = db.query(sql);
				expect(results).toHaveLength(0);
			});
		});
	});
});
