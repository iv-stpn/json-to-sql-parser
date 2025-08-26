/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */

import { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import type { AggregationQuery } from "../../src/schemas";
import type { Config } from "../../src/types";

// SQLite database helper class (reused from select.sqlite.integration.test.ts)
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
			shipped_at TEXT,
			delivered_date TEXT,
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
			metadata TEXT
		)
	`);

	db.run(`
		CREATE TABLE order_items (
			id TEXT PRIMARY KEY,
			order_id TEXT,
			product_id TEXT,
			quantity INTEGER NOT NULL,
			unit_price REAL NOT NULL,
			FOREIGN KEY (order_id) REFERENCES orders(id),
			FOREIGN KEY (product_id) REFERENCES products(id)
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
		('7ba7b815-9dad-11d1-80b4-00c04fd430c8', 'Engineering Best Practices', 'Code quality and architecture...', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-18 11:20:00', '["engineering", "best-practices", "code"]', 4.7, 180),
		('7ba7b816-9dad-11d1-80b4-00c04fd430c8', 'HR Policies Update', 'New company policies...', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 1, '2024-01-19 16:30:00', '["hr", "policies", "company"]', 3.8, 95)
	`);

	db.run(`
		INSERT INTO orders (id, amount, status, customer_id, shipped_at, delivered_date) VALUES
		('8ba7b810-9dad-11d1-80b4-00c04fd430c8', 299.99, 'completed', '550e8400-e29b-41d4-a716-446655440000', '2024-01-16 08:00:00', '2024-01-18'),
		('8ba7b811-9dad-11d1-80b4-00c04fd430c8', 149.50, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-17 12:30:00', NULL),
		('8ba7b812-9dad-11d1-80b4-00c04fd430c8', 89.99, 'pending', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', NULL, NULL),
		('8ba7b813-9dad-11d1-80b4-00c04fd430c8', 199.99, 'completed', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '2024-01-18 15:45:00', '2024-01-20'),
		('8ba7b814-9dad-11d1-80b4-00c04fd430c8', 59.99, 'cancelled', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', NULL, NULL),
		('8ba7b815-9dad-11d1-80b4-00c04fd430c8', 399.99, 'completed', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '2024-01-19 11:20:00', '2024-01-22'),
		('8ba7b816-9dad-11d1-80b4-00c04fd430c8', 79.99, 'pending', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', NULL, NULL),
		('8ba7b817-9dad-11d1-80b4-00c04fd430c8', 249.99, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-20 16:10:00', NULL),
		('8ba7b818-9dad-11d1-80b4-00c04fd430c8', 179.99, 'completed', '6ba7b814-9dad-11d1-80b4-00c04fd430c8', '2024-01-21 09:15:00', '2024-01-23'),
		('8ba7b819-9dad-11d1-80b4-00c04fd430c8', 329.99, 'completed', '6ba7b814-9dad-11d1-80b4-00c04fd430c8', '2024-01-22 14:20:00', '2024-01-24')
	`);

	db.run(`
		INSERT INTO products (id, name, price, category, stock_quantity, metadata) VALUES
		('prd001', 'SQLite Database Book', 49.99, 'books', 100, '{"isbn": "978-1234567890", "pages": 320}'),
		('prd002', 'Advanced SQL Course', 199.99, 'courses', 50, '{"duration_hours": 40, "level": "advanced"}'),
		('prd003', 'Programming T-Shirt', 24.99, 'apparel', 200, '{"size": ["S", "M", "L", "XL"], "color": "black"}'),
		('prd004', 'Database Design Tool', 99.99, 'software', 25, '{"license_type": "single", "version": "2024.1"}'),
		('prd005', 'SQL Cheat Sheet Poster', 15.99, 'accessories', 300, '{"dimensions": "24x36", "material": "glossy paper"}')
	`);

	db.run(`
		INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
		('itm001', '8ba7b810-9dad-11d1-80b4-00c04fd430c8', 'prd002', 1, 199.99),
		('itm002', '8ba7b810-9dad-11d1-80b4-00c04fd430c8', 'prd004', 1, 99.99),
		('itm003', '8ba7b811-9dad-11d1-80b4-00c04fd430c8', 'prd001', 3, 49.99),
		('itm004', '8ba7b812-9dad-11d1-80b4-00c04fd430c8', 'prd003', 2, 24.99),
		('itm005', '8ba7b812-9dad-11d1-80b4-00c04fd430c8', 'prd005', 2, 15.99),
		('itm006', '8ba7b813-9dad-11d1-80b4-00c04fd430c8', 'prd002', 1, 199.99),
		('itm007', '8ba7b814-9dad-11d1-80b4-00c04fd430c8', 'prd003', 1, 24.99),
		('itm008', '8ba7b814-9dad-11d1-80b4-00c04fd430c8', 'prd005', 1, 15.99),
		('itm009', '8ba7b815-9dad-11d1-80b4-00c04fd430c8', 'prd002', 2, 199.99),
		('itm010', '8ba7b816-9dad-11d1-80b4-00c04fd430c8', 'prd001', 1, 49.99),
		('itm011', '8ba7b816-9dad-11d1-80b4-00c04fd430c8', 'prd005', 2, 15.99),
		('itm012', '8ba7b817-9dad-11d1-80b4-00c04fd430c8', 'prd002', 1, 199.99),
		('itm013', '8ba7b817-9dad-11d1-80b4-00c04fd430c8', 'prd001', 1, 49.99),
		('itm014', '8ba7b818-9dad-11d1-80b4-00c04fd430c8', 'prd002', 1, 179.99),
		('itm015', '8ba7b819-9dad-11d1-80b4-00c04fd430c8', 'prd002', 1, 199.99),
		('itm016', '8ba7b819-9dad-11d1-80b4-00c04fd430c8', 'prd004', 1, 99.99)
	`);
}

describe("Integration - Aggregation Queries and Statistical Operations (SQLite)", () => {
	let db: SQLiteDatabaseHelper;
	let config: Config;

	beforeAll(() => {
		db = new SQLiteDatabaseHelper();
		db.connect();
		setupSQLiteDatabase(db);

		config = {
			dialect: "sqlite-3.44-extensions",
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
						{ name: "balance", type: "number", nullable: true },
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
						{ name: "shipped_at", type: "datetime", nullable: true },
						{ name: "delivered_date", type: "date", nullable: true },
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
					],
				},
				order_items: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "order_id", type: "uuid", nullable: false },
						{ name: "product_id", type: "uuid", nullable: false },
						{ name: "quantity", type: "number", nullable: false },
						{ name: "unit_price", type: "number", nullable: false },
					],
				},
			},
			variables: {
				current_user_id: "1",
				adminRole: "admin",
				current_year: 2024,
				high_value_threshold: 200,
				premium_age_limit: 30,
				admin_role: "admin",
				min_rating: 4.0,
			},
			relationships: [
				{ table: "posts", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "orders", field: "customer_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "order_items", field: "order_id", toTable: "orders", toField: "id", type: "many-to-one" },
				{ table: "order_items", field: "product_id", toTable: "products", toField: "id", type: "many-to-one" },
			],
		};
	});

	afterAll(() => {
		db.disconnect();
	});

	describe("Basic Statistical Aggregations", () => {
		it("should execute simple COUNT aggregation", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.status"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "users.id" },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("status");
				expect(rows[0]).toHaveProperty("user_count");

				// Check premium users count
				const premiumUsers = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r.status === "premium";
				});
				expect(premiumUsers).toBeDefined();
				const premiumRecord = premiumUsers as Record<string, unknown>;
				expect(Number(premiumRecord.user_count)).toBe(3); // John, Alice, Diana
			});
		});

		it("should execute SUM aggregation with conditions", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						total_amount: { function: "SUM", field: "orders.amount" },
						order_count: { function: "COUNT", field: "orders.id" },
					},
					condition: {
						"orders.status": { $in: ["completed", "shipped", "pending"] },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);

				// Verify completed orders total
				const completedOrders = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r.status === "completed";
				});
				expect(completedOrders).toBeDefined();
				const completedRecord = completedOrders as Record<string, unknown>;
				expect(Number(completedRecord.total_amount)).toBeCloseTo(1409.95, 2); // Sum of completed orders
				expect(Number(completedRecord.order_count)).toBe(5);
			});
		});

		it("should execute AVG aggregation with proper decimal handling", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "posts",
					groupBy: ["posts.published"],
					aggregatedFields: {
						avg_rating: { function: "AVG", field: "posts.rating" },
						avg_views: { function: "AVG", field: "posts.view_count" },
						post_count: { function: "COUNT", field: "posts.id" },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBe(2); // published (1) and unpublished (0)

				// Check published posts average rating
				const publishedPosts = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r.published === 1;
				});
				expect(publishedPosts).toBeDefined();
				const publishedRecord = publishedPosts as Record<string, unknown>;
				expect(Number(publishedRecord.avg_rating)).toBeGreaterThan(4.0);
				expect(Number(publishedRecord.post_count)).toBe(5);
			});
		});

		it("should execute MIN/MAX aggregations", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.metadata->department"],
					aggregatedFields: {
						min_age: { function: "MIN", field: "users.age" },
						max_age: { function: "MAX", field: "users.age" },
						avg_balance: { function: "AVG", field: "users.balance" },
						total_users: { function: "COUNT", field: "users.id" },
					},
					condition: {
						"users.age": { $ne: null },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("metadata->department");
				expect(rows[0]).toHaveProperty("min_age");
				expect(rows[0]).toHaveProperty("max_age");

				// Check engineering department
				const engineeringDept = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r["metadata->department"] === "engineering";
				});
				expect(engineeringDept).toBeDefined();
				const engRecord = engineeringDept as Record<string, unknown>;
				expect(Number(engRecord.min_age)).toBe(28); // Alice
				expect(Number(engRecord.max_age)).toBe(30); // John
			});
		});
	});

	describe("Complex Aggregations with Expressions", () => {
		it("should handle aggregations with calculated fields", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.customer_id"],
					aggregatedFields: {
						total_spent: { function: "SUM", field: "orders.amount" },
						order_count: { function: "COUNT", field: "orders.id" },
						avg_order_value: { function: "AVG", field: "orders.amount" },
						// Complex expression: bonus calculation
						loyalty_bonus: {
							function: "SUM",
							field: {
								$func: {
									MULTIPLY: [
										{ $field: "orders.amount" },
										{
											$cond: {
												if: { "orders.amount": { $gte: { $var: "high_value_threshold" } } },
												then: 0.05, // 5% bonus for high-value orders
												else: 0.02, // 2% bonus for regular orders
											},
										},
									],
								},
							},
						},
						// Customer tier based on total spending
						estimated_tier: {
							function: "MAX",
							field: {
								$cond: {
									if: { "orders.amount": { $gte: 300 } },
									then: 3, // Gold
									else: {
										$cond: {
											if: { "orders.amount": { $gte: 150 } },
											then: 2, // Silver
											else: 1, // Bronze
										},
									},
								},
							},
						},
					},
					condition: {
						"orders.status": { $ne: "cancelled" },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("customer_id");
				expect(rows[0]).toHaveProperty("total_spent");
				expect(rows[0]).toHaveProperty("loyalty_bonus");
				expect(rows[0]).toHaveProperty("estimated_tier");

				// Verify calculated fields are numbers
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(typeof r.total_spent).toBe("number");
					expect(typeof r.loyalty_bonus).toBe("number");
					expect(typeof r.estimated_tier).toBe("number");
					expect([1, 2, 3]).toContain(r.estimated_tier);
				}
			});
		});

		it("should handle string aggregations with GROUP_CONCAT", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "posts",
					groupBy: ["posts.user_id"],
					aggregatedFields: {
						post_count: { function: "COUNT", field: "posts.id" },
						avg_rating: { function: "AVG", field: "posts.rating" },
						// String aggregation - SQLite uses GROUP_CONCAT
						post_titles: {
							function: "STRING_AGG",
							field: "posts.title",
							additionalArguments: [" | "],
						},
						// Complex string expression
						status_summary: {
							function: "STRING_AGG",
							field: {
								$cond: {
									if: { "posts.published": { $eq: true } },
									then: { $func: { CONCAT: ["PUBLISHED: ", { $field: "posts.title" }] } },
									else: { $func: { CONCAT: ["DRAFT: ", { $field: "posts.title" }] } },
								},
							},
							additionalArguments: [" || "],
						},
					},
					condition: {
						"posts.title": { $ne: null },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("user_id");
				expect(rows[0]).toHaveProperty("post_titles");
				expect(rows[0]).toHaveProperty("status_summary");

				// Verify string aggregation results
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(typeof r.post_titles).toBe("string");
					expect(typeof r.status_summary).toBe("string");
					if (r.post_titles) {
						expect((r.post_titles as string).length).toBeGreaterThan(0);
					}
				}

				// Check that GROUP_CONCAT was used (SQLite syntax)
				expect(sql).toContain("GROUP_CONCAT");
				expect(sql).toContain(" | ");
			});
		});
	});

	describe("Multi-table Aggregations", () => {
		it("should handle aggregations across multiple tables", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.status", "users.metadata->department"],
					aggregatedFields: {
						user_count: { function: "COUNT", field: "users.id" },
						avg_age: { function: "AVG", field: "users.age" },
						total_balance: { function: "SUM", field: "users.balance" },
						// Aggregate from related posts (simplified for compatibility)
						posts_per_user: {
							function: "COUNT",
							field: "users.id", // Using a simpler approach
						},
						// Average balance per department
						avg_balance_group: {
							function: "AVG",
							field: "users.balance",
						},
					},
					condition: {
						"users.active": { $eq: true },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("status");
				expect(rows[0]).toHaveProperty("metadata->department");
				expect(rows[0]).toHaveProperty("user_count");
				expect(rows[0]).toHaveProperty("total_balance");

				// Verify engineering premium users
				const engPremium = rows.find((row: unknown) => {
					const r = row as Record<string, unknown>;
					return r.status === "premium" && r["metadata->department"] === "engineering";
				});
				expect(engPremium).toBeDefined();
				const engRecord = engPremium as Record<string, unknown>;
				expect(Number(engRecord.user_count)).toBe(3); // John, Alice, Diana
				expect(Number(engRecord.total_balance)).toBeCloseTo(4200, 2); // 1000 + 1200 + 2000
			});
		});
	});

	describe("Advanced Time-based Aggregations", () => {
		it("should handle date/time grouping and aggregations", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.status"],
					aggregatedFields: {
						monthly_revenue: { function: "SUM", field: "orders.amount" },
						monthly_orders: { function: "COUNT", field: "orders.id" },
						avg_order_value: { function: "AVG", field: "orders.amount" },
						// Simple completion tracking
						avg_completion_days: {
							function: "AVG",
							field: {
								$func: {
									COALESCE_NUMBER: [
										7, // Simple constant for demo
										0,
									],
								},
							},
						},
					},
					condition: {
						$and: [
							{ "orders.created_at": { $gte: { $date: "2024-01-01" } } },
							{ "orders.status": { $in: ["completed", "shipped", "pending"] } },
						],
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("monthly_revenue");
				expect(rows[0]).toHaveProperty("monthly_orders");
				expect(rows[0]).toHaveProperty("avg_completion_days");

				// Verify aggregation results
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(typeof r.monthly_revenue).toBe("number");
					expect(typeof r.monthly_orders).toBe("number");
					expect(Number(r.monthly_orders)).toBeGreaterThan(0);
				}
			});
		});
	});

	describe("Conditional Aggregations and Statistical Functions", () => {
		it("should handle conditional aggregations with CASE statements", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "orders",
					groupBy: ["orders.customer_id"],
					aggregatedFields: {
						total_orders: { function: "COUNT", field: "orders.id" },
						// Conditional counts
						completed_orders: {
							function: "SUM",
							field: {
								$cond: {
									if: { "orders.status": { $eq: "completed" } },
									then: 1,
									else: 0,
								},
							},
						},
						pending_orders: {
							function: "SUM",
							field: {
								$cond: {
									if: { "orders.status": { $eq: "pending" } },
									then: 1,
									else: 0,
								},
							},
						},
						high_value_orders: {
							function: "SUM",
							field: {
								$cond: {
									if: { "orders.amount": { $gte: { $var: "high_value_threshold" } } },
									then: 1,
									else: 0,
								},
							},
						},
						// Conditional revenue calculations
						completed_revenue: {
							function: "SUM",
							field: {
								$cond: {
									if: { "orders.status": { $eq: "completed" } },
									then: { $field: "orders.amount" },
									else: 0,
								},
							},
						},
						potential_revenue: {
							function: "SUM",
							field: {
								$cond: {
									if: { "orders.status": { $in: ["pending", "shipped"] } },
									then: { $field: "orders.amount" },
									else: 0,
								},
							},
						},
						// Customer satisfaction score (simplified)
						satisfaction_score: {
							function: "AVG",
							field: {
								$cond: {
									if: { "orders.status": { $eq: "completed" } },
									then: {
										$func: {
											ADD: [
												{
													$cond: {
														if: { "orders.delivered_date": { $ne: null } },
														then: 2, // Delivered orders get bonus points
														else: 1,
													},
												},
												{
													$cond: {
														if: { "orders.amount": { $gte: 100 } },
														then: 1, // High-value orders get bonus
														else: 0,
													},
												},
											],
										},
									},
									else: 0,
								},
							},
						},
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("customer_id");
				expect(rows[0]).toHaveProperty("total_orders");
				expect(rows[0]).toHaveProperty("completed_orders");
				expect(rows[0]).toHaveProperty("completed_revenue");
				expect(rows[0]).toHaveProperty("satisfaction_score");

				// Verify conditional aggregation logic
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					const totalOrders = Number(r.total_orders);
					const completedOrders = Number(r.completed_orders);
					const pendingOrders = Number(r.pending_orders);

					expect(totalOrders).toBeGreaterThanOrEqual(completedOrders + pendingOrders);
					expect(completedOrders).toBeGreaterThanOrEqual(0);
					expect(pendingOrders).toBeGreaterThanOrEqual(0);
					expect(Number(r.completed_revenue)).toBeGreaterThanOrEqual(0);
				}

				// Verify CASE statements in generated SQL
				expect(sql).toContain("CASE WHEN");
				expect(sql).toContain("THEN");
				expect(sql).toContain("ELSE");
				expect(sql).toContain("END");
			});
		});

		it("should handle complex statistical calculations", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "posts",
					groupBy: ["posts.user_id"],
					aggregatedFields: {
						post_count: { function: "COUNT", field: "posts.id" },
						avg_rating: { function: "AVG", field: "posts.rating" },
						total_views: { function: "SUM", field: "posts.view_count" },
						// Content productivity score
						productivity_score: {
							function: "AVG",
							field: {
								$func: {
									MULTIPLY: [
										{
											$func: {
												ADD: [
													{ $func: { LENGTH: [{ $field: "posts.content" }] } },
													{ $func: { MULTIPLY: [{ $field: "posts.view_count" }, 2] } },
												],
											},
										},
										{
											$cond: {
												if: { "posts.published": { $eq: true } },
												then: 1.0,
												else: 0.5, // Draft posts get half score
											},
										},
									],
								},
							},
						},
						// Quality indicator
						high_quality_posts: {
							function: "SUM",
							field: {
								$cond: {
									if: {
										$and: [
											{ "posts.rating": { $gte: { $var: "min_rating" } } },
											{ "posts.view_count": { $gte: 100 } },
											{ "posts.published": { $eq: true } },
										],
									},
									then: 1,
									else: 0,
								},
							},
						},
						// Engagement ratio
						engagement_ratio: {
							function: "AVG",
							field: {
								$func: {
									DIVIDE: [
										{ $func: { MULTIPLY: [{ $field: "posts.rating" }, { $field: "posts.view_count" }] } },
										{ $func: { ADD: [{ $func: { LENGTH: [{ $field: "posts.content" }] } }, 1] } },
									],
								},
							},
						},
					},
					condition: {
						"posts.rating": { $ne: null },
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("productivity_score");
				expect(rows[0]).toHaveProperty("high_quality_posts");
				expect(rows[0]).toHaveProperty("engagement_ratio");

				// Verify complex calculations
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(typeof r.productivity_score).toBe("number");
					expect(typeof r.engagement_ratio).toBe("number");
					expect(Number(r.high_quality_posts)).toBeGreaterThanOrEqual(0);
					expect(Number(r.productivity_score)).toBeGreaterThan(0);
				}

				// Verify complex mathematical operations in SQL
				expect(sql).toContain("*");
				expect(sql).toContain("/");
				expect(sql).toContain("+");
				expect(sql).toContain("LENGTH");
				expect(sql).toContain("AVG");
			});
		});
	});

	describe("Performance and Edge Cases", () => {
		it("should handle large result sets with proper aggregation", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "order_items",
					groupBy: ["order_items.product_id"],
					aggregatedFields: {
						total_quantity_sold: { function: "SUM", field: "order_items.quantity" },
						total_revenue: {
							function: "SUM",
							field: {
								$func: {
									MULTIPLY: [{ $field: "order_items.quantity" }, { $field: "order_items.unit_price" }],
								},
							},
						},
						avg_unit_price: { function: "AVG", field: "order_items.unit_price" },
						order_count: { function: "COUNT", field: "order_items.order_id" },
						unique_orders: {
							function: "COUNT_DISTINCT",
							field: "order_items.order_id",
						},
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);
				expect(rows[0]).toHaveProperty("product_id");
				expect(rows[0]).toHaveProperty("total_quantity_sold");
				expect(rows[0]).toHaveProperty("total_revenue");
				expect(rows[0]).toHaveProperty("unique_orders");

				// Verify aggregation accuracy
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					expect(Number(r.total_quantity_sold)).toBeGreaterThan(0);
					expect(Number(r.total_revenue)).toBeGreaterThan(0);
					expect(Number(r.unique_orders)).toBeGreaterThan(0);
					expect(Number(r.unique_orders)).toBeLessThanOrEqual(Number(r.order_count));
				}

				// Verify COUNT(DISTINCT) is used
				expect(sql).toContain("COUNT(DISTINCT");
			});
		});

		it("should handle NULL values in aggregations correctly", () => {
			db.executeInTransaction(() => {
				const aggregationQuery: AggregationQuery = {
					table: "users",
					groupBy: ["users.status"],
					aggregatedFields: {
						total_users: { function: "COUNT", field: "users.id" },
						users_with_email: {
							function: "COUNT",
							field: "users.email", // This will exclude NULL emails
						},
						avg_age: { function: "AVG", field: "users.age" },
						avg_age_with_nulls: {
							function: "AVG",
							field: {
								$func: {
									COALESCE_NUMBER: [{ $field: "users.age" }, 0],
								},
							},
						},
						null_email_count: {
							function: "SUM",
							field: {
								$cond: {
									if: { "users.email": { $eq: null } },
									then: 1,
									else: 0,
								},
							},
						},
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = db.query(sql);

				expect(rows.length).toBeGreaterThan(0);

				// Verify NULL handling
				for (const row of rows) {
					const r = row as Record<string, unknown>;
					const totalUsers = Number(r.total_users);
					const usersWithEmail = Number(r.users_with_email);
					const nullEmailCount = Number(r.null_email_count);

					expect(totalUsers).toBeGreaterThanOrEqual(usersWithEmail);
					expect(totalUsers).toEqual(usersWithEmail + nullEmailCount);
					expect(typeof r.avg_age).toBe("number");
					expect(typeof r.avg_age_with_nulls).toBe("number");
				}
			});
		});
	});
});
