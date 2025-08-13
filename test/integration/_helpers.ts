import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";

type DatabaseConfig = { host: string; port: number; database: string; user: string; password: string };
type ProcessResult = { code: number | null; signal: string | null; stdout?: string; stderr?: string };

const config: DatabaseConfig = {
	host: process.env.POSTGRES_HOST || "localhost",
	port: Number(process.env.POSTGRES_PORT) || 5432,
	database: process.env.POSTGRES_DB || "json_sql_parser_test",
	user: process.env.POSTGRES_USER || "testuser",
	password: process.env.POSTGRES_PASSWORD || "testpass",
};

function runCommand(
	command: string,
	args: string[] = [],
	options: { cwd?: string; silent?: boolean } = {},
): Promise<ProcessResult> {
	return new Promise((resolve) => {
		const proc = spawn(command, args, {
			stdio: options.silent ? "pipe" : "inherit",
			cwd: options.cwd || process.cwd(),
		});

		let stdout = "";
		let stderr = "";

		if (options.silent) {
			proc.stdout?.on("data", (data) => {
				stdout += data.toString();
			});

			proc.stderr?.on("data", (data) => {
				stderr += data.toString();
			});
		}

		proc.on("close", (code, signal) => {
			resolve({
				code,
				signal,
				stdout: options.silent ? stdout : undefined,
				stderr: options.silent ? stderr : undefined,
			});
		});
	});
}

async function checkDockerPrerequisites(): Promise<void> {
	// Check if Docker is available
	try {
		const dockerResult = await runCommand("docker", ["--version"], { silent: true });
		if (dockerResult.code !== 0) {
			throw new Error("Docker is not available");
		}
	} catch {
		throw new Error("Docker is not installed or not running. Please install Docker and ensure it's running.");
	}

	// Check if Docker Compose is available
	try {
		const composeResult = await runCommand("docker", ["compose", "version"], { silent: true });
		if (composeResult.code !== 0) {
			throw new Error("Docker Compose is not available");
		}
	} catch {
		throw new Error("Docker Compose is not available. Please ensure Docker Compose is installed.");
	}
}

async function isDockerComposeRunning(): Promise<boolean> {
	try {
		// Check if the specific postgres service is running
		const result = await runCommand("docker", ["compose", "ps", "postgres", "--format", "json"], { silent: true });

		if (result.code !== 0) {
			return false;
		}

		// Parse the JSON output to check if postgres service is running
		const output = result.stdout || "";
		if (!output.trim()) {
			return false;
		}

		try {
			const containerInfo = JSON.parse(output);
			return containerInfo.State === "running";
		} catch {
			// Fallback: check if output contains "running"
			return output.includes("running");
		}
	} catch {
		return false;
	}
}

async function ensureDockerComposeUp(): Promise<void> {
	// Check if already running
	const isRunning = await isDockerComposeRunning();

	if (isRunning) {
		console.log("✅ PostgreSQL container is already running");
		return;
	}

	// Start Docker Compose
	console.log("🚀 Starting PostgreSQL container...");
	const composeUpResult = await runCommand("docker", ["compose", "up", "-d"]);

	if (composeUpResult.code !== 0) {
		throw new Error("Failed to start PostgreSQL container");
	}

	// Verify it's actually running
	const isNowRunning = await isDockerComposeRunning();
	if (!isNowRunning) {
		throw new Error("PostgreSQL container failed to start properly");
	}

	console.log("✅ PostgreSQL container started successfully");
}

async function waitForPostgres(maxAttempts = 30, delayMs = 1000): Promise<void> {
	console.log("⏳ Waiting for PostgreSQL to be ready...");

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		const client = new Client(config);

		try {
			await client.connect();
			await client.query("SELECT 1");
			await client.end();

			console.log(`✅ PostgreSQL is ready! (attempt ${attempt}/${maxAttempts})`);
			return;
		} catch {
			try {
				await client.end();
			} catch {
				// Ignore errors when closing failed connection
			}

			if (attempt === maxAttempts) {
				throw new Error(`PostgreSQL not ready after ${maxAttempts} attempts`);
			}

			await new Promise((resolve) => setTimeout(resolve, delayMs));
		}
	}
}

async function checkDatabaseSeeded(): Promise<boolean> {
	const client = new Client(config);

	try {
		await client.connect();

		// Check if the main tables exist and have data
		const tablesExist = await client.query(`
			SELECT COUNT(*) as table_count 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name IN ('users', 'posts', 'orders', 'data_storage')
		`);

		if (Number(tablesExist.rows[0].table_count) < 4) {
			return false;
		}

		// Check if tables have data
		const userCount = await client.query("SELECT COUNT(*) FROM users");
		const postCount = await client.query("SELECT COUNT(*) FROM posts");
		const orderCount = await client.query("SELECT COUNT(*) FROM orders");
		const dataStorageCount = await client.query("SELECT COUNT(*) FROM data_storage");

		return (
			Number(userCount.rows[0].count) >= 5 &&
			Number(postCount.rows[0].count) >= 5 &&
			Number(orderCount.rows[0].count) >= 8 &&
			Number(dataStorageCount.rows[0].count) >= 15
		);
	} catch {
		return false;
	} finally {
		await client.end();
	}
}

async function seedDatabase(): Promise<void> {
	console.log("🌱 Seeding database...");

	const client = new Client(config);

	try {
		await client.connect();

		// Read and execute the init.sql file
		const initSqlPath = join(dirname(fileURLToPath(import.meta.url)), "../sql/init.sql");
		const initSql = readFileSync(initSqlPath, "utf-8");

		// Execute the SQL (splitting by statements and filtering out empty ones)
		const statements = initSql
			.split(";")
			.map((stmt) => stmt.trim())
			.filter((stmt) => stmt.length > 0 && !stmt.startsWith("--"));

		for (const statement of statements) {
			if (statement.trim()) {
				await client.query(statement);
			}
		}

		console.log("✅ Database seeded successfully!");
	} catch (error) {
		console.error("❌ Failed to seed database:", error);
		throw error;
	} finally {
		await client.end();
	}
}

/**
 * Sets up the Docker environment and database for integration tests
 * This should be called in beforeAll hooks in integration test files
 */
export async function setupTestEnvironment(): Promise<void> {
	console.log("🔧 Setting up test environment...");

	// Check Docker prerequisites
	await checkDockerPrerequisites();

	// Ensure PostgreSQL with Docker Compose is running
	await ensureDockerComposeUp();

	// Wait for PostgreSQL server to be ready
	await waitForPostgres();

	// Check if database is already seeded
	const isAlreadySeeded = await checkDatabaseSeeded();
	if (isAlreadySeeded) {
		console.log("✅ Database is already seeded!");
	} else {
		// Seed the database
		await seedDatabase();
	}

	console.log("✅ Test environment setup complete!");
}

/**
 * Cleanup function for tests (optional - containers can be left running)
 */
export async function teardownTestEnvironment(): Promise<void> {
	console.log("🧹 Cleaning up test environment...");
	await runCommand("docker", ["compose", "down"]);
	console.log("✅ Test environment cleaned up!");
}

export class DatabaseHelper {
	private client: Client;
	private config: DatabaseConfig;

	constructor(config?: Partial<DatabaseConfig>) {
		this.config = {
			host: process.env.POSTGRES_HOST || "localhost",
			port: Number(process.env.POSTGRES_PORT) || 5432,
			database: process.env.POSTGRES_DB || "json_sql_parser_test",
			user: process.env.POSTGRES_USER || "testuser",
			password: process.env.POSTGRES_PASSWORD || "testpass",
			...config,
		};

		this.client = new Client(this.config);
	}

	async connect(): Promise<void> {
		let attempts = 0;
		const maxAttempts = 10;
		const delayMs = 1000;

		while (attempts < maxAttempts) {
			try {
				await this.client.connect();
				// Test the connection
				await this.client.query("SELECT 1");
				return;
			} catch (error) {
				attempts++;
				console.log(
					`Database connection attempt ${attempts}/${maxAttempts} failed:`,
					error instanceof Error ? error.message : String(error),
				);

				if (attempts >= maxAttempts) {
					throw new Error(`Failed to connect to database after ${maxAttempts} attempts`);
				}

				// Wait before retrying
				await new Promise((resolve) => setTimeout(resolve, delayMs));

				// Create a new client for the next attempt
				this.client = new Client(this.config);
			}
		}
	}

	async disconnect(): Promise<void> {
		await this.client.end();
	}

	async query(sql: string, params?: unknown[]): Promise<unknown[]> {
		const result = await this.client.query(sql, params);
		return result.rows;
	}

	async beginTransaction(): Promise<void> {
		await this.client.query("BEGIN");
	}

	async rollback(): Promise<void> {
		await this.client.query("ROLLBACK");
	}

	async commit(): Promise<void> {
		await this.client.query("COMMIT");
	}

	/**
	 * Execute a query within a transaction that automatically rolls back
	 * This ensures test isolation without affecting the database state
	 */
	async executeInTransaction<T>(fn: (helper: DatabaseHelper) => Promise<T>): Promise<T> {
		await this.beginTransaction();
		try {
			const result = await fn(this);
			return result;
		} finally {
			await this.rollback();
		}
	}

	/**
	 * Check if the database is properly seeded
	 */
	async isSeeded(): Promise<boolean> {
		try {
			const userCount = await this.client.query("SELECT COUNT(*) FROM users");
			const postCount = await this.client.query("SELECT COUNT(*) FROM posts");
			const orderCount = await this.client.query("SELECT COUNT(*) FROM orders");
			const dataStorageCount = await this.client.query("SELECT COUNT(*) FROM data_storage");

			return (
				Number(userCount.rows[0].count) >= 5 &&
				Number(postCount.rows[0].count) >= 5 &&
				Number(orderCount.rows[0].count) >= 8 &&
				Number(dataStorageCount.rows[0].count) >= 15
			);
		} catch {
			return false;
		}
	}

	/**
	 * Reset database to initial state by truncating and re-seeding
	 * Only use this in tests that specifically need a clean slate
	 */
	async resetDatabase(): Promise<void> {
		await this.client.query("TRUNCATE users, posts, orders, data_storage RESTART IDENTITY CASCADE");

		// Re-insert initial data
		const initSql = `
			-- Insert sample data for regular tables
			INSERT INTO users (name, email, age, active, status, metadata) VALUES
			('John Doe', 'john@example.com', 30, true, 'premium', '{"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}'),
			('Jane Smith', 'jane@example.com', 25, true, 'active', '{"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}'),
			('Bob Johnson', 'bob@example.com', 35, false, 'inactive', '{"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}'),
			('Alice Brown', 'alice@example.com', 28, true, 'premium', '{"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}'),
			('Charlie Wilson', null, 32, true, 'active', '{"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}');

			INSERT INTO posts (title, content, user_id, published, tags) VALUES
			('Getting Started with PostgreSQL', 'This is a comprehensive guide to PostgreSQL...', 1, true, '["database", "postgresql", "tutorial"]'),
			('Advanced SQL Queries', 'Learn advanced SQL techniques...', 1, true, '["sql", "advanced", "database"]'),
			('Marketing Strategies 2024', 'The latest marketing trends...', 2, true, '["marketing", "trends", "2024"]'),
			('Team Building Activities', 'Effective team building exercises...', 2, false, '["teamwork", "management", "hr"]'),
			('Sales Techniques', 'How to close more deals...', 3, false, '["sales", "techniques", "business"]');

			INSERT INTO orders (amount, status, customer_id) VALUES
			(299.99, 'completed', 1),
			(149.50, 'shipped', 1),
			(89.99, 'pending', 2),
			(199.99, 'completed', 2),
			(59.99, 'cancelled', 3),
			(399.99, 'completed', 4),
			(79.99, 'pending', 4),
			(249.99, 'shipped', 1);

			-- Insert equivalent data into data_storage table
			INSERT INTO data_storage (table_name, tenant_id, data) VALUES
			('users', 'current_tenant', '{"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}}'),
			('users', 'current_tenant', '{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25, "active": true, "status": "active", "metadata": {"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}}'),
			('users', 'current_tenant', '{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35, "active": false, "status": "inactive", "metadata": {"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}}'),
			('users', 'current_tenant', '{"id": 4, "name": "Alice Brown", "email": "alice@example.com", "age": 28, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}}'),
			('users', 'current_tenant', '{"id": 5, "name": "Charlie Wilson", "email": null, "age": 32, "active": true, "status": "active", "metadata": {"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}}'),

			('orders', 'current_tenant', '{"id": 1, "amount": 299.99, "status": "completed", "customer_id": 1}'),
			('orders', 'current_tenant', '{"id": 2, "amount": 149.50, "status": "shipped", "customer_id": 1}'),
			('orders', 'current_tenant', '{"id": 3, "amount": 89.99, "status": "pending", "customer_id": 2}'),
			('orders', 'current_tenant', '{"id": 4, "amount": 199.99, "status": "completed", "customer_id": 2}'),
			('orders', 'current_tenant', '{"id": 5, "amount": 59.99, "status": "cancelled", "customer_id": 3}'),
			('orders', 'current_tenant', '{"id": 6, "amount": 399.99, "status": "completed", "customer_id": 4}'),
			('orders', 'current_tenant', '{"id": 7, "amount": 79.99, "status": "pending", "customer_id": 4}'),
			('orders', 'current_tenant', '{"id": 8, "amount": 249.99, "status": "shipped", "customer_id": 1}');
		`;

		await this.client.query(initSql);
	}
}
