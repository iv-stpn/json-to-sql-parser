import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "pg";
import { getErrorMessage } from "../../src/utils";

type DatabaseConfig = { host: string; port: number; database: string; user: string; password: string };
type ProcessResult = { code: number | null; signal: string | null; stdout?: string; stderr?: string };

const config: DatabaseConfig = {
	host: process.env.POSTGRES_HOST || "localhost",
	port: Number(process.env.POSTGRES_PORT) || 5432,
	database: process.env.POSTGRES_DB || "json_sql_parser_test",
	user: process.env.POSTGRES_USER || "testuser",
	password: process.env.POSTGRES_PASSWORD || "testpass",
};

type RunCommandOptions = { cwd?: string; silent?: boolean };
function runCommand(command: string, args: string[] = [], options: RunCommandOptions = {}): Promise<ProcessResult> {
	return new Promise((resolve) => {
		const proc = spawn(command, args, { stdio: options.silent ? "pipe" : "inherit", cwd: options.cwd || process.cwd() });

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
			resolve({ code, signal, stdout: options.silent ? stdout : undefined, stderr: options.silent ? stderr : undefined });
		});
	});
}

async function checkDockerPrerequisites(): Promise<void> {
	// Check if Docker is available
	try {
		const dockerResult = await runCommand("docker", ["--version"], { silent: true });
		if (dockerResult.code !== 0) throw new Error("Docker is not available");
	} catch {
		throw new Error("Docker is not installed or not running. Please install Docker and ensure it's running.");
	}

	// Check if Docker Compose is available
	try {
		const composeResult = await runCommand("docker", ["compose", "version"], { silent: true });
		if (composeResult.code !== 0) throw new Error("Docker Compose is not available");
	} catch {
		throw new Error("Docker Compose is not available. Please ensure Docker Compose is installed.");
	}
}

async function isDockerComposeRunning(): Promise<boolean> {
	try {
		// Check if the specific postgres service is running
		const result = await runCommand("docker", ["compose", "ps", "postgres", "--format", "json"], { silent: true });
		if (result.code !== 0) return false;

		// Parse the JSON output to check if postgres service is running
		const output = result.stdout || "";
		if (!output.trim()) return false;

		try {
			const containerInfo = JSON.parse(output);
			return containerInfo.State === "running";
		} catch (error) {
			console.warn("Failed to parse Docker Compose output:", getErrorMessage(error));
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
	await new Promise((resolve) => setTimeout(resolve, 1500));

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		const client = new Client(config);

		try {
			await client.connect();
			await client.query("SELECT 1");
			await client.end();

			console.log(`✅ PostgreSQL is ready! (attempt ${attempt}/${maxAttempts})`);
			return;
		} catch (error) {
			console.error(`❌ PostgreSQL connection attempt ${attempt}/${maxAttempts} failed:`);
			console.error(getErrorMessage(error));
			try {
				await client.end();
			} catch {
				// Ignore errors when closing failed connection
			}

			if (attempt === maxAttempts) {
				console.error("📋 Container logs for debugging:");
				const logs = await getContainerLogs("postgres", 15);
				console.log(logs);
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
		console.error("❌ Failed to seed database:", getErrorMessage(error));
		console.error("📋 Container logs for debugging:");
		const logs = await getContainerLogs("postgres", 15);
		console.log(logs);
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
		const maxAttempts = 5;
		const delayMs = 1000;

		while (attempts < maxAttempts) {
			try {
				await this.client.connect();
				// Test the connection
				await this.client.query("SELECT 1");
				return;
			} catch (error) {
				attempts++;
				console.error(`Database connection attempt ${attempts}/${maxAttempts} failed:`);
				console.error(getErrorMessage(error));

				console.error("📋 Container logs for debugging:");
				const logs = await getContainerLogs("postgres", 15);
				console.log(logs);

				if (attempts >= maxAttempts) throw new Error(`Failed to connect to database after ${maxAttempts} attempts`);

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
}

async function getContainerLogs(serviceName = "postgres", lines = 50): Promise<string> {
	try {
		const result = await runCommand("docker", ["compose", "logs", "--tail", lines.toString(), serviceName], { silent: true });
		if (result.code === 0 && result.stdout) return result.stdout;

		return result.stderr || "No logs available";
	} catch (error) {
		return `Failed to get container logs: ${getErrorMessage(error)}`;
	}
}
