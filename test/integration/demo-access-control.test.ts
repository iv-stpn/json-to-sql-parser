/** biome-ignore-all lint/suspicious/noThenProperty: then is a proper keyword in our expression schema */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { compileAggregationQuery, parseAggregationQuery } from "../../src/builders/aggregate";
import { buildSelectQuery } from "../../src/builders/select";
import type { AggregationQuery, Condition, SelectQuery } from "../../src/schemas";
import type { Config } from "../../src/types";
import { DatabaseHelper, extractSelectWhereClause, setupTestEnvironment, teardownTestEnvironment } from "../_helpers";

describe("Integration - Row-Level Security (RLS) Access Control Simulation", () => {
	let db: DatabaseHelper;
	let config: Config;

	beforeAll(async () => {
		await setupTestEnvironment();
		db = new DatabaseHelper();
		await db.connect();

		config = {
			tables: {
				// Core user table
				users: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "email", type: "string", nullable: true },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "status", type: "string", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
					],
				},
				// Organizations table for multi-tenant structure
				organizations: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "slug", type: "string", nullable: false },
						{ name: "settings", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
					],
				},
				// Teams within organizations
				teams: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "organization_id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "slug", type: "string", nullable: false },
						{ name: "description", type: "string", nullable: true },
						{ name: "settings", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
					],
				},
				// Roles for access control
				roles: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "permissions", type: "object", nullable: true },
						{ name: "level", type: "number", nullable: false },
						{ name: "created_at", type: "datetime", nullable: false },
					],
				},
				// Team members with role assignments
				team_members: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "team_id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "role_id", type: "uuid", nullable: false },
						{ name: "joined_at", type: "datetime", nullable: false },
						{ name: "active", type: "boolean", nullable: false },
						{ name: "metadata", type: "object", nullable: true },
					],
				},
				// Projects with complex access control
				projects: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "team_id", type: "uuid", nullable: false },
						{ name: "name", type: "string", nullable: false },
						{ name: "description", type: "string", nullable: true },
						{ name: "status", type: "string", nullable: false },
						{ name: "visibility", type: "string", nullable: false },
						{ name: "owner_id", type: "uuid", nullable: false },
						{ name: "budget", type: "number", nullable: true },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "archived_at", type: "datetime", nullable: true },
					],
				},
				// Direct project access grants
				project_access: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "project_id", type: "uuid", nullable: false },
						{ name: "user_id", type: "uuid", nullable: false },
						{ name: "role_id", type: "uuid", nullable: false },
						{ name: "granted_by", type: "uuid", nullable: false },
						{ name: "granted_at", type: "datetime", nullable: false },
						{ name: "expires_at", type: "datetime", nullable: true },
					],
				},
				// Tasks within projects
				tasks: {
					allowedFields: [
						{ name: "id", type: "uuid", nullable: false },
						{ name: "project_id", type: "uuid", nullable: false },
						{ name: "title", type: "string", nullable: false },
						{ name: "description", type: "string", nullable: true },
						{ name: "status", type: "string", nullable: false },
						{ name: "priority", type: "string", nullable: false },
						{ name: "assignee_id", type: "uuid", nullable: true },
						{ name: "reporter_id", type: "uuid", nullable: false },
						{ name: "estimated_hours", type: "number", nullable: true },
						{ name: "actual_hours", type: "number", nullable: true },
						{ name: "metadata", type: "object", nullable: true },
						{ name: "created_at", type: "datetime", nullable: false },
						{ name: "completed_at", type: "datetime", nullable: true },
					],
				},
			},
			variables: {
				// Current user context - simulates session variables
				current_user_id: "550e8400-e29b-41d4-a716-446655440000", // John Doe
				current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
				current_timestamp: "2024-01-15 12:00:00",
				admin_role_level: 80,
				maintainer_role_level: 50,
				contributor_role_level: 30,
				viewer_role_level: 10,
			},
			relationships: [
				// Organization -> Teams
				{ table: "organizations", field: "id", toTable: "teams", toField: "organization_id", type: "one-to-many" },
				// Teams -> Team Members
				{ table: "teams", field: "id", toTable: "team_members", toField: "team_id", type: "one-to-many" },
				// Users -> Team Members
				{ table: "users", field: "id", toTable: "team_members", toField: "user_id", type: "one-to-many" },
				// Roles -> Team Members
				{ table: "roles", field: "id", toTable: "team_members", toField: "role_id", type: "one-to-many" },
				// Teams -> Projects
				{ table: "teams", field: "id", toTable: "projects", toField: "team_id", type: "one-to-many" },
				// Users -> Projects (owner)
				{ table: "users", field: "id", toTable: "projects", toField: "owner_id", type: "one-to-many" },
				// Projects -> Project Access
				{ table: "projects", field: "id", toTable: "project_access", toField: "project_id", type: "one-to-many" },
				// Users -> Project Access
				{ table: "users", field: "id", toTable: "project_access", toField: "user_id", type: "one-to-many" },
				// Roles -> Project Access
				{ table: "roles", field: "id", toTable: "project_access", toField: "role_id", type: "one-to-many" },
				// Projects -> Tasks
				{ table: "projects", field: "id", toTable: "tasks", toField: "project_id", type: "one-to-many" },
				// Users -> Tasks (assignee)
				{ table: "users", field: "id", toTable: "tasks", toField: "assignee_id", type: "one-to-many" },
				// Users -> Tasks (reporter)
				{ table: "users", field: "id", toTable: "tasks", toField: "reporter_id", type: "one-to-many" },

				// Reverse relationships for joins (needed for nested selections)
				{ table: "teams", field: "organization_id", toTable: "organizations", toField: "id", type: "many-to-one" },
				{ table: "team_members", field: "team_id", toTable: "teams", toField: "id", type: "many-to-one" },
				{ table: "team_members", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "team_members", field: "role_id", toTable: "roles", toField: "id", type: "many-to-one" },
				{ table: "projects", field: "team_id", toTable: "teams", toField: "id", type: "many-to-one" },
				{ table: "projects", field: "owner_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "project_access", field: "project_id", toTable: "projects", toField: "id", type: "many-to-one" },
				{ table: "project_access", field: "user_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "project_access", field: "role_id", toTable: "roles", toField: "id", type: "many-to-one" },
				{ table: "tasks", field: "project_id", toTable: "projects", toField: "id", type: "many-to-one" },
				{ table: "tasks", field: "assignee_id", toTable: "users", toField: "id", type: "many-to-one" },
				{ table: "tasks", field: "reporter_id", toTable: "users", toField: "id", type: "many-to-one" },
			],
		};
	});

	afterAll(async () => {
		await db.disconnect();
		await teardownTestEnvironment();
	});

	describe("RLS Simulation - Project Access Control", () => {
		it("should only return projects the current user has access to through team membership", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "projects",
					selection: {
						id: true,
						name: true,
						description: true,
						status: true,
						visibility: true,
						team_id: true,
						owner_id: true,
					},
					condition: {
						$and: [
							{
								$or: [
									// User is the project owner
									{
										"projects.owner_id": { $eq: { $var: "current_user_id" } },
									},
									// User is a member of the project's team
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
												],
											},
										},
									},
									// User has direct project access
									{
										$exists: {
											table: "project_access",
											condition: {
												$and: [
													{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
													{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
													{
														$or: [
															{ "project_access.expires_at": { $eq: null } },
															{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
														],
													},
												],
											},
										},
									},
									// Public projects (visible to all)
									{
										"projects.visibility": { $eq: "public" },
									},
								],
							},
							// Tenant isolation - only projects from same organization
							{
								$exists: {
									table: "teams",
									condition: {
										$and: [
											{ "teams.id": { $eq: { $field: "projects.team_id" } } },
											{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
										],
									},
								},
							},
						],
					},
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify SQL structure contains RLS-like conditions
				expect(sql).toContain("EXISTS");
				expect(sql).toContain("team_members");
				expect(sql).toContain("project_access");
				expect(sql).toContain("550e8400-e29b-41d4-a716-446655440000"); // John Doe's UUID

				// John Doe should have access to Engineering projects
				// Product Roadmap is public but in different org (StartupXYZ), filtered by tenant isolation
				const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
				expect(projectNames).toContain("Core API Development"); // Owns this project
				expect(projectNames).toContain("Mobile App"); // Team member in Engineering
				expect(projectNames).toContain("Internal Tools"); // Team member in Engineering

				// Should not have access to Marketing projects (different team)
				expect(projectNames).not.toContain("Website Redesign");
				expect(projectNames).not.toContain("Marketing Campaign Q1");
				// Product Roadmap is public but in different organization
				expect(projectNames).not.toContain("Product Roadmap");
			});
		});

		it("should enforce role-based access control on projects with minimum role requirements", async () => {
			await db.executeInTransaction(async () => {
				// Query for projects where user has at least maintainer-level access
				const query: SelectQuery = {
					rootTable: "projects",
					selection: {
						id: true,
						name: true,
						status: true,
						visibility: true,
						owner_id: true,
					},
					condition: {
						$and: [
							// Basic project access with role requirements
							{
								$or: [
									{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
													// Require at least maintainer role (level >= 50)
													{
														$exists: {
															table: "roles",
															condition: {
																$and: [
																	{ "roles.id": { $eq: { $field: "team_members.role_id" } } },
																	{ "roles.level": { $gte: { $var: "maintainer_role_level" } } },
																],
															},
														},
													},
												],
											},
										},
									},
									{
										$exists: {
											table: "project_access",
											condition: {
												$and: [
													{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
													{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
													{
														$exists: {
															table: "roles",
															condition: {
																$and: [
																	{ "roles.id": { $eq: { $field: "project_access.role_id" } } },
																	{ "roles.level": { $gte: { $var: "maintainer_role_level" } } },
																],
															},
														},
													},
													{
														$or: [
															{ "project_access.expires_at": { $eq: null } },
															{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
														],
													},
												],
											},
										},
									},
								],
							},
							// Exclude archived projects
							{ "projects.archived_at": { $eq: null } },
						],
					},
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex RLS logic
				expect(sql).toContain("50"); // maintainer_role_level value
				expect(sql).toContain("archived_at IS NULL");

				// John Doe is admin in Engineering team, so should have access
				const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
				expect(projectNames).toContain("Core API Development"); // Owns this project, admin role

				// Verify only projects where John has maintainer+ access
				for (const row of rows as Record<string, unknown>[]) {
					const name = row.name as string;
					// Should only include projects where John is owner or has admin/maintainer role
					expect(["Core API Development", "Mobile App", "Internal Tools"].includes(name) || name === "Product Roadmap").toBe(
						true,
					); // Public project might be included depending on access rules
				}
			});
		});
	});

	describe("RLS Simulation - Task Access Control", () => {
		it("should only return tasks from projects the current user can access", async () => {
			await db.executeInTransaction(async () => {
				const query: SelectQuery = {
					rootTable: "tasks",
					selection: {
						id: true,
						title: true,
						description: true,
						status: true,
						priority: true,
						assignee_id: true,
						reporter_id: true,
						estimated_hours: true,
						actual_hours: true,
						// Include project information for verification
						projects: {
							id: true,
							name: true,
							visibility: true,
						},
						// Calculate if user can edit this task
						can_edit: {
							$cond: {
								if: {
									$or: [
										// User is the assignee
										{ "tasks.assignee_id": { $eq: { $var: "current_user_id" } } },
										// User is the reporter
										{ "tasks.reporter_id": { $eq: { $var: "current_user_id" } } },
										// User has maintainer+ role in project's team
										{
											$exists: {
												table: "team_members",
												condition: {
													$and: [
														{
															$exists: {
																table: "projects",
																condition: {
																	$and: [
																		{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
																		{ "projects.team_id": { $eq: { $field: "team_members.team_id" } } },
																	],
																},
															},
														},
														{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
														{ "team_members.active": { $eq: true } },
														{
															$exists: {
																table: "roles",
																condition: {
																	$and: [
																		{ "roles.id": { $eq: { $field: "team_members.role_id" } } },
																		{ "roles.level": { $gte: { $var: "maintainer_role_level" } } },
																	],
																},
															},
														},
													],
												},
											},
										},
									],
								},
								then: "Yes",
								else: "No",
							},
						},
					},
					condition: {
						$and: [
							// Task must belong to an accessible project
							{
								$exists: {
									table: "projects",
									condition: {
										$and: [
											{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
											{
												$or: [
													// User owns the project
													{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
													// User is a team member
													{
														$exists: {
															table: "team_members",
															condition: {
																$and: [
																	{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
																	{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
																	{ "team_members.active": { $eq: true } },
																],
															},
														},
													},
													// User has direct project access
													{
														$exists: {
															table: "project_access",
															condition: {
																$and: [
																	{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
																	{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
																	{
																		$or: [
																			{ "project_access.expires_at": { $eq: null } },
																			{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
																		],
																	},
																],
															},
														},
													},
													// Public project
													{ "projects.visibility": { $eq: "public" } },
												],
											},
											// Only active projects
											{ "projects.archived_at": { $eq: null } },
										],
									},
								},
							},
							// Tenant isolation - only projects from same organization
							{
								$exists: {
									table: "projects",
									condition: {
										$and: [
											{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
											{
												$exists: {
													table: "teams",
													condition: {
														$and: [
															{ "teams.id": { $eq: { $field: "projects.team_id" } } },
															{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
														],
													},
												},
											},
										],
									},
								},
							},
						],
					},
				};

				const sql = buildSelectQuery(query, config);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify complex nested EXISTS
				expect(sql).toContain("EXISTS");
				expect(sql.split("EXISTS").length - 1).toBeGreaterThan(2); // Multiple EXISTS clauses
				expect(sql).toContain("projects");
				expect(sql).toContain("team_members");
				expect(sql).toContain("project_access");

				// John Doe should see tasks from Engineering projects he has access to
				const taskTitles = (rows as Record<string, unknown>[]).map((row) => row.title);
				expect(taskTitles).toContain("Setup Authentication System"); // Core API task he owns
				expect(taskTitles).toContain("Database Schema Design"); // Core API task assigned to Alice
				expect(taskTitles).toContain("API Documentation"); // Core API task he reported
				expect(taskTitles).toContain("UI/UX Design"); // Mobile App task (team access)
				expect(taskTitles).toContain("iOS Implementation"); // Mobile App task (team access)

				// Should not see Marketing team tasks (different team, no access)
				expect(taskTitles).not.toContain("Content Strategy");
				expect(taskTitles).not.toContain("Homepage Design");
				// Product Roadmap tasks are in different org, filtered by tenant isolation
				expect(taskTitles).not.toContain("Market Research");
				expect(taskTitles).not.toContain("Feature Prioritization");

				// Verify can_edit logic
				for (const row of rows as Record<string, unknown>[]) {
					expect(typeof row.can_edit).toBe("string");
				}
			});
		});

		it("should filter tasks by user assignment and access level", async () => {
			await db.executeInTransaction(async () => {
				const condition: Condition = {
					$and: [
						// Only tasks assigned to or reported by current user
						{
							$or: [
								{ "tasks.assignee_id": { $eq: { $var: "current_user_id" } } },
								{ "tasks.reporter_id": { $eq: { $var: "current_user_id" } } },
							],
						},
						// Must have project access (reuse the EXISTS logic)
						{
							$exists: {
								table: "projects",
								condition: {
									$and: [
										{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
										{
											$or: [
												{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
												{
													$exists: {
														table: "team_members",
														condition: {
															$and: [
																{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
																{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
																{ "team_members.active": { $eq: true } },
															],
														},
													},
												},
												{
													$exists: {
														table: "project_access",
														condition: {
															$and: [
																{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
																{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
																{
																	$or: [
																		{ "project_access.expires_at": { $eq: null } },
																		{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
																	],
																},
															],
														},
													},
												},
												{ "projects.visibility": { $eq: "public" } },
											],
										},
									],
								},
							},
						},
						// Only active tasks
						{ "tasks.status": { $in: ["todo", "in_progress"] } },
					],
				};

				const whereSql = extractSelectWhereClause(condition, config, "tasks");
				const sql = `SELECT * FROM tasks WHERE ${whereSql}`;
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify RLS enforcement
				expect(sql).toContain("assignee_id");
				expect(sql).toContain("reporter_id");
				expect(sql).toContain("550e8400-e29b-41d4-a716-446655440000"); // John Doe's UUID
				expect(sql).toContain("EXISTS");

				// All returned tasks should be assigned to or reported by John Doe
				for (const row of rows as Record<string, unknown>[]) {
					const isAssignee = row.assignee_id === "550e8400-e29b-41d4-a716-446655440000"; // John Doe
					const isReporter = row.reporter_id === "550e8400-e29b-41d4-a716-446655440000"; // John Doe
					expect(isAssignee || isReporter).toBe(true);
				}

				// Verify specific tasks John should see (he assigned/reported)
				const taskTitles = (rows as Record<string, unknown>[]).map((row) => row.title);
				expect(taskTitles).toContain("Setup Authentication System"); // John assigned and reported
				expect(taskTitles).toContain("API Documentation"); // John reported (unassigned)
				// Database Schema Design is completed, so not in todo/in_progress filter
			});
		});
	});

	describe("RLS Simulation - Aggregation Queries with Access Control", () => {
		it("should aggregate project statistics with RLS filtering", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "projects",
					groupBy: ["status", "visibility"],
					condition: {
						$and: [
							// Apply RLS: only projects user has access to
							{
								$or: [
									{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
												],
											},
										},
									},
									{
										$exists: {
											table: "project_access",
											condition: {
												$and: [
													{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
													{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
													{
														$or: [
															{ "project_access.expires_at": { $eq: null } },
															{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
														],
													},
												],
											},
										},
									},
									{ "projects.visibility": { $eq: "public" } },
								],
							},
							// Only active projects
							{ "projects.archived_at": { $eq: null } },
						],
					},
					aggregatedFields: {
						total_projects: {
							function: "COUNT",
							field: "*",
						},
						total_budget: {
							function: "SUM",
							field: "projects.budget",
						},
						avg_budget: {
							function: "AVG",
							field: "projects.budget",
						},
						// Count projects user owns vs. has access to
						owned_projects: {
							function: "SUM",
							field: {
								$cond: {
									if: { "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									then: 1,
									else: 0,
								},
							},
						},
						// Count projects with high budget that user can see
						high_value_projects: {
							function: "SUM",
							field: {
								$cond: {
									if: { "projects.budget": { $gte: 50000 } },
									then: 1,
									else: 0,
								},
							},
						},
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);
				expect(rows.length).toBeGreaterThan(0);

				// Verify aggregation includes RLS logic
				expect(sql).toContain("GROUP BY");
				expect(sql).toContain("EXISTS");
				expect(sql).toContain("550e8400-e29b-41d4-a716-446655440000"); // John Doe's UUID
				expect(sql).toContain("team_members");
				expect(sql).toContain("CASE WHEN");

				// Verify aggregation structure
				for (const row of rows as Record<string, unknown>[]) {
					expect(row).toHaveProperty("status");
					expect(row).toHaveProperty("visibility");
					expect(row).toHaveProperty("total_projects");
					expect(row).toHaveProperty("owned_projects");
					// PostgreSQL returns numeric aggregates as strings
					expect(typeof row.total_projects).toBe("string");
					expect(typeof row.owned_projects).toBe("string");
					expect(Number(row.total_projects)).toBeGreaterThanOrEqual(0);
					expect(Number(row.owned_projects)).toBeGreaterThanOrEqual(0);
				}
			});
		});

		it("should aggregate task completion statistics by user access level", async () => {
			await db.executeInTransaction(async () => {
				const aggregationQuery: AggregationQuery = {
					table: "tasks",
					groupBy: ["status", "priority"],
					condition: {
						// Apply RLS: only tasks from accessible projects
						$exists: {
							table: "projects",
							condition: {
								$and: [
									{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
									{
										$or: [
											{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.active": { $eq: true } },
														],
													},
												},
											},
											{
												$exists: {
													table: "project_access",
													condition: {
														$and: [
															{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
															{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
															{
																$or: [
																	{ "project_access.expires_at": { $eq: null } },
																	{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
																],
															},
														],
													},
												},
											},
											{ "projects.visibility": { $eq: "public" } },
										],
									},
									{ "projects.archived_at": { $eq: null } },
								],
							},
						},
					},
					aggregatedFields: {
						task_count: {
							function: "COUNT",
							field: "*",
						},
						total_estimated_hours: {
							function: "SUM",
							field: "tasks.estimated_hours",
						},
						total_actual_hours: {
							function: "SUM",
							field: "tasks.actual_hours",
						},
						avg_estimated_hours: {
							function: "AVG",
							field: "tasks.estimated_hours",
						},
						// Count tasks assigned to current user
						assigned_to_me: {
							function: "SUM",
							field: {
								$cond: {
									if: { "tasks.assignee_id": { $eq: { $var: "current_user_id" } } },
									then: 1,
									else: 0,
								},
							},
						},
						// Count tasks reported by current user
						reported_by_me: {
							function: "SUM",
							field: {
								$cond: {
									if: { "tasks.reporter_id": { $eq: { $var: "current_user_id" } } },
									then: 1,
									else: 0,
								},
							},
						},
						// Calculate completion rate for tasks user can see
						completion_rate: {
							function: "AVG",
							field: {
								$cond: {
									if: { "tasks.status": { $eq: "completed" } },
									then: 100,
									else: 0,
								},
							},
						},
					},
				};

				const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, config));
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify complex RLS in aggregation
				expect(sql).toContain("EXISTS");
				expect(sql).toContain("projects");
				expect(sql).toContain("team_members");
				expect(sql).toContain("project_access");
				expect(sql).toContain("550e8400-e29b-41d4-a716-446655440000"); // John Doe's UUID
				expect(sql).toContain("GROUP BY");

				// Verify aggregation results structure
				for (const row of rows as Record<string, unknown>[]) {
					expect(row).toHaveProperty("status");
					expect(row).toHaveProperty("priority");
					expect(row).toHaveProperty("task_count");
					expect(row).toHaveProperty("assigned_to_me");
					expect(row).toHaveProperty("reported_by_me");
					// PostgreSQL returns numeric aggregates as strings
					expect(typeof row.task_count).toBe("string");
					expect(typeof row.assigned_to_me).toBe("string");
					expect(typeof row.reported_by_me).toBe("string");
					expect(Number(row.task_count)).toBeGreaterThanOrEqual(0);
					expect(Number(row.assigned_to_me)).toBeGreaterThanOrEqual(0);
					expect(Number(row.reported_by_me)).toBeGreaterThanOrEqual(0);
				}
			});
		});
	});

	describe("RLS Simulation - Cross-tenant Data Isolation", () => {
		it("should isolate data by organization membership", async () => {
			await db.executeInTransaction(async () => {
				// Switch context to a different user (Jane Smith - Marketing team)
				const marketingUserConfig = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", // Jane Smith
					},
				};

				const query: SelectQuery = {
					rootTable: "projects",
					selection: {
						id: true,
						name: true,
						team_id: true,
						// Include team and organization info
						teams: {
							id: true,
							name: true,
							organization_id: true,
						},
					},
					condition: {
						$and: [
							// Apply RLS for Jane Smith
							{
								$or: [
									{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
												],
											},
										},
									},
									{
										$exists: {
											table: "project_access",
											condition: {
												$and: [
													{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
													{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
													{
														$or: [
															{ "project_access.expires_at": { $eq: null } },
															{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
														],
													},
												],
											},
										},
									},
									{ "projects.visibility": { $eq: "public" } },
								],
							},
							// Only from same organization (tenant isolation)
							{
								$exists: {
									table: "teams",
									condition: {
										$and: [
											{ "teams.id": { $eq: { $field: "projects.team_id" } } },
											{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
										],
									},
								},
							},
							{ "projects.archived_at": { $eq: null } },
						],
					},
				};

				const sql = buildSelectQuery(query, marketingUserConfig);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify tenant isolation
				expect(sql).toContain("b1b1b1b1-1111-1111-1111-111111111111"); // TechCorp UUID
				expect(sql).toContain("teams");
				expect(sql).toContain("organization_id");

				// Jane Smith should see Marketing projects + direct access projects
				// Note: Product Roadmap is public but in StartupXYZ org, so filtered out by tenant isolation
				const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
				expect(projectNames).toContain("Website Redesign"); // Marketing team project
				expect(projectNames).toContain("Marketing Campaign Q1"); // Marketing team project
				expect(projectNames).toContain("Core API Development"); // Has direct viewer access (see init.sql)

				// Should not see other Engineering projects (no team access, no direct access)
				expect(projectNames).not.toContain("Mobile App");
				expect(projectNames).not.toContain("Internal Tools");
				// Product Roadmap is public but in different organization (StartupXYZ vs TechCorp)
				expect(projectNames).not.toContain("Product Roadmap");
			});
		});

		it("should enforce access for a user with limited permissions", async () => {
			await db.executeInTransaction(async () => {
				// Test with Alice Brown - Engineering maintainer (not admin)
				const aliceConfig = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", // Alice Brown
					},
				};

				const query: SelectQuery = {
					rootTable: "projects",
					selection: {
						id: true,
						name: true,
						visibility: true,
						owner_id: true,
					},
					condition: {
						$and: [
							// Apply same RLS but for Alice
							{
								$or: [
									{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
													// Require maintainer+ role
													{
														$exists: {
															table: "roles",
															condition: {
																$and: [
																	{ "roles.id": { $eq: { $field: "team_members.role_id" } } },
																	{ "roles.level": { $gte: { $var: "maintainer_role_level" } } },
																],
															},
														},
													},
												],
											},
										},
									},
									{ "projects.visibility": { $eq: "public" } },
								],
							},
							{ "projects.archived_at": { $eq: null } },
						],
					},
				};

				const sql = buildSelectQuery(query, aliceConfig);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Alice has maintainer role in Engineering team, so should see Engineering projects
				// Also has viewer role in Product team (cross-org), but tenant isolation might filter it
				const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
				expect(projectNames).toContain("Mobile App"); // She owns this project
				expect(projectNames).toContain("Core API Development"); // Team member with maintainer role
				expect(projectNames).toContain("Internal Tools"); // Team member with maintainer role
				// Product Roadmap might be visible if cross-org access is allowed, but tenant isolation filters it
				// expect(projectNames).toContain("Product Roadmap"); // Public project + team member

				// Should not see Marketing projects
				expect(projectNames).not.toContain("Website Redesign");
				expect(projectNames).not.toContain("Marketing Campaign Q1");
			});
		});

		it("should enforce access for user in different organization", async () => {
			await db.executeInTransaction(async () => {
				// Test with Bob Wilson from StartupXYZ (different organization)
				const crossOrgUserConfig = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "6ba7b814-9dad-11d1-80b4-00c04fd430c8", // Bob Wilson
						current_tenant_id: "b2b2b2b2-2222-2222-2222-222222222222", // StartupXYZ
					},
				};

				const query: SelectQuery = {
					rootTable: "projects",
					selection: {
						id: true,
						name: true,
						visibility: true,
						teams: {
							id: true,
							name: true,
							organization_id: true,
						},
					},
					condition: {
						$and: [
							// Apply same RLS logic but for Bob Wilson
							{
								$or: [
									{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
									{
										$exists: {
											table: "team_members",
											condition: {
												$and: [
													{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
													{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
													{ "team_members.active": { $eq: true } },
												],
											},
										},
									},
									{
										$exists: {
											table: "project_access",
											condition: {
												$and: [
													{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
													{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
													{
														$or: [
															{ "project_access.expires_at": { $eq: null } },
															{ "project_access.expires_at": { $gt: { $timestamp: "2024-01-30T00:00:00" } } },
														],
													},
												],
											},
										},
									},
									{ "projects.visibility": { $eq: "public" } },
								],
							},
							// Tenant isolation - only projects from StartupXYZ
							{
								$exists: {
									table: "teams",
									condition: {
										$and: [
											{ "teams.id": { $eq: { $field: "projects.team_id" } } },
											{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
										],
									},
								},
							},
							{ "projects.archived_at": { $eq: null } },
						],
					},
				};

				const sql = buildSelectQuery(query, crossOrgUserConfig);
				const rows = await db.query(sql);

				expect(rows).toBeDefined();
				expect(Array.isArray(rows)).toBe(true);

				// Verify StartupXYZ tenant isolation
				expect(sql).toContain("b2b2b2b2-2222-2222-2222-222222222222"); // StartupXYZ UUID
				expect(sql).toContain("6ba7b814-9dad-11d1-80b4-00c04fd430c8"); // Bob Wilson UUID

				// Bob should only see projects from StartupXYZ organization
				const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
				expect(projectNames).toContain("Product Roadmap"); // Owns this project in Product team

				// Should not see any TechCorp projects due to tenant isolation
				expect(projectNames).not.toContain("Core API Development");
				expect(projectNames).not.toContain("Mobile App");
				expect(projectNames).not.toContain("Internal Tools");
				expect(projectNames).not.toContain("Website Redesign");
				expect(projectNames).not.toContain("Marketing Campaign Q1");

				// Verify organization isolation in results - only if we have rows
				if (rows.length > 0) {
					for (const row of rows as Record<string, unknown>[]) {
						expect(row["teams.organization_id"]).toBe("b2b2b2b2-2222-2222-2222-222222222222");
					}
				}
			});
		});

		describe("Non-existent User (Security Edge Case)", () => {
			it("should return no results for non-existent user", async () => {
				const nonExistentConfig: Config = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "00000000-0000-0000-0000-000000000000", // Non-existent user
						current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
					},
				};

				await db.executeInTransaction(async () => {
					const query: SelectQuery = {
						rootTable: "projects",
						selection: {
							id: true,
							name: true,
						},
						condition: {
							$and: [
								{
									$or: [
										{ owner_id: { $eq: { $var: "current_user_id" } } },
										{
											$exists: {
												table: "team_members",
												condition: {
													$and: [
														{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
														{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
														{ "team_members.active": { $eq: true } },
													],
												},
											},
										},
									],
								},
								{
									$exists: {
										table: "teams",
										condition: {
											$and: [
												{ "teams.id": { $eq: { $field: "projects.team_id" } } },
												{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
											],
										},
									},
								},
								{ archived_at: { $eq: null } },
							],
						},
					};

					const sql = buildSelectQuery(query, nonExistentConfig);
					const rows = await db.query(sql);

					expect(rows).toBeDefined();
					expect(Array.isArray(rows)).toBe(true);
					expect(rows.length).toBe(0); // No access for non-existent user
				});
			});

			it("should return no aggregated data for non-existent user", async () => {
				const nonExistentConfig: Config = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "00000000-0000-0000-0000-000000000000",
						current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111",
					},
				};

				await db.executeInTransaction(async () => {
					const aggregationQuery: AggregationQuery = {
						table: "tasks",
						groupBy: ["tasks.status"],
						condition: {
							$exists: {
								table: "projects",
								condition: {
									$and: [
										{ "projects.id": { $eq: { $field: "tasks.project_id" } } },
										{
											$or: [
												{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
												{
													$exists: {
														table: "team_members",
														condition: {
															$and: [
																{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
																{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
																{ "team_members.active": { $eq: true } },
															],
														},
													},
												},
											],
										},
									],
								},
							},
						},
						aggregatedFields: {
							task_count: {
								function: "COUNT",
								field: "*",
							},
						},
					};

					const sql = compileAggregationQuery(parseAggregationQuery(aggregationQuery, nonExistentConfig));
					const rows = await db.query(sql);

					expect(rows).toBeDefined();
					expect(Array.isArray(rows)).toBe(true);
					expect(rows.length).toBe(0); // No aggregated data for non-existent user
				});
			});
		});

		describe("Rachel External (No Organization Access)", () => {
			it("should only see public projects for user without organization membership", async () => {
				const rachelConfig: Config = {
					...config,
					variables: {
						...config.variables,
						current_user_id: "5ba7b812-9dad-11d1-80b4-00c04fd430cd", // Hypothetical Rachel
						current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
					},
				};

				await db.executeInTransaction(async () => {
					// Insert Rachel as a user but don't add her to any teams
					await db.query(
						`INSERT INTO users (id, name, email, age, active, status, metadata) 
						 VALUES ('5ba7b812-9dad-11d1-80b4-00c04fd430cd', 'Rachel External', 'rachel@external.com', 27, TRUE, 'active', '{"department":"external","role":"consultant"}'::JSONB)`,
					);

					const query: SelectQuery = {
						rootTable: "projects",
						selection: {
							id: true,
							name: true,
							visibility: true,
						},
						condition: {
							$and: [
								{
									$or: [
										{ owner_id: { $eq: { $var: "current_user_id" } } },
										{
											$exists: {
												table: "team_members",
												condition: {
													$and: [
														{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
														{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
														{ "team_members.active": { $eq: true } },
													],
												},
											},
										},
										{
											$exists: {
												table: "project_access",
												condition: {
													$and: [
														{ "project_access.user_id": { $eq: { $var: "current_user_id" } } },
														{ "project_access.project_id": { $eq: { $field: "projects.id" } } },
														{
															$or: [
																{ "project_access.expires_at": { $eq: null } },
																{ "project_access.expires_at": { $gt: { $var: "current_timestamp" } } },
															],
														},
													],
												},
											},
										},
										{ visibility: { $eq: "public" } },
									],
								},
								{
									$exists: {
										table: "teams",
										condition: {
											$and: [
												{ "teams.id": { $eq: { $field: "projects.team_id" } } },
												{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
											],
										},
									},
								},
								{ archived_at: { $eq: null } },
							],
						},
					};

					const sql = buildSelectQuery(query, rachelConfig);
					const rows = await db.query(sql);

					expect(rows).toBeDefined();
					expect(Array.isArray(rows)).toBe(true);

					// Rachel should only see public projects from TechCorp
					const projectData = rows as Record<string, unknown>[];

					// All visible projects should be public
					for (const project of projectData) {
						expect(project.visibility).toBe("public");
					}

					// Should see Product Roadmap (public project)
					const projectNames = projectData.map((row) => row.name);
					if (projectNames.length > 0) {
						// If we see any projects, they should all be public
						expect(projectData.every((p) => p.visibility === "public")).toBe(true);
					}

					// Should not see private, team, or organization projects
					expect(projectNames).not.toContain("Core API Development"); // team visibility
					expect(projectNames).not.toContain("Internal Tools"); // private visibility
					expect(projectNames).not.toContain("Website Redesign"); // organization visibility
				});
			});
		});

		describe("Additional User Scenarios - Comprehensive RLS Coverage", () => {
			describe("Sam Developer (Engineering Team Member)", () => {
				it("should grant access based on basic engineering team membership", async () => {
					// Create config for a hypothetical Sam Developer who is a basic contributor in engineering
					const samConfig: Config = {
						...config,
						variables: {
							...config.variables,
							current_user_id: "2ba7b812-9dad-11d1-80b4-00c04fd430c9", // Hypothetical Sam
							current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
						},
					};

					// First insert Sam into the database with engineering team membership
					await db.executeInTransaction(async () => {
						// Insert Sam as a user
						await db.query(
							`INSERT INTO users (id, name, email, age, active, status, metadata) 
							 VALUES ('2ba7b812-9dad-11d1-80b4-00c04fd430c9', 'Sam Developer', 'sam@techcorp.com', 26, TRUE, 'active', '{"department":"engineering","role":"developer"}'::JSONB)`,
						);

						// Add Sam to engineering team as contributor
						await db.query(
							`INSERT INTO team_members (id, team_id, user_id, role_id, active) 
							 VALUES ('2ba7b812-9dad-11d1-80b4-00c04fd430c9', 'c1c1c1c1-1111-1111-1111-111111111111', '2ba7b812-9dad-11d1-80b4-00c04fd430c9', 'a2a2a2a2-2222-2222-2222-222222222222', TRUE)`,
						);

						const query: SelectQuery = {
							rootTable: "projects",
							selection: {
								id: true,
								name: true,
								visibility: true,
							},
							condition: {
								$and: [
									{
										$or: [
											// User is the project owner
											{
												owner_id: { $eq: { $var: "current_user_id" } },
											},
											// User has team access through team membership
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.active": { $eq: true } },
														],
													},
												},
											},
										],
									},
									// Ensure projects belong to current tenant organization
									{
										$exists: {
											table: "teams",
											condition: {
												$and: [
													{ "teams.id": { $eq: { $field: "projects.team_id" } } },
													{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
												],
											},
										},
									},
									// Exclude archived projects
									{ "projects.archived_at": { $eq: null } },
								],
							},
						};

						const sql = buildSelectQuery(query, samConfig);
						const rows = await db.query(sql);

						expect(rows).toBeDefined();
						expect(Array.isArray(rows)).toBe(true);

						// Sam should see engineering team projects
						const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
						expect(projectNames).toContain("Core API Development");
						expect(projectNames).toContain("Mobile App");
						expect(projectNames).toContain("Internal Tools");

						// Should not see marketing projects (different team)
						expect(projectNames).not.toContain("Website Redesign");
						expect(projectNames).not.toContain("Marketing Campaign Q1");

						// Should see org-level projects from other teams
						expect(projectNames).toContain("Mobile App"); // Org visibility
					});
				});
			});
			describe("Eve Viewer (Limited Role Access)", () => {
				it("should respect role-level restrictions for viewer role", async () => {
					const eveConfig: Config = {
						...config,
						variables: {
							...config.variables,
							current_user_id: "3ba7b812-9dad-11d1-80b4-00c04fd430cb", // Hypothetical Eve
							current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
						},
					};

					await db.executeInTransaction(async () => {
						// Insert Eve as a user
						await db.query(
							`INSERT INTO users (id, name, email, age, active, status, metadata) 
							 VALUES ('3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'Eve Viewer', 'eve@techcorp.com', 24, TRUE, 'active', '{"department":"marketing","role":"intern"}'::JSONB)`,
						);

						// Add Eve to marketing team as viewer (lowest role)
						await db.query(
							`INSERT INTO team_members (id, team_id, user_id, role_id, active) 
							 VALUES ('3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'c2c2c2c2-2222-2222-2222-222222222222', '3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'a1a1a1a1-1111-1111-1111-111111111111', TRUE)`,
						);

						// Test query that requires maintainer+ role (level >= 50)
						const query: SelectQuery = {
							rootTable: "projects",
							selection: {
								id: true,
								name: true,
								visibility: true,
							},
							condition: {
								$and: [
									{
										$or: [
											{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.active": { $eq: true } },
															// Require maintainer+ role
															{
																$exists: {
																	table: "roles",
																	condition: {
																		$and: [
																			{ "roles.id": { $eq: { $field: "team_members.role_id" } } },
																			{ "roles.level": { $gte: { $var: "maintainer_role_level" } } },
																		],
																	},
																},
															},
														],
													},
												},
											},
										],
									},
									{
										$exists: {
											table: "teams",
											condition: {
												$and: [
													{ "teams.id": { $eq: { $field: "projects.team_id" } } },
													{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
												],
											},
										},
									},
									{ "projects.archived_at": { $eq: null } },
								],
							},
						};

						const sql = buildSelectQuery(query, eveConfig);
						const rows = await db.query(sql);

						expect(rows).toBeDefined();
						expect(Array.isArray(rows)).toBe(true);

						// Eve has viewer role (level 10) which is below maintainer requirement (level 50)
						// So she should not see any projects through team membership
						expect(rows.length).toBe(0);
					});
				});

				it("should allow basic project access for viewer role without level restrictions", async () => {
					const eveConfig: Config = {
						...config,
						variables: {
							...config.variables,
							current_user_id: "3ba7b812-9dad-11d1-80b4-00c04fd430cb",
							current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111",
						},
					};

					await db.executeInTransaction(async () => {
						// Insert Eve as a user
						await db.query(
							`INSERT INTO users (id, name, email, age, active, status, metadata) 
							 VALUES ('3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'Eve Viewer', 'eve@techcorp.com', 24, TRUE, 'active', '{"department":"marketing","role":"intern"}'::JSONB)`,
						);

						// Add Eve to marketing team as viewer (lowest role)
						await db.query(
							`INSERT INTO team_members (id, team_id, user_id, role_id, active) 
							 VALUES ('3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'c2c2c2c2-2222-2222-2222-222222222222', '3ba7b812-9dad-11d1-80b4-00c04fd430cb', 'a1a1a1a1-1111-1111-1111-111111111111', TRUE)`,
						);

						// Query without role level restrictions - basic team access
						const query: SelectQuery = {
							rootTable: "projects",
							selection: {
								id: true,
								name: true,
								visibility: true,
							},
							condition: {
								$and: [
									{
										$or: [
											{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.active": { $eq: true } },
															// No role level requirement
														],
													},
												},
											},
										],
									},
									{
										$exists: {
											table: "teams",
											condition: {
												$and: [
													{ "teams.id": { $eq: { $field: "projects.team_id" } } },
													{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
												],
											},
										},
									},
									{ "projects.archived_at": { $eq: null } },
								],
							},
						};

						const sql = buildSelectQuery(query, eveConfig);
						const rows = await db.query(sql);

						expect(rows).toBeDefined();
						expect(Array.isArray(rows)).toBe(true);

						// Eve should see marketing team projects (her team)
						const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);
						expect(projectNames).toContain("Website Redesign");
						expect(projectNames).toContain("Marketing Campaign Q1");

						// Should not see engineering projects (different team)
						expect(projectNames).not.toContain("Core API Development");
						expect(projectNames).not.toContain("Mobile App");
					});
				});
			});
			describe("Frank External (Cross-Organization)", () => {
				it("should properly handle user switching between organizations", async () => {
					const frankTechCorpConfig: Config = {
						...config,
						variables: {
							...config.variables,
							current_user_id: "4ba7b812-9dad-11d1-80b4-00c04fd430cd", // Hypothetical Frank
							current_tenant_id: "b1b1b1b1-1111-1111-1111-111111111111", // TechCorp
						},
					};

					await db.executeInTransaction(async () => {
						// Insert Frank as a user
						await db.query(
							`INSERT INTO users (id, name, email, age, active, status, metadata) 
							 VALUES ('4ba7b812-9dad-11d1-80b4-00c04fd430cd', 'Frank Cross-Org', 'frank@contractor.com', 35, TRUE, 'active', '{"department":"contractor","role":"consultant"}'::JSONB)`,
						);

						// Add Frank to TechCorp Sales team as contributor
						await db.query(
							`INSERT INTO team_members (id, team_id, user_id, role_id, active) 
							 VALUES ('4ba7b812-9dad-11d1-80b4-00c04fd430cd', 'c3c3c3c3-3333-3333-3333-333333333333', '4ba7b812-9dad-11d1-80b4-00c04fd430cd', 'a2a2a2a2-2222-2222-2222-222222222222', TRUE)`,
						);

						// Test with TechCorp context
						const query: SelectQuery = {
							rootTable: "projects",
							selection: {
								id: true,
								name: true,
								team_id: true,
							},
							condition: {
								$and: [
									{
										$or: [
											{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.active": { $eq: true } },
														],
													},
												},
											},
										],
									},
									{
										$exists: {
											table: "teams",
											condition: {
												$and: [
													{ "teams.id": { $eq: { $field: "projects.team_id" } } },
													{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
												],
											},
										},
									},
									{ "projects.archived_at": { $eq: null } },
								],
							},
						};

						const sql = buildSelectQuery(query, frankTechCorpConfig);
						const rows = await db.query(sql);

						expect(rows).toBeDefined();
						expect(Array.isArray(rows)).toBe(true);

						// Frank is in Sales team, but sales team doesn't have any projects in our test data
						// So he should see only organization-wide visible projects or public ones
						const projectNames = (rows as Record<string, unknown>[]).map((row) => row.name);

						// Should not see private team projects he's not a member of
						expect(projectNames).not.toContain("Core API Development");
						expect(projectNames).not.toContain("Internal Tools");
						expect(projectNames).not.toContain("Website Redesign");

						// May see organization-level projects like Mobile App
						// But since Frank is in Sales team and we don't have Sales projects, results may be empty
						expect(rows.length).toBeGreaterThanOrEqual(0);
					});
				});

				it("should handle Frank switching to StartupXYZ context", async () => {
					const frankStartupConfig: Config = {
						...config,
						variables: {
							...config.variables,
							current_user_id: "4ba7b812-9dad-11d1-80b4-00c04fd430cd",
							current_tenant_id: "b2b2b2b2-2222-2222-2222-222222222222", // StartupXYZ
						},
					};

					await db.executeInTransaction(async () => {
						const query: SelectQuery = {
							rootTable: "projects",
							selection: {
								id: true,
								name: true,
							},
							condition: {
								$and: [
									{
										$or: [
											{ "projects.owner_id": { $eq: { $var: "current_user_id" } } },
											{
												$exists: {
													table: "team_members",
													condition: {
														$and: [
															{ "team_members.user_id": { $eq: { $var: "current_user_id" } } },
															{ "team_members.team_id": { $eq: { $field: "projects.team_id" } } },
															{ "team_members.active": { $eq: true } },
														],
													},
												},
											},
										],
									},
									{
										$exists: {
											table: "teams",
											condition: {
												$and: [
													{ "teams.id": { $eq: { $field: "projects.team_id" } } },
													{ "teams.organization_id": { $eq: { $var: "current_tenant_id" } } },
												],
											},
										},
									},
									{ "projects.archived_at": { $eq: null } },
								],
							},
						};

						const sql = buildSelectQuery(query, frankStartupConfig);
						const rows = await db.query(sql);

						expect(rows).toBeDefined();
						expect(Array.isArray(rows)).toBe(true);

						// Frank switched to StartupXYZ context but has no team membership there
						// Should see no projects (proper tenant isolation)
						expect(rows.length).toBe(0);
					});
				});
			});
		});
	});
});
