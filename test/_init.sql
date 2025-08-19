-- Initialize database for JSON-to-SQL parser integration tests

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create regular tables with UUID primary keys
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER,
    active BOOLEAN NOT NULL DEFAULT true,
    balance DECIMAL(10,2) DEFAULT 0.00,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    role VARCHAR(50) NOT NULL DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    birth_date DATE,
    metadata JSONB
);

CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    user_id UUID REFERENCES users(id),
    published BOOLEAN NOT NULL DEFAULT false,
    priority INTEGER,
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP
);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    customer_id UUID REFERENCES users(id),
    items JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_date DATE
);

-- Create data table for key-value storage 
CREATE TABLE data_storage (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(100) NOT NULL DEFAULT 'current_tenant',
    data JSONB NOT NULL,
    deleted_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- COMPLEX RLS STRUCTURE: Teams, Roles, Members, and Projects
-- =============================================================================

-- Organizations table
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Teams within organizations
CREATE TABLE teams (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) NOT NULL,
    description TEXT,
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(organization_id, slug)
);

-- Roles definition
CREATE TABLE roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    permissions JSONB DEFAULT '[]',
    level INTEGER NOT NULL DEFAULT 0, -- Higher number = more permissions
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Team members with roles
CREATE TABLE team_members (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID REFERENCES roles(id),
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT true,
    metadata JSONB DEFAULT '{}',
    UNIQUE(team_id, user_id)
);

-- Projects with complex access control
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(50) DEFAULT 'active',
    visibility VARCHAR(20) DEFAULT 'team', -- 'private', 'team', 'organization', 'public'
    owner_id UUID REFERENCES users(id),
    budget DECIMAL(12,2) DEFAULT 0.00,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP NULL
);

-- Project access control
CREATE TABLE project_access (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID REFERENCES roles(id),
    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    UNIQUE(project_id, user_id)
);

-- Tasks within projects
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(50) DEFAULT 'todo',
    priority VARCHAR(20) DEFAULT 'medium',
    assignee_id UUID REFERENCES users(id),
    reporter_id UUID REFERENCES users(id),
    estimated_hours DECIMAL(5,2),
    actual_hours DECIMAL(5,2),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL
);

-- No RLS implementation - will be simulated in tests with WHERE clauses

-- =============================================================================
-- END RLS STRUCTURE
-- =============================================================================

-- Create indexes for better performance
CREATE INDEX idx_users_active ON users(active);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_metadata ON users USING GIN(metadata);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);

-- Data table indexes
CREATE INDEX idx_data_storage_table_name ON data_storage(table_name);
CREATE INDEX idx_data_storage_tenant_id ON data_storage(tenant_id);
CREATE INDEX idx_data_storage_data ON data_storage USING GIN(data);
CREATE INDEX idx_data_storage_deleted_at ON data_storage(deleted_at);

-- RLS structure indexes for better performance
CREATE INDEX idx_organizations_slug ON organizations(slug);
CREATE INDEX idx_teams_organization_id ON teams(organization_id);
CREATE INDEX idx_teams_slug ON teams(organization_id, slug);
CREATE INDEX idx_roles_name ON roles(name);
CREATE INDEX idx_roles_level ON roles(level);
CREATE INDEX idx_team_members_team_id ON team_members(team_id);
CREATE INDEX idx_team_members_user_id ON team_members(user_id);
CREATE INDEX idx_team_members_active ON team_members(active);
CREATE INDEX idx_projects_team_id ON projects(team_id);
CREATE INDEX idx_projects_owner_id ON projects(owner_id);
CREATE INDEX idx_projects_visibility ON projects(visibility);
CREATE INDEX idx_projects_status ON projects(status);
CREATE INDEX idx_projects_archived_at ON projects(archived_at);
CREATE INDEX idx_project_access_project_id ON project_access(project_id);
CREATE INDEX idx_project_access_user_id ON project_access(user_id);
CREATE INDEX idx_project_access_expires_at ON project_access(expires_at);
CREATE INDEX idx_tasks_project_id ON tasks(project_id);
CREATE INDEX idx_tasks_assignee_id ON tasks(assignee_id);
CREATE INDEX idx_tasks_reporter_id ON tasks(reporter_id);
CREATE INDEX idx_tasks_status ON tasks(status);

-- Insert sample data for regular tables with specific UUIDs
INSERT INTO users (id, name, email, age, active, status, birth_date, metadata, balance) VALUES
('550e8400-e29b-41d4-a716-446655440000', 'John Doe', 'john@example.com', 30, true, 'premium', '1994-01-15', '{"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}', 1000.00),
('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Jane Smith', 'jane@example.com', 25, true, 'active', '1999-03-22', '{"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}', 1500.00),
('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Bob Johnson', 'bob@example.com', 35, false, 'inactive', '1989-07-08', '{"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}', 800.00),
('6ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Alice Brown', 'alice@example.com', 28, true, 'premium', '1996-11-30', '{"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}', 1200.00),
('6ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Charlie Wilson', null, 32, true, 'active', '1992-05-18', '{"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}', 900.00);

INSERT INTO posts (id, title, content, user_id, published, published_at, tags) VALUES
('7ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Getting Started with PostgreSQL', 'This is a comprehensive guide to PostgreSQL...', '550e8400-e29b-41d4-a716-446655440000', true, '2024-01-15 10:30:00', '["database", "postgresql", "tutorial"]'),
('7ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Advanced SQL Queries', 'Learn advanced SQL techniques...', '550e8400-e29b-41d4-a716-446655440000', true, '2024-01-16 14:45:00', '["sql", "advanced", "database"]'),
('7ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Marketing Strategies 2024', 'The latest marketing trends...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', true, '2024-01-17 09:15:00', '["marketing", "trends", "2024"]'),
('7ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Team Building Activities', 'Effective team building exercises...', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', false, null, '["teamwork", "management", "hr"]'),
('7ba7b814-9dad-11d1-80b4-00c04fd430c8', 'Sales Techniques', 'How to close more deals...', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', false, null, '["sales", "techniques", "business"]');

INSERT INTO orders (id, amount, status, customer_id, shipped_at, delivered_date) VALUES
('8ba7b810-9dad-11d1-80b4-00c04fd430c8', 299.99, 'completed', '550e8400-e29b-41d4-a716-446655440000', '2024-01-16 08:00:00', '2024-01-18'),
('8ba7b811-9dad-11d1-80b4-00c04fd430c8', 149.50, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-17 12:30:00', null),
('8ba7b812-9dad-11d1-80b4-00c04fd430c8', 89.99, 'pending', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', null, null),
('8ba7b813-9dad-11d1-80b4-00c04fd430c8', 199.99, 'completed', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '2024-01-18 15:45:00', '2024-01-20'),
('8ba7b814-9dad-11d1-80b4-00c04fd430c8', 59.99, 'cancelled', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', null, null),
('8ba7b815-9dad-11d1-80b4-00c04fd430c8', 399.99, 'completed', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '2024-01-19 11:20:00', '2024-01-22'),
('8ba7b816-9dad-11d1-80b4-00c04fd430c8', 79.99, 'pending', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', null, null),
('8ba7b817-9dad-11d1-80b4-00c04fd430c8', 249.99, 'shipped', '550e8400-e29b-41d4-a716-446655440000', '2024-01-20 16:10:00', null);

-- Insert equivalent data into data_storage table for JSON-based storage tests

INSERT INTO data_storage (table_name, tenant_id, data) VALUES
('users', 'current_tenant', '{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "John Doe", "email": "john@example.com", "age": 30, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}}'),
('users', 'current_tenant', '{"id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8", "name": "Jane Smith", "email": "jane@example.com", "age": 25, "active": true, "status": "active", "metadata": {"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}}'),
('users', 'current_tenant', '{"id": "fc9540cc-bf8a-458d-b33d-eb306811be9c", "name": "Bob Johnson", "email": "bob@example.com", "age": 35, "active": false, "status": "inactive", "metadata": {"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}}'),
('users', 'current_tenant', '{"id": "7ba7b812-9dad-11d1-80b4-00c04fd430c9", "name": "Alice Brown", "email": "alice@example.com", "age": 28, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}}'),
('users', 'current_tenant', '{"id": "bb2266a9-b593-466f-89f0-ea02e0a07775", "name": "Charlie Wilson", "email": null, "age": 32, "active": true, "status": "active", "metadata": {"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}}'),

('orders', 'current_tenant', '{"id": "8ba7b810-9dad-11d1-80b4-00c04fd430c8", "amount": 299.99, "status": "completed", "customer_id": "550e8400-e29b-41d4-a716-446655440000"}'),
('orders', 'current_tenant', '{"id": "0e429a8e-25dd-4234-9dd2-09af0cf90603", "amount": 149.50, "status": "shipped", "customer_id": "550e8400-e29b-41d4-a716-446655440000"}'),
('orders', 'current_tenant', '{"id": "4e25ea04-dc7e-4d33-9050-ed6eea388656", "amount": 89.99, "status": "pending", "customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}'),
('orders', 'current_tenant', '{"id": "91f425f5-5393-4bfc-aeed-8b11a5dad417", "amount": 199.99, "status": "completed", "customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}'),
('orders', 'current_tenant', '{"id": "cd7eb254-cec5-4690-a648-e535ca4b39e2", "amount": 59.99, "status": "cancelled", "customer_id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8"}'),
('orders', 'current_tenant', '{"id": "ac0fe41e-b8e4-44f3-b7f2-9cdaf14adc2f", "amount": 399.99, "status": "completed", "customer_id": "7ba7b812-9dad-11d1-80b4-00c04fd430c9"}'),
('orders', 'current_tenant', '{"id": "3f5d23d0-af17-4cb1-b045-4f55b163fe26", "amount": 79.99, "status": "pending", "customer_id": "7ba7b812-9dad-11d1-80b4-00c04fd430c9"}'),
('orders', 'current_tenant', '{"id": "531adaed-393c-4a65-831f-518af3881476", "amount": 249.99, "status": "shipped", "customer_id": "550e8400-e29b-41d4-a716-446655440000"}');

-- Add some data for different tenants to test isolation
INSERT INTO data_storage (table_name, tenant_id, data) VALUES
('users', 'other_tenant', '{"id": "9ba7b810-9dad-11d1-80b4-00c04fd430c8", "name": "Other User", "email": "other@example.com", "age": 40, "active": true, "status": "active", "birth_date": "1984-12-01", "created_at": "2024-01-20T10:00:00"}'),
('orders', 'other_tenant', '{"id": "06330cb1-3241-4428-b7ca-8a66edf2f5d1", "amount": 500.00, "status": "completed", "customer_id": "9ba7b810-9dad-11d1-80b4-00c04fd430c8", "created_at": "2024-01-20T14:00:00", "shipped_at": "2024-01-21T10:00:00", "delivered_date": "2024-01-23"}');

-- Add some soft-deleted records to test deletion filtering
INSERT INTO data_storage (table_name, tenant_id, data, deleted_at) VALUES
('users', 'current_tenant', '{"id": "82eceb40-0639-453b-ba17-e942b7d6d208", "name": "Deleted User", "email": "deleted@example.com", "age": 45, "active": false, "status": "deleted", "birth_date": "1979-08-25", "created_at": "2024-01-21T10:00:00"}', CURRENT_TIMESTAMP);

-- =============================================================================
-- SAMPLE DATA FOR RLS STRUCTURE (Teams, Roles, Members, Projects)
-- =============================================================================

-- Insert roles with different permission levels
INSERT INTO roles (id, name, permissions, level) VALUES
('a1a1a1a1-1111-1111-1111-111111111111', 'viewer', '["read"]', 10),
('a2a2a2a2-2222-2222-2222-222222222222', 'contributor', '["read", "create", "update"]', 30),
('a3a3a3a3-3333-3333-3333-333333333333', 'maintainer', '["read", "create", "update", "delete"]', 50),
('a4a4a4a4-4444-4444-4444-444444444444', 'admin', '["read", "create", "update", "delete", "manage_members"]', 80),
('a5a5a5a5-5555-5555-5555-555555555555', 'owner', '["all"]', 100);

-- Insert organizations
INSERT INTO organizations (id, name, slug, settings) VALUES
('b1b1b1b1-1111-1111-1111-111111111111', 'TechCorp', 'techcorp', '{"theme": "corporate", "features": ["advanced_analytics"]}'),
('b2b2b2b2-2222-2222-2222-222222222222', 'StartupXYZ', 'startupxyz', '{"theme": "modern", "features": ["basic_analytics"]}');

-- Insert teams
INSERT INTO teams (id, organization_id, name, slug, description, settings) VALUES
('c1c1c1c1-1111-1111-1111-111111111111', 'b1b1b1b1-1111-1111-1111-111111111111', 'Engineering', 'engineering', 'Software development team', '{"department": "tech"}'),
('c2c2c2c2-2222-2222-2222-222222222222', 'b1b1b1b1-1111-1111-1111-111111111111', 'Marketing', 'marketing', 'Marketing and growth team', '{"department": "business"}'),
('c3c3c3c3-3333-3333-3333-333333333333', 'b1b1b1b1-1111-1111-1111-111111111111', 'Sales', 'sales', 'Sales team', '{"department": "business"}'),
('c4c4c4c4-4444-4444-4444-444444444444', 'b2b2b2b2-2222-2222-2222-222222222222', 'Product', 'product', 'Product development team', '{"department": "tech"}');

-- Insert team members (using existing users)
INSERT INTO team_members (id, team_id, user_id, role_id, joined_at, active, metadata) VALUES
-- John Doe - Engineering Admin at TechCorp
('d1d1d1d1-1111-1111-1111-111111111111', 'c1c1c1c1-1111-1111-1111-111111111111', '550e8400-e29b-41d4-a716-446655440000', 'a4a4a4a4-4444-4444-4444-444444444444', '2024-01-01 09:00:00', true, '{"hire_date": "2024-01-01", "seniority": "senior"}'),
-- Alice Brown - Engineering Maintainer at TechCorp  
('d2d2d2d2-2222-2222-2222-222222222222', 'c1c1c1c1-1111-1111-1111-111111111111', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 'a3a3a3a3-3333-3333-3333-333333333333', '2024-01-02 09:00:00', true, '{"hire_date": "2024-01-02", "seniority": "junior"}'),
-- Jane Smith - Marketing Admin at TechCorp
('d3d3d3d3-3333-3333-3333-333333333333', 'c2c2c2c2-2222-2222-2222-222222222222', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'a4a4a4a4-4444-4444-4444-444444444444', '2024-01-03 09:00:00', true, '{"hire_date": "2024-01-03", "seniority": "manager"}'),
-- Bob Johnson - Sales Contributor at TechCorp (inactive user)
('d4d4d4d4-4444-4444-4444-444444444444', 'c3c3c3c3-3333-3333-3333-333333333333', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'a2a2a2a2-2222-2222-2222-222222222222', '2024-01-04 09:00:00', true, '{"hire_date": "2024-01-04", "seniority": "representative"}'),
-- Charlie Wilson - Product Owner at StartupXYZ
('d5d5d5d5-5555-5555-5555-555555555555', 'c4c4c4c4-4444-4444-4444-444444444444', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 'a5a5a5a5-5555-5555-5555-555555555555', '2024-01-05 09:00:00', true, '{"hire_date": "2024-01-05", "seniority": "coordinator"}'),
-- Alice Brown also in Product team as Viewer (cross-team membership)
('d6d6d6d6-6666-6666-6666-666666666666', 'c4c4c4c4-4444-4444-4444-444444444444', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 'a1a1a1a1-1111-1111-1111-111111111111', '2024-01-06 09:00:00', true, '{"hire_date": "2024-01-06", "cross_team": true}');

-- Insert projects with different visibility levels
INSERT INTO projects (id, team_id, name, description, status, visibility, owner_id, budget, metadata, archived_at) VALUES
-- Engineering projects
('e1e1e1e1-1111-1111-1111-111111111111', 'c1c1c1c1-1111-1111-1111-111111111111', 'Core API Development', 'Main API for the platform', 'active', 'team', '550e8400-e29b-41d4-a716-446655440000', 50000.00, '{"priority": "high", "technology": "node.js"}', NULL),
('e2e2e2e2-2222-2222-2222-222222222222', 'c1c1c1c1-1111-1111-1111-111111111111', 'Mobile App', 'iOS and Android mobile application', 'active', 'organization', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 75000.00, '{"priority": "medium", "technology": "react-native"}', NULL),
('e3e3e3e3-3333-3333-3333-333333333333', 'c1c1c1c1-1111-1111-1111-111111111111', 'Internal Tools', 'Development and testing tools', 'active', 'private', '550e8400-e29b-41d4-a716-446655440000', 15000.00, '{"priority": "low", "technology": "python"}', NULL),
-- Marketing projects
('e4e4e4e4-4444-4444-4444-444444444444', 'c2c2c2c2-2222-2222-2222-222222222222', 'Website Redesign', 'Company website overhaul', 'active', 'organization', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 25000.00, '{"priority": "high", "technology": "next.js"}', NULL),
('e5e5e5e5-5555-5555-5555-555555555555', 'c2c2c2c2-2222-2222-2222-222222222222', 'Marketing Campaign Q1', 'Q1 2024 marketing initiatives', 'completed', 'team', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 40000.00, '{"priority": "medium", "quarter": "Q1"}', NULL),
-- Product projects
('e6e6e6e6-6666-6666-6666-666666666666', 'c4c4c4c4-4444-4444-4444-444444444444', 'Product Roadmap', 'Strategic product planning', 'active', 'public', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 10000.00, '{"priority": "high", "type": "planning"}', NULL),
-- Archived project
('e7e7e7e7-7777-7777-7777-777777777777', 'c1c1c1c1-1111-1111-1111-111111111111', 'Legacy System', 'Old system maintenance', 'archived', 'team', '550e8400-e29b-41d4-a716-446655440000', 5000.00, '{"priority": "low", "deprecated": "true"}', '2024-12-01 00:00:00');

-- Insert project access grants (direct project access beyond team membership)
INSERT INTO project_access (id, project_id, user_id, role_id, granted_by, granted_at, expires_at) VALUES
-- Give Jane Smith (Marketing) viewer access to Core API project
('f1f1f1f1-1111-1111-1111-111111111111', 'e1e1e1e1-1111-1111-1111-111111111111', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'a1a1a1a1-1111-1111-1111-111111111111', '550e8400-e29b-41d4-a716-446655440000', '2024-01-10 10:00:00', NULL),
-- Give Charlie Wilson contributor access to Mobile App project
('f2f2f2f2-2222-2222-2222-222222222222', 'e2e2e2e2-2222-2222-2222-222222222222', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 'a2a2a2a2-2222-2222-2222-222222222222', '550e8400-e29b-41d4-a716-446655440000', '2024-01-11 10:00:00', NULL),
-- Temporary access that has expired
('f3f3f3f3-3333-3333-3333-333333333333', 'e1e1e1e1-1111-1111-1111-111111111111', '6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'a1a1a1a1-1111-1111-1111-111111111111', '550e8400-e29b-41d4-a716-446655440000', '2024-01-05 10:00:00', '2024-01-15 10:00:00');

-- Insert tasks
INSERT INTO tasks (id, project_id, title, description, status, priority, assignee_id, reporter_id, estimated_hours, actual_hours, metadata, completed_at) VALUES
-- Core API tasks
('a1b1a1b1-1111-1111-1111-111111111111', 'e1e1e1e1-1111-1111-1111-111111111111', 'Setup Authentication System', 'Implement JWT-based authentication', 'in_progress', 'high', '550e8400-e29b-41d4-a716-446655440000', '550e8400-e29b-41d4-a716-446655440000', 40.0, 25.5, '{"complexity": "high", "epic": "auth"}', NULL),
('a2b2a2b2-2222-2222-2222-222222222222', 'e1e1e1e1-1111-1111-1111-111111111111', 'Database Schema Design', 'Design and implement core database schema', 'completed', 'high', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '550e8400-e29b-41d4-a716-446655440000', 20.0, 22.0, '{"complexity": "medium", "epic": "database"}', '2024-01-20 15:30:00'),
('a3b3a3b3-3333-3333-3333-333333333333', 'e1e1e1e1-1111-1111-1111-111111111111', 'API Documentation', 'Write comprehensive API documentation', 'todo', 'medium', NULL, '550e8400-e29b-41d4-a716-446655440000', 15.0, NULL, '{"complexity": "low", "epic": "docs"}', NULL),
-- Mobile App tasks
('a4b4a4b4-4444-4444-4444-444444444444', 'e2e2e2e2-2222-2222-2222-222222222222', 'UI/UX Design', 'Design mobile app interface', 'completed', 'high', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 60.0, 65.0, '{"complexity": "high", "platform": "both"}', '2024-01-25 14:00:00'),
('a5b5a5b5-5555-5555-5555-555555555555', 'e2e2e2e2-2222-2222-2222-222222222222', 'iOS Implementation', 'Implement iOS native features', 'in_progress', 'high', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', '6ba7b812-9dad-11d1-80b4-00c04fd430c8', 80.0, 45.0, '{"complexity": "high", "platform": "ios"}', NULL),
-- Website Redesign tasks
('a6b6a6b6-6666-6666-6666-666666666666', 'e4e4e4e4-4444-4444-4444-444444444444', 'Content Strategy', 'Plan website content and structure', 'completed', 'medium', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 25.0, 28.0, '{"complexity": "medium", "type": "planning"}', '2024-01-18 11:00:00'),
('a7b7a7b7-7777-7777-7777-777777777777', 'e4e4e4e4-4444-4444-4444-444444444444', 'Homepage Design', 'Design new homepage layout', 'in_progress', 'high', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 35.0, 20.0, '{"complexity": "high", "type": "design"}', NULL),
-- Product Roadmap tasks
('a8b8a8b8-8888-8888-8888-888888888888', 'e6e6e6e6-6666-6666-6666-666666666666', 'Market Research', 'Conduct competitive analysis', 'completed', 'high', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 30.0, 32.0, '{"complexity": "medium", "type": "research"}', '2024-01-22 16:00:00'),
('a9b9a9b9-9999-9999-9999-999999999999', 'e6e6e6e6-6666-6666-6666-666666666666', 'Feature Prioritization', 'Prioritize features for next quarter', 'todo', 'medium', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', '6ba7b813-9dad-11d1-80b4-00c04fd430c8', 20.0, NULL, '{"complexity": "low", "type": "planning"}', NULL);
