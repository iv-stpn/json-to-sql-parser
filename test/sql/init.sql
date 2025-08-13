-- Initialize database for JSON-to-SQL parser integration tests

-- Create regular tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER,
    active BOOLEAN NOT NULL DEFAULT true,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    user_id INTEGER REFERENCES users(id),
    published BOOLEAN NOT NULL DEFAULT false,
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    customer_id INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create data table for JSON-based storage (multi-tenant style)
CREATE TABLE data_storage (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(100) NOT NULL DEFAULT 'current_tenant',
    data JSONB NOT NULL,
    deleted_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

-- Insert equivalent data into data_storage table for JSON-based storage tests
INSERT INTO data_storage (table_name, tenant_id, data) VALUES
('users', 'current_tenant', '{"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}}'),
('users', 'current_tenant', '{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25, "active": true, "status": "active", "metadata": {"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}}'),
('users', 'current_tenant', '{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35, "active": false, "status": "inactive", "metadata": {"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}}'),
('users', 'current_tenant', '{"id": 4, "name": "Alice Brown", "email": "alice@example.com", "age": 28, "active": true, "status": "premium", "metadata": {"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}}'),
('users', 'current_tenant', '{"id": 5, "name": "Charlie Wilson", "email": null, "age": 32, "active": true, "status": "active", "metadata": {"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}}'),

('posts', 'current_tenant', '{"id": 1, "title": "Getting Started with PostgreSQL", "content": "This is a comprehensive guide to PostgreSQL...", "user_id": 1, "published": true, "tags": ["database", "postgresql", "tutorial"]}'),
('posts', 'current_tenant', '{"id": 2, "title": "Advanced SQL Queries", "content": "Learn advanced SQL techniques...", "user_id": 1, "published": true, "tags": ["sql", "advanced", "database"]}'),
('posts', 'current_tenant', '{"id": 3, "title": "Marketing Strategies 2024", "content": "The latest marketing trends...", "user_id": 2, "published": true, "tags": ["marketing", "trends", "2024"]}'),
('posts', 'current_tenant', '{"id": 4, "title": "Team Building Activities", "content": "Effective team building exercises...", "user_id": 2, "published": false, "tags": ["teamwork", "management", "hr"]}'),
('posts', 'current_tenant', '{"id": 5, "title": "Sales Techniques", "content": "How to close more deals...", "user_id": 3, "published": false, "tags": ["sales", "techniques", "business"]}'),

('orders', 'current_tenant', '{"id": 1, "amount": 299.99, "status": "completed", "customer_id": 1}'),
('orders', 'current_tenant', '{"id": 2, "amount": 149.50, "status": "shipped", "customer_id": 1}'),
('orders', 'current_tenant', '{"id": 3, "amount": 89.99, "status": "pending", "customer_id": 2}'),
('orders', 'current_tenant', '{"id": 4, "amount": 199.99, "status": "completed", "customer_id": 2}'),
('orders', 'current_tenant', '{"id": 5, "amount": 59.99, "status": "cancelled", "customer_id": 3}'),
('orders', 'current_tenant', '{"id": 6, "amount": 399.99, "status": "completed", "customer_id": 4}'),
('orders', 'current_tenant', '{"id": 7, "amount": 79.99, "status": "pending", "customer_id": 4}'),
('orders', 'current_tenant', '{"id": 8, "amount": 249.99, "status": "shipped", "customer_id": 1}');

-- Add some data for different tenants to test isolation
INSERT INTO data_storage (table_name, tenant_id, data) VALUES
('users', 'other_tenant', '{"id": 1, "name": "Other User", "email": "other@example.com", "age": 40, "active": true, "status": "active"}'),
('orders', 'other_tenant', '{"id": 1, "amount": 500.00, "status": "completed", "customer_id": 1}');

-- Add some soft-deleted records to test deletion filtering
INSERT INTO data_storage (table_name, tenant_id, data, deleted_at) VALUES
('users', 'current_tenant', '{"id": 6, "name": "Deleted User", "email": "deleted@example.com", "age": 45, "active": false, "status": "deleted"}', CURRENT_TIMESTAMP);
