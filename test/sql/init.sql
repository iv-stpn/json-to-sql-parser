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
    status VARCHAR(50) NOT NULL DEFAULT 'active',
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
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP
);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    customer_id UUID REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_date DATE
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

-- Insert sample data for regular tables with specific UUIDs
INSERT INTO users (id, name, email, age, active, status, birth_date, metadata) VALUES
('550e8400-e29b-41d4-a716-446655440000', 'John Doe', 'john@example.com', 30, true, 'premium', '1994-01-15', '{"department": "engineering", "role": "senior", "settings": {"theme": "dark", "notifications": true}}'),
('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Jane Smith', 'jane@example.com', 25, true, 'active', '1999-03-22', '{"department": "marketing", "role": "manager", "settings": {"theme": "light", "notifications": false}}'),
('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'Bob Johnson', 'bob@example.com', 35, false, 'inactive', '1989-07-08', '{"department": "sales", "role": "representative", "settings": {"theme": "dark", "notifications": true}}'),
('6ba7b812-9dad-11d1-80b4-00c04fd430c8', 'Alice Brown', 'alice@example.com', 28, true, 'premium', '1996-11-30', '{"department": "engineering", "role": "junior", "settings": {"theme": "light", "notifications": true}}'),
('6ba7b813-9dad-11d1-80b4-00c04fd430c8', 'Charlie Wilson', null, 32, true, 'active', '1992-05-18', '{"department": "hr", "role": "coordinator", "settings": {"theme": "dark", "notifications": false}}');

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
