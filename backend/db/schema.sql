-- Database schema for CS Messaging Web App

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Agents table
CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    is_online BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Messages table
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    message_body TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_from_customer BOOLEAN NOT NULL DEFAULT true,
    agent_id INTEGER,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'assigned', 'responded')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(user_id),
    FOREIGN KEY (agent_id) REFERENCES agents(id)
);
-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_messages_customer_id ON messages(customer_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_agents_online ON agents(is_online);