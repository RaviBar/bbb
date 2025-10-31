const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, 'cs_messaging.db');
const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  // Customers
  db.run(`
    CREATE TABLE IF NOT EXISTS customers (
      user_id INTEGER PRIMARY KEY,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Agents
  db.run(`
    CREATE TABLE IF NOT EXISTS agents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      is_online BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Messages
  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER NOT NULL,
      message_body TEXT NOT NULL,
      timestamp DATETIME NOT NULL,
      is_from_customer BOOLEAN NOT NULL DEFAULT 1,
      agent_id INTEGER,
      current_agent_id INTEGER,                         -- <— who currently owns this conversation
      urgency_level TEXT DEFAULT 'normal' CHECK (urgency_level IN ('normal','high')),
      status TEXT DEFAULT 'pending' CHECK (status IN ('pending','assigned','responded')),
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (customer_id) REFERENCES customers(user_id),
      FOREIGN KEY (agent_id) REFERENCES agents(id),
      FOREIGN KEY (current_agent_id) REFERENCES agents(id)
    )
  `);

  // Indexes
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_customer_id ON messages(customer_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_current_agent ON messages(current_agent_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_messages_urgency ON messages(urgency_level)');

  console.log('SQLite database initialized successfully!');
});

module.exports = db;
