const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, 'cs_messaging.db');
const db = new sqlite3.Database(dbPath);

const urgentKeywords = ['loan', 'approval', 'disbursed', 'urgent', 'help', 
  'immediate', 'rejected', 'denied', 'payment', 
  'batch number', 'validate', 'review', 'crb', 
  'clearance', 'pay'];

db.serialize(() => {
  db.run(`PRAGMA busy_timeout=5000;`);

  db.all(`PRAGMA table_info(messages);`, (err, cols) => {
    if (err) throw err;
    const hasUrgency = cols.some(c => c.name === 'urgency_level');
    const todo = () => {
      db.all(`SELECT id, message_body FROM messages WHERE is_from_customer = 1;`, [], (e, rows) => {
        if (e) throw e;
        const stmt = db.prepare(`UPDATE messages SET urgency_level = ? WHERE id = ?`);
        rows.forEach(({ id, message_body }) => {
          const text = (message_body || '').toLowerCase();
          const isUrgent = urgentKeywords.some(k => text.includes(k));
          stmt.run(isUrgent ? 'high' : 'normal', id);
        });
        stmt.finalize((fe) => {
          if (fe) throw fe;
          console.log('Urgency levels updated successfully!');
          db.close();
        });
      });
    };
    if (!hasUrgency) {
      db.run(`ALTER TABLE messages ADD COLUMN urgency_level TEXT DEFAULT 'normal' CHECK (urgency_level IN ('normal','high'))`, (ae) => {
        if (ae) throw ae;
        todo();
      });
    } else {
      todo();
    }
  });
});