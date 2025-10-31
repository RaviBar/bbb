const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');

const dbDir = path.join(__dirname, 'data');
if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir);
}

const dbPath = path.join(dbDir, 'cs_messaging.db');
const schemaPath = path.join(__dirname, 'schema.sql');

const schema = fs.readFileSync(schemaPath, 'utf8');

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) {
        console.error('Error opening database:', err);
        return;
    }
    
    db.exec(schema, (err) => {
        if (err) {
            console.error('Error creating schema:', err);
            return;
        }
        console.log('Database initialized successfully!');
        db.close();
    });
});