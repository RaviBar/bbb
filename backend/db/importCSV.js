// backend/db/importCSV.js
const fs = require('fs');
const csv = require('csv-parser');
const db = require('./database');
const path = require('path'); 

async function importCSVData() {
  try {
    // --- CHANGE 1: IDEMPOTENCY CHECK ---
    // Check if messages already exist before importing.
    const messageCount = await db.get('SELECT COUNT(*) as count FROM messages');
    if (messageCount && messageCount.count > 0) {
      console.log('Database already populated. Skipping CSV import.');
      return; 
    }
   

    console.log('Database is empty. Starting CSV import...');
    
    const csvPath = process.argv[2] || path.join(__dirname, '../GeneralistRails_Project_MessageData.csv');
    
    if (!fs.existsSync(csvPath)) {
      console.log(`CSV file not found at: ${csvPath}`);
      console.log('Please ensure "GeneralistRails_Project_MessageData.csv" is in the "backend" directory.');
      console.log('Usage: node db/importCSV.js <path-to-csv-file>');
      return;
    }

    const customers = new Set();
    const messages = [];

    // Parse CSV file
    await new Promise((resolve, reject) => {
      fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
          const userId = parseInt(row['User ID']);
          const timestamp = new Date(row['Timestamp (UTC)']);
          const messageBody = row['Message Body'];

          if (!isNaN(userId) && messageBody) { 
            customers.add(userId);
            const lower = messageBody.toLowerCase();
            const urgentKeywords = [
              'loan', 'approval', 'disbursed', 'urgent', 'help', 
              'immediate', 'rejected', 'denied', 'payment', 
              'batch number', 'validate', 'review', 'crb', 
              'clearance', 'pay'
            ];
            const isUrgent = urgentKeywords.some(k => lower.includes(k));
            const urgency = isUrgent ? 'high' : 'normal';
            messages.push({
              customer_id: userId,
              message_body: messageBody,
              timestamp: timestamp.toISOString(),
              is_from_customer: true,
              status: 'pending',
              urgency_level: urgency
            });
          }
        })
        .on('end', resolve)
        .on('error', reject);
    });

    const insertCustomerSQL = db.type === 'postgres' 
      ? 'INSERT INTO customers (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING'
      : 'INSERT OR IGNORE INTO customers (user_id) VALUES (?)';

    // Insert customers
    console.log(`Found ${customers.size} unique customers`);
    for (const userId of customers) {
      await db.run(insertCustomerSQL, [userId]);
    }

    console.log(`Importing ${messages.length} messages...`);
    
    for (const message of messages) {
      try {
        await db.run(
          'INSERT INTO messages (customer_id, message_body, timestamp, is_from_customer, status, urgency_level) VALUES (?, ?, ?, ?, ?, ?)',
          [message.customer_id, message.message_body, message.timestamp, message.is_from_customer, message.status, message.urgency_level]
        );
      }catch (error) {
        console.error('Error inserting message:', error);
      }
    }

    console.log('CSV import completed successfully!');
    
    const finalCustomerCount = await db.get('SELECT COUNT(*) as count FROM customers');
    const finalMessageCount = await db.get('SELECT COUNT(*) as count FROM messages');
    
    console.log(`\nSummary:`);
    console.log(`- Customers: ${finalCustomerCount.count}`);
    console.log(`- Messages: ${finalMessageCount.count}`);
    console.log(`- Database Type: ${db.type}`);

  } catch (error) {
    console.error('Error importing CSV data:', error);
  } finally {

  }
}

if (require.main === module) {
  importCSVData().finally(() => {
    if (db.type !== 'postgres') { 
       db.close();
    }
  });
}

module.exports = { importCSVData };