const fs = require('fs');
const csv = require('csv-parser');
const db = require('./database');

async function importCSVData() {
  try {
    console.log('Starting CSV import...');
    
    const csvPath = process.argv[2] || '../GeneralistRails_Project_MessageData.csv';
    
    if (!fs.existsSync(csvPath)) {
      console.log('CSV file not found. Please provide the correct path.');
      console.log('Usage: node importCSV.js <path-to-csv-file>');
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

          customers.add(userId);
          messages.push({
            customer_id: userId,
            message_body: messageBody,
            timestamp: timestamp.toISOString(),
            is_from_customer: true,
            status: 'pending'
          });
        })
        .on('end', resolve)
        .on('error', reject);
    });

    // Insert customers
    console.log(`Found ${customers.size} unique customers`);
    for (const userId of customers) {
      await db.run('INSERT OR IGNORE INTO customers (user_id) VALUES (?)', [userId]);
    }

    // Insert messages
    console.log(`Importing ${messages.length} messages...`);
    let insertedCount = 0;
    
    for (const message of messages) {
      try {
        await db.run(
          'INSERT INTO messages (customer_id, message_body, timestamp, is_from_customer, status) VALUES (?, ?, ?, ?, ?)',
          [message.customer_id, message.message_body, message.timestamp, message.is_from_customer, message.status]
        );
        insertedCount++;
      } catch (error) {
        console.error('Error inserting message:', error);
      }
    }

    console.log('CSV import completed successfully!');
    
    const customerCount = await db.get('SELECT COUNT(*) as count FROM customers');
    const messageCount = await db.get('SELECT COUNT(*) as count FROM messages');
    
    console.log(`\nSummary:`);
    console.log(`- Customers: ${customerCount.count}`);
    console.log(`- Messages: ${messageCount.count}`);
    console.log(`- Database Type: ${db.type}`);

  } catch (error) {
    console.error('Error importing CSV data:', error);
  } finally {
    db.close();
  }
}

if (require.main === module) {
  importCSVData();
}

module.exports = { importCSVData };