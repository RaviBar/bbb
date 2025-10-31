const express = require('express');
const cors = require('cors');
const { createServer } = require('http');
const { Server } = require('socket.io');
const db = require('./db/database');
require('dotenv').config();

const app = express();
const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.FRONTEND_URL || 'http://localhost:5173',
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors({
  origin: process.env.FRONTEND_URL || 'http://localhost:5173',
  credentials: true
}));
app.use(express.json());

// Routes
app.use('/api/messages', require('./routes/messages')(io));
app.use('/api/agents', require('./routes/agents')(io));

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// [NEW] Define this helper function here
const handleSimulatedMessage = async (data, callback) => {
  try {
    const { customerId, messageBody } = data;
    if (!customerId || !messageBody) {
      return callback({ success: false, error: 'Customer ID and message body are required' });
    }

    await db.run('INSERT OR IGNORE INTO customers (user_id) VALUES (?)', [customerId]);

    const lower = messageBody.toLowerCase();
    const urgentKeywords = [
      'loan', 'approval', 'disbursed', 'urgent', 'help', 
      'immediate', 'rejected', 'denied', 'payment', 
      'batch number', 'validate', 'review', 'crb', 
      'clearance', 'pay'
    ];
    const isUrgent = urgentKeywords.some(k => lower.includes(k));
    const urgency = isUrgent ? 'high' : 'normal';

    const r = await db.run(
      `INSERT INTO messages (customer_id, message_body, timestamp, is_from_customer, status, urgency_level, current_agent_id)
       VALUES (?, ?, ?, 1, 'pending', ?, NULL)`,
      [customerId, messageBody, new Date().toISOString(), urgency]
    );
    
    const newMessage = await db.get('SELECT * FROM messages WHERE id = ?', [r.lastID]);
    
    const conversationRoom = `conversation-${customerId}`;
    io.to(conversationRoom).emit('new-message', newMessage);
    io.emit('new-customer-message', { id: r.lastID, customer_id: customerId });
    
    callback({ success: true, messageId: r.lastID });
  } catch (e) {
    console.error('Error in simulation socket:', e);
    callback({ success: false, error: 'Failed to send message' });
  }
};


// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  
  socket.on('agent-login', (agentId) => {
    socket.join(`agent-${agentId}`);
    console.log(`Agent ${agentId} joined room`);
  });
  
  socket.on('join-conversation', (customerId) => {
    socket.join(`conversation-${customerId}`);
    console.log(`Client joined conversation ${customerId}`);
  });
  
  socket.on('leave-conversation', (customerId) => {
    socket.leave(`conversation-${customerId}`);
    console.log(`Client left conversation ${customerId}`);
  });
  
  // [NEW] Handle simulator message via socket
  socket.on('simulate-message', handleSimulatedMessage);

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`WebSocket server ready`);
});

module.exports = { app, io };

