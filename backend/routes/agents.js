const express = require('express');
const db = require('../db/database');

module.exports = (io) => {
  const router = express.Router();

  // GET /api/agents - Get all agents
  router.get('/', async (req, res) => {
    try {
      const rows = await db.query('SELECT * FROM agents ORDER BY created_at DESC');
      res.json(rows);
    } catch (error) {
      console.error('Error fetching agents:', error);
      res.status(500).json({ error: 'Failed to fetch agents' });
    }
  });

  // POST /api/agents/login - Simple agent login (no authentication)
  router.post('/login', async (req, res) => {
    try {
      const { name } = req.body;
      
      if (!name) {
        return res.status(400).json({ error: 'Agent name is required' });
      }
      
      // Check if agent exists
      let agent = await db.get('SELECT * FROM agents WHERE name = ?', [name]);
      
      if (!agent) {
        // Create new agent
        const result = await db.run('INSERT INTO agents (name, is_online) VALUES (?, ?)', [name, true]);
        agent = { id: result.lastID, name, is_online: true };
      } else {
        // Update existing agent to online
        await db.run('UPDATE agents SET is_online = ? WHERE name = ?', [true, name]);
        agent = { ...agent, is_online: true };
      }
      
      // Emit agent login event
      io.emit('agent-logged-in', agent);
      
      res.json({ 
        success: true, 
        agent,
        message: 'Login successful' 
      });
    } catch (error) {
      console.error('Error logging in agent:', error);
      res.status(500).json({ error: 'Failed to login agent' });
    }
  });

  // POST /api/agents/logout - Agent logout
  router.post('/logout', async (req, res) => {
    try {
      const { agentId } = req.body;
      
      if (!agentId) {
        return res.status(400).json({ error: 'Agent ID is required' });
      }
      
      await db.run('UPDATE agents SET is_online = ? WHERE id = ?', [false, agentId]);
      
      // Emit agent logout event
      io.emit('agent-logged-out', { agentId });
      
      res.json({ 
        success: true, 
        message: 'Logout successful' 
      });
    } catch (error) {
      console.error('Error logging out agent:', error);
      res.status(500).json({ error: 'Failed to logout agent' });
    }
  });

  return router;
};