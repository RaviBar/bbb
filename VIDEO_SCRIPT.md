# Video Recording Script for CS Messaging Web App

## Overview (30 seconds)
- "This is the Branch CS Messaging Web App demonstration"
- "Built with React frontend, Node.js backend, and PostgreSQL database"
- "Allows multiple agents to respond to customer inquiries efficiently"

## Setup Demo (1 minute)
1. **Show the project structure**
   - "Monorepo with frontend and backend directories"
   - "Package.json files for both React and Node.js"

2. **Database setup**
   - "PostgreSQL database with proper schema"
   - "CSV import utility for customer messages"

3. **Environment configuration**
   - "Environment variables for database connection"
   - "CORS configuration for frontend-backend communication"

## Core Features Demo (3 minutes)

### 1. Agent Login (30 seconds)
- "Simple agent login - no authentication required"
- "Enter any agent name to access the system"
- "Agent status is tracked in the database"

### 2. Dashboard View (1 minute)
- "Dashboard shows all customer conversations"
- "Message previews with status indicators"
- "Real-time updates via polling every 5 seconds"
- "Pending message counts and agent assignments"

### 3. Conversation Management (1 minute)
- "Click on any conversation to view full thread"
- "Customer messages on the left, agent responses on the right"
- "Real-time message updates every 3 seconds"
- "Send responses directly from the conversation view"

### 4. Message Simulator (30 seconds)
- "Built-in simulator to test new customer messages"
- "Enter customer ID and message body"
- "Messages appear immediately in the dashboard"

## Technical Highlights (1 minute)

### Backend API
- "RESTful API with Express.js"
- "PostgreSQL database with proper relationships"
- "CORS-enabled for frontend communication"

### Frontend Features
- "React with modern hooks and routing"
- "TailwindCSS for responsive design"
- "Real-time polling for message updates"

### Database Design
- "Customers, agents, and messages tables"
- "Proper foreign key relationships"
- "Status tracking for message workflow"

## Code Walkthrough (1 minute)

### Key Files
1. **Backend Routes** (`backend/routes/messages.js`)
   - "Message CRUD operations"
   - "Agent response handling"
   - "Customer message simulation"

2. **Frontend Components** (`frontend/src/components/`)
   - "Dashboard with conversation list"
   - "ConversationView with real-time updates"
   - "MessageSimulator for testing"

3. **Database Schema** (`backend/db/schema.sql`)
   - "Proper table relationships"
   - "Indexes for performance"
   - "Status constraints"

## Deployment Ready (30 seconds)
- "Cloud-ready with Railway/Render for backend"
- "Vercel deployment for frontend"
- "Docker Compose for local development"
- "Comprehensive documentation included"

## Total Time: ~6 minutes

## Tips for Recording
- Use a clean terminal with good contrast
- Show the browser developer tools network tab during API calls
- Demonstrate the real-time updates by sending messages from simulator
- Keep the demo focused on core functionality
- Have the CSV data pre-imported for immediate demo

