# CS Messaging Web App for Branch

A customer service messaging web application built with React and Node.js that allows multiple agents to respond to customer inquiries in a streamlined fashion.

## Features

- **Multi-agent Support**: Multiple agents can log in simultaneously and respond to messages
- **Real-time Updates**: Polling-based updates for new messages (every 3-5 seconds)
- **Message Management**: View and respond to customer messages with conversation threading
- **Customer Simulation**: Built-in message simulator for testing
- **Modern UI**: Clean, responsive interface built with React and TailwindCSS
- **Database Integration**: SQLITE database with proper schema and CSV import

## Tech Stack

- **Frontend**: React 18, Vite, TailwindCSS
- **Backend**: Node.js, Express
- **Database**: SQLITE
- **Deployment**: Cloud-ready (Vercel + Railway/Render)

## Quick Start

### Prerequisites

- Node.js (v16 or higher)
- PostgreSQL (v12 or higher)
- npm or yarn

### Local Development Setup

1. **Clone and Install Dependencies**
   ```bash
   git clone <repository-url>
   cd cs-messaging-web-app
   npm install
   cd backend && npm install
   cd ../frontend && npm install
   ```

2. **Database Setup**
   ```bash
   # Create PostgreSQL database
   createdb cs_messaging
   
   # Set up environment variables
   cp backend/.env.example backend/.env
   # Edit backend/.env with your database credentials
   ```

3. **Import CSV Data**
   ```bash
   cd backend
   # Place your CSV file in the project root
   node db/importCSV.js ../GeneralistRails_Project_MessageData.csv
   ```

4. **Start Development Servers**
   ```bash
   # From project root
   npm run dev
   ```

   This will start:
   - Backend server on http://localhost:5000
   - Frontend server on http://localhost:5173

5. **Access the Application**
   - Open http://localhost:5173 in your browser
   - Login with any agent name
   - Start responding to customer messages!

## Environment Variables

Create a `.env` file in the `backend` directory:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cs_messaging
DB_USER=postgres
DB_PASSWORD=your_password

# Server Configuration
PORT=5000
NODE_ENV=development

# Frontend URL (for CORS)
FRONTEND_URL=http://localhost:5173
```

## API Endpoints

### Messages
- `GET /api/messages` - Get all conversations with latest message preview
- `GET /api/messages/:customerId` - Get conversation thread for specific customer
- `POST /api/messages/:messageId/respond` - Send agent response
- `POST /api/messages/send` - Simulate customer message

### Agents
- `GET /api/agents` - Get all agents
- `POST /api/agents/login` - Simple agent login (no authentication)
- `POST /api/agents/logout` - Agent logout

## Database Schema

### Tables
- **customers**: Stores customer information (user_id, created_at)
- **agents**: Stores agent information (id, name, is_online, created_at)
- **messages**: Stores all messages (id, customer_id, message_body, timestamp, is_from_customer, agent_id, status)

### Message Status
- `pending` - New customer message awaiting response
- `assigned` - Message assigned to an agent
- `responded` - Agent has responded to the message

## Deployment

### Frontend (Vercel)
1. Connect your GitHub repository to Vercel
2. Set build command: `cd frontend && npm run build`
3. Set output directory: `frontend/dist`
4. Deploy!

### Backend (Railway/Render)
1. Connect your GitHub repository
2. Set build command: `cd backend && npm install`
3. Set start command: `cd backend && npm start`
4. Add environment variables
5. Deploy!

### Database (Railway/Render PostgreSQL)
1. Create a PostgreSQL service
2. Update your backend environment variables with the new database URL
3. Run the CSV import script on the deployed database

## Usage Guide

### For Agents
1. **Login**: Enter your name to access the system
2. **Dashboard**: View all customer conversations with status indicators
3. **Respond**: Click on a conversation to view messages and send responses
4. **Simulator**: Use the message simulator to test new customer messages

### For Testing
1. Use the Message Simulator to create test customer messages
2. Different customer IDs create separate conversations
3. Messages appear in real-time on the dashboard
4. Test the full conversation flow

## Project Structure

```
cs-messaging-web-app/
├── backend/
│   ├── db/
│   │   ├── schema.sql          # Database schema
│   │   └── importCSV.js       # CSV import utility
│   ├── routes/
│   │   ├── messages.js         # Message API endpoints
│   │   └── agents.js          # Agent API endpoints
│   ├── server.js              # Express server
│   ├── package.json
│   └── .env.example
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Login.jsx
│   │   │   ├── Dashboard.jsx
│   │   │   ├── ConversationView.jsx
│   │   │   └── MessageSimulator.jsx
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── package.json
│   └── vite.config.js
├── package.json
└── README.md
```

## Development Notes

- **No Authentication**: As per requirements, no authentication system is implemented
- **Polling**: Uses basic polling instead of WebSockets for real-time updates
- **CSV Import**: The provided CSV data represents customer messages only
- **Agent Responses**: Agents respond through the UI, not through CSV data

## Troubleshooting

### Common Issues
1. **Database Connection**: Ensure PostgreSQL is running and credentials are correct
2. **Port Conflicts**: Change ports in environment variables if needed
3. **CORS Issues**: Update FRONTEND_URL in backend .env file
4. **CSV Import**: Ensure CSV file path is correct and file exists

### Logs
- Backend logs: Check terminal running `npm run dev:backend`
- Frontend logs: Check browser console
- Database logs: Check PostgreSQL logs

## Contributing

This is a demonstration project for Branch's CS Messaging system. For production use, consider adding:
- Authentication and authorization
- WebSocket support for real-time updates
- Message encryption
- Advanced search and filtering
- Analytics and reporting
- Mobile responsiveness improvements

## License

MIT License - See LICENSE file for details

