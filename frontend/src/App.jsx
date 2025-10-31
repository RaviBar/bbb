import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { SocketProvider } from './contexts/SocketContext';
import { FilterProvider } from './contexts/FilterContext';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import ConversationView from './components/ConversationView';
import Sidebar from './components/Sidebar'; 
import ThemeToggle from './components/ThemeToggle';
import './App.css';
import logo from '../public/branch-logo.png'; 
import { apiFetch } from './api';

function App() {
  const [agent, setAgent] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const savedAgent = localStorage.getItem('agent');
    if (savedAgent) {
      setAgent(JSON.parse(savedAgent));
    }
  }, []);

  const handleLogin = async (agentName) => {
    setIsLoading(true);
    try {
      const response = await apiFetch('/api/agents/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: agentName }),
      });

      if (response.ok) {
        const data = await response.json();
        setAgent(data.agent);
        localStorage.setItem('agent', JSON.stringify(data.agent));
      } else {
        const error = await response.json();
        alert(`Login failed: ${error.error}`);
      }
    } catch (error) {
      console.error('Login error:', error);
      alert('Login failed. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleLogout = async () => {
    try {
      if (agent) {
        await apiFetch('/api/agents/logout', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ agentId: agent.id }),
        });
      }
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      setAgent(null);
      localStorage.removeItem('agent');
    }
  };

  return (
    <FilterProvider>
      <SocketProvider>
        <Router>
          <div className="min-h-screen bg-gray-50 dark:bg-gray-950">
            
            {/* --- HEADER --- */}
            <header className="bg-white dark:bg-gray-900 shadow-sm dark:border-b dark:border-gray-700 sticky top-0 z-10 h-[65px]">
              <div className="max-w-full mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex justify-between items-center py-4">
                  <div className="flex items-center">
                  <img src={logo} alt="Branch International Logo" className="h-8 w-auto" />
                    <h1 className="text-xl font-semibold text-blue-900 dark:text-white ml-[3px]">International</h1>
                    {agent && (
                      <span className="ml-4 px-3 py-1 bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 text-sm font-medium rounded-full">
                        Agent: {agent.name}
                      </span>
                    )}
                  </div>
                  {agent && (
                    <div className="flex items-center space-x-2">
                      <ThemeToggle />
                      <button
                        onClick={handleLogout}
                        className="px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-md hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500"
                      >
                        Logout
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </header>

            <div className="flex">
              
              {agent && <Sidebar />}
              {/* Main Content Area */}
              <main className="flex-1 p-4 sm:p-6 lg:p-8 max-w-full overflow-x-auto">
                <Routes>
                  <Route 
                    path="/login" 
                    element={
                      agent ? <Navigate to="/dashboard" replace /> : 
                      <Login onLogin={handleLogin} isLoading={isLoading} />
                    } 
                  />
                  <Route 
                    path="/dashboard" 
                    element={
                      agent ? <Dashboard agent={agent} /> : 
                      <Navigate to="/login" replace />
                    } 
                  />
                  <Route 
                    path="/conversation/:customerId" 
                    element={
                      agent ? <ConversationView agent={agent} /> : 
                      <Navigate to="/login" replace />
                    } 
                  />
                  {/* The /simulator route is no longer needed since it's in the sidebar */}
                  <Route 
                    path="/simulator" 
                    element={
                      <Navigate to="/dashboard" replace />
                    } 
                  />
                  <Route path="/" element={<Navigate to="/login" replace />} />
                </Routes>
              </main>
            </div>
          </div>
        </Router>
      </SocketProvider>
    </FilterProvider>
  );
}

export default App;

