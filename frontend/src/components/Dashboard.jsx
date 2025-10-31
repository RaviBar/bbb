import React, { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useSocket } from '../contexts/SocketContext';
import { useFilters } from '../contexts/FilterContext'; 
import { apiFetch } from '../api';
const Dashboard = ({ agent }) => {
  const [conversations, setConversations] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const { socket, isConnected } = useSocket();

  const { searchTerm, filters, isFiltered } = useFilters();

  const fetchConversations = async () => {
    const isFirstLoad = isLoading;
    try {
      const response = await apiFetch('/api/messages');
      if (!response.ok) throw new Error('Failed to fetch conversations');
      const data = await response.json();
      setConversations(Array.isArray(data) ? data : []);
      setError(null);
    } catch (err) {
      setError('Network error');
      console.error('Error fetching conversations:', err);
    } finally {
      if (isFirstLoad) {
        setIsLoading(false);
      }
    }
  };

  useEffect(() => {
    fetchConversations(); 

    if (socket) {
      socket.on('new-customer-message', fetchConversations);
      socket.on('conversation-updated', fetchConversations);
      socket.emit('agent-login', agent.id);
    }
    
    return () => {
      if (socket) {
        socket.off('new-customer-message', fetchConversations);
        socket.off('conversation-updated', fetchConversations);
      }
    };
  }, [socket, agent]); 

  const filteredConversations = useMemo(() => {
    return conversations.filter(convo => {
      const searchLower = searchTerm.toLowerCase();
      
      if (searchTerm) {
        const inId = convo.customer_id.toString().includes(searchLower);
        const inMessage = (convo.latest_message || '').toLowerCase().includes(searchLower);
        if (!inId && !inMessage) {
          return false;
        }
      }

      if (filters.status !== 'all') {
        const computedStatus = convo.pending_count > 0 ? 'pending' : (convo.current_agent_id ? 'assigned' : convo.status);
        
        if (filters.status === 'pending' && convo.pending_count === 0) return false;
        if (filters.status === 'responded' && convo.status !== 'responded') return false;
        if (filters.status === 'assigned' && !convo.current_agent_id) return false;
      }

      if (filters.priority !== 'all') {
        if (convo.urgency_level !== filters.priority) {
          return false;
        }
      }
      
      if (filters.assignment !== 'all') {
        const isMine = convo.current_agent_id === agent.id;
        const isUnassigned = convo.current_agent_id === null;

        if (filters.assignment === 'mine' && !isMine) {
          return false;
        }
        if (filters.assignment === 'unassigned' && !isUnassigned) {
          return false;
        }
      }
      return true;
    });
  }, [conversations, searchTerm, filters, agent.id]);

  const getStatusColor = (status, pendingCount, currentAgentId) => {
    if (pendingCount > 0) return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200';
    if (currentAgentId === agent.id) return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
    if (currentAgentId && currentAgentId !== agent.id) return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
    if (status === 'responded') return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
    if (status === 'assigned') return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200';
    return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200';
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return '';
    const date = new Date(timestamp);
    const diff = (Date.now() - date.getTime()) / 3600000;
    if (diff < 1) return `${Math.round(diff * 60)}m ago`;
    if (diff < 24) return `${Math.round(diff)}h ago`;
    return date.toLocaleDateString();
  };

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600 dark:border-indigo-400"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div className="flex items-center space-x-4">
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Customer Conversations</h2>
          <div className="flex items-center space-x-2">
            <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="text-sm text-gray-600 dark:text-gray-400">
              {isConnected ? 'Connected' : 'Disconnected'}
            </span>
          </div>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4 dark:bg-red-900 dark:border-red-700 dark:text-red-200">
          <div className="text-red-800 dark:text-red-200">{error}</div>
        </div>
      )}

      <div className="bg-white dark:bg-gray-800 shadow overflow-hidden sm:rounded-md">
        <ul className="divide-y divide-gray-200 dark:divide-gray-700">
          {filteredConversations.length === 0 ? (
            <li className="px-6 py-4 text-center text-gray-500 dark:text-gray-400">
              {isFiltered 
                ? "No conversations match your filters." 
                : "No conversations found. Use the Message Simulator to create test incoming messages."
              }
            </li>
          ) : (
            filteredConversations.map((conversation) => {
              const mine       = conversation.current_agent_id === agent.id;
              const assigned   = Boolean(conversation.current_agent_id);
              const someoneElse = assigned && !mine;

              const StatusPill = () => (
                <span
                  className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor(
                    conversation.status,
                    conversation.pending_count,
                    conversation.current_agent_id
                  )}`}
                >
                  {conversation.pending_count > 0 ? `${conversation.pending_count} pending` : conversation.status}
                </span>
              );

              const AssignmentBadge = () => {
                if (mine) {
                  return (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
                      Assigned to You
                    </span>
                  );
                }
                if (someoneElse) {
                  return (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">
                      In Progress (Agent: {conversation.current_agent_name || '...'})
                    </span>
                  );
                }
                return (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200">
                    Unassigned
                  </span>
                );
              };

              const UrgencyBadge = () => {
                const u = (conversation.urgency_level || 'normal').toLowerCase();
                const cls = u === 'high' ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200' : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200';
                const label = u === 'high' ? 'High' : 'Normal';
                return (
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${cls}`}>
                    {label} Priority
                  </span>
                );
              };

              if (someoneElse) {
                return (
                  <li key={conversation.customer_id}>
                    <div className="block px-6 py-4 bg-gray-100 dark:bg-gray-900 cursor-not-allowed select-none" aria-disabled>
                      <div className="flex items-center justify-between">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center space-x-2 sm:space-x-3">
                            <p className="text-sm font-medium text-gray-500 dark:text-gray-400 truncate">
                              Customer #{conversation.customer_id}
                            </p>
                            <StatusPill />
                            <AssignmentBadge />
                            <UrgencyBadge />
                          </div>
                          <div className="mt-1">
                            <p className="text-sm text-gray-500 dark:text-gray-400 truncate">
                              {conversation.latest_message}
                            </p>
                          </div>
                        </div>
                        <div className="flex-shrink-0 text-right">
                          <p className="text-sm text-gray-500 dark:text-gray-400">{formatTimestamp(conversation.latest_timestamp)}</p>
                          <p className="text-xs text-gray-400 dark:text-gray-500">{conversation.total_messages} messages</p>
                        </div>
                      </div>
                    </div>
                  </li>
                );
              }

              return (
                <li key={conversation.customer_id}>
                  <Link to={`/conversation/${conversation.customer_id}`} className="block hover:bg-gray-50 dark:hover:bg-gray-700 px-6 py-4">
                    <div className="flex items-center justify-between">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center space-x-2 sm:space-x-3">
                          <p className="text-sm font-medium text-indigo-600 dark:text-indigo-400 truncate">
                            Customer #{conversation.customer_id}
                          </p>
                          <StatusPill />
                          <AssignmentBadge />
                          <UrgencyBadge />
                        </div>
                        <div className="mt-1">
                          <p className="text-sm text-gray-600 dark:text-gray-300 truncate">{conversation.latest_message}</p>
                        </div>
                      </div>
                      <div className="flex-shrink-0 text-right">
                        <p className="text-sm text-gray-500 dark:text-gray-400">{formatTimestamp(conversation.latest_timestamp)}</p>
                        <p className="text-xs text-gray-400 dark:text-gray-500">{conversation.total_messages} messages</p>
                      </div>
                    </div>
                  </Link>
                </li>
              );
            })
          )}
        </ul>
      </div>
    </div>
  );
};

export default Dashboard;