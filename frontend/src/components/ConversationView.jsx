import React, { useState, useEffect, useRef } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useSocket } from '../contexts/SocketContext';
import { apiFetch } from '../api';
const ConversationView = ({ agent }) => {
  const { customerId } = useParams();
  const [messages, setMessages] = useState([]);
  const [newMessage, setNewMessage] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [isSending, setIsSending] = useState(false);
  const [error, setError] = useState(null);
  const { socket, isConnected } = useSocket();
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const fetchMessages = async () => {
    try {
      const response = await apiFetch(`/api/messages/${customerId}`);
      if (response.ok) {
        const data = await response.json();
        setMessages(data);
        setError(null);
      } else {
        setError('Failed to fetch messages');
      }
    } catch (err) {
      setError('Network error');
      console.error('Error fetching messages:', err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchMessages();
    
    if (socket) {
      socket.emit('join-conversation', customerId);
      
      const handleNewMessage = (message) => {
        if (message.customer_id === parseInt(customerId)) {
          console.log('New message received:', message);
          setMessages(prev => ([...prev, message]));
        }
      };
      
      socket.on('new-message', handleNewMessage);
      socket.on('conversation-updated', fetchMessages);
    }
    
    return () => {
      if (socket) {
        socket.emit('leave-conversation', customerId);
        socket.off('new-message');
        socket.off('conversation-updated', fetchMessages);
      }
    };
  }, [socket, customerId]);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSendMessage = async (e) => {
    e.preventDefault();
    if (!newMessage.trim()) return;

    setIsSending(true);
    try {
      const latestCustomerMessage = messages
        .filter(msg => msg.is_from_customer)
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))[0];

      if (!latestCustomerMessage) {
        alert('No customer message found to respond to');
        setIsSending(false);
        return;
      }

      const response = await apiFetch(`/api/messages/${latestCustomerMessage.id}/respond`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          messageBody: newMessage.trim(),
          agentId: agent.id,
        }),
      });

      if (response.ok) {
        setNewMessage('');
      } else {
        const errorData = await response.json();
        alert(`Failed to send message: ${errorData.error}`);
        await fetchMessages();
      }
    } catch (err) {
      console.error('Error sending message:', err);
      alert('Failed to send message. Please try again.');
    } finally {
      setIsSending(false);
    }
  };

  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600 dark:border-indigo-400"></div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl"> 
      <div className="mb-6 flex justify-between items-center">
        <div>
          <Link
            to="/dashboard"
            className="text-indigo-600 hover:text-indigo-500 dark:text-indigo-400 dark:hover:text-indigo-300 text-sm font-medium"
          >
            ← Back to Dashboard
          </Link>
          <h2 className="mt-2 text-2xl font-bold text-gray-900 dark:text-white">
            Conversation with Customer #{customerId}
          </h2>
        </div>
        <div className="flex items-center space-x-2">
            <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="text-sm text-gray-600 dark:text-gray-400">
              {isConnected ? 'Connected' : 'Disconnected'}
            </span>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-6 dark:bg-red-900 dark:border-red-700">
          <div className="text-red-800 dark:text-red-200">{error}</div>
        </div>
      )}

      <div className="bg-white dark:bg-gray-800 shadow rounded-lg">
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-lg font-medium text-gray-900 dark:text-white">Messages</h3>
        </div>
        
        <div className="px-6 py-4 h-96 overflow-y-auto dark:bg-gray-800">
          {messages.length === 0 ? (
            <p className="text-gray-500 dark:text-gray-400 text-center py-8">No messages found</p>
          ) : (
            <div className="space-y-4">
              {messages.map((message) => (
                <div
                  key={message.id}
                  className={`flex ${message.is_from_customer ? 'justify-start' : 'justify-end'}`}
                >
                  <div
                    className={`max-w-xs lg:max-w-md px-4 py-2 rounded-lg ${
                      message.is_from_customer
                        ? 'bg-gray-100 text-gray-900 dark:bg-gray-700 dark:text-gray-100'
                        : 'bg-indigo-600 text-white'
                    }`}
                  >
                    <p className="text-sm whitespace-pre-wrap">{message.message_body}</p>
                    <p
                      className={`text-xs mt-1 ${
                        message.is_from_customer ? 'text-gray-500 dark:text-gray-400' : 'text-indigo-200'
                      }`}
                    >
                      {formatTimestamp(message.timestamp)}
                      {!message.is_from_customer && message.agent_name && (
                        <span> • {message.agent_name}</span>
                      )}
                    </p>
                  </div>
                </div>
              ))}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700">
          <form onSubmit={handleSendMessage} className="flex space-x-4">
            <input
              type="text"
              value={newMessage}
              onChange={(e) => setNewMessage(e.target.value)}
              placeholder="Type your response..."
              className="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
              disabled={isSending}
            />
            <button
              type="submit"
              disabled={isSending || !newMessage.trim()}
              className="px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSending ? 'Sending...' : 'Send'}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
};

export default ConversationView;

