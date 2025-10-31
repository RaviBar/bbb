import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useSocket } from '../contexts/SocketContext';

const MessageSimulator = () => {
  const [customerId, setCustomerId] = useState('');
  const [messageBody, setMessageBody] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [success, setSuccess] = useState(false);
  const { isConnected } = useSocket();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!customerId.trim() || !messageBody.trim()) return;

    setIsSending(true);
    setSuccess(false);
    
    try {
      const response = await fetch('/api/messages/send', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          customerId: parseInt(customerId),
          messageBody: messageBody.trim(),
        }),
      });

      if (response.ok) {
        setSuccess(true);
        setCustomerId('');
        setMessageBody('');
        setTimeout(() => setSuccess(false), 3000);
      } else {
        const error = await response.json();
        alert(`Failed to send message: ${error.error}`);
      }
    } catch (err) {
      console.error('Error sending message:', err);
      alert('Failed to send message. Please try again.');
    } finally {
      setIsSending(false);
    }
  };

  return (
    <div className="max-w-2xl mx-auto">
      <div className="mb-6">
        <Link
          to="/dashboard"
          className="text-indigo-600 hover:text-indigo-500 text-sm font-medium"
        >
          ← Back to Dashboard
        </Link>
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">
              Customer Message Simulator
            </h2>
            <p className="mt-1 text-sm text-gray-600">
              Simulate incoming customer messages for testing purposes
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}></div>
            <span className="text-sm text-gray-600">
              {isConnected ? 'Real-time' : 'Offline'}
            </span>
          </div>
        </div>
      </div>

      {success && (
        <div className="bg-green-50 border border-green-200 rounded-md p-4 mb-6">
          <div className="text-green-800">Message sent successfully!</div>
        </div>
      )}

      <div className="bg-white shadow rounded-lg">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-900">Send Test Message</h3>
        </div>
        
        <form onSubmit={handleSubmit} className="px-6 py-4 space-y-4">
          <div>
            <label htmlFor="customer-id" className="block text-sm font-medium text-gray-700">
              Customer ID
            </label>
            <input
              type="number"
              id="customer-id"
              value={customerId}
              onChange={(e) => setCustomerId(e.target.value)}
              placeholder="Enter customer ID (e.g., 1234)"
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
              disabled={isSending}
              required
            />
          </div>
          
          <div>
            <label htmlFor="message-body" className="block text-sm font-medium text-gray-700">
              Message Body
            </label>
            <textarea
              id="message-body"
              rows={4}
              value={messageBody}
              onChange={(e) => setMessageBody(e.target.value)}
              placeholder="Enter the customer message..."
              className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
              disabled={isSending}
              required
            />
          </div>
          
          <div className="flex justify-end">
            <button
              type="submit"
              disabled={isSending || !customerId.trim() || !messageBody.trim()}
              className="px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSending ? 'Sending...' : 'Send Message'}
            </button>
          </div>
        </form>
      </div>

      <div className="mt-8 bg-blue-50 border border-blue-200 rounded-md p-4">
        <h4 className="text-sm font-medium text-blue-800 mb-2">Tips:</h4>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• Use any customer ID number (e.g., 1234, 5678)</li>
          <li>• Messages will appear in the dashboard as new conversations</li>
          <li>• You can respond to messages from the conversation view</li>
          <li>• Try different customer IDs to simulate multiple customers</li>
        </ul>
      </div>
    </div>
  );
};

export default MessageSimulator;

