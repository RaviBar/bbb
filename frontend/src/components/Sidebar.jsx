import React, { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { useSocket } from '../contexts/SocketContext';
import { useFilters } from '../contexts/FilterContext';

const FilterButton = ({ children, onClick, isActive, className = '' }) => (
  <button
    onClick={onClick}
    className={`w-full text-left px-3 py-1.5 text-sm rounded-md ${
      isActive
        ? 'bg-indigo-100 text-indigo-700 dark:bg-indigo-800 dark:text-white'
        : 'text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700'
    } ${className}`}
  >
    {children}
  </button>
);

const Simulator = () => {
  const [customerId, setCustomerId] = useState('');
  const [messageBody, setMessageBody] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [success, setSuccess] = useState(false);
  const { socket } = useSocket();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!customerId.trim() || !messageBody.trim() || !socket) return;

    setIsSending(true);
    setSuccess(false);
    
    try {
      socket.emit('simulate-message', {
        customerId: parseInt(customerId),
        messageBody: messageBody.trim(),
      }, (response) => {
        if (response.success) {
          setSuccess(true);
          setCustomerId('');
          setMessageBody('');
          setTimeout(() => setSuccess(false), 3000);
        } else {
          alert(`Failed to send message: ${response.error}`);
        }
        setIsSending(false);
      });
    } catch (err) {
      console.error('Error sending message:', err);
      alert('Failed to send message. Please try again.');
      setIsSending(false);
    }
  };
  
  return (
    <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
      <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">
        Message Simulator
      </h3>
      <form onSubmit={handleSubmit} className="space-y-3">
        {success && (
          <div className="text-xs text-green-700 dark:text-green-400">
            Message sent successfully!
          </div>
        )}
        <div>
          <input
            type="number"
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            placeholder="Customer ID (e.g., 1234)"
            className="block w-full px-2 py-1.5 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-xs dark:bg-gray-700 dark:border-gray-600 dark:text-white"
            disabled={isSending}
            required
          />
        </div>
        <div>
          <textarea
            rows={3}
            value={messageBody}
            onChange={(e) => setMessageBody(e.target.value)}
            placeholder="Enter test message..."
            className="block w-full px-2 py-1.5 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-xs dark:bg-gray-700 dark:border-gray-600 dark:text-white"
            disabled={isSending}
            required
          />
        </div>
        <button
          type="submit"
          disabled={isSending || !customerId.trim() || !messageBody.trim()}
          className="w-full px-3 py-1.5 bg-indigo-600 text-white text-xs font-medium rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-50"
        >
          {isSending ? 'Sending...' : 'Send Test Message'}
        </button>
      </form>
    </div>
  );
};


const Sidebar = () => {
  const { searchTerm, setSearchTerm, filters, setFilters, clearFilters, isFiltered } = useFilters();

  const handleFilterChange = (filterType, value) => {
    setFilters(prev => ({ ...prev, [filterType]: value }));
  };

  return (
    <aside className="w-64 flex-shrink-0 border-r border-gray-200 bg-white dark:bg-gray-900 dark:border-gray-700 h-screen sticky top-[65px] overflow-y-auto pb-10">
      <div className="p-4 space-y-4">
        
        {/* Navigation */}
        <div className="space-y-1">
          <NavLink 
            to="/dashboard"
            className={({ isActive }) =>
              `flex items-center px-3 py-2 text-sm font-medium rounded-md ${
              isActive 
                ? 'bg-gray-100 text-gray-900 dark:bg-gray-800 dark:text-white' 
                : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'
            }`
            }
          >
            Dashboard
          </NavLink>
        </div>

        {/* Search */}
        <div>
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="Search ID or message..."
            className="block w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm dark:bg-gray-800 dark:border-gray-600 dark:text-white"
          />
        </div>

        {/* Filter Labels */}
        <div className="space-y-3 pt-2">
          <div className="flex justify-between items-center">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Filters</h3>
            {isFiltered && (
              <button
                onClick={clearFilters}
                className="text-xs text-indigo-600 dark:text-indigo-400 hover:underline"
              >
                Clear All
              </button>
            )}
          </div>

          <div className="space-y-1">
            <h4 className="text-sm font-medium text-gray-900 dark:text-white">Assignment</h4>
            <FilterButton onClick={() => handleFilterChange('assignment', 'all')} isActive={filters.assignment === 'all'}>All</FilterButton>
            <FilterButton onClick={() => handleFilterChange('assignment', 'mine')} isActive={filters.assignment === 'mine'}>Assigned to You</FilterButton>
            <FilterButton onClick={() => handleFilterChange('assignment', 'unassigned')} isActive={filters.assignment === 'unassigned'}>Unassigned</FilterButton>
          </div>

          <div className="space-y-1">
            <h4 className="text-sm font-medium text-gray-900 dark:text-white">Status</h4>
            <FilterButton onClick={() => handleFilterChange('status', 'all')} isActive={filters.status === 'all'}>All</FilterButton>
            <FilterButton onClick={() => handleFilterChange('status', 'pending')} isActive={filters.status === 'pending'}>Pending</FilterButton>
            <FilterButton onClick={() => handleFilterChange('status', 'responded')} isActive={filters.status === 'responded'}>Responded</FilterButton>
          </div>
          
          <div className="space-y-1">
            <h4 className="text-sm font-medium text-gray-900 dark:text-white">Priority</h4>
            <FilterButton onClick={() => handleFilterChange('priority', 'all')} isActive={filters.priority === 'all'}>All</FilterButton>
            <FilterButton onClick={() => handleFilterChange('priority', 'high')} isActive={filters.priority === 'high'} className="!text-red-700 dark:!text-red-400">High</FilterButton>
            <FilterButton onClick={() => handleFilterChange('priority', 'normal')} isActive={filters.priority === 'normal'}>Normal</FilterButton>
          </div>
        </div>

        {/* Simulator */}
        <div className="pt-4">
          <Simulator />
        </div>

      </div>
    </aside>
  );
};

export default Sidebar;

