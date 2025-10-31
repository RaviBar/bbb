import React, { createContext, useContext, useState } from 'react';

const FilterContext = createContext();

export const useFilters = () => {
  const context = useContext(FilterContext);
  if (!context) {
    throw new Error('useFilters must be used within a FilterProvider');
  }
  return context;
};

export const FilterProvider = ({ children }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filters, setFilters] = useState({
    status: 'all',    
    priority: 'all',   
    assignment: 'all', 
  });

  const clearFilters = () => {
    setSearchTerm('');
    setFilters({ status: 'all', priority: 'all', assignment: 'all' });
  };
  
  const isFiltered = searchTerm || filters.status !== 'all' || filters.priority !== 'all' || filters.assignment !== 'all';

  const value = {
    searchTerm,
    setSearchTerm,
    filters,
    setFilters,
    clearFilters,
    isFiltered
  };

  return (
    <FilterContext.Provider value={value}>
      {children}
    </FilterContext.Provider>
  );
};

