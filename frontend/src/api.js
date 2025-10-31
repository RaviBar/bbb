export const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

/**
 * @param {string} url - The URL path (e.g., '/api/messages')
 * @param {object} options - The standard fetch options (method, headers, body)
 * @returns {Promise<Response>}
 */
export const apiFetch = (url, options) => {
  const fullUrl = url.startsWith('http') ? url : `${API_URL}${url}`;
  return fetch(fullUrl, options);
};
