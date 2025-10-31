let API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';
// Sanitize value (remove trailing slash or accidental semicolon) so built URLs are valid
API_URL = API_URL.replace(/;+$|\/+$/g, '');
console.log('Using API_URL =', API_URL);

/**
 * @param {string} url - The URL path (e.g., '/api/messages')
 * @param {object} options - The standard fetch options (method, headers, body)
 * @returns {Promise<Response>}
 */
export const apiFetch = (url, options) => {
  const fullUrl = url.startsWith('http') ? url : `${API_URL}${url}`;
  return fetch(fullUrl, options);
};
