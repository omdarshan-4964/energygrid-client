const crypto = require('crypto');

/**
 * Generate MD5 signature for API authentication
 * @param {string} url - The API endpoint URL
 * @param {string} token - Secret token from .env
 * @param {number} timestamp - Unix timestamp in milliseconds
 * @returns {string} MD5 hash in hexadecimal format
 */
function generateSignature(url, token, timestamp) {
  const payload = `${url}${token}${timestamp}`;
  return crypto.createHash('md5').update(payload).digest('hex');
}

/**
 * Split array into chunks of specified size
 * @param {Array} array - Array to chunk
 * @param {number} size - Size of each chunk (max 10 for API)
 * @returns {Array} Array of chunks
 */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * Sleep utility for rate limiting (1 request per second)
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise} Promise that resolves after delay
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get formatted range string for batch display
 * @param {Array} array - Array of items
 * @returns {string} Formatted range string (e.g., "SN-000 to SN-009")
 */
function getBatchRange(array) {
  if (!array || array.length === 0) return '';
  if (array.length === 1) return array[0];
  return `${array[0]} to ${array[array.length - 1]}`;
}

module.exports = {
  generateSignature,
  chunkArray,
  sleep,
  getBatchRange
};
