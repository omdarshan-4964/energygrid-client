require('dotenv').config();
const axios = require('axios');
const fs = require('fs').promises;
const { generateSignature, chunkArray, sleep, getBatchRange } = require('./utils');

// Configuration Constants
const API_BASE_URL = process.env.API_BASE_URL || 'http://localhost:3000';
const SECRET_TOKEN = process.env.SECRET_TOKEN;
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

// API Constraints
const BATCH_SIZE = 10; // Max serial numbers per request
const RATE_LIMIT_MS = 1000; // 1 request per second
const MAX_RETRIES = 3;
const REQUEST_TIMEOUT_MS = 10000; // 10 second timeout

// Serial Number Configuration
const SERIAL_NUMBER_COUNT = 500;
const SERIAL_NUMBER_PREFIX = 'SN-';
const SERIAL_NUMBER_PADDING = 3;

// Environment Validation
if (!SECRET_TOKEN) {
  console.error('❌ FATAL: SECRET_TOKEN not found in environment variables');
  console.error('💡 Create a .env file with: SECRET_TOKEN=interview_token_123');
  process.exit(1);
}

// Enhanced Logger
const logger = {
  debug: (msg) => LOG_LEVEL === 'debug' && console.log(`[DEBUG] ${msg}`),
  info: (msg) => console.log(msg),
  error: (msg) => console.error(msg)
};

/**
 * Fetch telemetry data for a batch of serial numbers
 * @param {Array} serialNumbers - Array of serial numbers (max 10)
 * @param {number} retryCount - Current retry attempt
 * @returns {Promise} API response data
 */
async function fetchBatchTelemetry(serialNumbers, retryCount = 0) {
  const url = '/device/real/query';
  const fullUrl = `${API_BASE_URL}${url}`;
  const timestamp = Date.now();
  const signature = generateSignature(url, SECRET_TOKEN, timestamp);

  try {
    logger.debug(`Fetching batch: ${getBatchRange(serialNumbers)}`);
    
    const response = await axios.post(
      fullUrl,
      { sn_list: serialNumbers },
      {
        headers: {
          'Content-Type': 'application/json',
          'Signature': signature,
          'Timestamp': timestamp.toString()
        },
        timeout: REQUEST_TIMEOUT_MS
      }
    );

    return response.data;
  } catch (error) {
    // Handle rate limiting (429) or network errors with exponential backoff
    if (error.response?.status === 429 || error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
      if (retryCount < MAX_RETRIES) {
        const backoffDelay = Math.pow(2, retryCount) * 1000;
        logger.info(`⚠️  Rate limited or error. Retrying in ${backoffDelay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
        await sleep(backoffDelay);
        return fetchBatchTelemetry(serialNumbers, retryCount + 1);
      }
    }

    logger.error(`❌ Failed to fetch batch ${getBatchRange(serialNumbers)}: ${error.message}`);
    throw error;
  }
}

/**
 * Main aggregator function to fetch all inverter telemetry data
 * @param {Object} options - Configuration options
 * @param {boolean} options.saveToDisk - Whether to save results to JSON file
 * @returns {Promise<Array>} Array of telemetry data
 */
async function aggregateAllTelemetry(options = {}) {
  const { saveToDisk = false } = options;
  
  logger.info('🚀 Starting EnergyGrid Data Aggregator...\n');

  // Generate serial numbers
  const allSerialNumbers = Array.from({ length: SERIAL_NUMBER_COUNT }, (_, i) => 
    `${SERIAL_NUMBER_PREFIX}${String(i).padStart(SERIAL_NUMBER_PADDING, '0')}`
  );

  // Split into batches
  const batches = chunkArray(allSerialNumbers, BATCH_SIZE);
  const totalBatches = batches.length;
  
  logger.info(`📦 Total devices: ${allSerialNumbers.length}`);
  logger.info(`📊 Batches to process: ${totalBatches}\n`);

  const allTelemetryData = [];
  const startTime = Date.now();
  let successCount = 0;
  let failureCount = 0;

  for (let i = 0; i < totalBatches; i++) {
    const batch = batches[i];
    logger.info(`[${i + 1}/${totalBatches}] Fetching batch: ${getBatchRange(batch)}`);

    try {
      const data = await fetchBatchTelemetry(batch);
      allTelemetryData.push(...data.data);
      successCount++;
      logger.info(`✅ Success: ${data.data.length} records retrieved`);
    } catch (error) {
      failureCount++;
      logger.error(`❌ Batch failed permanently: ${getBatchRange(batch)}`);
    }

    // Rate limiting: Wait 1 second before next request (except for last batch)
    if (i < totalBatches - 1) {
      await sleep(RATE_LIMIT_MS);
    }
  }

  const endTime = Date.now();
  const duration = ((endTime - startTime) / 1000).toFixed(2);

  // Summary
  logger.info(`\n✨ Aggregation Complete!`);
  logger.info(`📈 Total records fetched: ${allTelemetryData.length}/${allSerialNumbers.length}`);
  logger.info(`✅ Successful batches: ${successCount}/${totalBatches}`);
  if (failureCount > 0) {
    logger.info(`❌ Failed batches: ${failureCount}/${totalBatches}`);
  }
  logger.info(`⏱️  Time elapsed: ${duration}s`);

  // Save to disk if requested
  if (saveToDisk && allTelemetryData.length > 0) {
    const filename = `telemetry-${new Date().toISOString().replace(/:/g, '-')}.json`;
    try {
      await fs.writeFile(
        filename,
        JSON.stringify({
          metadata: {
            timestamp: new Date().toISOString(),
            totalRecords: allTelemetryData.length,
            duration: `${duration}s`,
            successRate: `${((successCount / totalBatches) * 100).toFixed(2)}%`
          },
          data: allTelemetryData
        }, null, 2)
      );
      logger.info(`💾 Data saved to: ${filename}`);
    } catch (error) {
      logger.error(`⚠️  Failed to save data: ${error.message}`);
    }
  }

  return allTelemetryData;
}

// Run if executed directly
if (require.main === module) {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const saveToDisk = args.includes('--save') || args.includes('-s');
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
Usage: node aggregator.js [options]

Options:
  -s, --save    Save telemetry data to JSON file
  -h, --help    Display this help message

Environment Variables:
  SECRET_TOKEN    API authentication token (required)
  API_BASE_URL    API base URL (default: http://localhost:3000)
  LOG_LEVEL       Logging level: info|debug (default: info)

Examples:
  node aggregator.js              # Run aggregation
  node aggregator.js --save       # Run and save to file
  LOG_LEVEL=debug node aggregator.js  # Run with debug logging
    `);
    process.exit(0);
  }

  aggregateAllTelemetry({ saveToDisk })
    .then(data => {
      logger.info('\n📄 Sample data (first 3 records):');
      console.log(JSON.stringify(data.slice(0, 3), null, 2));
      process.exit(0);
    })
    .catch(error => {
      logger.error(`💥 Fatal error: ${error.message}`);
      logger.debug(error.stack);
      process.exit(1);
    });
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  logger.info('\n\n⚠️  Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('\n\n⚠️  Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

module.exports = { aggregateAllTelemetry };
