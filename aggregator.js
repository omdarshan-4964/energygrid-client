require('dotenv').config();
const axios = require('axios');
const { generateSignature, chunkArray, sleep } = require('./utils');

// Configuration
const API_BASE_URL = process.env.API_BASE_URL || 'http://localhost:3000';
const SECRET_TOKEN = process.env.SECRET_TOKEN;
const BATCH_SIZE = 10; // Max serial numbers per request
const RATE_LIMIT_MS = 1000; // 1 request per second
const MAX_RETRIES = 3;

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
    const response = await axios.post(
      fullUrl,
      { sn_list: serialNumbers },
      {
        headers: {
          'Content-Type': 'application/json',
          'Signature': signature,
          'Timestamp': timestamp.toString()
        }
      }
    );

    return response.data;
  } catch (error) {
    // Handle rate limiting (429) or network errors with exponential backoff
    if (error.response?.status === 429 || error.code === 'ECONNREFUSED') {
      if (retryCount < MAX_RETRIES) {
        const backoffDelay = Math.pow(2, retryCount) * 1000;
        console.log(`⚠️  Rate limited or error. Retrying in ${backoffDelay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
        await sleep(backoffDelay);
        return fetchBatchTelemetry(serialNumbers, retryCount + 1);
      }
    }

    console.error(`❌ Failed to fetch batch ${serialNumbers[0]}-${serialNumbers[serialNumbers.length - 1]}:`, error.message);
    throw error;
  }
}

/**
 * Main aggregator function to fetch all 500 inverter telemetry data
 */
async function aggregateAllTelemetry() {
  console.log('🚀 Starting EnergyGrid Data Aggregator...\n');

  // Generate serial numbers: SN-000 to SN-499
  const allSerialNumbers = Array.from({ length: 500 }, (_, i) => 
    `SN-${String(i).padStart(3, '0')}`
  );

  // Split into batches of 10
  const batches = chunkArray(allSerialNumbers, BATCH_SIZE);
  console.log(`📦 Total devices: ${allSerialNumbers.length}`);
  console.log(`📊 Batches to process: ${batches.length}\n`);

  const allTelemetryData = [];
  const startTime = Date.now();

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    console.log(`[${i + 1}/${batches.length}] Fetching batch: ${batch[0]} to ${batch[batch.length - 1]}`);

    try {
      const data = await fetchBatchTelemetry(batch);
      allTelemetryData.push(...data.data);
      console.log(`✅ Success: ${data.data.length} records retrieved`);
    } catch (error) {
      console.error(`❌ Batch failed permanently: ${batch[0]} to ${batch[batch.length - 1]}`);
    }

    // Rate limiting: Wait 1 second before next request (except for last batch)
    if (i < batches.length - 1) {
      await sleep(RATE_LIMIT_MS);
    }
  }

  const endTime = Date.now();
  const duration = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n✨ Aggregation Complete!`);
  console.log(`📈 Total records fetched: ${allTelemetryData.length}/${allSerialNumbers.length}`);
  console.log(`⏱️  Time elapsed: ${duration}s`);

  return allTelemetryData;
}

// Run if executed directly
if (require.main === module) {
  aggregateAllTelemetry()
    .then(data => {
      console.log('\n📄 Sample data (first 3 records):');
      console.log(JSON.stringify(data.slice(0, 3), null, 2));
    })
    .catch(error => {
      console.error('💥 Fatal error:', error.message);
      process.exit(1);
    });
}

module.exports = { aggregateAllTelemetry };
