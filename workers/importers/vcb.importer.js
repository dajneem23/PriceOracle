const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
const { createLogger } = require('../utils/logger');
require('dotenv').config();

const logger = createLogger('vcb.importer');
const DATA_DIR = path.resolve(__dirname, '../data/vcb');
const SOURCE_NAME = 'VietcomBank';
const BASE_CURRENCY = 'VND'; // VCB quotes foreign currency in VND

// Database connection
const pool = new Pool({
    host: process.env.POSTGRES_HOST || 'localhost',
    port: process.env.POSTGRES_PORT || 5432,
    database: process.env.POSTGRES_DB || 'timeseries',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres'
});

/**
 * Parse VCB rate string to number
 * @param {string} rateStr - Rate string like "25,380.00" or "-"
 * @returns {number|null} Parsed rate or null
 */
function parseRate(rateStr) {
    if (!rateStr || rateStr === '-') {
        return null;
    }
    // Remove commas and parse
    return parseFloat(rateStr.replace(/,/g, ''));
}

/**
 * Parse VCB datetime string
 * @param {string} dateTimeStr - DateTime string like "1/29/2026 9:25:44 AM"
 * @returns {Date} Parsed date
 */
function parseDateTime(dateTimeStr) {
    // VCB format: "M/D/YYYY H:MM:SS AM/PM"
    return new Date(dateTimeStr);
}

/**
 * Get or create source ID
 * @param {Object} client - Database client
 * @param {string} sourceName - Source name
 * @returns {Promise<number>} Source ID
 */
async function getOrCreateSource(client, sourceName) {
    const result = await client.query(
        `INSERT INTO sources (name, priority) 
         VALUES ($1, 0) 
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name 
         RETURNING id`,
        [sourceName]
    );
    return result.rows[0].id;
}

/**
 * Get or create currency pair ID
 * @param {Object} client - Database client
 * @param {string} baseCurrency - Base currency code
 * @param {string} quoteCurrency - Quote currency code
 * @returns {Promise<number>} Pair ID
 */
async function getOrCreateCurrencyPair(client, baseCurrency, quoteCurrency) {
    const symbol = `${baseCurrency}${quoteCurrency}`;
    const result = await client.query(
        `INSERT INTO currency_pairs (symbol, base_currency, quote_currency) 
         VALUES ($1, $2, $3) 
         ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol 
         RETURNING id`,
        [symbol, baseCurrency, quoteCurrency]
    );
    return result.rows[0].id;
}

/**
 * Transform VCB data to fx_ticks format
 * @param {Object} vcbData - Raw VCB data
 * @param {Date} timestamp - Timestamp for the data
 * @param {number} sourceId - Source ID
 * @param {Object} client - Database client for getting pair IDs
 * @returns {Promise<Array>} Array of tick records
 */
async function transformVCBData(vcbData, timestamp, sourceId, client) {
    const ticks = [];
    const exrates = vcbData.ExrateList?.Exrate || [];

    for (const rate of exrates) {
        const currencyCode = rate['@_CurrencyCode'];
        const buy = parseRate(rate['@_Buy']);
        const transfer = parseRate(rate['@_Transfer']);
        const sell = parseRate(rate['@_Sell']);

        // Skip if no valid rates
        if (!transfer && !buy && !sell) {
            logger.debug({ currencyCode }, 'Skipping currency - no valid rates');
            continue;
        }

        // Get pair ID (e.g., USDVND, EURVND, etc.)
        const pairId = await getOrCreateCurrencyPair(client, currencyCode, BASE_CURRENCY);

        // VCB provides:
        // - Buy: Bank buys foreign currency (customer sells) - this is BID from customer perspective
        // - Transfer: Mid rate
        // - Sell: Bank sells foreign currency (customer buys) - this is ASK from customer perspective
        
        const bid = buy || transfer; // Use buy rate as bid, fallback to transfer
        const mid = transfer || ((buy + sell) / 2); // Use transfer as mid, or calculate average
        const ask = sell || transfer; // Use sell rate as ask, fallback to transfer

        if (bid && mid && ask) {
            ticks.push({
                time: timestamp,
                pair_id: pairId,
                source_id: sourceId,
                bid: bid,
                mid: mid,
                ask: ask,
                volume: null // VCB doesn't provide volume
            });
        }
    }

    return ticks;
}

/**
 * Insert ticks into database
 * @param {Object} client - Database client
 * @param {Array} ticks - Array of tick records
 * @returns {Promise<number>} Number of inserted records
 */
async function insertTicks(client, ticks) {
    if (ticks.length === 0) {
        return 0;
    }

    // Use parameterized batch insert
    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const tick of ticks) {
        values.push(
            `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6})`
        );
        params.push(
            tick.time,
            tick.pair_id,
            tick.source_id,
            tick.bid,
            tick.mid,
            tick.ask,
            tick.volume
        );
        paramIndex += 7;
    }

    const query = `
        INSERT INTO fx_ticks (time, pair_id, source_id, bid, mid, ask, volume)
        VALUES ${values.join(', ')}
        ON CONFLICT (time, pair_id, source_id) DO UPDATE SET
            bid = EXCLUDED.bid,
            mid = EXCLUDED.mid,
            ask = EXCLUDED.ask,
            volume = EXCLUDED.volume
    `;

    await client.query(query, params);
    return ticks.length;
}

/**
 * Import VCB data from JSON file
 * @param {string} filepath - Path to JSON file
 * @returns {Promise<Object>} Import result
 */
async function importVCBData(filepath) {
    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');

        logger.info({ filepath }, 'Reading file');
        const content = await fs.readFile(filepath, 'utf-8');
        const data = JSON.parse(content);

        // Parse timestamp from VCB data
        const vcbDateTime = data.ExrateList?.DateTime;
        if (!vcbDateTime) {
            throw new Error('No DateTime found in VCB data');
        }

        const timestamp = parseDateTime(vcbDateTime);
        logger.info({ timestamp: timestamp.toISOString() }, 'Data timestamp');

        // Get or create source
        const sourceId = await getOrCreateSource(client, SOURCE_NAME);
        logger.info({ sourceId, source: SOURCE_NAME }, 'Source ID');

        // Transform data
        logger.info('Transforming data');
        const ticks = await transformVCBData(data, timestamp, sourceId, client);
        logger.info({ count: ticks.length }, 'Transformed ticks');

        // Insert ticks
        logger.info('Inserting ticks');
        const insertedCount = await insertTicks(client, ticks);
        logger.info({ insertedCount }, 'Inserted ticks');

        await client.query('COMMIT');

        return {
            success: true,
            filepath,
            timestamp: timestamp.toISOString(),
            ticksCount: insertedCount,
            source: SOURCE_NAME
        };
    } catch (error) {
        await client.query('ROLLBACK');
        logger.error({ error: error.message, stack: error.stack }, 'Error importing VCB data');
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Import latest VCB data
 */
async function importLatest() {
    const latestPath = path.join(DATA_DIR, 'vcb_latest.json');
    return await importVCBData(latestPath);
}

/**
 * Import all VCB data files
 */
async function importAll() {
    const files = await fs.readdir(DATA_DIR);
    const jsonFiles = files.filter(f => f.endsWith('.json') && f !== 'vcb_latest.json');

    logger.info({ count: jsonFiles.length }, 'Found VCB data files');

    const results = [];
    for (const file of jsonFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            logger.info({ file }, 'Processing file');
            const result = await importVCBData(filepath);
            results.push(result);
        } catch (error) {
            logger.error({ file, error: error.message }, 'Failed to import file');
            results.push({
                success: false,
                filepath,
                error: error.message
            });
        }
    }

    return results;
}

// Run if called directly
if (require.main === module) {
    const args = process.argv.slice(2);
    const mode = args[0] || 'latest';

    const run = async () => {
        try {
            if (mode === 'all') {
                logger.info('Importing all VCB data files');
                const results = await importAll();
                const successful = results.filter(r => r.success).length;
                logger.info({ successful, total: results.length }, 'Import completed');
            } else {
                logger.info('Importing latest VCB data');
                const result = await importLatest();
                logger.info({ ticksCount: result.ticksCount }, 'Import completed');
            }
            process.exit(0);
        } catch (error) {
            logger.error({ error: error.message }, 'Import failed');
            process.exit(1);
        } finally {
            await pool.end();
        }
    };

    run();
}

module.exports = async (job) => {
    const options = job.data || {};
    logger.info({ options }, `Processing job ${job.id}`);
    try {
        if (options.mode === 'all') {
            return await importAll();
        } else {
            return await importLatest();
        }
    } catch (error) {
        throw error;
    }
}

// Usage:
// node vcb.importer.js          # Import latest
// node vcb.importer.js all      # Import all files
