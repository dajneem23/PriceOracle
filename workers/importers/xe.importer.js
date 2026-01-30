const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
const { createLogger } = require('../utils/logger');
require('dotenv').config();

const logger = createLogger('xe.importer');
const DATA_DIR = path.resolve(__dirname, '../data/xe');
const SOURCE_NAME = 'XE.com';

// Database connection
const pool = new Pool({
    host: process.env.POSTGRES_HOST || 'localhost',
    port: process.env.POSTGRES_PORT || 5432,
    database: process.env.POSTGRES_DB || 'timeseries',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres'
});

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
 * Transform XE.com data to fx_ticks format
 * @param {Object} xeData - Raw XE data
 * @param {number} sourceId - Source ID
 * @param {Object} client - Database client for getting pair IDs
 * @returns {Promise<Array>} Array of tick records
 */
async function transformXeData(xeData, sourceId, client) {
    const ticks = [];
    
    const fromCurrency = xeData.fromCurrency;
    const toCurrency = xeData.toCurrency;
    
    if (!fromCurrency || !toCurrency) {
        throw new Error('Invalid XE data: missing currency information');
    }
    
    // Get pair ID
    const pairId = await getOrCreateCurrencyPair(client, fromCurrency, toCurrency);
    logger.info(`  Processing ${fromCurrency}/${toCurrency}`);
    
    // Process charting data (historical rates)
    if (xeData.charting?.batchList) {
        for (const batch of xeData.charting.batchList) {
            const startTime = batch.startTime;
            const interval = batch.interval;
            const rates = batch.rates || [];
            
            for (let i = 0; i < rates.length; i++) {
                const rate = rates[i];
                
                // Skip invalid rates
                if (!rate || rate < 0.01) {
                    continue;
                }
                
                const timestamp = new Date(startTime + (i * interval));
                
                // XE provides mid rate
                const mid = rate;
                const spread = mid * 0.0001; // 1 basis point spread estimate
                const bid = mid - spread / 2;
                const ask = mid + spread / 2;
                
                ticks.push({
                    time: timestamp,
                    pair_id: pairId,
                    source_id: sourceId,
                    bid: bid,
                    mid: mid,
                    ask: ask,
                    volume: null
                });
            }
        }
    }
    
    // Process midmarket data (current rate)
    if (xeData.midmarket?.rates?.[toCurrency]) {
        const midmarketData = xeData.midmarket.rates[toCurrency];
        const rate = midmarketData.rate;
        
        if (rate) {
            const timestamp = new Date(xeData.capturedAt || Date.now());
            const mid = rate;
            const spread = mid * 0.0001;
            const bid = mid - spread / 2;
            const ask = mid + spread / 2;
            
            ticks.push({
                time: timestamp,
                pair_id: pairId,
                source_id: sourceId,
                bid: bid,
                mid: mid,
                ask: ask,
                volume: null
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

    // Deduplicate ticks by (time, pair_id, source_id) - keep the last occurrence
    const tickMap = new Map();
    for (const tick of ticks) {
        const key = `${tick.time.toISOString()}_${tick.pair_id}_${tick.source_id}`;
        tickMap.set(key, tick);
    }
    const uniqueTicks = Array.from(tickMap.values());

    if (uniqueTicks.length < ticks.length) {
        logger.info(`Deduplicated ${ticks.length - uniqueTicks.length} duplicate ticks`);
    }

    // Batch insert to avoid overwhelming the database
    const chunkSize = 1000;
    let totalInserted = 0;

    for (let i = 0; i < uniqueTicks.length; i += chunkSize) {
        const chunk = uniqueTicks.slice(i, i + chunkSize);
        
        const values = [];
        const params = [];
        let paramIndex = 1;

        for (const tick of chunk) {
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
        totalInserted += chunk.length;
        
        if (uniqueTicks.length > chunkSize) {
            logger.info(`  Inserted ${totalInserted}/${uniqueTicks.length} ticks...`);
        }
    }

    return totalInserted;
}

/**
 * Import XE.com data from JSON file
 * @param {string} filepath - Path to JSON file
 * @returns {Promise<Object>} Import result
 */
async function importXeData(filepath) {
    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');

        logger.info(`Reading ${filepath}...`);
        const content = await fs.readFile(filepath, 'utf-8');
        const data = JSON.parse(content);

        // Get or create source
        const sourceId = await getOrCreateSource(client, SOURCE_NAME);
        logger.info(`Source ID: ${sourceId} (${SOURCE_NAME})`);

        // Transform data
        logger.info('Transforming data...');
        const ticks = await transformXeData(data, sourceId, client);
        logger.info(`Transformed ${ticks.length} ticks`);

        // Insert ticks
        logger.info('Inserting ticks...');
        const insertedCount = await insertTicks(client, ticks);
        logger.info(`✓ Inserted ${insertedCount} ticks`);

        await client.query('COMMIT');

        return {
            success: true,
            filepath,
            ticksCount: insertedCount,
            source: SOURCE_NAME
        };
    } catch (error) {
        await client.query('ROLLBACK');
        logger.error({ error: error.message, stack: error.stack }, 'Error importing XE.com data');
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Import latest XE.com data
 */
async function importLatest() {
    const files = await fs.readdir(DATA_DIR);
    const latestFiles = files.filter(f => f.includes('_latest.json'));
    
    logger.info(`Found ${latestFiles.length} latest files`);
    
    const results = [];
    for (const file of latestFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            logger.info(`\n--- Processing ${file} ---`);
            const result = await importXeData(filepath);
            results.push(result);
        } catch (error) {
            logger.error({ file, error: error.message }, 'Failed to import file');
            results.push({
                success: false,
                filepath: file,
                error: error.message
            });
        }
    }
    
    return results;
}

/**
 * Import all XE.com data files
 */
async function importAll() {
    const files = await fs.readdir(DATA_DIR);
    const jsonFiles = files.filter(f => f.endsWith('.json') && !f.includes('_latest'));

    logger.info(`Found ${jsonFiles.length} XE data files`);

    const results = [];
    for (const file of jsonFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            logger.info(`\n--- Processing ${file} ---`);
            const result = await importXeData(filepath);
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
                logger.info('Importing all XE.com data files...\n');
                const results = await importAll();
                const successful = results.filter(r => r.success).length;
                logger.info(`\n✓ Completed: ${successful}/${results.length} files imported successfully`);
            } else {
                logger.info('Importing latest XE.com data...\n');
                const results = await importLatest();
                const successful = results.filter(r => r.success).length;
                const totalTicks = results.filter(r => r.success).reduce((sum, r) => sum + (r.ticksCount || 0), 0);
                logger.info(`\n✓ Import completed: ${totalTicks} total ticks from ${successful}/${results.length} files`);
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
// node xe.importer.js          # Import latest
// node xe.importer.js all      # Import all files
