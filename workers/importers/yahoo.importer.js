const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
const { createLogger } = require('../utils/logger');
require('dotenv').config();

const logger = createLogger('yahoo.importer');
const DATA_DIR = path.resolve(__dirname, '../data/yahoo');
const SOURCE_NAME = 'Yahoo Finance';

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
 * Parse Yahoo Finance symbol to extract currencies
 * @param {string} symbol - Yahoo symbol like "VND=X" or "EURUSD=X"
 * @returns {Object} {baseCurrency, quoteCurrency}
 */
function parseYahooSymbol(symbol) {
    // Yahoo format: "VND=X" means USD/VND, "EURUSD=X" means EUR/USD
    const cleanSymbol = symbol.replace('=X', '');
    
    if (cleanSymbol.length === 3) {
        // Single currency like "VND" - assume USD as base
        return {
            baseCurrency: 'USD',
            quoteCurrency: cleanSymbol
        };
    } else if (cleanSymbol.length === 6) {
        // Pair like "EURUSD"
        return {
            baseCurrency: cleanSymbol.substring(0, 3),
            quoteCurrency: cleanSymbol.substring(3, 6)
        };
    }
    
    throw new Error(`Unable to parse Yahoo symbol: ${symbol}`);
}

/**
 * Transform Yahoo Finance data to fx_ticks format
 * @param {Object} yahooData - Raw Yahoo data
 * @param {number} sourceId - Source ID
 * @param {Object} client - Database client for getting pair IDs
 * @returns {Promise<Array>} Array of tick records
 */
async function transformYahooData(yahooData, sourceId, client) {
    const ticks = [];
    const result = yahooData?.chart?.result?.[0];
    
    if (!result) {
        throw new Error('Invalid Yahoo Finance data structure');
    }

    const meta = result.meta;
    const timestamps = result.timestamp;
    const quotes = result.indicators?.quote?.[0];

    if (!timestamps || !quotes) {
        throw new Error('No price data found in Yahoo response');
    }

    // Parse currency pair from symbol
    const { baseCurrency, quoteCurrency } = parseYahooSymbol(meta.symbol);
    const pairId = await getOrCreateCurrencyPair(client, baseCurrency, quoteCurrency);

    console.log(`  Processing ${baseCurrency}/${quoteCurrency} (${timestamps.length} data points)`);

    // Process each timestamp
    for (let i = 0; i < timestamps.length; i++) {
        const timestamp = new Date(timestamps[i] * 1000); // Unix timestamp to Date
        const open = quotes.open?.[i];
        const high = quotes.high?.[i];
        const low = quotes.low?.[i];
        const close = quotes.close?.[i];
        const volume = quotes.volume?.[i];

        // Skip if no valid price data
        if (!close && !open && !high && !low) {
            continue;
        }

        // For FX data, we don't have bid/ask spread from Yahoo
        // Use close price as mid, estimate bid/ask with small spread
        const mid = close || open || (high + low) / 2;
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
            volume: volume || null
        });
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
            console.log(`  Inserted ${totalInserted}/${uniqueTicks.length} ticks...`);
        }
    }

    return totalInserted;
}

/**
 * Import Yahoo Finance data from JSON file
 * @param {string} filepath - Path to JSON file
 * @returns {Promise<Object>} Import result
 */
async function importYahooData(filepath) {
    const client = await pool.connect();
    
    try {
        await client.query('BEGIN');

        console.log(`Reading ${filepath}...`);
        const content = await fs.readFile(filepath, 'utf-8');
        const data = JSON.parse(content);

        // Get or create source
        const sourceId = await getOrCreateSource(client, SOURCE_NAME);
        console.log(`Source ID: ${sourceId} (${SOURCE_NAME})`);

        // Transform data
        console.log('Transforming data...');
        const ticks = await transformYahooData(data, sourceId, client);
        console.log(`Transformed ${ticks.length} ticks`);

        // Insert ticks
        console.log('Inserting ticks...');
        const insertedCount = await insertTicks(client, ticks);
        console.log(`✓ Inserted ${insertedCount} ticks`);

        await client.query('COMMIT');

        return {
            success: true,
            filepath,
            ticksCount: insertedCount,
            source: SOURCE_NAME
        };
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Error importing Yahoo Finance data:', error);
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Import latest Yahoo Finance data
 */
async function importLatest() {
    const files = await fs.readdir(DATA_DIR);
    const latestFiles = files.filter(f => f.includes('_latest.json'));
    
    console.log(`Found ${latestFiles.length} latest files`);
    
    const results = [];
    for (const file of latestFiles) {
        const filepath = path.join(DATA_DIR, file);
        console.log(`\n--- Processing ${file} ---`);
        const result = await importYahooData(filepath);
        results.push(result);
    }
    
    return results;
}

/**
 * Import all Yahoo Finance data files
 */
async function importAll() {
    const files = await fs.readdir(DATA_DIR);
    const jsonFiles = files.filter(f => f.endsWith('.json') && !f.includes('_latest'));

    console.log(`Found ${jsonFiles.length} Yahoo data files`);

    const results = [];
    for (const file of jsonFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            console.log(`\n--- Processing ${file} ---`);
            const result = await importYahooData(filepath);
            results.push(result);
        } catch (error) {
            console.error(`Failed to import ${file}:`, error.message);
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
                console.log('Importing all Yahoo Finance data files...\n');
                const results = await importAll();
                const successful = results.filter(r => r.success).length;
                console.log(`\n✓ Completed: ${successful}/${results.length} files imported successfully`);
            } else {
                console.log('Importing latest Yahoo Finance data...\n');
                const results = await importLatest();
                const totalTicks = results.reduce((sum, r) => sum + (r.ticksCount || 0), 0);
                console.log(`\n✓ Import completed: ${totalTicks} total ticks from ${results.length} files`);
            }
            process.exit(0);
        } catch (error) {
            console.error('\n✗ Import failed:', error);
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
// node yahoo.importer.js          # Import latest
// node yahoo.importer.js all      # Import all files
