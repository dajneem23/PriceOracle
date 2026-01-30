const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
const { createLogger } = require('../utils/logger');
require('dotenv').config();

const logger = createLogger('reuters.importer');
const DATA_DIR = path.resolve(__dirname, '../data/reuters');
const SOURCE_NAME = 'Reuters';

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
 * Parse Reuters symbol to extract currencies
 * @param {string} symbol - Reuters symbol like "VND=X" or "EUR="
 * @returns {Object} {baseCurrency, quoteCurrency}
 */
function parseReutersSymbol(symbol) {
    // Reuters format: "VND=X" or "VND=" means USD/VND, "EUR=" means EUR/USD
    const cleanSymbol = symbol.replace(/[=X]/g, '');
    
    if (cleanSymbol.length === 3) {
        // Single currency - assume USD as base
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
    
    throw new Error(`Unable to parse Reuters symbol: ${symbol}`);
}

/**
 * Transform Reuters data to fx_ticks format
 * @param {Object} reutersData - Raw Reuters data
 * @param {number} sourceId - Source ID
 * @param {Object} client - Database client for getting pair IDs
 * @returns {Promise<Array>} Array of tick records
 */
async function transformReutersData(reutersData, sourceId, client) {
    const ticks = [];
    
    // Reuters data structure
    const data = reutersData.data || reutersData;
    const elements = data.elements || [];
    
    // Find the Quote element with price data
    const quoteElement = elements.find(el => el.resource === 'Quote' && el.data?.lastTrade);
    const headerElement = elements.find(el => el.resource === 'Xref' && el.data?.symbol);
    
    if (!quoteElement || !headerElement) {
        throw new Error('No quote data found in Reuters response');
    }

    const symbol = headerElement.data.symbol;
    const lastTrade = quoteElement.data.lastTrade;
    
    // Parse currency pair
    const { baseCurrency, quoteCurrency } = parseReutersSymbol(symbol);
    const pairId = await getOrCreateCurrencyPair(client, baseCurrency, quoteCurrency);
    
    console.log(`  Processing ${baseCurrency}/${quoteCurrency}`);
    
    // Extract price data
    const timestamp = new Date(lastTrade.date);
    const last = lastTrade.last || lastTrade.close;
    const open = lastTrade.open;
    const high = lastTrade.high;
    const low = lastTrade.low;
    const close = lastTrade.close;
    
    if (!last) {
        throw new Error('No price data available');
    }
    
    // Reuters typically provides last/close but not bid/ask
    // Estimate bid/ask with small spread
    const mid = last;
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
        volume: null // Reuters market data doesn't typically include volume for FX
    });
    
    // If we have OHLC data, we can create additional ticks
    // for open, high, low to capture intraday movements
    if (open && open !== last) {
        ticks.push({
            time: new Date(timestamp.getTime() - 3600000), // 1 hour earlier for open
            pair_id: pairId,
            source_id: sourceId,
            bid: open - spread / 2,
            mid: open,
            ask: open + spread / 2,
            volume: null
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

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const tick of uniqueTicks) {
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
 * Import Reuters data from JSON file
 * @param {string} filepath - Path to JSON file
 * @returns {Promise<Object>} Import result
 */
async function importReutersData(filepath) {
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
        const ticks = await transformReutersData(data, sourceId, client);
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
        console.error('Error importing Reuters data:', error);
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Import latest Reuters data
 */
async function importLatest() {
    const files = await fs.readdir(DATA_DIR);
    const latestFiles = files.filter(f => f.includes('_latest.json'));
    
    console.log(`Found ${latestFiles.length} latest files`);
    
    const results = [];
    for (const file of latestFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            console.log(`\n--- Processing ${file} ---`);
            const result = await importReutersData(filepath);
            results.push(result);
        } catch (error) {
            console.error(`Failed to import ${file}:`, error.message);
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
 * Import all Reuters data files
 */
async function importAll() {
    const files = await fs.readdir(DATA_DIR);
    const jsonFiles = files.filter(f => f.endsWith('.json') && !f.includes('_latest'));

    console.log(`Found ${jsonFiles.length} Reuters data files`);

    const results = [];
    for (const file of jsonFiles) {
        const filepath = path.join(DATA_DIR, file);
        try {
            console.log(`\n--- Processing ${file} ---`);
            const result = await importReutersData(filepath);
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
                console.log('Importing all Reuters data files...\n');
                const results = await importAll();
                const successful = results.filter(r => r.success).length;
                console.log(`\n✓ Completed: ${successful}/${results.length} files imported successfully`);
            } else {
                console.log('Importing latest Reuters data...\n');
                const results = await importLatest();
                const successful = results.filter(r => r.success).length;
                const totalTicks = results.filter(r => r.success).reduce((sum, r) => sum + (r.ticksCount || 0), 0);
                console.log(`\n✓ Import completed: ${totalTicks} total ticks from ${successful}/${results.length} files`);
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
// node reuters.importer.js          # Import latest
// node reuters.importer.js all      # Import all files
