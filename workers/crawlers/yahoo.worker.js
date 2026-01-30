const fs = require('fs').promises;
const path = require('path');
const { chromium } = require('playwright');
const { Queue } = require('bullmq');
const { createLogger } = require('../utils/logger');

const logger = createLogger('yahoo.worker');

const DATA_DIR = path.resolve(__dirname, '../data/yahoo');

/**
 * Get Unix timestamp for a date
 * @param {Date} date 
 * @returns {number} Unix timestamp in seconds
 */
function getUnixTimestamp(date) {
    return Math.floor(date.getTime() / 1000);
}

/**
 * Try to fetch Yahoo Finance data directly via API with cookies
 * @param {string} symbol - Currency pair symbol (e.g., 'VND=X' for USD/VND)
 * @param {string} interval - Time interval (1m, 5m, 15m, 1h, 1d, 1wk, 1mo)
 * @param {Date} startDate - Start date for data
 * @param {Date} endDate - End date for data
 * @param {string} cookies - Cookie string to use for authentication
 * @param {string} proxy - Proxy URL (optional, note: native fetch doesn't support proxies)
 */
async function fetchYahooFinanceDataDirect(symbol, interval = '1d', startDate = null, endDate = null, cookies = '', proxy = null) {
    // Default to last 7 days if no dates provided
    if (!endDate) {
        endDate = new Date();
    }
    if (!startDate) {
        startDate = new Date();
        startDate.setDate(startDate.getDate() - 7);
    }

    const period1 = getUnixTimestamp(startDate);
    const period2 = getUnixTimestamp(endDate);

    const chartApiUrl = `https://query2.finance.yahoo.com/v8/finance/chart/${symbol}?period1=${period1}&period2=${period2}&interval=${interval}&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&source=cosaic`;

    logger.info({ symbol, startDate: startDate.toISOString(), endDate: endDate.toISOString(), interval }, 'Attempting direct API call with cookies');
    if (proxy) logger.warn({ proxy }, 'Proxy specified but native fetch doesn\'t support proxies');

    try {
        const response = await fetch(chartApiUrl, {
            method: 'GET',
            headers: {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'referer': `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/`,
                'cookie': cookies,
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            mode: 'cors',
            credentials: 'include'
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        logger.info('Direct API call successful');

        return data;
    } catch (error) {
        logger.warn({ error: error.message }, 'Direct API call failed');
        throw error;
    }
}

/**
 * Fetch Yahoo Finance chart data for a currency pair using Playwright
 * @param {string} symbol - Currency pair symbol (e.g., 'VND=X' for USD/VND)
 * @param {string} interval - Time interval (1m, 5m, 15m, 1h, 1d, 1wk, 1mo)
 * @param {Date} startDate - Start date for data
 * @param {Date} endDate - End date for data
 * @param {string} proxy - Proxy URL (optional, e.g., 'socks5://127.0.0.1:9050')
 */
async function fetchYahooFinanceData(symbol, interval = '1d', startDate = null, endDate = null, proxy = null, wsEndpoint = null) {
    // Default to last 7 days if no dates provided
    if (!endDate) {
        endDate = new Date();
    }
    if (!startDate) {
        startDate = new Date();
        startDate.setDate(startDate.getDate() - 7);
    }

    const period1 = getUnixTimestamp(startDate);
    const period2 = getUnixTimestamp(endDate);

    const chartApiUrl = `https://query2.finance.yahoo.com/v8/finance/chart/${symbol}?period1=${period1}&period2=${period2}&interval=${interval}&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&source=cosaic`;
    const pageUrl = `https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/`;

    logger.info({ symbol, startDate: startDate.toISOString(), endDate: endDate.toISOString(), interval }, 'Fetching Yahoo Finance data');
    if (proxy) logger.info({ proxy }, 'Using proxy');

    const launchOptions = {
        headless: true,
        ...(wsEndpoint && {
            wsEndpoint: wsEndpoint
        })
    };

    if (proxy) {
        launchOptions.proxy = { server: proxy };
    }

    const browser = await chromium.launch(launchOptions);

    try {
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        });

        const page = await context.newPage();

        // Set up request interception to capture the API response
        let apiResponse = null;
        page.on('response', async (response) => {
            const url = response.url();
            if (url.includes('query2.finance.yahoo.com/v8/finance/chart/')) {
                try {
                    apiResponse = await response.json();
                    logger.info('Captured API response from network');
                } catch (e) {
                    logger.warn('Could not parse API response as JSON');
                }
            }
        });

        // First, visit the Yahoo Finance page to establish session/cookies
        logger.info({ url: pageUrl }, 'Visiting page');
        await page.goto(pageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });

        logger.info('Page loaded, cookies established');

        // If we didn't capture the response from the page load, fetch it directly with cookies
        if (!apiResponse) {
            logger.info('Fetching chart data with session cookies');
            const response = await page.goto(chartApiUrl, {
                waitUntil: 'load',
                timeout: 30000
            });

            const text = await response.text();
            apiResponse = JSON.parse(text);
            logger.info('Fetched chart data directly');
        }

        await context.close();
        return apiResponse;

    } finally {
        await browser.close();
    }
}

/**
 * Save data to files (timestamped and latest)
 * @param {Object} data - Data to save
 * @param {string} prefix - File prefix (e.g., 'yahoo')
 * @param {string} identifier - Unique identifier for the file (e.g., 'VND_X')
 * @param {string} dataDir - Directory to save files
 * @returns {Promise<{filepath: string, latestPath: string}>}
 */
async function saveData(data, prefix, identifier, dataDir) {
    const timestamp = Date.now();
    const filename = `${prefix}_${identifier}_${timestamp}.json`;
    const filepath = path.join(dataDir, filename);

    // Ensure data directory exists
    await fs.mkdir(dataDir, { recursive: true });

    // Save JSON content with timestamp
    await fs.writeFile(filepath, JSON.stringify(data, null, 2), 'utf-8');
    logger.info({ filepath }, 'JSON saved');

    // Also save as latest
    const latestFilename = `${prefix}_${identifier}_latest.json`;
    const latestPath = path.join(dataDir, latestFilename);
    await fs.writeFile(latestPath, JSON.stringify(data, null, 2), 'utf-8');
    logger.info({ latestPath }, 'Latest JSON saved');

    return { filepath, latestPath };
}

/**
 * Crawl Yahoo Finance data and save to file
 * @param {Object} options
 * @param {string} options.symbol - Currency pair symbol (default: 'VND=X')
 * @param {string} options.interval - Time interval (default: '1d')
 * @param {Date} options.startDate - Start date
 * @param {Date} options.endDate - End date
 * @param {string} options.cookies - Cookie string for direct API call (optional)
 * @param {string} options.proxy - Proxy URL (optional)
 */
async function crawlYahooFinance(options = {}) {
    const {
        symbol = 'VND=X',
        interval = '1d',
        startDate = null,
        endDate = null,
        cookies = process.env.YAHOO_COOKIES || '',
        proxy = process.env.YAHOO_PROXY || null,
        wsEndpoint = null
    } = options;

    try {
        let jsonData;
        let method = 'browser';

        // If cookies are provided, try direct API call first
        if (cookies) {
            try {
                jsonData = await fetchYahooFinanceDataDirect(symbol, interval, startDate, endDate, cookies, proxy);
                method = 'direct-api';
            } catch (error) {
                logger.info('Direct API failed, falling back to browser method');
                jsonData = await fetchYahooFinanceData(symbol, interval, startDate, endDate, proxy, wsEndpoint);
            }
        } else {
            // Use browser method if no cookies provided
            jsonData = await fetchYahooFinanceData(symbol, interval, startDate, endDate, proxy, wsEndpoint);
        }

        // Save data to files
        const safeSymbol = symbol.replace(/[=\/]/g, '_');
        const identifier = `${safeSymbol}_${interval}`;
        const { filepath, latestPath } = await saveData(jsonData, 'yahoo', identifier, DATA_DIR);

        // Extract metadata
        const result = jsonData?.chart?.result?.[ 0 ];
        const meta = result?.meta;
        const timestamps = result?.timestamp;
        const dataPoints = timestamps?.length || 0;

        logger.info({ dataPoints, currency: meta?.currency, exchange: meta?.exchangeName, price: meta?.regularMarketPrice }, 'Parsed data');

        return {
            success: true,
            filepath,
            latestPath,
            timestamp: new Date().toISOString(),
            symbol,
            interval,
            method,
            dataPoints,
            size: JSON.stringify(jsonData).length,
            meta
        };
    } catch (error) {
        logger.error({ error: error.message, stack: error.stack }, 'Error crawling Yahoo Finance');
        throw error;
    }
}

/**
 * Parse command line arguments
 * @param {string[]} args - Command line arguments
 * @param {Object} argMap - Map of argument names to handler functions
 * @returns {Object} Parsed options
 */
function parseArgs(args, argMap) {
    const options = {};

    for (let i = 0; i < args.length; i++) {
        const arg = args[ i ];
        const handler = argMap[ arg ];

        if (handler) {
            if (typeof handler === 'function') {
                const result = handler(args[ i + 1 ], options);
                if (result !== undefined) {
                    Object.assign(options, result);
                }
                if (args[ i + 1 ] && !args[ i + 1 ].startsWith('--')) {
                    i++; // Skip next arg if it was consumed
                }
            } else if (typeof handler === 'string') {
                // Simple mapping: --arg value -> options[handler] = value
                options[ handler ] = args[ i + 1 ];
                i++;
            } else if (typeof handler === 'boolean') {
                // Boolean flag: --arg -> options[arg] = true
                options[ arg.slice(2) ] = true;
            }
        }
    }

    return options;
}

// Run if called directly
if (require.main === module) {
    const args = process.argv.slice(2);
    const options = parseArgs(args, {
        '--symbol': 'symbol',
        '--interval': 'interval',
        '--days': (val) => {
            const days = parseInt(val);
            return {
                endDate: new Date(),
                startDate: new Date(Date.now() - days * 24 * 60 * 60 * 1000)
            };
        },
        '--cookies': 'cookies',
        '--proxy': 'proxy',
        '--ws-endpoint': 'wsEndpoint'
    });

    crawlYahooFinance(options)
        .then(result => {
            logger.info({
                symbol: result.symbol,
                interval: result.interval,
                method: result.method,
                dataPoints: result.dataPoints,
                filepath: result.filepath
            }, 'Crawl completed successfully');
            process.exit(0);
        })
        .catch(error => {
            logger.error({ error: error.message }, 'Crawl failed');
            process.exit(1);
        });
}

module.exports = async (job) => {
    const options = job.data || {};
    logger.info({ options }, `Processing job ${job.id}`);
    try {
        const result = await crawlYahooFinance(options);

        // Auto-trigger import job if flag is set
        if (options.autoImport) {
            logger.info('Auto-import enabled, triggering IMPORTER_YAHOO job');
            const importQueue = new Queue('IMPORTER_YAHOO', {
                connection: {
                    url: process.env.REDIS_URL
                }
            });
            await importQueue.add('import', { mode: 'latest', symbol: result.symbol });
            await importQueue.close();
            logger.info('Import job added to queue');
        }

        return result;
    } catch (error) {
        throw error;
    }
}

// # With cookies (direct API - faster, no browser)
// node yahoo.worker.js --symbol VND=X --interval 1d --cookies "A1=...; A3=...; GUC=..."

// # Via environment variable
// export YAHOO_COOKIES="A1=...; A3=...; GUC=..."
// npm run crawl:yahoo

// # Without cookies (browser method - auto fallback)
// npm run crawl:yahoo