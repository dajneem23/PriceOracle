const fs = require('fs').promises;
const path = require('path');
const { chromium } = require('playwright');
const { Queue } = require('bullmq');
const { createLogger } = require('../utils/logger');

const logger = createLogger('xe.worker');

const DATA_DIR = path.resolve(__dirname, '../data/xe');
const XE_CHARTING_API = 'https://www.xe.com/api/protected/charting-rates/';
const XE_MIDMARKET_API = 'https://www.xe.com/api/protected/midmarket-converter/';
// Authorization: Basic btoa("lodestar:pugsnax")
const XE_AUTH_TOKEN = process.env.XE_AUTH_TOKEN || 'Basic bG9kZXN0YXI6cHVnc25heA==';

/**
 * Try to fetch XE.com data directly via API with cookies
 * @param {string} fromCurrency - Source currency (default: 'USD')
 * @param {string} toCurrency - Target currency (default: 'VND')
 * @param {string} cookies - Cookie string to use for authentication
 * @param {string} authToken - Authorization token (optional, uses default if not provided)
 * @param {string} proxy - Proxy URL (optional, note: native fetch doesn't support proxies)
 */
async function fetchXeDataDirect(fromCurrency = 'USD', toCurrency = 'VND', cookies = '', authToken = XE_AUTH_TOKEN, proxy = null) {
    logger.info({ fromCurrency, toCurrency }, 'Attempting direct API calls with cookies');
    if (proxy) logger.warn({ proxy }, 'Proxy specified but native fetch doesn\'t support proxies');

    const chartingUrl = `${XE_CHARTING_API}?fromCurrency=${fromCurrency}&toCurrency=${toCurrency}&crypto=true`;
    const midmarketUrl = XE_MIDMARKET_API;

    try {
        const headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': authToken,
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'referer': `https://www.xe.com/currencyconverter/convert/?Amount=1&From=${fromCurrency}&To=${toCurrency}`,
            'cookie': cookies,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        };

        // Fetch charting rates
        logger.info('Fetching charting rates');
        const chartingResponse = await fetch(chartingUrl, {
            method: 'GET',
            headers,
            mode: 'cors',
            credentials: 'include'
        });

        if (!chartingResponse.ok) {
            throw new Error(`Charting API HTTP ${chartingResponse.status}: ${chartingResponse.statusText}`);
        }

        const chartingData = await chartingResponse.json();
        logger.info('Charting rates fetched successfully');

        // Fetch midmarket converter
        logger.info('Fetching midmarket converter');
        const midmarketResponse = await fetch(midmarketUrl, {
            method: 'GET',
            headers,
            mode: 'cors',
            credentials: 'include'
        });

        if (!midmarketResponse.ok) {
            throw new Error(`Midmarket API HTTP ${midmarketResponse.status}: ${midmarketResponse.statusText}`);
        }

        const midmarketData = await midmarketResponse.json();
        logger.info('Midmarket data fetched successfully');

        return {
            method: 'direct-api',
            fromCurrency,
            toCurrency,
            capturedAt: new Date().toISOString(),
            charting: chartingData,
            midmarket: midmarketData
        };
    } catch (error) {
        logger.warn({ error: error.message }, 'Direct API call failed');
        throw error;
    }
}

/**
 * Fetch XE.com data using Playwright
 * @param {string} fromCurrency - Source currency (default: 'USD')
 * @param {string} toCurrency - Target currency (default: 'VND')
 * @param {string} proxy - Proxy URL (optional, e.g., 'socks5://127.0.0.1:9050')
 */
async function fetchXeData(fromCurrency = 'USD', toCurrency = 'VND', proxy = null, wsEndpoint = null) {
    const pageUrl = `https://www.xe.com/currencycharts/?from=${fromCurrency}&to=${toCurrency}`;

    logger.info({ fromCurrency, toCurrency, url: pageUrl }, 'Fetching XE.com data');
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

        // Capture API responses
        const apiResponses = {
            charting: null,
            midmarket: null
        };

        page.on('response', async (response) => {
            const url = response.url();

            if (url.includes('/api/protected/charting-rates/')) {
                try {
                    const data = await response.json();
                    apiResponses.charting = data;
                    logger.info('Captured charting rates API response');
                } catch (e) {
                    logger.warn('Could not parse charting response as JSON');
                }
            } else if (url.includes('/api/protected/midmarket-converter/')) {
                try {
                    const data = await response.json();
                    apiResponses.midmarket = data;
                    logger.info('Captured midmarket converter API response');
                } catch (e) {
                    logger.warn('Could not parse midmarket response as JSON');
                }
            }
        });

        // Visit the XE.com page
        await page.goto(pageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });

        logger.info('Page loaded');

        // Wait a bit more to ensure all API calls complete
        await page.waitForTimeout(3000);

        await context.close();

        if (!apiResponses.charting && !apiResponses.midmarket) {
            throw new Error('No API responses captured');
        }

        logger.info('Captured API responses');

        return {
            method: 'browser',
            fromCurrency,
            toCurrency,
            capturedAt: new Date().toISOString(),
            charting: apiResponses.charting,
            midmarket: apiResponses.midmarket
        };

    } finally {
        await browser.close();
    }
}

/**
 * Save data to files (timestamped and latest)
 * @param {Object} data - Data to save
 * @param {string} prefix - File prefix (e.g., 'xe')
 * @param {string} identifier - Unique identifier for the file (e.g., 'USD_VND')
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
 * Crawl XE.com data and save to file
 * @param {Object} options
 * @param {string} options.fromCurrency - Source currency (default: 'USD')
 * @param {string} options.toCurrency - Target currency (default: 'VND')
 * @param {string} options.cookies - Cookie string for direct API call (optional)
 * @param {string} options.authToken - Authorization token (optional)
 * @param {string} options.proxy - Proxy URL (optional)
 */
async function crawlXe(options = {}) {
    const {
        fromCurrency = 'USD',
        toCurrency = 'VND',
        cookies = process.env.XE_COOKIES || '',
        authToken = XE_AUTH_TOKEN,
        direct = false,
        proxy = process.env.XE_PROXY || null,
        wsEndpoint = null
    } = options;

    try {
        let capturedData;

        // If cookies are provided, try direct API call first
        if (direct) {
            try {
                capturedData = await fetchXeDataDirect(fromCurrency, toCurrency, cookies, authToken, proxy);
            } catch (error) {
                logger.info('Direct API failed, falling back to browser method');
                capturedData = await fetchXeData(fromCurrency, toCurrency, proxy, options.wsEndpoint);
            }
        } else {
            // Use browser method if no cookies provided or direct flag not set
            capturedData = await fetchXeData(fromCurrency, toCurrency, proxy, options.wsEndpoint);
        }

        // Save data to files
        const pairName = `${fromCurrency}_${toCurrency}`;
        const { filepath, latestPath } = await saveData(capturedData, 'xe', pairName, DATA_DIR);

        // Extract metadata
        const chartingPoints = capturedData.charting?.bpi?.length || 0;
        const midmarketRate = capturedData.midmarket?.rates?.[toCurrency]?.rate || null;

        logger.info({ chartingPoints, midmarketRate }, 'Data extracted');

        return {
            success: true,
            filepath,
            latestPath,
            timestamp: new Date().toISOString(),
            fromCurrency,
            toCurrency,
            method: capturedData.method,
            chartingPoints,
            midmarketRate,
            size: JSON.stringify(capturedData).length
        };
    } catch (error) {
        logger.error({ error: error.message, stack: error.stack }, 'Error crawling XE.com');
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
        '--from': 'fromCurrency',
        '--to': 'toCurrency',
        '--cookies': 'cookies',
        '--auth-token': (val) => ({ authToken: val.startsWith('Basic ') ? val : `Basic ${val}` }),
        '--proxy': 'proxy',
        '--direct': () => ({ direct: true }),
        '--ws-endpoint': 'wsEndpoint'
    });

    crawlXe(options)
        .then(result => {
            logger.info({
                pair: `${result.fromCurrency}/${result.toCurrency}`,
                method: result.method,
                chartingPoints: result.chartingPoints,
                midmarketRate: result.midmarketRate,
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
        const result = await crawlXe(options);
        
        // Auto-trigger import job if flag is set
        if (options.autoImport) {
            logger.info('Auto-import enabled, triggering IMPORTER_XE job');
            const importQueue = new Queue('IMPORTER_XE', {
                connection: {
                    url: process.env.REDIS_URL
                }
            });
            await importQueue.add('import', { 
                mode: 'latest', 
                fromCurrency: result.fromCurrency,
                toCurrency: result.toCurrency 
            });
            await importQueue.close();
            logger.info('Import job added to queue');
        }
        
        return result;
    } catch (error) {
        throw error;
    }
}

// # Default: USD/VND
// npm run crawl:xe

// # Custom currency pair
// node xe.worker.js --from USD --to EUR

// # With cookies (direct API - faster, no browser)
// node xe.worker.js --from USD --to VND --cookies "session=...; token=..."

// # Via environment variable
// export XE_COOKIES="session=...; token=..."
// npm run crawl:xe