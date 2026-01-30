const fs = require('fs').promises;
const path = require('path');
const { chromium } = require('playwright');
const { Queue } = require('bullmq');
const { createLogger } = require('../utils/logger');
require('dotenv').config();

const logger = createLogger('reuters.worker');


const DATA_DIR = path.resolve(__dirname, '../data/reuters');
const REUTERS_API_URL = 'https://api.markitdigital.com/fwc-api-service/v1/fwc-universal-app';
const REUTERS_AUTH_TOKEN = process.env.REUTERS_AUTH_TOKEN || 'Bearer Z3E0SGH6bO8jBQfYLjAY3esdHEi3';
const DEFAULT_XID = parseInt(process.env.REUTERS_XID) || 611986; // VND=X
const DEFAULT_COOKIE = process.env.REUTERS_COOKIES || 'isGpcEnabled=0&datestamp=Thu+Jan+29+2026+10%3A34%3A39+GMT%2B0700+(Indochina+Time)&version=202509.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0fc33449-96b7-4d03-bc05-0e8a0652ac99&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=1%3A1%2C2%3A1%2C3%3A1%2C4%3A1&AwaitingReconsent=false';
/**
 * Extract token and xid from Reuters page HTML
 * @param {string} symbol - Currency pair symbol (e.g., 'VND=X' for USD/VND)
 * @param {string} proxy - Proxy URL (optional, e.g., 'http://user:pass@proxy:port')
 * @returns {Promise<{token: string, xid: number}>}
 */
async function extractTokenFromPage({ symbol = 'VND=X', proxy = null, wsEndpoint }) {
    const pageUrl = `https://www.reuters.com/markets/quote/${symbol}/`;

    logger.info({ url: pageUrl }, 'Extracting token from page');
    if (proxy) logger.info({ proxy }, 'Using proxy');

    const launchOptions = {
        ...(wsEndpoint && {
            wsEndpoint: wsEndpoint
        }),
        headless: true,
        args: [ '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process',
            '--disable-gpu'
        ]
    };

    if (proxy) {
        launchOptions.proxy = { server: proxy };
    }

    logger.info({ launchOptions }, 'Connecting to browser');

    const browser = await chromium.launch(launchOptions);



    try {
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        });

        const page = await context.newPage();

        //clear cookies and set custom cookies
        await context.clearCookies();

        await page.setExtraHTTPHeaders({
            'cookie': DEFAULT_COOKIE
        });


        await page.goto(pageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });
        // await page.waitForTimeout(50000 * 10);

        const pageContent = await page.content();
        if (pageContent.includes('Access is temporarily restricted') ||
            pageContent.includes('Access Denied') ||
            pageContent.includes('blocked')) {
            await context.close();
            throw new Error('Access is temporarily restricted by Reuters. Please try again later or use a different IP/proxy.');
        }



        // Extract Fusion.contentCache from the page
        const fusionData = await page.evaluate(() => {
            if (window.Fusion && window.Fusion.contentCache && window.Fusion.globalContent) {
                const tokenData = window.Fusion.contentCache[ 'graphql-proxy-v1' ]?.[ '{\"query_name\":\"get_market_public_token\"}' ];
                const xidData = window.Fusion.globalContent?.cfData;

                return {
                    token: tokenData?.data?.result?.data?.getMarketPublicToken?.accessToken,
                    tokenType: tokenData?.data?.result?.data?.getMarketPublicToken?.tokenType,
                    xid: xidData?.xid
                };
            }
            return null;
        });

        await context.close();

        if (!fusionData || !fusionData.token) {
            throw new Error('Could not extract token from page');
        }

        const authToken = `${fusionData.tokenType || 'Bearer'} ${fusionData.token}`;
        logger.info('Extracted token and xid from page');

        return {
            token: authToken,
            xid: fusionData.xid || DEFAULT_XID
        };

    } finally {
        await browser.close();
    }
}

/**
 * Try to fetch Reuters data directly via API
 * @param {number} xid - The market/symbol ID (e.g., 611986 for VND=X)
 * @param {string} authToken - Authorization token (optional, uses default if not provided)
 */
async function fetchReutersApiDirect(xid, authToken = REUTERS_AUTH_TOKEN) {
    logger.info({ xid }, 'Attempting direct API call');

    try {
        const response = await fetch(REUTERS_API_URL, {
            method: 'POST',
            headers: {
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': authToken,
                'cache-control': 'no-cache',
                'content-type': 'application/json',
                'pragma': 'no-cache',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'cross-site',
                'referer': 'https://www.reuters.com/'
            },
            body: JSON.stringify([
                { key: 'marketAppId', value: 'fwc-currency-detailed-quote' },
                { key: 'xid', value: xid },
                { key: 'showLinks', value: false }
            ]),
            mode: 'cors',
            credentials: 'include'
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        logger.info('Direct API call successful');

        return {
            method: 'direct-api',
            xid,
            capturedAt: new Date().toISOString(),
            data
        };
    } catch (error) {
        logger.warn({ error: error.message }, 'Direct API call failed');
        throw error;
    }
}

/**
 * Fetch Reuters market data using Playwright
 * @param {string} symbol - Currency pair symbol (e.g., 'VND=X' for USD/VND)
 * @param {string} proxy - Proxy URL (optional, e.g., 'http://user:pass@proxy:port')
 * @param {string} wsEndpoint - Browser WebSocket endpoint for connecting to an existing browser instance (optional)
 */
async function fetchReutersData({ symbol = 'VND=X', proxy = null, wsEndpoint = null }) {
    const pageUrl = `https://www.reuters.com/markets/quote/${symbol}/`;

    logger.info({ symbol, url: pageUrl }, 'Fetching Reuters data');
    if (proxy) logger.info({ proxy }, 'Using proxy');

    //TODO: handle bot detection if occurs
    const launchOptions = {
        headless: true,
        wsEndpoint,
        args: [ '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process',
            '--disable-gpu'
        ]
    };

    if (proxy) {
        launchOptions.proxy = { server: proxy };
    }
    logger.info({ launchOptions }, 'Connecting to browser');

    const browser = await chromium.launch(launchOptions);

    try {
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        });

        const page = await context.newPage();

        // Capture all API responses from fwc-universal-app
        const apiResponses = [];
        page.on('response', async (response) => {
            const url = response.url();
            if (url.includes('api.markitdigital.com/fwc-api-service/v1/fwc-universal-app')) {
                try {
                    const data = await response.json();
                    apiResponses.push({
                        url: url,
                        status: response.status(),
                        data: data
                    });
                    logger.info({ url }, 'Captured API response');
                } catch (e) {
                    logger.warn({ url }, 'Could not parse response as JSON');
                }
            }
        });

        // Visit the Reuters page
        await page.goto(pageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });

        logger.info('Page loaded');

        // Check if access is restricted
        const pageContent = await page.content();
        if (pageContent.includes('Access is temporarily restricted') ||
            pageContent.includes('Access Denied') ||
            pageContent.includes('blocked')) {
            await context.close();
            throw new Error('Access is temporarily restricted by Reuters. Please try again later or use a different IP/proxy.');
        }

        // Wait a bit more to ensure all API calls complete
        await page.waitForTimeout(3000);

        await context.close();

        if (apiResponses.length === 0) {
            throw new Error('No API responses captured from fwc-universal-app');
        }

        logger.info({ count: apiResponses.length }, 'Captured API responses');

        // Return all captured responses
        return {
            symbol,
            capturedAt: new Date().toISOString(),
            responses: apiResponses
        };

    } finally {
        await browser.close();
    }
}

/**
 * Save data to files (timestamped and latest)
 * @param {Object} data - Data to save
 * @param {string} prefix - File prefix (e.g., 'reuters')
 * @param {string} identifier - Unique identifier for the file (e.g., 'VND_X')
 * @param {string} dataDir - Directory to save files
 * @param {Object} latestData - Optional different data for latest file (e.g., capturedData.data only)
 * @returns {Promise<{filepath: string, latestPath: string}>}
 */
async function saveData(data, prefix, identifier, dataDir, latestData = null) {
    const timestamp = Date.now();
    const filename = `${prefix}_${identifier}_${timestamp}.json`;
    const filepath = path.join(dataDir, filename);

    // Ensure data directory exists
    await fs.mkdir(dataDir, { recursive: true });

    // Save JSON content with timestamp
    await fs.writeFile(filepath, JSON.stringify(data, null, 2), 'utf-8');
    logger.info({ filepath }, 'JSON saved');

    // Also save as latest (use latestData if provided, otherwise use data)
    const latestFilename = `${prefix}_${identifier}_latest.json`;
    const latestPath = path.join(dataDir, latestFilename);
    await fs.writeFile(latestPath, JSON.stringify(latestData || data, null, 2), 'utf-8');
    logger.info({ latestPath }, 'Latest JSON saved');

    return { filepath, latestPath };
}

/**
 * Crawl Reuters data and save to file
 * @param {Object} options
 * @param {string} options.symbol - Currency pair symbol (default: 'VND=X')
 * @param {number} options.xid - Market/symbol ID for direct API call (optional, will be extracted if not provided)
 * @param {string} options.authToken - Authorization token (optional, will be extracted if not provided)
 * @param {boolean} options.extractToken - Whether to extract token from page (default: true)
 * @param {string} options.proxy - Proxy URL (optional)
 */
async function crawlReuters(options = {}) {
    const {
        symbol = 'VND=X',
        xid = null,
        authToken = null,
        extractToken = true,
        proxy = process.env.REUTERS_PROXY || null,
        wsEndpoint
    } = options;

    try {
        let capturedData;
        let finalXid = xid || DEFAULT_XID;
        let finalAuthToken = authToken || REUTERS_AUTH_TOKEN;

        // Extract token and xid from page if not provided and extractToken is true
        if (extractToken && (!xid || !authToken)) {
            try {
                logger.info('Extracting token and xid from page');
                const pageData = await extractTokenFromPage({ symbol, proxy, wsEndpoint });
                if (!xid) finalXid = pageData.xid;
                if (!authToken) finalAuthToken = pageData.token;
                logger.info({ xid: finalXid }, 'Using xid');
            } catch (error) {
                logger.warn({ error: error.message }, 'Token extraction failed, falling back to defaults');
            }
        }

        // Try direct API call first, fall back to browser if it fails
        try {
            capturedData = await fetchReutersApiDirect(finalXid, finalAuthToken);
        } catch (error) {
            logger.info('Direct API failed, falling back to browser method');
            capturedData = await fetchReutersData({ symbol, proxy, wsEndpoint });
        }

        // Save data to files
        const safeSymbol = symbol.replace(/[=\/]/g, '_');
        const { filepath, latestPath } = await saveData(capturedData, 'reuters', safeSymbol, DATA_DIR, capturedData.data);

        return {
            success: true,
            filepath,
            latestPath,
            timestamp: new Date().toISOString(),
            symbol,
            xid: finalXid,
            method: capturedData.method || 'browser',
            responsesCount: capturedData.responses?.length || (capturedData.data ? 1 : 0),
            size: JSON.stringify(capturedData).length
        };
    } catch (error) {
        logger.error({ error: error.message, stack: error.stack }, 'Error crawling Reuters');
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
        '--xid': (val) => ({ xid: parseInt(val) }),
        '--auth-token': (val) => ({ authToken: val.startsWith('Bearer ') ? val : `Bearer ${val}` }),
        '--proxy': 'proxy',
        '--ws-endpoint': 'wsEndpoint',
        '--no-extract': () => ({ extractToken: false })
    });

    crawlReuters(options)
        .then(result => {
            logger.info({
                symbol: result.symbol,
                xid: result.xid,
                method: result.method,
                responsesCount: result.responsesCount,
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
        const result = await crawlReuters(options);
        
        // Auto-trigger import job if flag is set
        if (options.autoImport) {
            logger.info('Auto-import enabled, triggering IMPORTER_REUTERS job');
            const importQueue = new Queue('IMPORTER_REUTERS', {
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



// # With auth token (automatically adds "Bearer " prefix if needed)
// node reuters.worker.js --xid 611986 --auth-token "Z3E0SGH6bO8jBQfYLjAY3esdHEi3"

// # Or with full Bearer token
// node reuters.worker.js --xid 611986 --auth-token "Bearer Z3E0SGH6bO8jBQfYLjAY3esdHEi3"
//node reuters.worker.js --ws-endpoint "wss://production-sfo.browserless.io/?token=$BROWERLESS_API_KEY"
// # Via environment variable
// export REUTERS_AUTH_TOKEN="Bearer YOUR_TOKEN"
// export REUTERS_XID=611986
// npm run crawl:reuters