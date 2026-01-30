const fs = require('fs').promises;
const path = require('path');
const { XMLParser } = require('fast-xml-parser');
const { Queue } = require('bullmq');
const { createLogger } = require('../utils/logger');

const logger = createLogger('vcb.worker');

const VCB_URL = 'https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx';
const DATA_DIR = path.resolve(__dirname, '../data/vcb');

/**
 * Save data to files (timestamped and latest)
 * @param {string} xmlContent - XML content to save
 * @param {Object} jsonData - JSON data to save
 * @param {string} prefix - File prefix (e.g., 'vcb_exchange_rates')
 * @param {string} dataDir - Directory to save files
 * @returns {Promise<{xmlFilepath: string, jsonFilepath: string}>}
 */
async function saveData(xmlContent, jsonData, prefix, dataDir) {
    const timestamp = Date.now();
    const xmlFilename = `${prefix}_${timestamp}.xml`;
    const jsonFilename = `${prefix}_${timestamp}.json`;
    const xmlFilepath = path.join(dataDir, xmlFilename);
    const jsonFilepath = path.join(dataDir, jsonFilename);

    // Ensure data directory exists
    await fs.mkdir(dataDir, { recursive: true });

    // Save XML content
    await fs.writeFile(xmlFilepath, xmlContent, 'utf-8');
    logger.info({ xmlFilepath }, 'XML saved');

    // Save JSON content
    await fs.writeFile(jsonFilepath, JSON.stringify(jsonData, null, 2), 'utf-8');
    logger.info({ jsonFilepath }, 'JSON saved');

    // Also save as latest files
    const latestXmlPath = path.join(dataDir, 'vcb_latest.xml');
    const latestJsonPath = path.join(dataDir, 'vcb_latest.json');
    await fs.writeFile(latestXmlPath, xmlContent, 'utf-8');
    await fs.writeFile(latestJsonPath, JSON.stringify(jsonData, null, 2), 'utf-8');
    logger.info('Latest files saved');

    return { xmlFilepath, jsonFilepath };
}

/**
 * Crawl VCB exchange rates and save to files
 * @param {Object} options
 * @param {string} options.proxy - Proxy URL (optional, e.g., 'socks5://127.0.0.1:9050')
 * Note: Native Node.js fetch doesn't support proxies. For proxy support, consider using external libraries.
 */
async function crawlVCBExchangeRates(options = {}) {
    const {
        proxy = process.env.VCB_PROXY || null
    } = options;

    try {
        logger.info({ url: VCB_URL }, 'Fetching XML');
        if (proxy) logger.warn({ proxy }, 'Proxy specified but native fetch doesn\'t support proxies');

        // Fetch the XML content
        const response = await fetch(VCB_URL, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/xml, text/xml, */*',
                'Accept-Language': 'en-US,en;q=0.9',
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const xmlContent = await response.text();

        // Parse XML to JSON
        const parser = new XMLParser({
            ignoreAttributes: false,
            attributeNamePrefix: '@_',
            textNodeName: '#text'
        });
        const jsonData = parser.parse(xmlContent);

        // Save data to files
        const { xmlFilepath, jsonFilepath } = await saveData(xmlContent, jsonData, 'vcb_exchange_rates', DATA_DIR);

        // Extract and log exchange rate count
        const exrateCount = jsonData?.ExrateList?.Exrate?.length || 0;
        logger.info({ exrateCount }, 'Parsed exchange rates');

        return {
            success: true,
            xmlFilepath,
            jsonFilepath,
            timestamp: new Date().toISOString(),
            xmlSize: xmlContent.length,
            jsonSize: JSON.stringify(jsonData).length,
            exchangeRatesCount: exrateCount
        };
    } catch (error) {
        logger.error({ error: error.message, stack: error.stack }, 'Error crawling VCB exchange rates');
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
        '--proxy': 'proxy'
    });

    crawlVCBExchangeRates(options)
        .then(result => {
            logger.info({ result }, 'Crawl completed successfully');
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
        const result = await crawlVCBExchangeRates(options);
        
        // Auto-trigger import job if flag is set
        if (options.autoImport) {
            logger.info('Auto-import enabled, triggering IMPORTER_VCB job');
            const importQueue = new Queue('IMPORTER_VCB', {
                connection: {
                    url: process.env.REDIS_URL
                }
            });
            await importQueue.add('import', { mode: 'latest' });
            await importQueue.close();
            logger.info('Import job added to queue');
        }
        
        return result;
    } catch (error) {
        throw error;
    }
}
