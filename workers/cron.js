#!/usr/bin/env node
require('dotenv').config();

const cron = require('node-cron');
const { Queue } = require('bullmq');
const { createLogger } = require('./utils/logger');
const CONFIGS = require("./config")

const logger = createLogger('cron');

const CONNECTION_OPTIONS = {
    connection: {
        url: process.env.REDIS_URL || 'redis://localhost:6379',
    }
};

const DEFAULT_QUEUE_OPTIONS = {
    ...CONNECTION_OPTIONS,
    defaultJobOptions: {
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 60_000,
        },
        removeOnComplete: true,
        removeOnFail: false,
    },
};

// Initialize queues
const queues = {
    WORKER_VCB: new Queue('WORKER_VCB', DEFAULT_QUEUE_OPTIONS),
    WORKER_XE: new Queue('WORKER_XE', DEFAULT_QUEUE_OPTIONS),
    WORKER_REUTERS: new Queue('WORKER_REUTERS', DEFAULT_QUEUE_OPTIONS),
    WORKER_YAHOO: new Queue('WORKER_YAHOO', DEFAULT_QUEUE_OPTIONS),
};

/**
 * Schedule VCB exchange rates crawl
 * Every minute for testing, adjust as needed
 */
cron.schedule('* * * * *', async () => {
    try {
        const timestamp = new Date();
        timestamp.setSeconds(0, 0);
        const jobId = `vcb-${timestamp.getTime()}`;
        
        logger.info('Running scheduled VCB crawl job');
        await queues.WORKER_VCB.add('crawl', CONFIGS.WORKER_VCB, {
            jobId
        });
        logger.info('VCB crawl job added to queue');
    } catch (error) {
        logger.error({ error: error.message }, 'Failed to add VCB crawl job');
    }
});

/**
 * Schedule Yahoo Finance crawl
 * Every minute for testing
 */
cron.schedule('* * * * *', async () => {
    try {
        const timestamp = new Date();
        timestamp.setSeconds(0, 0);
        
        logger.info('Running scheduled Yahoo crawl job');
        
        // Add multiple currency pairs
        const symbols = ['VND=X', 'EUR=X', 'JPY=X'];
        
        for (const symbol of symbols) {
            const jobId = `yahoo-${symbol.replace('=', '')}-${timestamp.getTime()}`;
            await queues.WORKER_YAHOO.add('crawl', { ...CONFIGS.WORKER_YAHOO, symbol }, {
                jobId
            });
            logger.info({ symbol }, 'Yahoo crawl job added to queue');
        }
    } catch (error) {
        logger.error({ error: error.message }, 'Failed to add Yahoo crawl jobs');
    }
});

/**
 * Schedule Reuters crawl
 * Every minute for testing
 */
cron.schedule('* * * * *', async () => {
    try {
        const timestamp = new Date();
        timestamp.setSeconds(0, 0);
        const jobId = `reuters-${timestamp.getTime()}`;
        
        logger.info('Running scheduled Reuters crawl job');
        await queues.WORKER_REUTERS.add('crawl', CONFIGS.WORKER_REUTERS, {
            jobId
        });
        logger.info('Reuters crawl job added to queue');
    } catch (error) {
        logger.error({ error: error.message }, 'Failed to add Reuters crawl job');
    }
});

/**
 * Schedule XE.com crawl
 * Every minute for testing
 */
cron.schedule('* * * * *', async () => {
    try {
        const timestamp = new Date();
        timestamp.setSeconds(0, 0);
        
        logger.info('Running scheduled XE crawl job');
        
        // Add multiple currency pairs
        const pairs = [
            { from: 'USD', to: 'VND' },
            { from: 'EUR', to: 'VND' },
            { from: 'JPY', to: 'VND' }
        ];
        
        for (const pair of pairs) {
            const jobId = `xe-${pair.from}${pair.to}-${timestamp.getTime()}`;
            await queues.WORKER_XE.add('crawl', {
                ...CONFIGS.WORKER_XE,
                fromCurrency: pair.from,
                toCurrency: pair.to,
            }, {
                jobId
            });
            logger.info({ pair: `${pair.from}/${pair.to}` }, 'XE crawl job added to queue');
        }
    } catch (error) {
        logger.error({ error: error.message }, 'Failed to add XE crawl jobs');
    }
});

// Graceful shutdown
process.on('SIGINT', async () => {
    logger.info('Shutting down cron scheduler...');
    
    // Close all queues
    for (const [name, queue] of Object.entries(queues)) {
        try {
            await queue.close();
            logger.info({ queue: name }, 'Queue closed');
        } catch (error) {
            logger.error({ queue: name, error: error.message }, 'Error closing queue');
        }
    }
    
    logger.info('Cron scheduler stopped');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    logger.info('Received SIGTERM, shutting down...');
    process.kill(process.pid, 'SIGINT');
});

logger.info('Cron scheduler started');
logger.info('Schedules:');
logger.info('  - VCB: Every minute');
logger.info('  - Yahoo: Every minute (VND=X, EUR=X, JPY=X)');
logger.info('  - Reuters: Every minute (VND=X)');
logger.info('  - XE: Every minute (USD/VND, EUR/VND, JPY/VND)');
logger.info('Press Ctrl+C to stop');

// Keep the process running
process.stdin.resume();
