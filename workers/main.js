require('dotenv').config();

const path = require('path');
const { Queue, Worker, WorkerOptions, QueueOptions } = require('bullmq');
const Redis = require("ioredis");
const { createLogger } = require('./utils/logger');

const logger = createLogger('main');
const redisLogger = createLogger('redis');

const client = new Redis(process.env.REDIS_URL);

client.on("error", function (err) {
    redisLogger.error({ error: err.message }, "Redis Client Error");
});

const subscriber = new Redis(process.env.REDIS_URL);

subscriber.on("error", function (err) {
    redisLogger.error({ error: err.message }, "Redis Subscriber Error");
});

const CONNECTION_OPTIONS = {
    connection: {
        url: process.env.REDIS_URL,
    },
    createClient: function (type, redisOpts) {
        switch (type) {
            case "client":
                return client;
            case "subscriber":
                return subscriber;
            case "bclient":
                return new Redis(process.env.REDIS_URL, redisOpts);
            default:
                throw new Error("Unexpected connection type: ", type);
        }
    },
};

/**
 *  @type {WorkerOptions}
 */
const DEFAULT_WORKER_OPTIONS = {
    ...CONNECTION_OPTIONS,
    lockDuration: 300_000, // 5 minutes
    concurrency: 1,
    removeOnComplete: true,
    removeOnFail: false,
    useWorkerThreads: true,
    autorun: false,
    // TODO: add bullmq-otel
    //telemetry
}
/**
 * @type {QueueOptions}
 */
const DEFAULT_QUEUE_OPTIONS = {
    ...CONNECTION_OPTIONS,
    defaultJobOptions: {
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 60_000, // 1 minute
        },
        removeOnComplete: true,
        removeOnFail: false,
    },
};

const WOKERS = Object.freeze({
    "WORKER_VCB": new Worker("WORKER_VCB", path.join(__dirname, 'crawlers', 'vcb.worker.js'), DEFAULT_WORKER_OPTIONS),
    "WORKER_XE": new Worker("WORKER_XE", path.join(__dirname, 'crawlers', 'xe.worker.js'), DEFAULT_WORKER_OPTIONS),
    "WORKER_REUTERS": new Worker("WORKER_REUTERS", path.join(__dirname, 'crawlers', 'reuters.worker.js'), DEFAULT_WORKER_OPTIONS),
    "WORKER_YAHOO": new Worker("WORKER_YAHOO", path.join(__dirname, 'crawlers', 'yahoo.worker.js'), DEFAULT_WORKER_OPTIONS),

    "IMPORTER_VCB": new Worker("IMPORTER_VCB", path.join(__dirname, 'importers', 'vcb.importer.js'), DEFAULT_WORKER_OPTIONS),
    "IMPORTER_XE": new Worker("IMPORTER_XE", path.join(__dirname, 'importers', 'xe.importer.js'), DEFAULT_WORKER_OPTIONS),
    "IMPORTER_REUTERS": new Worker("IMPORTER_REUTERS", path.join(__dirname, 'importers', 'reuters.importer.js'), DEFAULT_WORKER_OPTIONS),
    "IMPORTER_YAHOO": new Worker("IMPORTER_YAHOO", path.join(__dirname, 'importers', 'yahoo.importer.js'), DEFAULT_WORKER_OPTIONS),
})

const QUEUES = {
    "WORKER_VCB": new Queue("WORKER_VCB", { ...DEFAULT_QUEUE_OPTIONS, }),
    "WORKER_XE": new Queue("WORKER_XE", { ...DEFAULT_QUEUE_OPTIONS, }),
    "WORKER_REUTERS": new Queue("WORKER_REUTERS", { ...DEFAULT_QUEUE_OPTIONS, }),
    "WORKER_YAHOO": new Queue("WORKER_YAHOO", { ...DEFAULT_QUEUE_OPTIONS, }),

    //importers
    "IMPORTER_VCB": new Queue("IMPORTER_VCB", { ...DEFAULT_QUEUE_OPTIONS, }),
    "IMPORTER_XE": new Queue("IMPORTER_XE", { ...DEFAULT_QUEUE_OPTIONS, }),
    "IMPORTER_REUTERS": new Queue("IMPORTER_REUTERS", { ...DEFAULT_QUEUE_OPTIONS, }),
    "IMPORTER_YAHOO": new Queue("IMPORTER_YAHOO", { ...DEFAULT_QUEUE_OPTIONS, }),
};


if (require.main === module) {
    logger.info("Workers and Queues initialized:");
    for (const [ name, worker ] of Object.entries(WOKERS)) {
        // logger.info(` - ${name}`);
        if(process.env.ACTIVE_WORKERS 
            && process.env.ACTIVE_WORKERS.split(',').includes(name.replace('WORKER_','').replace('IMPORTER_',''))
        ) {
            logger.info(`   -> Activating worker ${name}`);
            worker.run();
        }
    }
    // logger.info("Queues:");
    // for (const [ name, queue ] of Object.entries(QUEUES)) {
    //     logger.info(` - ${name}`);
    // }
}




