#!/usr/bin/env node
require('dotenv').config();

const { Queue } = require('bullmq');
const { createLogger } = require('../utils/logger');

const logger = createLogger('jobs.cli');

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

const QUEUE_NAMES = [
    'WORKER_VCB',
    'WORKER_XE',
    'WORKER_REUTERS',
    'WORKER_YAHOO',
    'IMPORTER_VCB',
    'IMPORTER_XE',
    'IMPORTER_REUTERS',
    'IMPORTER_YAHOO'
];

/**
 * Get or create queue instance
 * @param {string} queueName - Name of the queue
 * @returns {Queue}
 */
function getQueue(queueName) {
    return new Queue(queueName, DEFAULT_QUEUE_OPTIONS);
}

/**
 * Parse command line arguments
 * @param {string[]} args - Command line arguments
 * @returns {Object} Parsed options
 */
function parseArgs(args) {
    const options = {
        command: null,
        queue: null,
        data: {},
        repeat: null
    };
    
    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        
        if (arg === 'add' || arg === 'list' || arg === 'clear' || arg === 'remove') {
            options.command = arg;
        } else if (arg === '--queue' && args[i + 1]) {
            options.queue = args[i + 1];
            i++;
        } else if (arg === '--data' && args[i + 1]) {
            try {
                options.data = JSON.parse(args[i + 1]);
            } catch (e) {
                console.error('Error: Invalid JSON for --data');
                process.exit(1);
            }
            i++;
        } else if (arg === '--repeat' && args[i + 1]) {
            const repeatValue = args[i + 1];
            if (repeatValue.match(/^\d+$/)) {
                // Interval in milliseconds
                options.repeat = { every: parseInt(repeatValue) };
            } else {
                // Cron pattern
                options.repeat = { pattern: repeatValue };
            }
            i++;
        } else if (arg === '--id' && args[i + 1]) {
            options.jobId = args[i + 1];
            i++;
        }
    }
    
    return options;
}

/**
 * Add a job to a queue
 * @param {string} queueName - Name of the queue
 * @param {Object} data - Job data
 * @param {Object} repeat - Repeat options
 */
async function addJob(queueName, data = {}, repeat = null) {
    if (!QUEUE_NAMES.includes(queueName)) {
        console.error(`Error: Queue "${queueName}" not found`);
        console.log('Available queues:', QUEUE_NAMES.join(', '));
        process.exit(1);
    }
    
    const queue = getQueue(queueName);
    
    try {
        const jobOptions = repeat ? { repeat } : {};
        const job = await queue.add('job', data, jobOptions);
        
        console.log(`✓ Added ${repeat ? 'repeating ' : ''}job to ${queueName}`);
        console.log(`  Job ID: ${job.id}`);
        if (repeat) {
            console.log(`  Repeat: ${JSON.stringify(repeat)}`);
        }
        if (Object.keys(data).length > 0) {
            console.log(`  Data: ${JSON.stringify(data)}`);
        }
    } finally {
        await queue.close();
    }
}

/**
 * List jobs in a queue
 * @param {string} queueName - Name of the queue (optional)
 */
async function listJobs(queueName = null) {
    const queues = queueName ? [queueName] : QUEUE_NAMES;
    
    for (const name of queues) {
        if (!QUEUE_NAMES.includes(name)) {
            console.error(`Error: Queue "${name}" not found`);
            continue;
        }
        
        const queue = getQueue(name);
        
        try {
            const [waiting, active, completed, failed, delayed, repeatable] = await Promise.all([
                queue.getWaiting(),
                queue.getActive(),
                queue.getCompleted(),
                queue.getFailed(),
                queue.getDelayed(),
                queue.getRepeatableJobs()
            ]);
            
            console.log(`\n${name}:`);
            console.log(`  Waiting: ${waiting.length}`);
            console.log(`  Active: ${active.length}`);
            console.log(`  Completed: ${completed.length}`);
            console.log(`  Failed: ${failed.length}`);
            console.log(`  Delayed: ${delayed.length}`);
            console.log(`  Repeatable: ${repeatable.length}`);
            
            if (repeatable.length > 0) {
                console.log('\n  Repeatable Jobs:');
                for (const job of repeatable) {
                    console.log(`    - Key: ${job.key}`);
                    console.log(`      Pattern: ${job.pattern || `every ${job.every}ms`}`);
                    console.log(`      Next: ${new Date(job.next).toISOString()}`);
                }
            }
            
            if (failed.length > 0) {
                console.log('\n  Recent Failed Jobs:');
                for (const job of failed.slice(0, 3)) {
                    console.log(`    - ID: ${job.id}`);
                    console.log(`      Reason: ${job.failedReason}`);
                    console.log(`      Attempts: ${job.attemptsMade}/${job.opts.attempts}`);
                }
            }
        } finally {
            await queue.close();
        }
    }
}

/**
 * Clear jobs in a queue
 * @param {string} queueName - Name of the queue
 */
async function clearQueue(queueName) {
    if (!QUEUE_NAMES.includes(queueName)) {
        console.error(`Error: Queue "${queueName}" not found`);
        console.log('Available queues:', QUEUE_NAMES.join(', '));
        process.exit(1);
    }
    
    const queue = getQueue(queueName);
    
    try {
        await queue.obliterate({ force: true });
        console.log(`✓ Cleared all jobs in queue: ${queueName}`);
    } finally {
        await queue.close();
    }
}

/**
 * Remove a specific repeatable job
 * @param {string} queueName - Name of the queue
 * @param {string} jobId - Job ID or repeat key
 */
async function removeJob(queueName, jobId) {
    if (!QUEUE_NAMES.includes(queueName)) {
        console.error(`Error: Queue "${queueName}" not found`);
        process.exit(1);
    }
    
    const queue = getQueue(queueName);
    
    try {
        const repeatable = await queue.getRepeatableJobs();
        const job = repeatable.find(j => j.key.includes(jobId) || j.id === jobId);
        
        if (job) {
            await queue.removeRepeatableByKey(job.key);
            console.log(`✓ Removed repeatable job: ${job.key}`);
        } else {
            // Try to remove as regular job
            const regularJob = await queue.getJob(jobId);
            if (regularJob) {
                await regularJob.remove();
                console.log(`✓ Removed job: ${jobId}`);
            } else {
                console.error(`Error: Job "${jobId}" not found`);
            }
        }
    } finally {
        await queue.close();
    }
}

/**
 * Show usage information
 */
function showUsage() {
    console.log('Job Queue Management CLI\n');
    console.log('Usage:');
    console.log('  node cli/jobs.js <command> [options]\n');
    console.log('Commands:');
    console.log('  add       Add a job to a queue');
    console.log('  list      List jobs in queue(s)');
    console.log('  clear     Clear all jobs in a queue');
    console.log('  remove    Remove a specific job\n');
    console.log('Options:');
    console.log('  --queue <name>           Queue name (required for add/clear/remove)');
    console.log('  --data <json>            Job data as JSON string');
    console.log('  --repeat <cron|interval> Repeat pattern (cron or milliseconds)');
    console.log('  --id <jobId>             Job ID (for remove command)\n');
    console.log('Available Queues:');
    console.log(`  ${QUEUE_NAMES.join(', ')}\n`);
    console.log('Examples:');
    console.log('  # Add single job');
    console.log('  node cli/jobs.js add --queue WORKER_VCB\n');
    console.log('  # Add job with data');
    console.log('  node cli/jobs.js add --queue WORKER_YAHOO --data \'{"symbol":"EURUSD=X"}\'\n');
    console.log('  # Add repeating job (every 6 hours)');
    console.log('  node cli/jobs.js add --queue WORKER_VCB --repeat "0 */6 * * *"\n');
    console.log('  # Add repeating job (every hour in milliseconds)');
    console.log('  node cli/jobs.js add --queue WORKER_VCB --repeat 3600000\n');
    console.log('  # List specific queue');
    console.log('  node cli/jobs.js list --queue WORKER_VCB\n');
    console.log('  # List all queues');
    console.log('  node cli/jobs.js list\n');
    console.log('  # Clear queue');
    console.log('  node cli/jobs.js clear --queue WORKER_VCB\n');
    console.log('  # Remove job');
    console.log('  node cli/jobs.js remove --queue WORKER_VCB --id <jobId>');
}

// Main execution
if (require.main === module) {
    const args = process.argv.slice(2);
    
    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        showUsage();
        process.exit(0);
    }
    
    const options = parseArgs(args);
    
    const run = async () => {
        try {
            switch (options.command) {
                case 'add':
                    if (!options.queue) {
                        console.error('Error: --queue is required for add command');
                        process.exit(1);
                    }
                    await addJob(options.queue, options.data, options.repeat);
                    break;
                    
                case 'list':
                    await listJobs(options.queue);
                    break;
                    
                case 'clear':
                    if (!options.queue) {
                        console.error('Error: --queue is required for clear command');
                        process.exit(1);
                    }
                    await clearQueue(options.queue);
                    break;
                    
                case 'remove':
                    if (!options.queue || !options.jobId) {
                        console.error('Error: --queue and --id are required for remove command');
                        process.exit(1);
                    }
                    await removeJob(options.queue, options.jobId);
                    break;
                    
                default:
                    console.error(`Error: Unknown command "${options.command}"`);
                    console.log('Run with --help for usage information');
                    process.exit(1);
            }
            
            process.exit(0);
        } catch (error) {
            console.error('Error:', error.message);
            process.exit(1);
        }
    };
    
    run();
}

module.exports = { addJob, listJobs, clearQueue, removeJob };
