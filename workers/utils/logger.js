const pino = require('pino');
const path = require('path');
const fs = require('fs');

// Ensure logs directory exists
const logsDir = path.join(__dirname, '..', 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

/**
 * Create a logger instance for a specific module
 * @param {string} moduleName - Name of the module (e.g., 'vcb.worker', 'main')
 * @returns {pino.Logger} Pino logger instance
 */
function createLogger(moduleName) {
    const timestamp = new Date().toISOString().split('T')[ 0 ]; // YYYY-MM-DD
    const logFile = path.join(logsDir, `${moduleName}_${timestamp}.log`);

    return pino({
        level: process.env.LOG_LEVEL || 'info',
        timestamp: pino.stdTimeFunctions.isoTime,
        transport: {
            target: 'pino-pretty',
            options: {
                colorize: false,
                translateTime: 'yyyy-mm-dd HH:MM:ss',
                ignore: 'pid,hostname',
                destination: logFile
            }
        }
        // formatters: {
        //     level: (label) => {
        //         return { level: label };
        //     }
        // }
    }, pino.destination({
        dest: logFile,
        sync: false,
        mkdir: true
    }));
}

module.exports = { createLogger };
