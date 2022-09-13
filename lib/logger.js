'use strict';

const _ = require(`lodash`);
const bunyan = require(`bunyan`);
const path = require(`path`);

const nconf = require(`../config/config`);

const LOG_LEVEL = Object.freeze({
    TRACE: `trace`,
    DEBUG: `debug`,
    INFO: `info`,
    WARN: `warn`,
    ERROR: `error`,
    FATAL: `fatal`
});

/**
 * Parse configuration
 * @param config {Object} Configuration object
 * @returns {Object}
 */
const parseConfig = (config) => {
    // Do not persist changes
    const cfgCopy = _.cloneDeep(config);

    // Transform our config to bunyan convention
    for (const stream of cfgCopy.streams) {
        if ([`file`, `rotating-file`].includes(stream.type)) {
            stream.path = path.isAbsolute(stream.path) ? stream.path : path.join(__dirname, `/../`, stream.path);
        } else if (stream.type === `console`) {
            stream.type = `raw`;

            stream.stream = {
                write: (data) => {
                    process.stdout.write(`${JSON.stringify(data)}\n`);
                }
            };
        }
    }

    return cfgCopy;
};

class Logger {
    constructor(config = void 0) {
        this.logger = bunyan.createLogger(parseConfig(config || nconf.get(`logs:migrations`)));
    }

    /**
     * Generic log function respecting the presented level
     * @param data      {string|Object} Data for logger
     * @param level     {string}        Message log level
     */
    log (data, level) {
        const msg = data?.msg ?? data?.message ?? ``;
        this.logger[level](data, msg);
    }
}

const singleton = new Logger();

module.exports = {
    /**
     * Trace level logging
     * @param message {string|Object} Data send to logger
     */
    trace(message) {
        singleton.log(message, LOG_LEVEL.TRACE);
    },

    /**
     * Debug level logging
     * @param message {string|Object} Data send to logger
     */
    debug(message) {
        singleton.log(message, LOG_LEVEL.DEBUG);
    },

    /**
     * Info level logging
     * @param message {string|Object} Data send to logger
     */
    info(message) {
        singleton.log(message, LOG_LEVEL.INFO);
    },

    /**
     * Warn level logging
     * @param message {string|Object} Data send to logger
     */
    warn(message) {
        singleton.log(message, LOG_LEVEL.WARN);
    },

    /**
     * Error level logging
     * @param message {string|Object} Data send to logger
     */
    error(message) {
        singleton.log(message, LOG_LEVEL.ERROR);
    },

    /**
     * Fatal level logging
     * @param message {string|Object} Data send to logger
     */
    fatal(message) {
        singleton.log(message, LOG_LEVEL.FATAL);
    }
};
