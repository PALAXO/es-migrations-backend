import _  from 'lodash';
import bunyan from 'bunyan';
import path from 'path';

import nconf from '../config/config.js';

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
            stream.path = path.isAbsolute(stream.path) ? stream.path : path.join(import.meta.dirname, `/../`, stream.path);
        } else if (stream.type === `console`) {
            stream.type = `raw`;

            stream.stream = {
                write: (data) => {
                    const result = {
                        uid: singleton.uid
                    };
                    _.defaults(result, data);
                    process.stdout.write(`${JSON.stringify(result)}\n`);
                }
            };
        }
    }

    return cfgCopy;
};

class Logger {
    constructor(config = void 0, uid = void 0) {
        this.logger = bunyan.createLogger(parseConfig(config || nconf.get(`logs:migrations`)));
        this.uid = uid;
    }

    /**
     * Generic log function respecting the presented level
     * @param data      {string|Object} Data for logger
     * @param level     {string}        Message log level
     */
    log(data, level) {
        const msg = data?.msg ?? data?.message ?? ``;
        this.logger[level](data, msg);
    }
}

let singleton = new Logger();

export default {
    /**
     * Changes config
     * @param config {Object}
     * @param uid {string}
     */
    setLoggerConfig(config = void 0, uid = void 0) {
        singleton = new Logger(config, uid);
    },
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
