'use strict';

module.exports = {
    /** @type {Record<string, string>} */
    synchronisationTypes: Object.freeze({
        STOP: `STOP`,
        SERIAL: `SERIAL`,
        INDICES: `INDICES`,
        BULK: `BULK`,
        PUT: `PUT`,
        DOCUMENTS: `DOCUMENTS`,
        SCRIPT: `SCRIPT`
    })
};