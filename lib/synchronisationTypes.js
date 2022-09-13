'use strict';

module.exports = {
    synchronisationTypes: Object.freeze({
        STOP: Symbol(`stop`),
        SERIAL: Symbol(`serial`),
        INDICES: Symbol(`indices`),
        BULK: Symbol(`bulk`),
        PUT: Symbol(`put`),
        DOCUMENTS: Symbol(`documents`),
        SCRIPT: Symbol(`script`)
    })
};