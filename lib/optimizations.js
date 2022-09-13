'use strict';

const _ = require(`lodash`);
const nconf = require(`../config/config`);
const LIMITS = nconf.get(`options:limits`);
const OPTIMIZATIONS = nconf.get(`options:optimizations`);
const RETRIES = nconf.get(`es:retries`);

//Cache for indices restrictions
const restrictionsCache = {
    enabled: false,
    fields: [],
    data: {}
};

//Optimizations cache
let optimizations = {};

//Info about last queue change
const queueChange = {};
//Info about last bul bytes change
const bulkBytesChange = {};

/**
 * Creates optimization cache
 * @param cachedData {{}} Optional cache from previous run
 */
function createCache(cachedData = void 0) {
    optimizations = cachedData || {};

    if (!optimizations.averageDocumentSizes) {
        optimizations.averageDocumentSizes = {};
    }
    if (!OPTIMIZATIONS.dynamic.persistent || !optimizations.queueSize) {
        optimizations.queueSize = LIMITS.queue.defaultSize;
    }
    if (!OPTIMIZATIONS.dynamic.persistent || !optimizations.bulkBytes) {
        optimizations.bulkBytes = OPTIMIZATIONS.targetBulkSize.defaultMB * 1024 * 1024;
    }
}

/**
 * Returns optimization cache
 * @returns {{}}
 */
function getCache() {
    const myOptimizations = { ...optimizations };
    if (!OPTIMIZATIONS.dynamic.persistent) {
        delete myOptimizations.queueSize;
        delete myOptimizations.bulkBytes;
    }

    return myOptimizations;
}


//==================================================== RESTRICTIONS ====================================================
/**
 * Return true if index restrictions are enabled
 * @returns {boolean}
 */
function isRestrictionsEnabled() {
    return restrictionsCache.enabled;
}

/**
 * Enables index restrictions
 * @param fields {Array<string>} Fields to be restricted
 * @param originalData {{}} Object with original data
 */
function enableRestrictions(fields, originalData) {
    restrictionsCache.enabled = true;
    restrictionsCache.fields = fields;

    for (const index of Object.keys(originalData)) {
        const data = originalData[index];
        restrictionsCache.data[index] = {
            reference: void 0,
            data: data.data
        };
        restrictionsCache.data[data.alias] = {
            reference: index,
            data: void 0
        };
    }
}

/**
 * Disables restrictions and returns correct index data
 * @returns {{}}
 */
function disableRestrictions() {
    const output = {};
    if (isRestrictionsEnabled()) {
        restrictionsCache.enabled = false;
        for (const index of Object.keys(restrictionsCache.data)) {
            if (!_.isEmpty(restrictionsCache.data[index].data)) {
                output[index] = restrictionsCache.data[index].data;
            }
        }
    }

    return output;
}

/**
 * Returns restricted fields
 * @returns {Array<string>}
 */
function getRestrictionsFields() {
    if (isRestrictionsEnabled()) {
        return restrictionsCache.fields;
    }
}

/**
 * Returns empty (restricted) value of given field
 * @param field {string}
 * @returns {boolean|number}
 */
function getRestrictionsEmptyValue(field) {
    switch (field) {
        case `number_of_replicas`: return 0;
        case `auto_expand_replicas`: return false;
        case `refresh_interval`: return -1;
    }
}

/**
 * Returns default value of given field
 * @param field {string}
 * @returns {any}
 */
function getRestrictionsDefaultValue(field) {
    switch (field) {
        case `number_of_replicas`: return `1`;
        default: return null;
    }
}

/**
 * Returns real value of given restricted index
 * @param index {string}
 * @param field {string}
 * @returns {number|boolean|string}
 */
function getRestrictionsIndexValue(index, field) {
    if (isRestrictionsEnabled()) {
        if (!_.isUndefined(restrictionsCache.data[index].data?.[field])) {
            return restrictionsCache.data[index].data[field];
        } else {
            const reference = restrictionsCache.data[index].reference;
            return restrictionsCache.data[reference].data[field];
        }
    }
}

/**
 * Sets real values for given restricted index
 * @param index {string}
 * @param data {{}} Values to set
 * @param reference {string} Specify to create reference (alias) to real index
 * @param fromReference {boolean} True when we are specifying data from reference (alias)
 */
function setRestrictionsIndex(index, data, reference = void 0, fromReference = false) {
    if (isRestrictionsEnabled()) {
        if (!_.isNil(reference)) {
            restrictionsCache.data[index] = {
                reference: reference,
                data: void 0
            };
        } else {
            if (fromReference) {
                index = restrictionsCache.data[index].reference;
            }

            restrictionsCache.data[index] = {
                reference: void 0,
                data: data
            };
        }
    }
}

/**
 * Removes real data of restricted index from cache
 * @param index {string}
 */
function removeRestrictionsIndex(index) {
    if (isRestrictionsEnabled()) {
        delete restrictionsCache.data[index];
    }
}

/**
 * Is given index cached fro restrictions?
 * @param index {string}
 * @returns {boolean}
 */
function isKnownRestrictionIndex(index) {
    if (isRestrictionsEnabled()) {
        return (!!restrictionsCache.data[index]);
    } else {
        return false;
    }
}


//======================================================= COMMON =======================================================
/**
 * Returns optimal bulk size for given ODM
 * @param MainOdm
 * @returns {Promise<number>}   Best bulk size
 */
async function getBulkSize(MainOdm) {
    if (!OPTIMIZATIONS.targetBulkSize.enabled) {
        //Return default
        return Math.min(MainOdm._maxBulkSize ?? LIMITS.bulk.defaultSize, LIMITS.bulk.defaultSize);

    } else {
        //Get all possible ODM types
        let allTypes = [MainOdm];
        if (MainOdm.hasTypes()) {
            allTypes = await MainOdm.getTypes();
        }

        //Ensure we have sample for each ODM type
        let documentMaxSize = -1;
        let UsedTypedOdm = void 0;
        for (const TypedOdm of allTypes) {
            if (!optimizations.averageDocumentSizes[TypedOdm._alias]) {
                const randomDocs = await TypedOdm.search({}, 0, OPTIMIZATIONS.targetBulkSize.sampleSize, true);
                if (randomDocs.length > 0) {
                    const sources = randomDocs.map((doc) => doc._source);
                    const byteSize = (new TextEncoder().encode(JSON.stringify(sources))).length;
                    _updateDocumentSize(TypedOdm, (byteSize / sources.length));
                }
            }

            if (optimizations.averageDocumentSizes[TypedOdm._alias]) {
                //Update ODM type with the biggest average document size
                const documentSize = optimizations.averageDocumentSizes[TypedOdm._alias];
                if (documentSize > documentMaxSize) {
                    documentMaxSize = documentSize;
                    UsedTypedOdm = TypedOdm;
                }
            }
        }

        if (UsedTypedOdm) {
            //Return size for the biggest ODM
            return _getBulkSize(UsedTypedOdm);
        } else {
            //No data found... return default
            return Math.min(MainOdm._maxBulkSize ?? LIMITS.bulk.defaultSize, LIMITS.bulk.defaultSize);
        }
    }
}

/**
 * Calculates and updates average document size for given ODM
 * @param TypedOdm
 * @param byteSize {number} Search/scroll stream byte size
 * @param docCount {number} Document count in stream
 */
function updateDocumentSize(TypedOdm = void 0, byteSize, docCount) {
    if (!TypedOdm) {
        return;
    } else if (TypedOdm.hasTypes()) {
        throw Error(`Internal error, ODM used to update bulk sizes has types.`);
    }

    if (docCount > 0) {
        //Remove size of metadata
        byteSize -= (296 + docCount * 136);
        if (byteSize <= 0) {
            byteSize = 1;
        }

        //It is a difference whether we compute average document size from 1 or from 10000 documents
        //We consider full scale (=== 1) as a max allowed bulk size
        const bulkScale = docCount / LIMITS.bulk.maxSize;
        //Then we multiply bulk scale by low-pass coefficient
        const totalScale = bulkScale * OPTIMIZATIONS.targetBulkSize.lowPassCoefficient;

        //And we use this number to first order low-pass filter
        const batchDocumentSize = Math.floor(byteSize / docCount);
        const originalDocumentSize = optimizations?.averageDocumentSizes?.[TypedOdm._alias] ?? batchDocumentSize;
        _updateDocumentSize(TypedOdm, ((totalScale * originalDocumentSize) + ((1 - totalScale) * batchDocumentSize)));
    }
}

/**
 * Returns queue size
 * @returns {number}
 */
function getQueueSize() {
    return Math.max(Math.floor(optimizations.queueSize), LIMITS.queue.minSize);
}

//====================================================== DYNAMIC ======================================================
/**
 * Updates optimal bulk bytes and queue size
 * NOT IMPLEMENTED
 */
function updateStats() {
    //Not used
}

/**
 * Called when we receive 429 - Too Many Requests
 * Reduces ES load - queue and bulk bytes
 * @param counter {number} How long to sleep
 * @returns {Promise<void>}
 */
async function error429(counter = 0) {
    if (OPTIMIZATIONS.dynamic.limiting) {
        //Magic numbers
        _changeQueueSize(-0.25, true);
        _changeBulkBytes(-0.10, true);
    }

    await _sleep(counter);
}

/**
 * Called when we receive 413 - Payload Too Large
 * Reduces ES bulk load - bulk bytes
 */
function error413() {
    if (OPTIMIZATIONS.dynamic.limiting) {
        //Magic numbers
        _changeBulkBytes(-0.20, true);
    }
}


//====================================================== Internal ======================================================
/**
 * Returns optimal bulk size for given typed ODM
 * @param TypedOdm
 * @returns {number}
 * @private
 */
function _getBulkSize(TypedOdm) {
    const documentSize = optimizations.averageDocumentSizes[TypedOdm._alias];
    const bulkSize = Math.floor(optimizations.bulkBytes / documentSize);
    const maxLimit = Math.min(TypedOdm._maxBulkSize ?? LIMITS.bulk.maxSize, LIMITS.bulk.maxSize);
    return Math.max(Math.min(bulkSize, maxLimit), LIMITS.bulk.minSize);
}

/**
 * Updates average document size of given typed ODM
 * @param TypedOdm
 * @param documentSize {number} Average size of a single document
 * @private
 */
function _updateDocumentSize(TypedOdm, documentSize) {
    optimizations.averageDocumentSizes[TypedOdm._alias] = Math.max(Math.floor(documentSize), 1);
}

/**
 * Changes optimal bulk bytes
 * @param value {number} Relative change
 * @param isError {boolean} Is changed because of error?
 * @private
 */
function _changeBulkBytes(value = 0, isError = false) {
    if (_canChange(bulkBytesChange, isError)) {
        const minBytes = OPTIMIZATIONS.targetBulkSize.minMB * 1024 * 1024;
        const maxBytes = OPTIMIZATIONS.targetBulkSize.maxMB * 1024 * 1024;
        const bulkBytes = optimizations.bulkBytes * (value + 1);
        optimizations.bulkBytes = Math.min(Math.max(bulkBytes, minBytes), maxBytes);
    }
}

/**
 * Changes optimal queue size
 * @param value {number} Relative change
 * @param isError {boolean} Is changed because of error?
 * @private
 */
function _changeQueueSize(value = 0, isError = false) {
    if (_canChange(queueChange, isError)) {
        const queueSize = optimizations.queueSize * (value + 1);
        optimizations.queueSize = Math.min(Math.max(queueSize, LIMITS.queue.minSize), LIMITS.queue.maxSize);
    }
}

/**
 * Checks if we can do the change
 * @param lastChange {{time: number, isError: boolean}} When change happened the last time
 * @param isError {boolean} Is this changed happening because of error?
 * @returns {boolean} True if we can change
 * @private
 */
function _canChange(lastChange, isError = false) {
    const now = Date.now();
    if (!_.isNil(lastChange.time) && (isError === false || lastChange.isError === true)) {
        const minDifference = (lastChange.isError) ? OPTIMIZATIONS.dynamic.limitingInterval : OPTIMIZATIONS.dynamic.monitoringInterval;
        if ((now - lastChange.time) < (0.9 * minDifference)) {    //Allow some time space
            return false;
        }
    }

    lastChange.time = now;
    lastChange.isError = isError;
    return true;
}

/**
 * Sleeps for some time
 * Exponential increase
 * @param counter {number} Exponent
 * @returns {Promise<void>}
 * @private
 */
async function _sleep(counter) {
    const sleepTime = 100 * Math.pow(RETRIES.base, counter);
    const drift = sleepTime / 5;
    const max = sleepTime + drift;
    const min = sleepTime - drift;

    const random = (Math.random() * (max - min)) + min;
    await new Promise((resolve) => {
        setTimeout(resolve, random);
    });
}

module.exports = {
    createCache: createCache,
    getCache: getCache,

    isRestrictionsEnabled: isRestrictionsEnabled,
    enableRestrictions: enableRestrictions,
    disableRestrictions: disableRestrictions,
    getRestrictionsFields: getRestrictionsFields,
    getRestrictionsEmptyValue: getRestrictionsEmptyValue,
    getRestrictionsDefaultValue: getRestrictionsDefaultValue,
    getRestrictionsIndexValue: getRestrictionsIndexValue,
    setRestrictionsIndex: setRestrictionsIndex,
    removeRestrictionsIndex: removeRestrictionsIndex,
    isKnownRestrictionIndex: isKnownRestrictionIndex,

    getBulkSize: getBulkSize,
    updateDocumentSize: updateDocumentSize,
    getQueueSize: getQueueSize,

    updateStats: updateStats,
    error429: error429,
    error413: error413
};