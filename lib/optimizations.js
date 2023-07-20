'use strict';

const _ = require(`lodash`);
const nconf = require(`../config/config`);
const LIMITS = nconf.get(`options:limits`);
const OPTIMIZATIONS = nconf.get(`options:optimizations`);
const RETRIES = nconf.get(`es:retries`);
const TARGET_BULK_BYTE_SIZE = OPTIMIZATIONS.targetBulkSize.targetMB * 1024 * 1024;
const BRACKET_SIZE = Buffer.byteLength(`[]`);
const COMMA_SIZE = Buffer.byteLength(`,`);

/** @type {{enabled: boolean, fields: Array<string>, data: Record<string, {reference: string, data: Record<string, *>}>}} */
let restrictionsCache = {
    enabled: false,
    fields: [],
    data: {}
};
/** @type {Record<string, number>} */
let averageDocumentSizes = {};
/** @type {Record<string, number>} */
let documentCounts = {};
/** @type {Record<string, {size: number, count: number}>} */
const threadDocumentChanges = {};


//=================================================== DOCUMENT SIZES ===================================================
/**
 * Returns average document sizes
 * @returns {Record<string, number>}
 */
function getAverageDocumentSizes() {
    if (!_.isEmpty(averageDocumentSizes)) {
        //Sort average document sizes by the key
        const keys = Object.keys(averageDocumentSizes);
        keys.sort((a, b) => a.localeCompare(b, `en`));

        const sortedAverageDocumentSizes = {};
        keys.forEach((key) => sortedAverageDocumentSizes[key] = averageDocumentSizes[key]);
        averageDocumentSizes = sortedAverageDocumentSizes;
    }

    return averageDocumentSizes;
}

/**
 * Returns cached document counts in indices
 * @returns {Record<string, number>}
 */
function getDocumentCounts() {
    return documentCounts;
}

/**
 * Sets average document sizes object
 * @param latestAverageDocumentSizes {Record<string, number>}
 */
function setAverageDocumentSizes(latestAverageDocumentSizes = void 0) {
    if (latestAverageDocumentSizes) {
        averageDocumentSizes = latestAverageDocumentSizes;
    }
}

/**
 * Sets document counts
 * @param newDocumentCounts {Record<string, number>}
 */
function setDocumentCounts(newDocumentCounts = void 0) {
    if (newDocumentCounts) {
        documentCounts = newDocumentCounts;
    }
}

/**
 * Runs initial documents sampling (if needed) over given ODM model
 * @param MyOdm {{}} ODM model to run sampling with
 * @returns {Promise<void>}
 */
async function initialSampling(MyOdm) {
    documentCounts[MyOdm.alias] = await MyOdm.count();

    if (!averageDocumentSizes[MyOdm.alias]) {
        const randomDocs = await MyOdm.search({}, 0, OPTIMIZATIONS.targetBulkSize.sampleSize, { source: true });
        if (randomDocs.length > 0) {
            const sources = randomDocs.map((doc) => doc._source);
            const fullSize = Buffer.byteLength(JSON.stringify(sources));
            const commasSize = COMMA_SIZE * (randomDocs.length - 1);
            const byteSize = fullSize - BRACKET_SIZE - commasSize;

            updateAverageDocumentSizes(MyOdm.alias, (byteSize / randomDocs.length));
        }
    }
}

/**
 * Updates average document size of given index
 * @param alias {string} Alias of the index to be updated
 * @param documentSize {number} New average size of a single document
 */
function updateAverageDocumentSizes(alias, documentSize) {
    averageDocumentSizes[alias] = Math.max(Math.floor(documentSize), 1);
}

/**
 * Recalculates index average document sizes
 * @param alias {string}
 * @param newSize {number}
 * @param totalDocuments {number}
 * @param newDocuments {number}
 */
function recalculateAverageDocumentSizes(alias, newSize, totalDocuments, newDocuments) {
    documentCounts[alias] = totalDocuments;
    updateAverageDocumentSizes(alias, _computeNewAverageSize(averageDocumentSizes[alias], newSize, totalDocuments, newDocuments));
}

/**
 * Returns document changes observed in this thread
 * @returns {Record<string, {size: number, count: number}>}
 */
function getThreadDocumentChanges() {
    return threadDocumentChanges;
}

/**
 * Returns optimal bulk size for given ODM
 * @param MyOdm {{}}
 * @returns {Promise<number>} Optimal bulk size
 */
async function getBulkSize(MyOdm) {
    if (!OPTIMIZATIONS.targetBulkSize.enabled) {
        //Return default
        return Math.min(MyOdm._maxBulkSize ?? LIMITS.bulk.defaultSize, LIMITS.bulk.defaultSize);

    } else {
        if (averageDocumentSizes[MyOdm.alias]) {
            const documentSize = averageDocumentSizes[MyOdm.alias];
            const bulkSize = Math.floor(TARGET_BULK_BYTE_SIZE / documentSize);
            const maxLimit = Math.min(MyOdm._maxBulkSize ?? LIMITS.bulk.maxSize, LIMITS.bulk.maxSize);
            return Math.max(Math.min(bulkSize, maxLimit), LIMITS.bulk.minSize);

        } else {
            //No data -> return default
            return Math.min(MyOdm._maxBulkSize ?? LIMITS.bulk.defaultSize, LIMITS.bulk.defaultSize);
        }
    }
}

/**
 * Calculates and updates average document size for given ODM
 * @param MyOdm {{}}
 * @param dataArray {Array<{}>} Array with source data
 */
async function updateDocumentSize(MyOdm = void 0, dataArray) {
    if (!MyOdm) {
        return;
    }

    if (dataArray.length > 0) {
        const fullSize = Buffer.byteLength(JSON.stringify(dataArray));
        const commasSize = COMMA_SIZE * (dataArray.length - 1);
        const byteSize = fullSize - BRACKET_SIZE - commasSize;

        const originalDocumentSize = averageDocumentSizes[MyOdm.alias];
        const batchDocumentSize = Math.floor(byteSize / dataArray.length);
        let indexDocuments = Math.max(documentCounts[MyOdm.alias] ?? 1, 1);
        if (dataArray.length > indexDocuments) {
            documentCounts[MyOdm.alias] = dataArray.length;
            indexDocuments = dataArray.length;
        }
        const newDocumentSize = _computeNewAverageSize(originalDocumentSize, batchDocumentSize, indexDocuments, dataArray.length);
        updateAverageDocumentSizes(MyOdm.alias, newDocumentSize);

        if (!threadDocumentChanges[MyOdm.alias]) {
            threadDocumentChanges[MyOdm.alias] = {
                size: 0,
                count: 0
            };
        }
        threadDocumentChanges[MyOdm.alias].size += byteSize;
        threadDocumentChanges[MyOdm.alias].count += dataArray.length;
    }
}

/**
 * Calculates new average size
 * @param originalSize {number}
 * @param newSize {number}
 * @param totalDocuments {number}
 * @param newDocuments {number}
 * @returns {number}
 */
function _computeNewAverageSize(originalSize = void 0, newSize, totalDocuments, newDocuments) {
    if (_.isNil(newSize)) {
        throw Error(`New size is not defined`);
    }

    newSize = Math.max(newSize ?? 1, 1);
    originalSize = (originalSize) ? Math.max(originalSize ?? 1, 1) : newSize;

    totalDocuments = Math.max(totalDocuments ?? 1, 1);
    newDocuments = Math.max(newDocuments ?? 1, 1);
    const sizeScale = Math.tanh(newDocuments / totalDocuments);

    return ((sizeScale * newSize) + ((1 - sizeScale) * originalSize));
}


//==================================================== RESTRICTIONS ====================================================
/**
 * Returns restrictions data
 * @returns {{enabled: boolean, fields: Array<string>, data: Record<string, {reference: string, data: Record<string, *>}>}}
 */
function getRestrictions() {
    return restrictionsCache;
}

/**
 * Sets restrictions data
 * @param newRestrictionCache {{enabled: boolean, fields: Array<string>, data: Record<string, {reference: string, data: Record<string, *>}>}}
 */
function setRestrictions(newRestrictionCache = void 0) {
    if (newRestrictionCache) {
        restrictionsCache = newRestrictionCache;
    }
}

/**
 * Updates data of the restrictions
 * @param index {string}
 * @param data {{reference: string, data: Record<string, *>}}
 */
function updateRestrictions(index, data) {
    restrictionsCache.data[index] = data;
}

/**
 * Returns true if index restrictions are enabled
 * @returns {boolean}
 */
function isRestrictionsEnabled() {
    return restrictionsCache.enabled;
}

/**
 * Enables index restrictions
 * @param fields {Array<string>} Fields to be restricted
 * @param originalData {Record<string, {alias: string, data: Record<string, *>}>} Object with original data
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
 * @returns {Record<string, Record<string, *>>}
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
 * @returns {*}
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
 * @returns {*}
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
 * @returns {*}
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
 * @param data {Record<string, *>} Values to set
 * @param reference {string} Specify to create reference (alias) to real index
 * @param fromReference {boolean} True when we are specifying data from reference (alias)
 */
function setRestrictionsIndex(index, data = void 0, reference = void 0, fromReference = false) {
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
 * Is given index cached for restrictions?
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
 * Sleeps for some time
 * Exponential increase
 * @param counter {number} Exponent
 * @returns {Promise<void>}
 */
async function sleep(counter) {
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
    getAverageDocumentSizes: getAverageDocumentSizes,
    getDocumentCounts: getDocumentCounts,
    setAverageDocumentSizes: setAverageDocumentSizes,
    setDocumentCounts: setDocumentCounts,
    initialSampling: initialSampling,
    updateAverageDocumentSizes: updateAverageDocumentSizes,
    recalculateAverageDocumentSizes: recalculateAverageDocumentSizes,
    getThreadDocumentChanges: getThreadDocumentChanges,
    getBulkSize: getBulkSize,
    updateDocumentSize: updateDocumentSize,

    getRestrictions: getRestrictions,
    setRestrictions: setRestrictions,
    updateRestrictions: updateRestrictions,
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

    sleep: sleep
};
