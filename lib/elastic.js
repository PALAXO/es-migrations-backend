'use strict';

const buffer = require(`buffer`);
const esOdm = require(`es-odm`);
const _ = require(`lodash`);
const oboe = require(`oboe`);
const { PassThrough } = require(`stream`);
const logger = require(`./logger`);
const nconf = require(`../config/config`);
const optimizations = require(`./optimizations`);

esOdm.setLoggerConfig(nconf.get(`logs:elasticsearch`));

const OPTIMIZATIONS = nconf.get(`options:optimizations`);
const BLOCK_INDICES = nconf.get(`options:blockIndices`);
const SCROLL_TIMEOUT = nconf.get(`es:scrollTimeout`);
const RETRIES = nconf.get(`es:retries`);
const META_INDEX = `meta`;
const MAX_STRING_LENGTH = buffer.constants.MAX_STRING_LENGTH;

//ES client singleton
let client;

//Monitor task
let monitor;

/**
 * Creates elastic client and ODM models
 * @param tenant {string} Tenant
 * @param host {string} ES host
 * @param indicesInfo {{}} Indices info map
 * @returns {Promise<void>}
 * @private
 */
async function createElastic(tenant, host, indicesInfo) {
    logger.debug(`Creating ES client and ODM models...`);

    //ES client
    esOdm.setClient(host);
    client = esOdm.esClient.client;

    module.exports._esErrors = esOdm.esErrors;

    //Indices map, from all to main: <main indices || typed indices> => <main indices>
    if (_.isEmpty(indicesInfo?.INDICES) || !_.isObject(indicesInfo?.INDICES)) {
        throw Error(`No indices specified.`);
    }
    module.exports._indicesMap = Object.freeze({ ..._checkMainIndices(indicesInfo.INDICES), ..._checkTypedIndices(indicesInfo.INDICES, indicesInfo.TYPES) });

    //All usable indices, key === value: <main indices || typed indices> => <main indices || typed indices>
    module.exports._allIndices = Object.keys(this._indicesMap).reduce((sum, key) => {
        sum[key] = key;
        return sum;
    }, {});

    //ODM models
    const myOdms = {};
    const myRestrictedOdms = {};
    const exclusiveTypes = {};

    // = Create main indices
    for (const [index, indexInfo] of Object.entries(indicesInfo.INDICES)) {
        myOdms[index] = [];
        myRestrictedOdms[index] = [];

        //Initial version
        const myClass = _createClass(indexInfo.name, (indexInfo.types?.initial === true) ? `*` : void 0, tenant);
        if (indexInfo.maxBulkSize) {
            myClass._maxBulkSize = indexInfo.maxBulkSize;
        }
        myOdms[index].push({
            from: `-1.-1.-1`,
            class: myClass
        });
        myRestrictedOdms[index].push({
            from: `-1.-1.-1`,
            class: myClass.immediateRefresh(false)
        });

        if (!_.isEmpty(indexInfo.types?.versions)) {
            //Typing changed
            for (const version of indexInfo.types.versions) {
                const myClass = _createClass(indexInfo.name, (version.types) ? `*` : void 0, tenant);
                if (indexInfo.maxBulkSize) {
                    myClass._maxBulkSize = indexInfo.maxBulkSize;
                }

                myOdms[index].push({
                    from: version.from,
                    class: myClass
                });
                myRestrictedOdms[index].push({
                    from: version.from,
                    class: myClass.immediateRefresh(false)
                });
            }
        }
    }

    // = Create typed indices
    if (!_.isEmpty(indicesInfo.TYPES)) {
        for (const [index, indexInfo] of Object.entries(indicesInfo.TYPES)) {
            if (!_.isEmpty(indexInfo.exclusiveType)) {
                exclusiveTypes[index] = indexInfo.exclusiveType;

            } else {
                myOdms[index] = [];
                myRestrictedOdms[index] = [];

                const myClass = _createClass(indexInfo.typeof.name, indexInfo.inclusiveType, tenant);
                if (indexInfo.typeof.maxBulkSize) {     //Indent maxBulkSize
                    myClass._maxBulkSize = indexInfo.typeof.maxBulkSize;
                }

                myOdms[index].push({
                    from: `-1.-1.-1`,
                    class: myClass
                });
                myRestrictedOdms[index].push({
                    from: `-1.-1.-1`,
                    class: myClass.immediateRefresh(false)
                });
            }
        }
    }

    const sortFunction = function (a, b) {
        const aVersions = a.from.split(/[.:]/);
        const bVersions = b.from.split(/[.:]/);
        if (aVersions[0] !== bVersions[0]) {
            return parseInt(aVersions[0], 10) - parseInt(bVersions[0], 10);
        } else if (aVersions[1] !== bVersions[1]) {
            return parseInt(aVersions[1], 10) - parseInt(bVersions[1], 10);
        } else if (aVersions[2] !== bVersions[2]) {
            return parseInt(aVersions[2], 10) - parseInt(bVersions[2], 10);
        } else {
            return 0;
        }
    };
    for (const myOdm of Object.values(myOdms)) {
        myOdm.sort(sortFunction);
    }
    for (const myRestrictedOdm of Object.values(myRestrictedOdms)) {
        myRestrictedOdm.sort(sortFunction);
    }

    module.exports._odms = Object.freeze(myOdms);
    module.exports._restrictedOdms = Object.freeze(myRestrictedOdms);
    module.exports._exclusiveTypes = Object.freeze(exclusiveTypes);

    //Special (internal) index for meta data
    const MetaOdm = esOdm.createClass(META_INDEX, void 0, void 0, tenant);
    try {
        await callEs(`indices.stats`, {
            index: MetaOdm._alias
        });
    } catch (e) {
        await MetaOdm.createIndex();
    }
    module.exports._metaOdm = MetaOdm;

    logger.debug(`Created ES client and ODM models.`);
}

/**
 * Creates mocked version of ODM classes
 * @param indexName {string}
 * @param type {string}
 * @param tenant {string}
 * @returns
 * @private
 */
function _createClass(indexName, type, tenant) {
    return class MyClass extends esOdm.createClass(indexName, void 0, type, tenant) {
        //Create records
        static async createIndex(body = void 0, setAlias = true, ...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.createIndex(...arguments);
            }

            //===== Always create real index and probably even alias =====

            let myBody = _.cloneDeep(body);
            const fields = optimizations.getRestrictionsFields();
            const cache = {};
            for (const field of fields) {
                if (!_.isNil(myBody?.settings?.[field]) && !_.isNil(myBody?.settings?.index?.[field])) {
                    throw Error(`Error when creating an index - "${field}" specified both inside and outside "index" object!`);
                }

                const emptyValue = optimizations.getRestrictionsEmptyValue(field);
                if (!_.isNil(myBody?.settings?.[field])) {
                    cache[field] = myBody.settings[field];
                    myBody.settings[field] = emptyValue;

                } else if (!_.isNil(myBody?.settings?.index?.[field])) {
                    cache[field] = myBody.settings.index[field];
                    myBody.settings.index[field] = emptyValue;

                } else {
                    //Default
                    cache[field] = null;
                    if (_.isNil(myBody)) {
                        myBody = {
                            settings: {
                                index: {
                                    [field]: emptyValue
                                }
                            }
                        };
                    } else if (_.isNil(myBody.settings)) {
                        myBody.settings = {
                            index: {
                                [field]: emptyValue
                            }
                        };
                    } else if (_.isNil(myBody.settings.index)) {
                        myBody.settings.index = {
                            [field]: emptyValue
                        };
                    } else {
                        myBody.settings.index[field] = emptyValue;
                    }
                }
            }

            const newIndex = await super.createIndex(myBody, setAlias, ...args);
            optimizations.setRestrictionsIndex(newIndex, cache);
            if (setAlias) {
                optimizations.setRestrictionsIndex(this._alias, void 0, newIndex);
            }
            return newIndex;
        }

        static async cloneIndex(settings = void 0, ...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.cloneIndex(...arguments);
            }

            //===== Always create real index only =====

            let mySettings = _.cloneDeep(settings);
            const fields = optimizations.getRestrictionsFields();
            const cache = {};
            for (const field of fields) {
                if (!_.isNil(mySettings?.[field]) && !_.isNil(mySettings?.index?.[field])) {
                    throw Error(`Error when cloning an index - "${field}" specified both inside and outside "index" object!`);
                }

                const emptyValue = optimizations.getRestrictionsEmptyValue(field);
                if (!_.isNil(mySettings?.[field])) {
                    cache[field] =  mySettings[field];
                    mySettings[field] = emptyValue;

                } else if (!_.isNil(mySettings?.index?.[field])) {
                    cache[field] = mySettings.index[field];
                    mySettings.index[field] = emptyValue;

                } else {
                    //Use original
                    cache[field] = optimizations.getRestrictionsIndexValue(this._alias, field);

                    if (_.isNil(mySettings)) {
                        mySettings = {
                            index: {
                                [field]: emptyValue
                            }
                        };
                    } else if (_.isNil(mySettings?.index)) {
                        mySettings.index = {
                            [field]: emptyValue
                        };
                    } else {
                        mySettings.index[field] = emptyValue;
                    }
                }
            }

            const newIndex = await super.cloneIndex(mySettings, ...args);
            optimizations.setRestrictionsIndex(newIndex, cache);
            return newIndex;
        }

        static async aliasIndex(index, ...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.aliasIndex(...arguments);
            }

            //===== Always create alias only =====

            const aliasResult = await super.aliasIndex(index, ...args);
            optimizations.setRestrictionsIndex(this._alias, void 0, index);

            if (!optimizations.isKnownRestrictionIndex(index)) {
                //Edge case - we don't know the index
                const settings = await super.getSettings();
                const allValues = Object.values(settings)[0];

                const cache = {};
                const fields = optimizations.getRestrictionsFields();
                for (const field of fields) {
                    cache[field] = allValues.settings.index[field] ?? null;
                }
                optimizations.setRestrictionsIndex(index, cache);
            }

            return aliasResult;
        }

        //Delete records
        static async deleteAlias(...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.deleteAlias(...arguments);
            }

            //===== Always delete alias only =====

            const deleteResult = await super.deleteAlias(...args);
            optimizations.removeRestrictionsIndex(this._alias);
            return deleteResult;
        }

        static async deleteIndex(...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.deleteIndex(...arguments);
            }

            //===== Always delete index and alias =====

            const realIndex = await super.getIndex();
            const deleteResult = await super.deleteIndex(...args);
            optimizations.removeRestrictionsIndex(realIndex);
            optimizations.removeRestrictionsIndex(this._alias);
            return deleteResult;
        }

        //Mock data
        static async getSettings(...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.getSettings(...arguments);
            }

            //===== Mock ES output data =====

            const settings = await super.getSettings(...args);

            const fields = optimizations.getRestrictionsFields();
            for (const index of Object.keys(settings)) {
                for (const field of fields) {
                    let originalValue = optimizations.getRestrictionsIndexValue(index, field);
                    if (_.isNull(originalValue)) {
                        originalValue = optimizations.getRestrictionsDefaultValue(field);
                    }

                    if (!_.isNull(originalValue)) {
                        settings[index].settings.index[field] = originalValue;
                    } else {
                        delete settings[index].settings.index[field];
                    }
                }
            }

            return settings;
        }

        static async putSettings(settings, ...args) {
            if (!optimizations.isRestrictionsEnabled()) {
                return super.putSettings(...arguments);
            }

            //===== Mock ES input data =====

            const mySettings = _.cloneDeep(settings);
            const fields = optimizations.getRestrictionsFields();
            for (const field of fields) {
                if (!_.isNil(mySettings?.[field]) && !_.isNil(mySettings?.index?.[field])) {
                    throw Error(`Error when putting settings into an index - "${field}" specified both inside and outside "index" object!`);
                }
            }

            let odmTypes = [this];
            if (this.hasTypes()) {
                odmTypes = await this.getTypes();
            }

            for (const OdmType of odmTypes) {
                const cache = {};
                for (const field of fields) {
                    const emptyValue = optimizations.getRestrictionsEmptyValue(field);
                    if (!_.isNil(mySettings?.[field])) {
                        cache[field] = mySettings[field];
                        mySettings[field] = emptyValue;

                    } else if (!_.isNil(mySettings?.index?.[field])) {
                        cache[field] = mySettings.index[field];
                        mySettings.index[field] = emptyValue;

                    } else {
                        //Use original
                        cache[field] = optimizations.getRestrictionsIndexValue(OdmType._alias, field);

                        //Do not touch the body
                    }
                }
                optimizations.setRestrictionsIndex(OdmType._alias, cache, void 0, true);
            }

            return super.putSettings(mySettings, ...args);
        }
    };
}

/**
 * Checks main indices validity
 * @param mainIndices {{}}
 * @returns {{}}
 * @private
 */
function _checkMainIndices(mainIndices) {
    const map = {};
    const knownNames = [META_INDEX];     //Version index is reserved
    for (const [key, indexConfig] of Object.entries(mainIndices)) {
        if (_.isEmpty(indexConfig.name) || !_.isString(indexConfig.name)) {
            throw Error(`INDICES: In index '${key}' you must specify correct index name.`);
        }
        knownNames.forEach((knownName) => {
            if (knownName === indexConfig.name) {
                throw Error(`INDICES: In index '${key}' the name '${indexConfig.name}' has been already used.`);
            }
        });

        map[key] = key;
        knownNames.push(indexConfig.name);
    }
    return map;
}

/**
 * Checks typed indices validity
 * @param mainIndices {{}}
 * @param typedIndices {{}}
 * @returns {{}}
 * @private
 */
function _checkTypedIndices(mainIndices, typedIndices) {
    const map = {};
    if (_.isEmpty(typedIndices)) {
        return {};
    }

    const knownNames = Object.keys(mainIndices);
    for (const [key, indexConfig] of Object.entries(typedIndices)) {
        if (_.isEmpty(indexConfig.typeof) || !_.isObject(indexConfig.typeof) || !_.isString(indexConfig.typeof.name)) {
            throw Error(`TYPES: In typed index '${key}' you must specify property "typeof" and point it to valid index.`);
        } else if (!_.isEmpty(indexConfig.inclusiveType) && !_.isEmpty(indexConfig.exclusiveType)) {
            throw Error(`TYPES: In typed index '${key}' you cannot specify inclusiveType and exclusiveType at the same time.`);
        }

        const type = (!_.isEmpty(indexConfig.inclusiveType)) ? indexConfig.inclusiveType : indexConfig.exclusiveType;
        if (!_.isString(type)) {
            throw Error(`TYPES: In typed index '${key}' your specified type must be a string.`);
        } else if (type === `*`) {
            throw Error(`TYPES: In typed index '${key}' you cannot specify '*'.`);
        }

        const mainKey = Object.keys(mainIndices).find((key) => mainIndices[key] === indexConfig.typeof);
        if (_.isEmpty(mainKey)) {
            throw Error(`TYPES: In typed index '${key}' you have pointed to incorrect index.`);
        }/* else if (indexConfig.typeof.types !== true) {
            throw Error(`TYPES: In typed index '${key}' you have pointed to not typed index.`);
        }*/

        map[key] = mainKey;
        if (knownNames.includes(key)) {
            throw Error(`Index with key '${key}' has been already defined.`);
        } else {
            knownNames.push(key);
        }
    }
    return map;
}

/**
 * Returns correct ODM model for given version
 * @param odms {Array<{from: string, class: BaseModel}>} Array of ODM models
 * @param version {string}
 * @returns {BaseModel}
 * @private
 */
function _getOdmVersion(odms, version) {
    if (_.isEmpty(odms)) {
        throw Error(`Internal error - No ODM models.`);
    } else if (_.isEmpty(version || !_.isString(version))) {
        throw Error(`Internal error - No version for ODM models.`);
    }

    const requiredVersions = version.split(/[.:]/);     //1.5.0
    let lastOdm = void 0;
    for (const odm of odms) {
        const currentVersions = odm.from.split(/[.:]/);     //0.0.0,  1.7.5

        let diff = 0;
        if (requiredVersions[0] !== currentVersions[0]) {
            diff = parseInt(requiredVersions[0], 10) - parseInt(currentVersions[0], 10);
        } else if (requiredVersions[1] !== currentVersions[1]) {
            diff = parseInt(requiredVersions[1], 10) - parseInt(currentVersions[1], 10);
        } else if (requiredVersions[2] !== currentVersions[2]) {
            diff = parseInt(requiredVersions[2], 10) - parseInt(currentVersions[2], 10);
        }

        if (diff === 0) {
            lastOdm = odm.class;
            break;
        } else if (diff > 0) {
            lastOdm = odm.class;
        } else {
            break;
        }
    }

    if (_.isNil(lastOdm)) {
        throw Error(`Internal error - No ODM found.`);
    } else {
        return lastOdm;
    }
}

/**
 * Returns ODM model for given index and version
 * @param index {string}
 * @param version {string}
 * @returns {BaseModel}
 */
function getOdm(index, version) {
    return _getOdmVersion(this._odms[index], version);
}

/**
 * Returns restricted ODM model for given index and version
 * @param index {string}
 * @param version {string}
 * @returns {BaseModel}
 */
function getRestrictedOdm(index, version) {
    return _getOdmVersion(this._restrictedOdms[index], version);
}

/**
 * Calls ES query, retries in case of 429
 * @param path {string}
 * @param body {{}}
 * @param options {{}}
 * @param counter {number}
 * @returns {Promise<*>}
 */
async function callEs(path, body, options = void 0, counter = 0) {
    try {
        const func = _.get(client, path);

        if (options?.asStream) {
            //For stream we need meta as well to detect some errors
            options.meta = true;
        }

        const result = await func.call(client, body, options);
        if (options?.asStream) {
            return await parseStream(result);
        } else {
            return result;
        }

    } catch (e) {
        if (e.statusCode === 429 && counter < RETRIES.maxRetries) {
            logger.warn(`ES returns error 429 - Too many requests, will try again.`);
            await optimizations.error429(counter);
            counter++;

            return callEs(path, body, options, counter);
        } else {
            throw e;
        }
    }
}

/**
 * Lock given tenant, updates script compilations rate
 * @param tenant {string}
 * @returns {Promise<void>}
 */
async function lockTenant(tenant) {
    await callEs(`cluster.putSettings`, {
        persistent : {
            'script.max_compilations_rate': `use-context`,
            'script.context.update.max_compilations_rate': `1000/1m`
        }
    });
    logger.debug(`Script rates updated.`);

    if (BLOCK_INDICES) {
        logger.debug(`Locking tenant '${tenant}'...`);
        await callEs(`indices.putSettings`, {
            index: `${tenant}_*`,
            settings: {
                index: {
                    blocks: {
                        read: true,
                        write: true
                    }
                }
            }
        });
        logger.info(`Tenant '${tenant}' has been locked.`);
    }
}

/**
 * Refreshes and unlocks given tenant
 * @param tenant
 * @returns {Promise<void>}
 */
async function unlockTenant(tenant) {
    logger.debug(`Refreshing tenant '${tenant}'...`);
    await callEs(`indices.refresh`, {
        index: `${tenant}_*`
    });
    logger.debug(`Tenant '${tenant}' has been refreshed.`);

    if (BLOCK_INDICES) {
        logger.debug(`Unlocking tenant '${tenant}'...`);
        await callEs(`indices.putSettings`, {
            index: `${tenant}_*`,
            settings: {
                index: {
                    blocks: {
                        read: null,
                        write: null
                    }
                }
            }
        });
        logger.info(`Tenant '${tenant}' has been unlocked.`);
    }
}

/**
 * Opens indices to be used by given node, check main index existence
 * @param node {Node}
 * @returns {Promise<void>}
 */
async function openIndices(node) {
    const mainIndex = node._index;
    if (!_.isEmpty(mainIndex)) {
        //Check if main index exist, always only general type is checked
        //Other indices are not checked, as we don't know what's going to happen there
        const inputOdmExistence = await _checkIndexExistence(this.getOdm(mainIndex, node._migrations[0]._versionNumbers));
        if (!inputOdmExistence) {
            throw Error(`'${node._migrations[0].version}': Requested INDEX: '${mainIndex}' doesn't exist.`);
        }
    }

    //=== Input indices, main index included
    const inputIndices = node._inputIndices;
    const inputOdms = inputIndices.map((index) => this.getOdm(index, node._migrations[0]._versionNumbers));

    //Refresh
    for (const Odm of inputOdms) {
        try {
            await Odm.refresh();
        } catch (e) {
            //Doesn't exist, probably OK
        }
    }

    if (BLOCK_INDICES) {
        for (const Odm of inputOdms) {
            try {
                //Unlock read
                await Odm.putSettings({
                    blocks: {
                        read: false
                    }
                });
            } catch (e) {
                //Doesn't exist, probably OK
            }
        }

        //== Output indices, main index included
        const outputIndices = node._outputIndices;
        let outputOdms = outputIndices.map((index) => this.getOdm(index, node._migrations[0]._versionNumbers));
        //Filter only existing ones
        outputOdms = await getExistingModels(outputOdms);
        for (const Odm of outputOdms) {
            //Unlock write
            await Odm.putSettings({
                blocks: {
                    write: false
                }
            });
        }
    }
}

/**
 * Closes indices after the node has been finished
 * @param node {Node}
 * @returns {Promise<void>}
 */
async function closeIndices(node) {
    if (BLOCK_INDICES) {
        //== Output indices only, main index included
        const outputIndices = node._outputIndices;
        let outputOdms = outputIndices.map((index) => this.getOdm(index, node._migrations[0]._versionNumbers));

        //Filter only existing ones
        outputOdms = await getExistingModels(outputOdms);
        for (const Odm of outputOdms) {
            //And lock both read and write
            await Odm.putSettings({
                blocks: {
                    read: true,
                    write: true
                }
            });
        }
    }
}

/**
 * Migrates ES indices to use aliases
 * @param versionString {string}
 * @returns {Promise<void>}
 */
async function migrateToAliases(versionString) {
    logger.debug(`Checking if existing indices use aliases...`);

    let counter = 0;
    const mainOdms = Object.keys(this._indicesMap).filter((index) => this._indicesMap[index] === index).map((index) => this.getOdm(index, versionString));
    const existingModels = await getExistingModels(mainOdms);
    for (const Odm of existingModels) {
        logger.trace(`Checking alias '${Odm._alias}'...`);

        const realIndex = await Odm.getIndex();
        if (realIndex === Odm._alias) {
            logger.debug(`Index '${Odm._alias}' isn't aliased -> aliasing now...`);

            let newSettings;
            const originalSettings = await Odm.getSettings();
            if (Object.values(originalSettings)?.[0]?.settings?.index?.soft_deletes?.enabled === `false`) {
                //Ensure soft_deletes will be enabled on newly created indices
                newSettings = {
                    index: {
                        soft_deletes: {
                            enabled: true
                        }
                    }
                };
            }

            await Odm.putSettings({
                blocks: {
                    write: true
                }
            });

            const newIndex = await Odm.cloneIndex(newSettings);
            await Odm.deleteIndex();
            await Odm.aliasIndex(newIndex);
            counter++;

            await Odm.putSettings({
                blocks: {
                    write: null
                }
            });

            logger.info(`Index '${Odm._alias}' has been aliased, new index is '${newIndex}'.`);
        }
    }

    if (counter > 0) {
        logger.debug(`${counter} indices have been aliased.`);
    } else {
        logger.debug(`No index has been aliased.`);
    }
}

/**
 * Returns existing models for the input models
 * @param models {Array<{}>}
 * @returns {Promise<Array<{}>>}
 */
async function getExistingModels(models) {
    const existingOdms = [];
    for (const Odm of models) {
        if (Odm.hasTypes()) {
            const types = await Odm.getTypes();
            existingOdms.push(...types);
        } else {
            const indexExists = await Odm.indexExists();
            if (indexExists) {
                existingOdms.push(Odm);
            }
        }
    }

    return existingOdms;
}

/**
 * Sends ES bulk request, handles "Request Entity Too Large" and "Invalid string length"
 * It is necessary for the bulk elements to be of the same type, eg. you cannot mix save and delete requests
 * @param bulkSave {Array<{}>}
 * @param errorMessage {string}
 * @param TypedOdm
 * @param numberOfProcessed {number}
 * @param bulkSize {number}
 * @param responseCache
 * @returns {Promise<{}>}
 */
async function sendBulk(bulkSave, errorMessage = void 0, TypedOdm = void 0, numberOfProcessed = 0, bulkSize = bulkSave.length, responseCache = void 0) {
    try {
        while (numberOfProcessed < bulkSave.length) {
            const toProcess = (bulkSize >= bulkSave.length) ? bulkSave : bulkSave.slice(numberOfProcessed, numberOfProcessed + bulkSize);

            const esResult = await callEs(`bulk`, {
                operations: toProcess,
                refresh: false
            }, {
                asStream: true
            });

            if (esResult.errors) {
                let failedItems = esResult.items.filter((item) => !!item.index.error);
                failedItems = failedItems.map((item) => `${item.index._index}:${item.index._id} - ${item.index.error?.reason}`);

                if (_.isEmpty(errorMessage)) {
                    throw Error(`Failed when performing bulk operation - ${failedItems.join(`,`)}`);
                } else {
                    throw Error(`${errorMessage} - ${failedItems.join(`,`)}`);
                }
            }

            if (!_.isNil(TypedOdm)) {
                const sourceFields = [];
                toProcess.forEach((singleData, index) => {
                    if (index % 2 === 1) {
                        sourceFields.push(singleData);
                    }
                });
                await optimizations.updateDocumentSize(TypedOdm, sourceFields);
            }

            if (toProcess.length === bulkSave.length) {
                return esResult;
            } else if (_.isEmpty(responseCache)) {
                responseCache = {
                    took: esResult.took,
                    errors: false,
                    items: esResult.items
                };
            } else {
                responseCache.took += esResult.took;
                responseCache.items.push(...esResult.items);
            }

            numberOfProcessed += bulkSize;
            if (numberOfProcessed >= bulkSave.length) {
                return responseCache;
            }
        }

    } catch (e) {
        if (e.statusCode === 413 || e.message === `Invalid string length`) {
            if (bulkSize <= 2) {
                throw Error(`Bulk operation failed despite the bulk size has been ${bulkSize}.`);
            }
            logger.debug(`Sent bulk is too big, reducing the size and sending again.`);
            optimizations.error413();

            let newBulkSize = Math.floor(bulkSize / 2);
            if ((newBulkSize % 2) === 1) {
                //Ensure this is even
                newBulkSize += 1;
            }

            return sendBulk(bulkSave, errorMessage, TypedOdm, numberOfProcessed, newBulkSize, responseCache);

        } else {
            throw e;
        }
    }
}

/**
 * Parses ES stream
 * @param esResult {{}} ES response
 * @returns {Promise<{}>} Response object
 */
async function parseStream(esResult) {
    let totalLength = 0;

    const body = await new Promise((resolve) => {
        const chunkCache = [];
        let isPiped = false;
        const stream = esResult.body;

        stream.on(`data`, (newChunk) => {
            if (isPiped) {
                return;
            }

            if ((totalLength + newChunk.length) < MAX_STRING_LENGTH) {
                //Cache chunks as long as possible
                chunkCache.push(newChunk);
                totalLength += newChunk.length;

            } else {
                //Caching is no longer possible, as we could exceed max string length -> pass data to "oboe" to parse them
                logger.debug(`Received streams are too big and will be parsed using 3rd party library.`);

                const passThrough = new PassThrough();
                for (const myData of chunkCache) {
                    passThrough.push(myData);
                }
                passThrough.push(newChunk);
                stream.pipe(passThrough);
                isPiped = true;

                oboe(passThrough).done((res) => {
                    resolve(res);
                });
            }
        });

        stream.on(`end`, () => {
            if (isPiped) {
                //Response will come from oboe
                return void 0;
            } else if (chunkCache.length > 0) {
                //Concat chunks and parse JSON
                resolve(JSON.parse(Buffer.concat(chunkCache, totalLength).toString(`utf-8`)));
            } else {
                //No data
                resolve(void 0);
            }
        });
    });

    if (esResult.statusCode >= 400) {
        throw new esOdm.esErrors.ResponseError({
            body: body,
            statusCode: esResult.statusCode,
            headers: esResult.headers,
            meta: esResult.meta
        });
    } else {
        return body;
    }
}

/**
 * Checks if ODM index exists
 * @param Odm {{}}
 * @returns {Promise<boolean>} Exists?
 */
async function _checkIndexExistence(Odm) {
    try {
        const existing = await callEs(`indices.stats`, {
            index: Odm._alias
        });
        if (_.isEmpty(existing.indices)) {
            return false;
        }
    } catch (e) {
        return false;
    }

    return true;
}

/**
 * Restricts tenant indices
 * Disables refreshes and removes replicas
 * @param tenant {string}
 * @param nodes {Array<Node>}
 * @returns {Promise<void>}
 */
async function restrictIndices(tenant, nodes) {
    const fields = [];
    if (OPTIMIZATIONS.restrictions.replicas) {
        fields.push(`number_of_replicas`, `auto_expand_replicas`);
    }
    if (OPTIMIZATIONS.restrictions.refreshes) {
        fields.push(`refresh_interval`);
    }

    if (_.isEmpty(fields)) {
        return;
    }

    for (const node of nodes) {
        if (node._migrations.some((migration) => migration.hasUnsafeOperation())) {
            //Unsafe, this feature won't be used
            return;
        }
    }

    //Find all output ODMs
    const outputOdms = _.uniq(_.compact(nodes.reduce((sum, node) => {
        sum.push(...node._outputIndices.map((usedIndex) => this.getOdm(usedIndex, nodes[0]._migrations[0]._versionNumbers)));
        return sum;
    }, [])));

    const myStats = {};
    const toCheckOdms = [];

    //Download data of ALL known ODMs
    const allOdms = Object.values(this._indicesMap).map((index) => this.getOdm(index, nodes[0]._migrations[0]._versionNumbers));
    for (const MyOdm of allOdms) {
        let checkOutputOdm = false;
        if (outputOdms.includes(MyOdm)) {
            checkOutputOdm = true;
        }

        try {
            let typedOdms = [MyOdm];
            if (MyOdm.hasTypes()) {
                typedOdms = await MyOdm.getTypes();
            }

            for (const TypedOdm of typedOdms) {
                const settings = await TypedOdm.getSettings();
                for (const index of Object.keys(settings)) {
                    myStats[index] = {
                        alias: TypedOdm._parseIndex(index).alias,
                        isOutput: false,
                        data: {}
                    };
                    for (const field of fields) {
                        myStats[index].data[field] = settings[index].settings.index[field] ?? null;
                    }

                    if (checkOutputOdm) {
                        toCheckOdms.push(myStats[index]);
                    }
                }
            }
        } catch (e) {
            //Not exists, OK
        }
    }

    //Mark really used output indices
    for (const node of nodes) {
        for (const migration of node._migrations) {
            for (const data of toCheckOdms) {
                if (migration.isTypeOf(data.alias)) {
                    data.isOutput = true;
                }
            }
        }
    }

    //Disable restrictions
    for (const index of Object.keys(myStats)) {
        const body = {};
        if (myStats[index].isOutput) {
            //Disable all for output indices
            for (const field of fields) {
                body[field] = optimizations.getRestrictionsEmptyValue(field);
            }

        } else {
            //Do not disable replicas for not output indices
            const myFields = fields.filter((field) => ((field !== `number_of_replicas`) && (field !== `auto_expand_replicas`)));
            for (const field of myFields) {
                body[field] = optimizations.getRestrictionsEmptyValue(field);
            }
        }

        if (!_.isEmpty(body)) {
            await callEs(`indices.putSettings`, {
                index: index,
                settings: {
                    index: body
                }
            });
        }
    }

    //Save original counts
    optimizations.enableRestrictions(fields, myStats);
}

/**
 * Removes restrictions from tenant indices
 * Restores refreshes and replicas
 * @returns {Promise<void>}
 */
async function releaseIndices() {
    const restrictions = optimizations.disableRestrictions();

    //Restore correct data
    for (const index of Object.keys(restrictions)) {
        try {
            await callEs(`indices.putSettings`, {
                index: index,
                settings: {
                    index: {
                        ...restrictions[index]
                    }
                }
            });
        } catch (e) {
            //Not exists, OK
        }
    }
}

/**
 * Runs monitoring task
 */
function runMonitor() {
    if (OPTIMIZATIONS.dynamic.monitoring) {
        monitor = setTimeout(_monitor, OPTIMIZATIONS.dynamic.monitoringInterval);
    }
}

/**
 * Monitoring task
 * NOT IMPLEMENTED
 * @returns {Promise<void>}
 * @private
 */
async function _monitor() {
    //Load ES data
    optimizations.updateStats(/* ES data */);

    runMonitor();
}

/**
 * Stops monitoring task
 */
function stopMonitor() {
    if (!_.isNil(monitor)) {
        clearTimeout(monitor);
    }
}

/**
 * Iterates over ODM bulks
 * @param MainOdm
 * @param body {{}} Optional search body
 * @returns {AsyncGenerator<*[], void, *>}
 */
async function *bulkIterator(MainOdm, body = void 0) {
    if (_.isEmpty(body)) {
        body = {
            query: {
                match_all: {}
            }
        };
    }

    let esResult;
    try {
        //Initial search
        esResult = await callEs(`search`, {
            ...body,
            index: MainOdm._alias,
            from: 0,
            size: await optimizations.getBulkSize(MainOdm),
            scroll: SCROLL_TIMEOUT,
            seq_no_primary_term: true,
            version: true
        }, {
            asStream: true
        });

        const constructorCache = {};
        do {
            const documentBulk = esResult.hits.hits;

            if (!_.isEmpty(esResult._shards?.failures)) {
                throw new esOdm.esErrors.ResponseError({ body: esResult._shards.failures });
            } else if (documentBulk.length <= 0) {
                break;
            }

            const instances = [];
            for (const document of documentBulk) {
                const indexInfo = MainOdm._parseIndex(document._index);
                let Constructor = constructorCache[indexInfo.alias];
                if (!Constructor) {
                    Constructor = MainOdm;
                    if (indexInfo.type) {
                        Constructor = Constructor.type(indexInfo.type);
                    }

                    constructorCache[indexInfo.alias] = Constructor;
                }

                instances.push(new Constructor(document._source, document._id, document._version, document.highlight,
                    document._primary_term, document._seq_no, document._score));
            }
            yield instances;

            //Fetch new results
            esResult = await callEs(`scroll`, {
                scroll: SCROLL_TIMEOUT,
                scroll_id: esResult._scroll_id
            }, {
                asStream: true
            });

        } while (esResult.hits.hits.length > 0);

    } finally {
        if (esResult?._scroll_id) {
            await callEs(`clearScroll`, {
                scroll_id: esResult._scroll_id
            });
        }
    }
}

/**
 * Returns optimal bulk size for given ODM model
 * @param MainOdm
 * @returns {Promise<number>}
 */
async function getBulkSize(MainOdm) {
    return optimizations.getBulkSize(MainOdm);
}

/**
 * Checks if indices noted in meta index matches the real tenant indices
 * @param tenant {string}
 * @param meta
 * @returns {Promise<void>}
 */
async function checkExistingIndices(tenant, meta) {
    if (!_.isArray(meta.indices)) {
        logger.info(`Meta index doesn't contain any information about the indices, skipping the index presence check.`);
        return;
    }

    const indicesInfo = await callEs(`indices.stats`, {
        index: `${tenant}_*`
    });

    //Parse index main part
    let existingIndices = Object.keys(indicesInfo?.indices || {});
    existingIndices = _.uniq(existingIndices.map((myIndex) => myIndex.split(`-`)[0].substring(tenant.length + 1)));

    //Check differences
    const missingIndices = _.difference(meta.indices, existingIndices);
    const extraIndices = _.difference(existingIndices, meta.indices);

    let isOk = true;
    if (!_.isEmpty(missingIndices)) {
        logger.fatal(`Following indices are noted in meta index but missing in database: ${missingIndices.join(`, `)}.`);
        isOk = false;
    }
    if (!_.isEmpty(extraIndices)) {
        logger.fatal(`Following indices are NOT noted in meta index but exist in database: ${extraIndices.join(`, `)}.`);
        isOk = false;
    }

    if (!isOk) {
        throw Error(`Migrations cannot process because of mismatch in indices.`);
    }
}

/**
 * Saves all tenant indices into meta, only main part of the index is saved
 * Also removes not existing aliases from averageDocumentSizes optimization object
 * @param tenant {string}
 * @param meta
 * @returns {Promise<void>}
 */
async function saveNewIndices(tenant, meta) {
    const indicesInfo = await callEs(`indices.stats`, {
        index: `${tenant}_*`
    });

    if (_.isEmpty(indicesInfo?.indices)) {
        //No indices... Strange but OK
        meta.indices = [];

        if (!_.isEmpty(meta?._optimizations?.averageDocumentSizes)) {
            meta._optimizations.averageDocumentSizes = {};
        }

    } else {
        const myIndices = Object.keys(indicesInfo.indices);

        //Get sorted indices (aliases actually) without "type" part
        meta.indices = myIndices.map((myIndex) => myIndex.split(`-`)[0].substring(tenant.length + 1));
        meta.indices = _.uniq(meta.indices.sort((a,b) => a.localeCompare(b, `en`)));

        if (!_.isEmpty(meta?._optimizations?.averageDocumentSizes)) {
            //Remove not existing indices (aliases) from average document sizes

            //Get aliases from indices (including "type")
            const myAliases = _.uniq(myIndices.map((myIndex) => {
                const mainParts = myIndex.split(`-`);
                if (mainParts.length <= 1) {
                    return mainParts[0];
                }

                const secondaryParts = mainParts[1].split(`_`);
                if (secondaryParts.length <= 1) {
                    return mainParts[0];
                } else {
                    secondaryParts.shift();
                    return `${mainParts[0]}_${secondaryParts.join(`_`)}`;
                }
            }));

            //Remove not existing
            Object.keys(meta._optimizations.averageDocumentSizes).forEach((key) => {
                if (!myAliases.includes(key)) {
                    delete meta._optimizations.averageDocumentSizes[key];
                }
            });
        }
    }
}

module.exports = {
    _esErrors: void 0,
    indices: void 0,
    _metaOdm: void 0,

    createElastic: createElastic,
    getOdm: getOdm,
    getRestrictedOdm: getRestrictedOdm,
    callEs: callEs,
    lockTenant: lockTenant,
    unlockTenant: unlockTenant,
    openIndices: openIndices,
    closeIndices: closeIndices,
    migrateToAliases: migrateToAliases,
    getExistingModels: getExistingModels,
    sendBulk: sendBulk,

    restrictIndices: restrictIndices,
    releaseIndices: releaseIndices,

    runMonitor: runMonitor,
    stopMonitor: stopMonitor,

    checkExistingIndices: checkExistingIndices,
    saveNewIndices: saveNewIndices,

    utils: {
        BulkArray: esOdm.BulkArray,
        bulkIterator: bulkIterator,
        getBulkSize: getBulkSize
    }
};

