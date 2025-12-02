import buffer from 'buffer';
import * as esOdm from 'es-odm';
import _  from 'lodash';
import streamJson from 'stream-json';
import StreamAssembler from 'stream-json/Assembler.js';
import { PassThrough } from 'stream';

import logger from './logger.js';
import nconf from '../config/config.js';
import optimizations from './optimizations.js';

esOdm.setLoggerConfig(nconf.get(`logs:elasticsearch`));

const BLOCK_INDICES = nconf.get(`options:blockIndices`);
const REQUEST_TIMEOUT_SECONDS = nconf.get(`es:requestTimeoutSeconds`);
const PING_TIMEOUT_SECONDS = nconf.get(`es:pingTimeoutSeconds`);
const MAX_RETRIES = nconf.get(`es:maxRetries`);
const RETRIES = nconf.get(`es:retries`);
const META_INDEX = `meta`;
const MAX_STRING_LENGTH = buffer.constants.MAX_STRING_LENGTH;

//ES client singleton
let client;

let _esErrors, _indicesMap, odms, restrictedOdms, aliasToName, nameToKey, _metaOdm;

//Internal information about allowed indices
const allowedIndices = {};

/**
 * Creates elastic client and ODM models
 * @param tenant {string} Tenant
 * @param host {string} ES host
 * @param indicesInfo {{INDICES: Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}} Indices info map
 * @param elasticsearchConfig {{}}
 * @param uid {string}
 * @returns {Promise<void>}
 */
async function createElastic(tenant, host, indicesInfo, elasticsearchConfig = void 0, uid = void 0) {
    logger.debug(`Creating ES client and ODM models...`);

    //ES client
    if (elasticsearchConfig) {
        esOdm.setLoggerConfig(elasticsearchConfig);
    }
    if (uid) {
        esOdm.setLoggerUidFunction(() => uid);
    }
    esOdm.setClient(host, {
        requestTimeoutSeconds: REQUEST_TIMEOUT_SECONDS,
        pitTimeoutSeconds: REQUEST_TIMEOUT_SECONDS * 2,
        pingTimeout: PING_TIMEOUT_SECONDS,
        maxRetries: MAX_RETRIES
    });
    client = esOdm.esClient.client;

    _esErrors = esOdm.esErrors;

    //Indices map, from all to main: <main indices> => <main indices>
    if (_.isEmpty(indicesInfo?.INDICES) || !_.isObject(indicesInfo?.INDICES)) {
        throw Error(`No indices specified.`);
    }
    _indicesMap = Object.freeze(_checkMainIndices(indicesInfo.INDICES));

    //ODM models
    const myOdms = {};
    const myRestrictedOdms = {};
    aliasToName = {};
    nameToKey = {};

    // = Create main indices
    for (const [index, indexInfo] of Object.entries(indicesInfo.INDICES)) {
        //Initial version
        const MyClass = _createClass(indexInfo.name, tenant);
        aliasToName[MyClass.alias] = index;
        if (indexInfo.maxBulkSize) {
            MyClass._maxBulkSize = indexInfo.maxBulkSize;
        }

        myOdms[index] = MyClass;
        myRestrictedOdms[index] = MyClass.immediateRefresh(false);
        allowedIndices[index] = _generateAllowedObjects(indexInfo);
        nameToKey[MyClass._name] = index;
    }

    odms = Object.freeze(myOdms);
    restrictedOdms = Object.freeze(myRestrictedOdms);
    aliasToName = Object.freeze(aliasToName);
    nameToKey = Object.freeze(nameToKey);

    //Special (internal) index for meta data
    const MetaOdm = esOdm.createClass(META_INDEX, void 0, tenant);
    try {
        await callEs(`indices.stats`, {
            index: MetaOdm.alias
        });
    } catch (e) {
        await MetaOdm.createIndex();
    }
    _metaOdm = MetaOdm;

    logger.debug(`Created ES client and ODM models.`);
}

/**
 * Creates mocked version of ODM classes
 * @param indexName {string}
 * @param tenant {string}
 * @returns {*}
 */
function _createClass(indexName, tenant) {
    return class MyClass extends esOdm.createClass(indexName, void 0, tenant) {
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
                optimizations.setRestrictionsIndex(this.alias, void 0, newIndex);
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
                    cache[field] = optimizations.getRestrictionsIndexValue(this.alias, field);

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
            optimizations.setRestrictionsIndex(this.alias, void 0, index);

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
            optimizations.removeRestrictionsIndex(this.alias);
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
            optimizations.removeRestrictionsIndex(this.alias);
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
                    cache[field] = optimizations.getRestrictionsIndexValue(this.alias, field);

                    //Do not touch the body
                }
            }
            optimizations.setRestrictionsIndex(this.alias, cache, void 0, true);

            return super.putSettings(mySettings, ...args);
        }

        //Optimizations
        static async _getBulkSize() {
            return optimizations.getBulkSize(this);
        }
    };
}

/**
 * Checks main indices validity
 * @param mainIndices {Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}
 * @returns {Record<string, string>}
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
 * Creates internal array with information in which versions the index is allowed
 * @param indexInfo {{name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}}
 * @returns {Array<{major: number, minor: number, patch: number, allowed: boolean}>}
 */
function _generateAllowedObjects(indexInfo) {
    const indexAllowedInfo = indexInfo.allowed;
    if (!indexAllowedInfo) {
        return [{
            major: 0,
            minor: 0,
            patch: 0,
            allowed: true
        }];

    } else {
        if (!_.isBoolean(indexAllowedInfo.initial)) {
            throw Error(`Initial 'allowed' value is not specified in index '${indexInfo.name.toUpperCase()}'.`);
        }

        const values = [];
        //Initial value
        values.push({
            major: 0,
            minor: 0,
            patch: 0,
            allowed: (indexAllowedInfo.initial === true)
        });
        //Version changes
        indexAllowedInfo.versions?.map((item) => {
            const versionParts = item.from.split(`.`);
            const version = {
                major: parseInt(versionParts[0], 10),
                minor: parseInt(versionParts[1], 10),
                patch: parseInt(versionParts[2], 10),
                allowed: (item.allowed === true)
            };
            //Ensure there are no duplicities
            if (values.some((oldItem) => (oldItem.major === version.major) && (oldItem.minor === version.minor) && (oldItem.patch === version.patch))) {
                throw Error(`Found multiple 'allowed' specifications for version '${item.from}' in index '${indexInfo.name.toUpperCase()}'.`);
            }
            values.push(version);
        });

        //Sort
        values.sort((a, b) => {
            if (a.major !== b.major) {
                return a.major - b.major;
            } else if (a.minor !== b.minor) {
                return a.minor - b.minor;
            } else {
                return a.patch - b.patch;
            }
        });

        return values;
    }
}

/**
 * Checks if given indices are allowed for given version and returns the correct subset. Optionally throws when not allowed index is specified.
 * @param version {string} Version of the migration (may contain :position)
 * @param indices {string | Array<string>} Single or list of indices to be checked
 * @param throwable {boolean} Throw when not allowed index has been specified?
 * @returns {string | Array<string>} Resulting index/indices
 */
function getAllowedIndices(version, indices, throwable) {
    const isSingle = !_.isArray(indices);
    indices = _.castArray(indices);

    const maxVersionObject = {
        major: Number.MAX_SAFE_INTEGER,
        minor: Number.MAX_SAFE_INTEGER,
        patch: Number.MAX_SAFE_INTEGER
    };

    const internalVersion = version.split(`:`)[0];
    const internalVersionParts = internalVersion.split(`.`);
    const versionObject = {
        major: parseInt(internalVersionParts[0], 10),
        minor: parseInt(internalVersionParts[1], 10),
        patch: parseInt(internalVersionParts[2], 10)
    };

    const results = [];
    for (const index of indices) {
        const versionsInfo = allowedIndices[index];
        let correctVersionInfo;

        if (_.isArray(versionsInfo)) {
            for (let i = 0; i < versionsInfo.length; i++) {
                const from = versionsInfo[i];
                const to = ((i + 1) < versionsInfo.length) ? versionsInfo[i + 1] : maxVersionObject;

                if (_isInside(from, to, versionObject)) {
                    correctVersionInfo = versionsInfo[i];
                    break;
                }
            }
        }

        if (correctVersionInfo?.allowed === true) {
            results.push(index);
        } else if (throwable) {
            throw Error(`Usage of index '${index}' in version '${version}' is not allowed.`);
        }
    }

    return isSingle ? results[0] : results;
}

/**
 * Checks if given version is inside of given boundary
 * @param from {{major: number, minor: number, patch: number}} Inclusive
 * @param to {{major: number, minor: number, patch: number}} Exclusive
 * @param check {{major: number, minor: number, patch: number}}
 * @returns {boolean}
 */
function _isInside(from, to, check) {
    if (from.major > check.major || (from.major === check.major && from.minor > check.minor) || (from.major === check.major && from.minor === check.minor && from.patch > check.patch)) {
        return false;

    } else if (to.major < check.major || (to.major === check.major && to.minor < check.minor) || (to.major === check.major && to.minor === check.minor && to.patch <= check.patch)) {
        return false;

    } else {
        return true;
    }
}

/**
 * Calls ES query, retries in case of 429
 * @param path {string}
 * @param body {{}}
 * @param options {{asStream: boolean, meta: boolean}}
 * @param counter {number}
 * @returns {Promise<*>}
 */
async function callEs(path, body, options = void 0, counter = 0) {
    try {
        const lastDotIndex = path.lastIndexOf('.');
        let context, methodName;
        if (lastDotIndex === -1) {
            // Top-level function
            context = client;
            methodName = path;
        } else {
            // Nested function
            const namespacePath = path.substring(0, lastDotIndex);
            context = _.get(client, namespacePath);
            methodName = path.substring(lastDotIndex + 1);
        }

        if (!context || (typeof context[methodName] !== `function`)) {
            throw new Error(`ES Method not found: ${path}`);
        }

        if (options?.asStream) {
            //For stream we need meta as well to detect some errors
            options.meta = true;
        }

        const result = await context[methodName](body, options);
        if (options?.asStream) {
            return await _parseStream(result);
        } else {
            return result;
        }

    } catch (e) {
        if (e.statusCode === 429 && counter < RETRIES.maxRetries) {
            const debugLimit = Math.max(1, Math.floor(RETRIES.maxRetries / 2));
            if (counter < debugLimit) {
                logger.debug(`ES returns error 429 - Too many requests, will try again.`);
            } else {
                logger.warn(`ES returns error 429 - Too many requests, will try again.`);
            }
            await optimizations.sleep(counter);
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

    if (BLOCK_INDICES.read || BLOCK_INDICES.write) {
        logger.debug(`Locking tenant '${tenant}'...`);
        await callEs(`indices.putSettings`, {
            index: `${tenant}_*`,
            settings: {
                index: {
                    blocks: {
                        read: (BLOCK_INDICES.read) ? true : null,
                        write: (BLOCK_INDICES.write) ? true : null
                    }
                }
            }
        });
        logger.info(`Tenant '${tenant}' has been locked.`);
    }
}

/**
 * Refreshes and unlocks given tenant
 * @param tenant {string}
 * @returns {Promise<void>}
 */
async function unlockTenant(tenant) {
    logger.debug(`Refreshing tenant '${tenant}'...`);
    await callEs(`indices.refresh`, {
        index: `${tenant}_*`
    });
    logger.debug(`Tenant '${tenant}' has been refreshed.`);

    if (BLOCK_INDICES.read || BLOCK_INDICES.write) {
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
 * @param migrationProcess {MigrationProcess}
 * @returns {Promise<void>}
 */
async function openIndices(migrationProcess) {
    const mainIndex = migrationProcess._index;
    if (!_.isEmpty(mainIndex)) {
        //Check if main index exist
        //Other indices are not checked, as we don't know what's going to happen there
        const inputOdmExistence = await _checkIndexExistence(odms[mainIndex]);
        if (!inputOdmExistence) {
            throw Error(`'${migrationProcess._migrations[0].version}': Requested INDEX: '${mainIndex}' doesn't exist.`);
        }
    }

    //=== Input indices, main index included
    const inputIndices = migrationProcess._inputIndices;
    const inputOdms = inputIndices.map((index) => odms[index]);

    //Refresh
    for (const Odm of inputOdms) {
        try {
            await Odm.refresh();
        } catch (e) {
            //Doesn't exist, probably OK
        }
    }

    if (BLOCK_INDICES.read || BLOCK_INDICES.write) {
        if (BLOCK_INDICES.read) {
            for (const Odm of inputOdms) {
                try {
                    //Unlock read
                    await Odm.putSettings({
                        blocks: {
                            read: null
                        }
                    });
                } catch (e) {
                    //Doesn't exist, probably OK
                }
            }
        }

        //== Output indices, main index included
        const outputIndices = migrationProcess._outputIndices;
        let outputOdms = outputIndices.map((index) => odms[index]);
        //Filter only existing ones
        outputOdms = await getExistingModels(outputOdms);
        for (const Odm of outputOdms) {
            //Unlock both read and write
            await Odm.putSettings({
                blocks: {
                    read: null,
                    write: null
                }
            });
        }
    }
}

/**
 * Closes indices after the node has been finished
 * @param migrationProcess {MigrationProcess}
 * @returns {Promise<void>}
 */
async function closeIndices(migrationProcess) {
    if (BLOCK_INDICES.read || BLOCK_INDICES.write) {
        //== Output indices only, main index included
        const outputIndices = migrationProcess._outputIndices;
        let outputOdms = outputIndices.map((index) => odms[index]);

        //Filter only existing ones
        outputOdms = await getExistingModels(outputOdms);
        for (const Odm of outputOdms) {
            //And lock both read and write
            await Odm.putSettings({
                blocks: {
                    read: (BLOCK_INDICES.read) ? true : null,
                    write: (BLOCK_INDICES.write) ? true : null
                }
            });
        }
    }
}

/**
 * Migrates ES indices to use aliases
 * @returns {Promise<void>}
 */
async function migrateToAliases() {
    logger.debug(`Checking if existing indices use aliases...`);

    let counter = 0;
    const allOdms = Object.keys(_indicesMap).map((index) => odms[index]);
    const existingModels = await getExistingModels(allOdms);
    for (const Odm of existingModels) {
        logger.trace(`Checking alias '${Odm.alias}'...`);

        const realIndex = await Odm.getIndex();
        if (realIndex === Odm.alias) {
            logger.debug(`Index '${Odm.alias}' isn't aliased -> aliasing now...`);

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

            logger.info(`Index '${Odm.alias}' has been aliased, new index is '${newIndex}'.`);
        }
    }

    if (counter > 0) {
        logger.debug(`${counter} indices have been aliased.`);
    } else {
        logger.debug(`No index has been aliased.`);
    }
}

/**
 * Update ES indices that were not created in this ES major version
 * @returns {Promise<void>}
 */
async function updateIndices() {
    logger.info(`Checking for outdated indices...`);

    //Always update the meta index
    await _updateIndex(_metaOdm);

    const masterSettings = await _metaOdm.getSettings();
    const masterIndexBuild = Object.values(masterSettings)[0].settings.index.version?.created;

    let counter = 0;
    const allOdms = Object.values(odms);
    const existingModels = await getExistingModels(allOdms);
    for (const Odm of existingModels) {
        logger.debug(`Checking index '${Odm.alias}'...`);

        const settings = await Odm.getSettings();
        const indexBuild = Object.values(settings)[0].settings.index.version?.created;

        if (!indexBuild || (indexBuild !== masterIndexBuild)) {
            logger.info(`Index '${Odm.alias}' is outdated, updating now...`);
            await _updateIndex(Odm);
            logger.info(`Index '${Odm.alias}' has been updated.`);
            counter++;
        }
    }

    if (counter > 0) {
        logger.info(`${counter} indices have been updated.`);
    } else {
        logger.info(`No index needs to be updated.`);
    }
}

/**
 * Updates a single index by reindexing it
 * @param Odm {{}}
 * @returns {Promise<void>}
 */
async function _updateIndex(Odm) {
    //Refresh and unlock
    await Odm.refresh();
    await Odm.putSettings({
        blocks: {
            read: null,
            write: null
        }
    });

    //Fetch mapping and settings
    const mapping = await Odm.getMapping();
    const myMapping = Object.values(mapping)[0].mappings;

    const settings = await Odm.getSettings();
    const mySettings = Object.values(settings)[0].settings;

    //Create a new index
    const newIndex = await safeCreateIndex(Odm, mySettings, {}, myMapping);

    //Reindex
    const bulkSize = await Odm._getBulkSize();
    await Odm.reindex(newIndex, void 0, bulkSize, true);

    //Delete the old index and switch alias
    await Odm.deleteIndex();
    await Odm.aliasIndex(newIndex);
}

/**
 * Returns existing ODMs of given ODMs
 * @param allOdms {Array<{}>}
 * @returns {Promise<Array<{}>>}
 */
async function getExistingModels(allOdms) {
    const existingOdms = [];
    for (const Odm of allOdms) {
        const indexExists = await Odm.indexExists();
        if (indexExists) {
            existingOdms.push(Odm);
        }
    }

    return existingOdms;
}

/**
 * Sends ES bulk request, handles "Request Entity Too Large" and "Invalid string length"
 * It is necessary for the bulk elements to be of the same type, eg. you cannot mix save and delete requests
 * @param bulkSave {Array<{}>}
 * @param errorMessage {string}
 * @param MyOdm {{}}
 * @param numberOfProcessed {number}
 * @param bulkSize {number}
 * @param responseCache {{took: number, errors: boolean, items: Array<{}>}}
 * @returns {Promise<{}>}
 */
async function sendBulk(bulkSave, errorMessage = void 0, MyOdm = void 0, numberOfProcessed = 0, bulkSize = bulkSave.length, responseCache = void 0) {
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

            if (!_.isNil(MyOdm)) {
                const sourceFields = [];
                toProcess.forEach((singleData, index) => {
                    if (index % 2 === 1) {
                        sourceFields.push(singleData);
                    }
                });
                await optimizations.updateDocumentSize(MyOdm, sourceFields);
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

            let newBulkSize = Math.floor(bulkSize / 2);
            if ((newBulkSize % 2) === 1) {
                //Ensure this is even
                newBulkSize += 1;
            }

            return sendBulk(bulkSave, errorMessage, MyOdm, numberOfProcessed, newBulkSize, responseCache);

        } else {
            throw e;
        }
    }
}

/**
 * Tries to create a new ES index, automatically removes not allowed settings parameters
 * @param MyOdm
 * @param mySettings {{}} Original settings object
 * @param updatedSettings {{}} User changes on settings
 * @param myMapping {{}} Mapping object
 * @param iteration {number} How many times we tried to strip the invalid parameters
 * @param indexRetry {number} How many times we have to delete incorrect index (see note in code)
 * @returns {Promise<string>} Output index in case this was successful
 */
async function safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, iteration = 0, indexRetry = 0) {
    const knownForbiddenItems = [`index.creation_date`, `index.resize`, `index.provided_name`, `index.routing`, `index.uuid`, `index.version`, `index.routing_partition_size`, `index.soft_deletes`];
    for (const knownForbiddenItem of knownForbiddenItems) {
        _.set(mySettings, knownForbiddenItem, void 0);
    }

    try {
        //Prepare new index; do NOT set alias yet
        return await MyOdm.createIndex({
            settings: mySettings,
            mappings: myMapping
        }, false);

    } catch (esError) {
        logger.debug(`Error when trying to create new index, indexRetry: '${indexRetry}', iteration: '${iteration}', settings: '${JSON.stringify(mySettings)}', mapping: '${JSON.stringify(myMapping)}'.`);

        let rootCauses = esError.meta?.body?.error?.root_cause;
        if (_.isEmpty(rootCauses)) {
            throw Error(`Error when creating new index - ${esError.meta?.body?.error?.reason}.`);
        }
        if (!_.isArray(rootCauses)) {
            if (Object.keys(rootCauses).every((key) => _.isString(key) && !_.isEmpty(key) && Number.isInteger(+key) && parseInt(key, 10) >= 0)) {
                //Sometimes we receive array-like object with string keys -> make it an array
                rootCauses = Object.values(rootCauses);
            } else {
                rootCauses = _.castArray(rootCauses);
            }
        }
        rootCauses = (_.isArray(rootCauses)) ? rootCauses : _.castArray(rootCauses);
        const suppressed = esError.meta?.body?.error?.suppressed;

        if (rootCauses.length === 1 && rootCauses[0].type === `resource_already_exists_exception` && !suppressed) {
            //There is a bug in ES if we send way too much (failing) requests to create an index.
            //Once the request finally proceeds, it is stated that the index already exists.
            //But that's not possible, as we use random uuid-v4 which is re-generated for each request.
            //What seems to work is to delete this new index and start again.

            if (indexRetry < RETRIES.maxRetries) {
                await optimizations.sleep(indexRetry);
                indexRetry++;

                await callEs(`indices.delete`, {
                    index: rootCauses[0].index
                });
                return safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, 0, indexRetry);
            } else {
                throw Error(`Error when creating new index, ES too many times responded with "resource_already_exists_exception".`);
            }
        }

        if (iteration >= 10) {
            throw Error(`Error when creating new index, exceeded max number of retries - ${esError.meta?.body?.error?.reason}.`);
        }

        let errors = [...rootCauses];
        if (_.isArray(suppressed)) {
            errors.push(...suppressed);
        }

        errors = _.compact(errors);
        if (_.isEmpty(errors) || errors.some((error) => (_.isEmpty(error) || (_.isEmpty(error.type) || (_.isEmpty(error.reason)) || (error.type !== `illegal_argument_exception` && error.type !== `validation_exception`))))) {
            throw Error(`Error when creating new index - ${esError.meta?.body?.error?.reason}.`);
        }

        const illegalArgumentExceptions = errors.filter((error) => error.type === `illegal_argument_exception`);
        //Illegal argument exception + unknown settings
        const illegalUnknownSettings = illegalArgumentExceptions.map((error) => error.reason.match(/^unknown setting \[(.*?)]/));
        for (const illegalUnknownSetting of illegalUnknownSettings) {
            if (!_.isNil(illegalUnknownSetting) && !_.isNil(illegalUnknownSetting[1])) {
                _.set(mySettings, illegalUnknownSetting[1], void 0);
            }
        }
        //Illegal argument exception + private index settings
        const illegalPrivateSettings = illegalArgumentExceptions.map((error) => error.reason.match(/.*?private index setting \[(.*?)] can not be set explicitly/));
        for (const illegalPrivateSetting of illegalPrivateSettings) {
            if (!_.isNil(illegalPrivateSetting) && !_.isNil(illegalPrivateSetting[1])) {
                _.set(mySettings, illegalPrivateSetting[1], void 0);
            }
        }

        const validationExceptions = errors.filter((error) => error.type === `validation_exception`);
        //Validation exception + unknown settings
        const validationUnknownSettings = validationExceptions.map((error) => error.reason.match(/^unknown setting \[(.*?)]/));
        for (const validationUnknownSetting of validationUnknownSettings) {
            if (!_.isNil(validationUnknownSetting) && !_.isNil(validationUnknownSetting[1])) {
                _.set(mySettings, validationUnknownSetting[1], void 0);
            }
        }
        //Validation exception + private index settings
        const validationPrivateSettings = validationExceptions.map((error) => error.reason.match(/.*?private index setting \[(.*?)] can not be set explicitly/));
        for (const validationPrivateSetting of validationPrivateSettings) {
            if (!_.isNil(validationPrivateSetting) && !_.isNil(validationPrivateSetting[1])) {
                _.set(mySettings, validationPrivateSetting[1], void 0);
            }
        }

        _.mergeWith(mySettings, updatedSettings, (a, b) => _.isArray(b) ? b : void 0);

        return safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, iteration + 1, indexRetry);
    }
}

/**
 * Parses ES stream
 * @param esResult {{}} ES response
 * @returns {Promise<{}>} Response object
 */
async function _parseStream(esResult) {
    const body = await new Promise((resolve, reject) => {
        try {
            const chunkCache = [];
            let totalLength = 0;
            let isPiped = false;
            const stream = esResult.body;

            stream.on(`data`, (newChunk) => {
                try {
                    if (isPiped) {
                        return;
                    }

                    if ((totalLength + newChunk.length) < MAX_STRING_LENGTH) {
                        //Cache chunks as long as possible
                        chunkCache.push(newChunk);
                        totalLength += newChunk.length;

                    } else {
                        //Caching is no longer possible, as we could exceed max string length -> pass data to "stream-json" to parse them
                        const passThrough = new PassThrough();
                        for (const myData of chunkCache) {
                            passThrough.push(myData);
                        }
                        passThrough.push(newChunk);
                        stream.pipe(passThrough);
                        isPiped = true;

                        const pipeline = passThrough.pipe(new streamJson.Parser({ jsonStreaming: false }));
                        const streamAssembler = StreamAssembler.connectTo(pipeline);

                        pipeline.on(`end`, () => {
                            try {
                                return resolve(streamAssembler.current);
                            } catch (e) {
                                return reject(e);
                            }
                        });
                        pipeline.on(`error`, (e) => {
                            return reject(e);
                        });
                    }
                } catch (e) {
                    return reject(e);
                }
            });

            stream.on(`end`, () => {
                try {
                    if (isPiped) {
                        //Response will come from "stream-json"
                        return void 0;
                    } else if (chunkCache.length > 0) {
                        //Concat chunks and parse JSON
                        return resolve(JSON.parse(Buffer.concat(chunkCache, totalLength).toString(`utf-8`)));
                    } else {
                        //No data
                        return resolve(void 0);
                    }
                } catch (e) {
                    return reject(e);
                }
            });

        } catch (e) {
            return reject(e);
        }
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
            index: Odm.alias
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
    const fields = [`number_of_replicas`, `auto_expand_replicas`, `refresh_interval`];

    //Find all output ODMs
    const outputOdms = _.uniq(_.compact(nodes.reduce((sum, node) => {
        sum.push(...node._outputIndices.map((usedIndex) => odms[usedIndex]));
        return sum;
    }, [])));

    const myStats = {};

    //Download data of ALL known ODMs
    const allOdms = Object.values(_indicesMap).map((index) => odms[index]);
    for (const SingleOdm of allOdms) {
        try {
            const settings = await SingleOdm.getSettings();
            for (const index of Object.keys(settings)) {
                myStats[index] = {
                    alias: SingleOdm._parseIndex(index).alias,
                    isOutput: (outputOdms.includes(SingleOdm)),
                    data: {}
                };
                for (const field of fields) {
                    myStats[index].data[field] = settings[index].settings.index[field] ?? null;
                }
            }
        } catch (e) {
            //Not exists, OK
        }
    }

    //Enable restrictions
    for (const index of Object.keys(myStats)) {
        if (myStats[index].isOutput) {
            const body = {};
            for (const field of fields) {
                body[field] = optimizations.getRestrictionsEmptyValue(field);
            }

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

        //Get sorted indices (aliases actually)
        meta.indices = myIndices.map((myIndex) => myIndex.split(`-`)[0].substring(tenant.length + 1));
        meta.indices = _.uniq(meta.indices.sort((a, b) => a.localeCompare(b, `en`)));

        if (!_.isEmpty(meta?._optimizations?.averageDocumentSizes)) {
            //Remove not existing indices (aliases) from average document sizes

            //Get aliases from indices
            const myAliases = _.uniq(myIndices.map((myIndex) => myIndex.split(`-`)[0]));

            //Remove not existing
            Object.keys(meta._optimizations.averageDocumentSizes).forEach((key) => {
                if (!myAliases.includes(key)) {
                    delete meta._optimizations.averageDocumentSizes[key];
                }
            });
        }
    }
}

export default {
    /** @type {{}} */
    get _esErrors() { return _esErrors; },
    /** @type {Record<string, string>} */
    get _indicesMap() { return _indicesMap; },
    /** @type {Record<string, {}>} */
    get odms() { return odms; },
    /** @type {Record<string, {}>} */
    get restrictedOdms() { return restrictedOdms; },
    /** @type {Record<string, string>} */
    get aliasToName() { return aliasToName; },
    /** @type {Record<string, string>} */
    get nameToKey() { return nameToKey; },
    /** @type {{}} */
    get _metaOdm() { return _metaOdm; },

    /** @type {BulkArray} */
    BulkArray: esOdm.BulkArray,

    createElastic: createElastic,
    getAllowedIndices: getAllowedIndices,
    callEs: callEs,
    lockTenant: lockTenant,
    unlockTenant: unlockTenant,
    openIndices: openIndices,
    closeIndices: closeIndices,
    migrateToAliases: migrateToAliases,
    updateIndices: updateIndices,
    getExistingModels: getExistingModels,
    sendBulk: sendBulk,
    safeCreateIndex: safeCreateIndex,

    restrictIndices: restrictIndices,
    releaseIndices: releaseIndices,

    checkExistingIndices: checkExistingIndices,
    saveNewIndices: saveNewIndices,
};

