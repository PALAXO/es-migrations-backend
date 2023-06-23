'use strict';

let parentPort, workerData;
let _;
let elastic;
let utils;
let optimizations;
let nconf;

/** @type {number} */
let REPORT_SECONDS;

/** @type {boolean} */
let isTerminated;

/** @type {Array<MigrationWorker>} */
let migrationWorkers;

try {
    parentPort = require(`node:worker_threads`).parentPort;
    workerData = require(`node:worker_threads`).workerData;
    _ = require(`lodash`);
    elastic = require(`./elastic`);
    utils = require(`./utils`);
    optimizations = require(`./optimizations`);
    nconf = require(`../config/config`);

    REPORT_SECONDS = nconf.get(`options:reportSeconds`);

    isTerminated = false;

    utils.promisifyThreadCommunication(parentPort, async (message) => {
        try {
            let sendResponse = false;
            let responseData = void 0;
            let exit = false;
            switch (message.type) {
                case `__initialize`: {
                    await elastic.createElastic(workerData.tenant, workerData.esHost, workerData.indicesInfo);
                    optimizations.setRestrictions(message.data.restrictions);
                    optimizations.setDocumentCounts(message.data.documentCounts);
                    optimizations.setAverageDocumentSizes(message.data.averageDocumentSizes);

                    const Migration = require(`./Migration`);
                    const OrigMigration = require(workerData.migrationPath);
                    OrigMigration(Migration);

                    migrationWorkers = [];
                    for (let i = 0; i < workerData.concurrency; i++) {
                        const workerIndex = (workerData.physicalWorkerIndex * workerData.concurrency) + i;
                        migrationWorkers.push(new MigrationWorker(message.data, workerIndex));
                    }
                    sendResponse = true;
                    break;

                }
                case `__setUpdateFunctions`: {
                    await Promise.all(migrationWorkers.map((myWorker) => myWorker.setUpdateFunctions(message.data)));
                    sendResponse = true;
                    break;

                }
                case `__migrateExistingDocuments`: {
                    responseData = await Promise.all(migrationWorkers.map((myWorker) => myWorker.migrateExistingDocuments(message.data)));
                    sendResponse = true;
                    break;

                }
                case `__migrateVirtualDocuments`: {
                    responseData = await migrationWorkers[0].migrateVirtualDocuments(message.data);
                    sendResponse = true;
                    break;

                }
                case `__finalizeMigration`: {
                    responseData = await Promise.all(migrationWorkers.map((myWorker) => myWorker.finalizeMigration(message.data)));
                    sendResponse = true;
                    break;

                }
                case `__finalizeWorker`: {
                    responseData = await migrationWorkers[0].finalizeWorker();
                    sendResponse = true;
                    exit = true;
                    break;

                }

                case `__done`: {
                    parentPort._activePromises.get(message.promiseIndex).resolve(message.returnValue ?? {});
                    break;
                }
                case `__error`: {
                    try {
                        parentPort._activePromises.get(message.promiseIndex).reject(message.error);
                    } catch (err) {
                        throw Error(message.error);
                    }
                    break;
                }
                case `__userCall`: {
                    responseData = await Promise.all(migrationWorkers.map((myWorker) => myWorker._migrations[message.migrationIndex][message.functionName](message.data)));
                    sendResponse = true;
                    break;

                }
                case `__terminate`: {
                    isTerminated = true;
                    break;

                }
                default: {
                    throw Error(`Unknown message type '${message?.type}'`);
                }
            }

            if (sendResponse) {
                parentPort.postMessage({ type: `__done`, promiseIndex: message.promiseIndex, returnValue: responseData });
            }

            if (exit) {
                process.exit(0);
            }
        } catch (e) {
            parentPort.postMessage({
                type: `__error`,
                error: e,
                promiseIndex: message.promiseIndex
            });
        }
    });

} catch (e) {
    parentPort.postMessage({
        type: `__error`,
        error: e
    });
    process.exit(1);
}

class MigrationWorker {
    /**
     *
     * @param data {{migrations: Array<{filepath: string, classIndex: number, version:string, versionNumbers: string}>,
     * index: string, outputIndex: string, backgroundToProcessData: {}, outputOnlyIndices: Array<string>,
     * averageDocumentSizes: Record<string, number>, documentCounts: Record<string, number>,
     * restrictions: {enabled: boolean, fields: Array<string>, data: Record<string, {reference: string, data: Record<string, *>}>}}}
     * @param workerIndex {number}
     */
    constructor(data, workerIndex) {
        /** @type {string} */
        this.index = data.index;

        this.MyOdm = (this.index) ? elastic.getWorkerOdm(data.index, workerIndex, workerData.workerCount, false) : void 0;

        /** @type {string} */
        this.outputIndex = data.outputIndex;

        /** @type {number} */
        this.workerIndex = workerIndex;

        /** @type {Array<Migration>} */
        this._migrations = [];
        for (let migrationIndex = 0; migrationIndex < data.migrations.length; migrationIndex++) {
            const migrationData = data.migrations[migrationIndex];
            const Constructor = _.castArray(require(migrationData.filepath))[migrationData.classIndex];
            const myMigration = new Constructor(migrationData.version, migrationData.versionNumbers, void 0);
            myMigration.__filepath = migrationData.filepath;
            myMigration.__classIndex = migrationData.classIndex;
            myMigration.__isInitialized = true;
            myMigration.utils.workerIndex = workerIndex;
            myMigration.utils.workerCount = workerData.workerCount;
            myMigration.utils.postMaster = async (func, data = void 0) => {
                if (_.isNil(func) || !_.isFunction(func) || _.isNil(myMigration[func.name])) {
                    throw Error(`'${myMigration.version}': You have to specify the function to be called from the master context.`);
                }

                return parentPort._sendMessage({
                    type: `__userCall`,
                    migrationIndex: migrationIndex,
                    functionName: func.name,
                    data: data
                });
            };

            for (let dependencyMigrationIndex = 0; dependencyMigrationIndex < myMigration._info.dependencyMigrations.length; dependencyMigrationIndex++) {
                myMigration.DEPENDENCIES[myMigration._info.dependencyMigrations[dependencyMigrationIndex]] = data.backgroundToProcessData[migrationIndex][dependencyMigrationIndex];
            }

            const restrictedIndices = _.intersection(data.outputOnlyIndices, myMigration._info.outputIndices);
            for (const name of Object.keys(myMigration.ODM)) {
                const isRestricted = restrictedIndices.includes(name);
                myMigration.ODM[name] = elastic.getWorkerOdm(name, workerIndex, workerData.workerCount, !isRestricted);
            }

            this._migrations.push(myMigration);
        }

        /** @type {{enabled: boolean, fields: Array<string>, data: Record<string, {reference: string, data: Record<string, *>}>}} */
        this.originalRestrictions = data.restrictions ?? {};

        /** @type {number} */
        this.processedDocuments = 0;
    }

    /**
     * Sets update functions to this worker
     * @param data {Array<{updatedDocuments: Map<string, {functionName: string, fallbackSource: {}, isUsed: boolean, isForced: boolean}>, scriptCache: string}>}
     * @returns {Promise<void>}
     */
    async setUpdateFunctions(data) {
        for (let i = 0; i < this._migrations.length; i++) {
            this._migrations[i].__updatedDocuments = data[i].updatedDocuments;
            this._migrations[i].__scriptCache = new Function(`doc`, data[i].scriptCache);
        }
    }

    /**
     * Migrates existing documents in the worker thread
     * @param data {{ids: Array<string>, pitID: string}}
     * @returns {Promise<{toDelete: Map<string, Set<number>>, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}>}
     */
    async migrateExistingDocuments(data) {
        /** @type {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>} */
        const virtualDocuments = {};
        /** @type {Map<string, Set<number>>} */
        const toDelete = new Map();

        if (!isTerminated) {
            const body = {
                query: (data.ids) ? {
                    ids: {
                        values: data.ids
                    }
                } : {
                    match_all: {}
                }
            };

            let nextReport = Date.now() + ((0.9 * REPORT_SECONDS) * 1000);
            const myBulks = this.MyOdm.bulkIterator(body, { source: true, pitId: data.pitID });
            for await (const bulk of myBulks) {
                const toSave = await this._runMigrationsOverBulk(bulk, virtualDocuments, toDelete);
                if (toSave.length > 0) {
                    //And save result
                    const bulkSave = [];
                    toSave.forEach((document) => {
                        bulkSave.push({
                            index: {
                                _index: this.outputIndex,
                                _id: document._id,
                                version: document._version + 1,
                                version_type: `external_gte`
                            }
                        });
                        bulkSave.push(document);
                    });
                    await elastic.sendBulk(bulkSave, `Failed when re-saving existing documents`, this.MyOdm);
                    this.processedDocuments += toSave.length;
                }

                if (isTerminated) {
                    break;
                }

                if (nextReport < Date.now()) {
                    nextReport = Date.now() + (REPORT_SECONDS * 1000);
                    parentPort.postMessage({
                        type: `__report`,
                        workerIndex: this.workerIndex,
                        returnValue: this.processedDocuments
                    });
                }
            }
        }

        return {
            virtualDocuments,
            toDelete
        };
    }

    /**
     * Migrates virtual (not yet existing) documents
     * @param data {{migrationIndex: number, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>, toDelete: Map<string, Set<number>>}}
     * @returns {Promise<{toDelete: Map<string, Set<number>>, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}>}
     */
    async migrateVirtualDocuments(data) {
        const migrationIndex = data.migrationIndex;
        const virtualDocuments = data.virtualDocuments;
        const toDelete = data.toDelete;

        //Check if new documents should be created and migrate them
        if (!_.isEmpty(virtualDocuments[`${migrationIndex}`])) {
            const pseudoBulk = [...await this._prepareVirtuals(this.MyOdm, virtualDocuments[`${migrationIndex}`]?.knownIDs, virtualDocuments, migrationIndex, toDelete), ...await this._allocateVirtuals(this.MyOdm, virtualDocuments[`${migrationIndex}`]?.unknownIDs)];

            let processed = 0;
            const maxBulkSize = await this.MyOdm._getBulkSize();
            const isLastMigration = (migrationIndex === (this._migrations.length - 1));
            while (processed < pseudoBulk.length) {
                //Ensure pseudoBulk size doesn't exceed max allowed size
                const processedArray = (maxBulkSize >= pseudoBulk.length) ? pseudoBulk : pseudoBulk.slice(processed, processed + maxBulkSize);
                processed += maxBulkSize;

                const toSave = (isLastMigration) ?
                    processedArray :
                    await this._runMigrationsOverBulk(processedArray, virtualDocuments, toDelete, migrationIndex + 1);

                if (toSave.length > 0) {
                    //And save result
                    const bulkSave = [];
                    toSave.forEach((document) => {
                        bulkSave.push({
                            index: {
                                _index: this.outputIndex,
                                _id: document._id
                            }
                        });
                        bulkSave.push((isLastMigration) ? document._source : document);
                    });

                    await elastic.sendBulk(bulkSave, `Failed when saving newly created documents`, this.MyOdm);
                }
            }
        }

        return {
            virtualDocuments,
            toDelete
        };
    }

    /**
     * Finalizes single migration
     * @param data {{migrationIndex: number}}
     * @returns {Promise<{newDocuments: Array<{source: {}, id: string, isForced: boolean}>, usedUpdateIDs: Array<string>}>}
     */
    async finalizeMigration(data) {
        const usedUpdateIDs = [];
        const finalizingMigration = this._migrations[data.migrationIndex];
        for (const [id, updateData] of finalizingMigration.__updatedDocuments) {
            if (updateData.isUsed) {
                usedUpdateIDs.push(id);
            }
        }

        return {
            usedUpdateIDs: usedUpdateIDs,
            newDocuments: finalizingMigration.__createdDocuments
        };
    }

    /**
     * Finalizes worker
     * @returns {Promise<{messages: Record<string, {version: string, messages: Array<string>}>, localDocumentChanges: Record<string, {size: number, count: number}>}>}
     */
    async finalizeWorker() {
        const myRestrictions = optimizations.getRestrictions();
        if (this.originalRestrictions?.data && !_.isEqual(this.originalRestrictions.data, myRestrictions.data)) {
            throw Error(`Changing indices in worker threads is not supported.`);
        }

        return {
            messages: this._resetMessages(),
            localDocumentChanges: optimizations.getThreadDocumentChanges()
        };
    }

    //==================================================== Internal ====================================================

    /**
     * Migrates single ES bulk. Runs the beforeBulk and migrate functions for all the documents, takes care if document should be deleted, etc.
     * @param documents {Array<{}>} Input ES bulk
     * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
     * @param toDelete {Map<string, Set<number>>}
     * @param firstMigration {number}
     * @returns {Promise<Array<{}>>} ES bulk after all the migrations have been processed
     */
    async _runMigrationsOverBulk(documents, virtualDocuments, toDelete, firstMigration = 0) {
        //Prepare bulk in our format
        let myBulk = documents.map((document) => {
            //Shallow copy is necessary, as there may be some not-configurable values
            const myDocument = { ...document._source };
            const parsedIndex = this.MyOdm._parseIndex(document._index);
            Object.defineProperty(myDocument, `_id`, {  //Writable by default
                value: document._id,
                writable: false,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_alias`, {   //Not writable
                value: parsedIndex.alias,
                writable: false,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_version`, {   //Not writable
                value: document._version,
                writable: false,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_seq_no`, {   //Not writable
                value: document._seq_no,
                writable: false,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_primary_term`, {   //Not writable
                value: document._primary_term,
                writable: false,
                enumerable: false,
                configurable: true
            });

            return myDocument;
        });
        let newBulk = [];

        //Go through migrations
        for (let i = firstMigration; i < this._migrations.length; i++) {
            const migration = this._migrations[i];

            const runBulk = [...myBulk];
            if (!_.isEmpty(runBulk)) {
                await migration._runBeforeBulk(runBulk);
            }

            for (const myDocument of runBulk) {
                //Go through bulk and migrate

                const originalId = myDocument._id;
                Object.defineProperty(myDocument, `_id`, {
                    value: originalId,
                    writable: true,
                    enumerable: false,
                    configurable: true
                });

                await migration._runMigrate(myDocument);
                const newId = myDocument._id;

                //Now check if ID has changed
                if (originalId === newId) {
                    //No change -> pass document to the next round
                    newBulk.push(myDocument);

                } else if (_.isNull(newId)) {
                    //Null -> delete document
                    utils.deleteDocument(i, originalId, virtualDocuments, toDelete);

                } else if (_.isUndefined(newId)) {
                    //Undefined -> delete original document and re-save with auto generated ID
                    utils.deleteDocument(i, originalId, virtualDocuments, toDelete);
                    utils.createDocument(i, void 0, myDocument, virtualDocuments);

                } else if (_.isString(newId) && !_.isEmpty(newId)) {
                    //Changed -> delete original document and re-save with new ID
                    utils.deleteDocument(i, originalId, virtualDocuments, toDelete);
                    utils.createDocument(i, newId, myDocument, virtualDocuments);

                } else {
                    throw Error(`'${migration.version}': Invalid ID specified - '${newId}'.`);
                }
            }

            //Switch arrays
            myBulk = newBulk;
            newBulk = [];
        }

        return myBulk;
    }

    /**
     * Checks which documents should be newly created, ensures they don't exist in ES yet and prepares them.
     * @param Odm
     * @param knownDocuments {Map<string, {source: {}, isDeleted: boolean}>}
     * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
     * @param migrationIndex {number} Index of migration on which behalf we create the documents
     * @param toDelete {Map<string, Set<number>>}
     * @returns {Promise<Array<{}>>} List of prepared virtual documents, mimics ES search response
     */
    async _prepareVirtuals(Odm, knownDocuments, virtualDocuments, migrationIndex, toDelete) {
        const response = [];

        if (!knownDocuments || knownDocuments.size === 0) {
            return response;
        }

        //Check if documents don't already exist
        const toCheckDocumentIDs = [];
        for (const id of knownDocuments.keys()) {
            let isDeleted = false;
            //At first check if document wasn't already virtually created
            for (let j = migrationIndex - 1; j >= 0; j--) {
                //Check previous migrations only
                const checkedIteration = virtualDocuments[`${j}`]?.knownIDs;
                if (!checkedIteration || checkedIteration.size === 0) {
                    continue;
                }

                const existingVirtual = checkedIteration.get(id);
                if (existingVirtual) {
                    if (existingVirtual.isDeleted) {
                        //Was created but was also deleted -> that's OK
                        isDeleted = true;
                        break;
                    } else {
                        //Was created and still exists
                        throw Error(`'${this._migrations[migrationIndex].version}': Document at alias '${Odm.alias}' with ID '${id}' already exists.`);
                    }
                }
            }

            if (!isDeleted) {
                //Document not mentioned in virtuals -> check if marked on to be deleted list
                if (!toDelete.has(id)) {
                    //Not mentioned -> check it
                    toCheckDocumentIDs.push(id);
                }
            }
        }

        if (toCheckDocumentIDs.length > 0) {
            //Send request to ES
            const checkResult = await elastic.callEs(`mget`, {
                index: Odm.alias,
                ids: toCheckDocumentIDs,
                _source: false
            });

            const alreadyExistingIDs = [];
            if (checkResult.docs) {
                for (const checkDoc of checkResult.docs) {
                    if (checkDoc.found) {
                        alreadyExistingIDs.push(checkDoc._id);
                    }
                }
            }
            if (alreadyExistingIDs.length > 0) {
                //We found existing document/s
                throw Error(`'${this._migrations[migrationIndex].version}': You try to create documents which already exist in index '${Odm.alias}': ${alreadyExistingIDs.join(`,`)}`);
            }
        }

        for (const [id, preparedVirtual] of knownDocuments.entries()) {
            //Mimic ES search response
            response.push({
                _index: Odm.alias,
                _id: id,
                _version: 1,
                _source: preparedVirtual.source,
            });
        }
        return response;
    }

    /**
     * Creates an empty document in ES for virtual documents which don't specify an ID. Prepares and returns virtual documents.
     * @param Odm
     * @param unknownDocuments {Set<{source: {}}>}
     * @returns {Promise<Array<{}>>} List of prepared virtual documents, mimics ES search response
     */
    async _allocateVirtuals(Odm, unknownDocuments) {
        const response = [];

        if (!unknownDocuments || unknownDocuments.size === 0) {
            return response;
        }

        const bulkSave = [];
        const toAllocateVirtuals = [...unknownDocuments.values()];
        toAllocateVirtuals.forEach(() => {
            bulkSave.push({
                index: {
                    //Allocate in ES documents without ID (=== obtain ID)
                    _index: Odm.alias, //Send to original index intentionally
                }
            });
            bulkSave.push({});  //Send empty body intentionally
        });

        const bulkResponse = await elastic.sendBulk(bulkSave, `Failed when saving temporary documents`);
        const responseItems = bulkResponse.items;

        for (let i = 0; i < toAllocateVirtuals.length; i++) {
            const allocatedVirtual = toAllocateVirtuals[i];

            //Mimic ES search response
            response.push({
                _index: Odm.alias,
                _id: responseItems[i].index._id,
                _version: responseItems[i].index._version,
                _source: allocatedVirtual.source,
            });
        }

        return response;
    }

    /**
     * Returns and resets global messages/notes
     * @returns {Record<string, {version: string, messages: Array<string>}>}
     */
    _resetMessages() {
        const notes = global.MESSAGES;
        global.MESSAGES = {};
        return notes;
    }
}
