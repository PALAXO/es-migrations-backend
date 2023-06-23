'use strict';

let _;
let os;
let Worker;
let path;
let elastic;
let utils;
let logger;
let optimizations;
let nconf;

let SYNCHRONISATION_TYPES;

let CORRECT_SETTINGS;
let CHECK_INTERVALS;
let ALWAYS_NEW_INDICES;
let RETRIES;
let REPORT_SECONDS;
let PARALLELIZATION;

/** @type {MigrationProcess} */
let migrationProcess;

/** @type {boolean} */
let isTerminated;

try {
    _ = require(`lodash`);
    os = require(`os`);
    Worker = require(`node:worker_threads`).Worker;
    path = require(`path`);
    elastic = require(`./elastic`);
    utils = require(`./utils`);
    logger = require(`./logger`);
    optimizations = require(`./optimizations`);
    nconf = require(`../config/config`);

    SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;

    CORRECT_SETTINGS = nconf.get(`es:correctSettings`);
    CHECK_INTERVALS = nconf.get(`es:checkIntervals`);
    ALWAYS_NEW_INDICES = nconf.get(`options:optimizations:alwaysNewIndices`);
    RETRIES = nconf.get(`es:retries`);
    REPORT_SECONDS = nconf.get(`options:reportSeconds`);
    PARALLELIZATION = nconf.get(`options:parallelization`);

    isTerminated = false;

    logger.debug(`Started new process with PID '${process.pid}'`);

    process.on(`message`, async (value) => {
        try {
            if (value?.type === `__run`) {
                const data = value.data;

                await elastic.createElastic(data.metadata.tenant, data.metadata.esHost, data.metadata.indicesInfo);
                optimizations.setRestrictions(data.restrictions);
                optimizations.setAverageDocumentSizes(data.averageDocumentSizes);

                const Migration = require(`./Migration`);
                const OrigMigration = require(data.metadata.migrationPath);
                OrigMigration(Migration);

                migrationProcess = new MigrationProcess(data.type, data.index, data.inputIndices, data.outputIndices, data.migrations, data.backgroundToProcessData);

                const processToBackgroundData = await migrationProcess.run(data.metadata, data.backgroundToProcessData);

                process.send({
                    type: `__done`,
                    returnValue: {
                        messages: global.MESSAGES,
                        processToBackgroundData: processToBackgroundData,
                        averageDocumentSizes: optimizations.getAverageDocumentSizes(),
                        restrictions: optimizations.getRestrictions(),
                    }
                });
                process.exit(0);

            } else if (value?.type === `__terminate`) {
                if (!isTerminated) {
                    isTerminated = true;

                    if (migrationProcess?._workers) {
                        for (const worker of migrationProcess._workers) {
                            worker.postMessage({
                                type: `__terminate`
                            });
                        }
                    }
                }

            } else {
                throw Error(`Unknown message type '${value?.type}'`);
            }

        } catch (e) {
            process.send({
                type: `__error`,
                returnValue: {
                    message: e.toString()
                }
            });
            process.exit(2);
        }
    });

} catch (e) {
    process.send({
        type: `__error`,
        returnValue: {
            message: e.toString()
        }
    });
    process.exit(1);
}


class MigrationProcess {
    /**
     *
     * @param type {string}
     * @param index {string}
     * @param inputIndices {Array<string>}
     * @param outputIndices {Array<string>}
     * @param migrations {Array<{filepath: string, classIndex: number, version: string, versionNumbers: string}>}
     * @param backgroundToProcessData {Array<Array<*>>}
     */
    constructor(type, index = void 0, inputIndices = [], outputIndices = [], migrations = [], backgroundToProcessData = []) {
        /**
         * Node type, corresponds to the migration types
         * @type {string}
         */
        this._type = type;

        /**
         * Index of this node for BULK-like types
         * @type {string}
         */
        this._index = index;

        /**
         * List of all input indices, includes main index
         * @type {Array<string>}
         */
        this._inputIndices = inputIndices;

        /**
         * List of all output indices, includes main index
         * @type {Array<string>}
         */
        this._outputIndices = outputIndices;

        /**
         * List of all migrations in this node
         * @type {Array<Migration>}
         */
        this._migrations = [];
        for (let migrationIndex = 0; migrationIndex < migrations.length; migrationIndex++) {
            const migrationData = migrations[migrationIndex];
            const Constructor = _.castArray(require(migrationData.filepath))[migrationData.classIndex];
            const myMigration = new Constructor(migrationData.version, migrationData.versionNumbers, void 0);
            myMigration.__filepath = migrationData.filepath;
            myMigration.__classIndex = migrationData.classIndex;
            myMigration.utils.workerIndex = -1;
            myMigration.utils.workerCount = 0;

            for (let dependencyMigrationIndex = 0; dependencyMigrationIndex < myMigration._info.dependencyMigrations.length; dependencyMigrationIndex++) {
                myMigration.DEPENDENCIES[myMigration._info.dependencyMigrations[dependencyMigrationIndex]] = backgroundToProcessData[migrationIndex][dependencyMigrationIndex];
            }

            this._migrations.push(myMigration);
        }

        /**
         * Worker threads
         * @type {Array<Worker>}
         */
        this._workers = [];

        /** @type {number} */
        this._initialIndexDocuments = void 0;

        /** @type {Array<number>} */
        this._workersProcessedDocuments = void 0;

        /** @type {number} */
        this._reporter = void 0;
    }

    /**
     * Main function
     * @param metadata {{tenant: string, esHost: string, indicesInfo: {INDICES: Record<string, {name: string, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}, migrationPath: string}}
     * @param backgroundToProcessData {Array<Array<*>>}
     * @returns {Promise<Array<*>>}
     */
    async run(metadata, backgroundToProcessData) {
        logger.info(`Starting node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}].`);
        const start = process.hrtime();

        try {
            const outputOnlyIndices = this._restrictOutputOdmRefreshes();

            await elastic.openIndices(this);

            switch (this._type) {
                case SYNCHRONISATION_TYPES.STOP: {
                    this._migrations[0].note(this._migrations[0]._info.message);
                    break;
                }

                case SYNCHRONISATION_TYPES.SERIAL:
                case SYNCHRONISATION_TYPES.INDICES: {
                    //Prepare worker threads
                    await this._createWorkers(metadata, backgroundToProcessData, outputOnlyIndices);

                    this._reporter = setInterval(this._periodicReport.bind(this), REPORT_SECONDS * 1000);

                    //These are easy
                    await this._migrations[0]._runInitialize();

                    clearInterval(this._reporter);

                    await this._finalizeWorkers();
                    break;
                }

                case SYNCHRONISATION_TYPES.BULK: {
                    const MyOdm = elastic.restrictedOdms[this._index];

                    //Create new indices if necessary
                    const outputData = await this._prepareOutputIndex(MyOdm);

                    //Prepare worker threads
                    await this._createWorkers(metadata, backgroundToProcessData, outputOnlyIndices, outputData.outputIndex);

                    this._reporter = setInterval(this._periodicReport.bind(this), REPORT_SECONDS * 1000);

                    //Call initialize
                    await this._prepareMigrations(MyOdm);

                    //Migrate data using worker threads
                    const { virtualDocuments, toDelete } = await this._migrateData(MyOdm);

                    //Call finalize, then migrate to-be-created documents using single worker thread
                    await this._migrateVirtualDocuments(MyOdm, virtualDocuments, toDelete);

                    await this._finalizeWorkers();

                    clearInterval(this._reporter);

                    //Delete old indices OR delete records
                    await this._cleanIndices(outputData, virtualDocuments, toDelete);

                    break;
                }

                case SYNCHRONISATION_TYPES.DOCUMENTS: {
                    const MyOdm = elastic.restrictedOdms[this._index];

                    //Create new indices if necessary and create input-output index mapping
                    const outputData = await this._prepareOutputIndex(MyOdm);

                    //Prepare worker threads
                    await this._createWorkers(metadata, backgroundToProcessData, outputOnlyIndices, outputData.outputIndex);

                    this._reporter = setInterval(this._periodicReport.bind(this), REPORT_SECONDS * 1000);

                    //Call migrate
                    await this._prepareMigrations(MyOdm);

                    //Check which documents we have to download
                    let ids = [];
                    for (const migration of this._migrations) {
                        ids.push(...migration.__updatedDocuments.keys());
                    }
                    ids = _.uniq(ids);

                    //Download and migrate already existing documents using worker threads
                    const { virtualDocuments, toDelete } = await this._migrateData(MyOdm, ids);

                    //Migrate to-be-created documents using worker threads
                    await this._migrateVirtualDocuments(MyOdm, virtualDocuments, toDelete);

                    await this._finalizeWorkers();

                    clearInterval(this._reporter);

                    //Delete old indices OR delete records
                    await this._cleanIndices(outputData, virtualDocuments, toDelete);

                    break;
                }

                case SYNCHRONISATION_TYPES.PUT: {
                    const MyOdm = elastic.restrictedOdms[this._index];

                    let myMapping = void 0;
                    let mySettings = void 0;
                    for (const migration of this._migrations) {
                        const newMapping = await migration.putMapping();
                        if (!_.isEmpty(newMapping)) {
                            if (_.isEmpty(myMapping)) {
                                myMapping = {};
                            }

                            _.mergeWith(myMapping, newMapping, (a, b) => _.isArray(b) ? b : void 0);
                        }

                        const newSettings = await migration.putSettings();
                        if (!_.isEmpty(newSettings)) {
                            if (_.isEmpty(mySettings)) {
                                mySettings = {};
                            }

                            _correctSettings(newSettings);
                            _.mergeWith(mySettings, newSettings, (a, b) => _.isArray(b) ? b : void 0);
                        }
                    }

                    if (!_.isEmpty(myMapping)) {
                        await MyOdm.putMapping(myMapping);
                    }
                    if (!_.isEmpty(mySettings)) {
                        await MyOdm.putSettings(mySettings);
                    }

                    break;
                }

                case SYNCHRONISATION_TYPES.SCRIPT: {
                    const MyOdm = elastic.restrictedOdms[this._index];

                    //Create new indices if necessary and create input-output index mapping
                    const outputData = await this._prepareOutputIndex(MyOdm);

                    this._reporter = setInterval(this._periodicReport.bind(this), REPORT_SECONDS * 1000);

                    //Compile code
                    let codeCompiled = false;
                    let updateScript = `def root = ctx._source;`;
                    for (const migration of this._migrations) {
                        await migration._runInitialize(MyOdm.alias, false);
                        const migrationScript = migration.__scriptCache;

                        if (!_.isEmpty(migrationScript)) {
                            updateScript += migrationScript;
                            codeCompiled = true;
                        }
                    }

                    //Run task
                    let retryCounter = 0;
                    do {
                        let task;
                        const requiresReindex = outputData.isNewIndex;
                        if (requiresReindex) {
                            //We need to make reindex
                            const myUpdateScript = (codeCompiled) ? `${updateScript} ctx._version++;` : void 0;
                            task = await MyOdm.reindex(outputData.outputIndex, myUpdateScript, await MyOdm._getBulkSize(), false);

                        } else if (codeCompiled) {
                            //Just make updateByQuery
                            task = await MyOdm.updateByQuery({
                                query: {
                                    match_all: {}
                                },
                                script: {
                                    source: updateScript,
                                    lang: `painless`
                                }
                            }, await MyOdm._getBulkSize(), false);

                        } else {
                            break;
                        }

                        const isOk = await _waitForTask(task.task, retryCounter);
                        if (isOk) {
                            //Task completed
                            break;

                        } else {
                            //Error in task, rerun
                            retryCounter++;
                        }

                        // eslint-disable-next-line no-constant-condition
                    } while (true);

                    clearInterval(this._reporter);

                    //Delete old indices
                    await this._cleanIndices(outputData);

                    break;
                }
            }

            await elastic.closeIndices(this);

        } catch (e) {
            logger.fatal(`Error in node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}].`);
            logger.fatal(e);
            throw e;
        }

        const end = process.hrtime(start);
        if (isTerminated) {
            logger.info(`Node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}] settled.`);
        } else {
            logger.info(`Finished node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}] in ${utils.formatHRTime(end)} s.`);
        }

        return this._migrations.map((migration) => migration.__finalMigrationData);
    }

    /**
     * Creates and initializes worker threads
     * @param metadata {{tenant: string, esHost: string, indicesInfo: {INDICES: Record<string, {name: string, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}, migrationPath: string}}
     * @param backgroundToProcessData {Array<Array<*>>}
     * @param outputOnlyIndices {Array<string>}
     * @param outputIndex {string}
     * @returns {Promise<void>}
     */
    async _createWorkers(metadata, backgroundToProcessData, outputOnlyIndices, outputIndex = void 0) {
        const [physicalWorkerCount, concurrency] = this._getWorkerThreadCount();
        const workerCount = (physicalWorkerCount * concurrency);

        const allIndices = _.compact(_.uniq([...this._inputIndices, ...this._outputIndices]));
        for (const index of allIndices) {
            await optimizations.initialSampling(elastic.odms[index]);
        }

        if (this._index) {
            this._initialIndexDocuments = optimizations.getDocumentCounts()[elastic.odms[this._index].alias];
            this._workersProcessedDocuments = _.times(workerCount, _.constant(0));
        }

        for (let workerIndex = 0; workerIndex < physicalWorkerCount; workerIndex++) {
            const worker = new Worker(path.join(__dirname, `worker.js`), {
                workerData: {
                    tenant: metadata.tenant,
                    esHost: metadata.esHost,
                    indicesInfo: metadata.indicesInfo,
                    migrationPath: metadata.migrationPath,
                    physicalWorkerIndex: workerIndex,
                    concurrency: concurrency,
                    workerCount: workerCount
                }
            });
            utils.promisifyThreadCommunication(worker, async (message) => {
                try {
                    switch (message.type) {
                        case `__done`: {
                            worker._activePromises.get(message.promiseIndex).resolve(message.returnValue ?? {});
                            break;
                        }
                        case `__error`: {
                            try {
                                worker._activePromises.get(message.promiseIndex).reject(message.error);
                            } catch (err) {
                                throw Error(message.error);
                            }
                            break;
                        }
                        case `__userCall`: {
                            const responseData = await this._migrations[message.migrationIndex][message.functionName](message.data);
                            worker.postMessage({ type: `__done`, promiseIndex: message.promiseIndex, returnValue: responseData });
                            break;
                        }
                        case `__report`: {
                            const processedDocuments = message.returnValue;
                            if (!_.isNil(processedDocuments) && this._workersProcessedDocuments) {
                                this._workersProcessedDocuments[message.workerIndex] = processedDocuments;
                            }
                            break;
                        }
                        default: {
                            const errorMessage = `Unexpected worker message '${message}'`;
                            try {
                                worker._activePromises.get(message.promiseIndex).reject(errorMessage);
                            } catch (err) {
                                throw Error(errorMessage);
                            }
                        }
                    }
                } catch (err) {
                    process.send({
                        type: `__error`,
                        returnValue: {
                            message: err.toString()
                        }
                    });
                    process.exit(4);
                }
            });

            worker.on(`error`, (err) => {
                process.send({
                    type: `__error`,
                    returnValue: {
                        message: err.toString()
                    }
                });
                process.exit(3);
            });

            this._workers.push(worker);
        }

        const migrations = this._migrations.map((migration, migrationIndex) => {
            if (_hasWorkers(migration)) {
                migration.utils.workerCount = workerCount;
                migration.utils.postWorkers = async (func, data = void 0) => {
                    if (_.isNil(func) || !_.isFunction(func) || _.isNil(migration[func.name])) {
                        throw Error(`'${migration.version}': You have to specify the function to be called from workers context.`);
                    }

                    const threadResults = await Promise.all(
                        this._workers.map((worker) => worker._sendMessage({
                            type: `__userCall`,
                            migrationIndex: migrationIndex,
                            functionName: func.name,
                            data: data
                        }))
                    );

                    const finalResults = [];
                    threadResults.forEach((threadResult) => finalResults.push(...threadResult));
                    return finalResults;
                };
            }

            return {
                filepath: migration.__filepath,
                classIndex: migration.__classIndex,
                version: migration.version,
                versionNumbers: migration._versionNumbers,
            };
        });

        await Promise.all(
            this._workers.map((worker) => worker._sendMessage({
                type: `__initialize`,
                data: {
                    migrations,
                    index: this._index,
                    outputIndex: outputIndex,
                    backgroundToProcessData: backgroundToProcessData,
                    outputOnlyIndices: outputOnlyIndices,
                    averageDocumentSizes: optimizations.getAverageDocumentSizes(),
                    documentCounts: optimizations.getDocumentCounts(),
                    restrictions: optimizations.getRestrictions()
                }
            }))
        );
    }

    /**
     * Calls initialize function for all of the migrations
     * Also sends the update data to the workers
     * @param MyOdm
     * @returns {Promise<void>}
     */
    async _prepareMigrations(MyOdm) {
        for (const migration of this._migrations) {
            await migration._runInitialize(MyOdm.alias, true);
        }

        //Check if we have to update some documents
        const migrationsData = [];
        for (const migration of this._migrations) {
            migrationsData.push({
                updatedDocuments: migration.__updatedDocuments,
                scriptCache: migration.__scriptCache
            });
        }

        await Promise.all(
            this._workers.map((myWorker) => myWorker._sendMessage({
                type: `__setUpdateFunctions`,
                data: migrationsData
            }))
        );
    }

    /**
     * Migrates all existing data using worker threads
     * @param MyOdm
     * @param ids {Array<string>}
     * @returns {Promise<{ toDelete: Map<string, Set<number>>, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}> }>}
     */
    async _migrateData(MyOdm, ids = void 0) {
        if (ids && ids.length === 0) {
            return {
                virtualDocuments: {},
                toDelete: new Map()
            };

        } else {
            const pitID = await MyOdm.openPIT();
            const threadResults = await Promise.all(
                this._workers.map((myWorker) => myWorker._sendMessage({
                    type: `__migrateExistingDocuments`,
                    data: {
                        ids: ids,
                        pitID: pitID
                    }
                }))
            );
            await MyOdm.closePIT(pitID);
            const finalResults = [];
            threadResults.forEach((threadResult) => finalResults.push(...threadResult));

            //Update virtualDocuments and toDelete
            const virtualDocuments = _mixCreatedDocuments(finalResults);
            const toDelete = _mixDeletedDocuments(finalResults, virtualDocuments);

            return { virtualDocuments, toDelete };
        }
    }

    /**
     * Finalizes migrations one-by-one and migrates virtual (not yet existing) documents
     * @param MyOdm
     * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
     * @param toDelete {Map<string, Set<number>>}
     * @returns {Promise<void>}
     */
    async _migrateVirtualDocuments(MyOdm, virtualDocuments, toDelete) {
        //Migrate virtual (not yet saved) documents
        for (let migrationIndex = 0; migrationIndex < this._migrations.length; migrationIndex++) {
            //Go through all migrations one-by-one
            const migration = this._migrations[migrationIndex];

            //Finalize  migration
            await migration._runFinalize();

            //Then get data from the worker threads
            const threadResults = await Promise.all(
                this._workers.map((myWorker) => myWorker._sendMessage({
                    type: `__finalizeMigration`,
                    data: {
                        migrationIndex: migrationIndex
                    }
                }))
            );
            const finalResults = [];
            threadResults.forEach((threadResult) => finalResults.push(...threadResult));
            for (const finalResult of finalResults) {
                finalResult.usedUpdateIDs.forEach((updatedID) => {
                    const usedUpdate = migration.__updatedDocuments.get(updatedID);
                    usedUpdate.isUsed = true;
                });

                migration.__createdDocuments.push(...finalResult.newDocuments);
            }

            const notUsedUpdatesIDs = [];
            for (const [id, updateData] of migration.__updatedDocuments) {
                if (!updateData.isUsed && !!updateData.fallbackSource) {
                    //When fallback is available, create request to create a new document
                    migration.createDocument(updateData.fallbackSource, id);

                } else if (!updateData.isUsed && !updateData.isForced && !updateData.fallbackSource) {
                    //Check not used update functions
                    notUsedUpdatesIDs.push(id);
                }
            }
            if (notUsedUpdatesIDs.length > 0) {
                logger.warn(`'${migration.version}': Following document IDs were not presented to be updated by specified update function: ${ notUsedUpdatesIDs.join(`,`)}`);
            }

            //Now prepare all the document objects which accumulated in the migration
            for (const createDocument of migration.__createdDocuments) {
                utils.createDocument(migrationIndex, createDocument.id, createDocument.source, virtualDocuments);
            }

            //Check if new documents should be created and migrate them
            if (virtualDocuments[`${migrationIndex}`] && (virtualDocuments[`${migrationIndex}`].knownIDs.size > 0 || virtualDocuments[`${migrationIndex}`].unknownIDs.size > 0)) {
                const result = await this._workers[0]._sendMessage({
                    type: `__migrateVirtualDocuments`,
                    data: {
                        migrationIndex: migrationIndex,
                        virtualDocuments: virtualDocuments,
                        toDelete: toDelete
                    }
                });
                virtualDocuments = result.virtualDocuments;
                toDelete = result.toDelete;
            }
        }
    }

    /**
     * Finalizes workers
     * @returns {Promise<void>}
     */
    async _finalizeWorkers() {
        const threadResults = await Promise.all(
            this._workers.map((myWorker) => myWorker._sendMessage({
                type: `__finalizeWorker`
            }))
        );

        if (this._type === SYNCHRONISATION_TYPES.BULK) {
            const threadDocumentChangesList = threadResults.map((threadResult) => threadResult.localDocumentChanges);
            const aliasData = threadDocumentChangesList.reduce((sum, threadData) => {
                for (const [alias, values] of Object.entries(threadData)) {
                    if (!sum[alias]) {
                        sum[alias] = {
                            size: 0,
                            count: 0
                        };
                    }

                    sum[alias].size += values.size;
                    sum[alias].count += values.count;
                }
                return sum;
            }, {});
            for (const [alias, values] of Object.entries(aliasData)) {
                const aliasName = elastic.aliasToName[alias];
                if (values.count > 0 && aliasName) {
                    const MyOdm = elastic.odms[aliasName];

                    const newSize = Math.max(Math.floor(values.size / values.count), 1);
                    optimizations.recalculateAverageDocumentSizes(alias, newSize, await MyOdm.count(), values.count);
                }
            }
        }

        //Merge notes
        for (const threadResult of threadResults) {
            utils.mergeNotes(threadResult.messages);
        }
    }

    /**
     * Disables ES refresh on output-only ODMs
     * @returns {Array<string>}
     */
    _restrictOutputOdmRefreshes() {
        //Do not do refreshes on output only indices
        const outputOnlyIndices = _.difference(this._outputIndices, this._inputIndices);
        if (outputOnlyIndices.length <= 0) {
            return [];
        }

        for (const migration of this._migrations) {
            const restrictedIndices = _.intersection(outputOnlyIndices, migration._info.outputIndices);
            restrictedIndices.forEach((affectedIndex) => {
                if (migration.ODM[affectedIndex]) {
                    migration.ODM[affectedIndex] = elastic.restrictedOdms[affectedIndex];
                }
            });
        }
        return outputOnlyIndices;
    }

    /**
     * Creates index mapping object
     * When reindex should be performed, it creates a new one
     * @param MyOdm
     * @returns {Promise<{Odm, outputIndex: string, isNewIndex: boolean}>}
     */
    async _prepareOutputIndex(MyOdm) {
        let outputIndex = MyOdm.alias;
        let requiresReindex = ((this._type === SYNCHRONISATION_TYPES.BULK && ALWAYS_NEW_INDICES.bulk) ||
            (this._type === SYNCHRONISATION_TYPES.SCRIPT && ALWAYS_NEW_INDICES.script));
        let myMapping = void 0;
        let originalSettings = void 0;
        let mySettings = void 0;

        //Check if type requires reindex, create new mapping if so
        for (const migration of this._migrations) {
            if (_.isEmpty(myMapping) && (requiresReindex || migration._info.reindex || (migration._info.type === SYNCHRONISATION_TYPES.PUT))) {
                const mapping = await MyOdm.getMapping();
                myMapping = Object.values(mapping)[0].mappings;

                const settings = await MyOdm.getSettings();
                mySettings = Object.values(settings)[0].settings;
                if (!_.isNil(mySettings?.index?.soft_deletes)) {
                    //When index comes from ES7 snapshot, there will be "soft_deletes" property
                    delete mySettings.index.soft_deletes;
                }
                originalSettings = _.cloneDeep(mySettings);
            }

            if (migration._info.reindex) {
                await migration.reindex(myMapping, mySettings);
                _correctSettings(mySettings);
                requiresReindex = true;

            } else if (migration._info.type === SYNCHRONISATION_TYPES.PUT) {
                const newMapping = await migration.putMapping();
                if (!_.isEmpty(newMapping)) {
                    _.mergeWith(myMapping, newMapping, (a, b) => _.isArray(b) ? b : void 0);
                    requiresReindex = true;
                }

                const newSettings = await migration.putSettings();
                if (!_.isEmpty(newSettings)) {
                    _correctSettings(newSettings);
                    _.mergeWith(mySettings, newSettings, (a, b) => _.isArray(b) ? b : void 0);
                    requiresReindex = true;
                }
            }
        }

        if (requiresReindex) {
            const updatedSettings = _difference(mySettings, originalSettings);
            _.mergeWith(mySettings, updatedSettings, (a, b) => _.isArray(b) ? b : void 0);
            outputIndex = await _safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping);
        }

        return {
            Odm: MyOdm,
            outputIndex: outputIndex,
            isNewIndex: requiresReindex
        };
    }

    /**
     * Cleans indices after the migration process. Deletes old index in case of reindexing or deletes marked documents otherwise
     * @param outputData {{Odm, outputIndex: string, isNewIndex: boolean}}
     * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
     * @param toDelete {Map<string, Set<number>>}
     * @returns {Promise<void>}
     */
    async _cleanIndices(outputData, virtualDocuments = {}, toDelete = new Map()) {
        if (outputData.isNewIndex) {
            //For cloned indices, just delete the original index
            await outputData.Odm.deleteIndex();
            await outputData.Odm.aliasIndex(outputData.outputIndex);

        } else {
            const documentIDsToCheck = [...toDelete.keys()];

            //Check if marked documents are mentioned in the list of virtual documents
            const documentIDsToDelete = [];
            for (let i = documentIDsToCheck.length - 1; i >= 0; i--) {
                //Check documents from latest (so we can delete them from the list easily)
                const id = documentIDsToCheck[i];

                for (let j = this._migrations.length - 1; j >= 0; j--) {
                    //Go from the latest migration

                    const checkedIteration = virtualDocuments[`${j}`];
                    if (!checkedIteration || checkedIteration.knownIDs.size === 0) {
                        continue;
                    }

                    const existingVirtualDocument = checkedIteration.knownIDs.get(id);
                    if (existingVirtualDocument) {
                        //Document mentioned
                        if (existingVirtualDocument.isDeleted) {
                            //And marked to be deleted -> delete
                            documentIDsToDelete.push(id);
                        }
                        //Otherwise not marked -> do not delete
                        documentIDsToCheck.splice(i, 1);
                        break;
                    }
                }
            }
            //Delete not mentioned documents as well
            documentIDsToDelete.push(...documentIDsToCheck);

            if (documentIDsToDelete.length > 0) {
                //Finally delete the documents
                const deleteBulk = documentIDsToDelete.map((id) => {
                    return {
                        delete: {
                            _index: outputData.Odm.alias,
                            _id: id
                        }
                    };
                });
                await elastic.sendBulk(deleteBulk);
            }
        }
    }

    /**
     * Returns number of workers in form of an array. First number represents the worker threads to be created, the second is number of "virtual" workers per the thread.
     * User available number of workers is multiplication of these numbers.
     * @returns {[number, number]}
     */
    _getWorkerThreadCount() {
        if (this._type === SYNCHRONISATION_TYPES.DOCUMENTS) {
            return [1, 1];

        } else if (this._migrations.some((myMigration) => myMigration._info.singleWorker)) {
            return [1, 1];

        } else {
            const cpus = os.cpus().length;
            const ramMiB = os.totalmem() / 1024 / 1024;

            const ramLimitation = ramMiB / (PARALLELIZATION.targetProcesses * PARALLELIZATION.workerRAMLimitMiB);
            const maximumWorkers = Math.max(cpus, ramLimitation);
            const minimumWorkers = 1;

            const targetWorkers = Math.max(minimumWorkers, Math.min(maximumWorkers, Math.round(cpus * PARALLELIZATION.targetWorkersToCPUs)));

            if (!PARALLELIZATION.allowWorkersConcurrency) {
                return [targetWorkers, 1];

            } else {
                if ((targetWorkers % 2) === 0) {
                    return [targetWorkers / 2, 2];
                } else {
                    if (maximumWorkers >= (targetWorkers + 1)) {
                        return [(targetWorkers + 1) / 2, 2];
                    } else if (minimumWorkers <= (targetWorkers - 1)) {
                        return [(targetWorkers - 1) / 2, 2];
                    } else {
                        return [targetWorkers, 1];
                    }
                }
            }
        }
    }

    /**
     * Logs periodic process report
     */
    _periodicReport() {
        if (isTerminated) {
            logger.info(`Waiting for node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}] to settle...`);

        } else if (this._type === SYNCHRONISATION_TYPES.BULK) {
            const percents = ((_.sum(this._workersProcessedDocuments) / (this._initialIndexDocuments + 1)) * 100).toFixed(2);
            logger.info(`Processing node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}]: ~${percents} %`);

        } else {
            logger.info(`Processing node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}]...`);
        }
    }
}

/**
 * Tries to create new ES index, automatically removes not allowed settings parameters
 * @param MyOdm
 * @param mySettings {{}} Original settings object
 * @param updatedSettings {{}} User changes on settings
 * @param myMapping {{}} Mapping object
 * @param iteration {number} How many times we tried to strip the invalid parameters
 * @param indexRetry {number} How many times we have to delete incorrect index (see note in code)
 * @returns {Promise<string>} Output index in case this was successful
 */
async function _safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, iteration = 0, indexRetry = 0) {
    const knownForbiddenItems = [`index.creation_date`, `index.resize`, `index.provided_name`, `index.routing.allocation.initial_recovery`, `index.uuid`, `index.version`];
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
        if (esError.meta?.body?.error?.root_cause?.length === 1 &&
            esError.meta.body.error.root_cause[0].type === `resource_already_exists_exception` &&
            !esError.meta?.body?.error?.suppressed) {
            //There is a bug in ES if we send way too much (failing) requests to create an index.
            //Once the request finally proceeds, it is stated that the index already exists.
            //But that's not possible, as we use random uuid-v4 which is re-generated for each request.
            //What seems to work is to delete this new index and start again.

            if (indexRetry < RETRIES.maxRetries) {
                await optimizations.sleep(indexRetry);
                indexRetry++;

                await elastic.callEs(`indices.delete`, {
                    index: esError.meta.body.error.root_cause[0].index
                });
                return _safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, 0, indexRetry);
            } else {
                throw Error(`Error when creating new index, ES too many times responded with "resource_already_exists_exception".`);
            }
        }

        if (iteration >= 2) {
            throw Error(`Error when creating new index, exceeded max number of retries - ${esError.meta?.body?.error?.reason}.`);
        }

        const rootCause = esError.meta?.body?.error?.root_cause;
        if (_.isEmpty(rootCause)) {
            throw Error(`Error when creating new index - ${esError.meta?.body?.error?.reason}.`);
        }
        let errors = [...rootCause];

        const suppressed = esError.meta?.body?.error?.suppressed;
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

        return _safeCreateIndex(MyOdm, mySettings, updatedSettings, myMapping, iteration + 1, indexRetry);
    }
}

/**
 * Waits for single task to finish
 * @param taskId {string}
 * @param retryCounter {number}
 * @returns {Promise<boolean>}
 */
async function _waitForTask(taskId, retryCounter = 0) {
    //Wait less time for the first time
    await new Promise((resolve) => {
        setTimeout(resolve, CHECK_INTERVALS.task / 2);
    });

    do {
        const taskStatus = await elastic.callEs(`tasks.get`, {
            task_id: taskId
        });

        if (!taskStatus.completed) {
            //Not yet completed
            await new Promise((resolve) => {
                setTimeout(resolve, CHECK_INTERVALS.task);
            });

        } else {
            if (!_.isEmpty(taskStatus.error)) {
                //Error
                if ((taskStatus.error.type === `es_rejected_execution_exception`) && (retryCounter < RETRIES.maxRetries)) {
                    //429
                    logger.warn(`ES returns error 429 for task - Too many requests, will try again.`);
                    await optimizations.sleep(retryCounter);
                    return false;

                } else {
                    //Other error
                    throw new elastic._esErrors.ResponseError({
                        body: {
                            error: taskStatus.error
                        }
                    });
                }
            } else if (!_.isEmpty(taskStatus.response.failures)) {
                //Failures
                throw new elastic._esErrors.ResponseError({
                    body: taskStatus.response
                });

            } else {
                //Success
                return true;
            }
        }

        // eslint-disable-next-line no-constant-condition
    } while (true);
}

/**
 * If enabled in config, this moves all object properties into nested "index" object
 * @param settings {{}}
 */
function _correctSettings(settings) {
    if (CORRECT_SETTINGS) {
        Object.keys(settings).forEach((key) => {
            if (key !== `index`) {
                if (_.isNil(settings[`index`])) {
                    settings[`index`] = {};
                }

                settings[`index`][key] = settings[key];
                delete settings[key];
            }
        });
    }
}

/**
 * Finds and returns alters in object
 * @param newObj {{}}
 * @param origObj {{}}
 * @returns {{}}
 */
function _difference(newObj, origObj) {
    function changes(newObj, origObj) {
        return _.transform(newObj, function (result, value, key) {
            if (!_.isEqual(value, origObj[key])) {
                if (_.isArray(origObj[key])) {
                    result[key] = value;
                } else {
                    result[key] = (_.isObject(value) && _.isObject(origObj[key])) ? changes(value, origObj[key]) : value;
                }
            }
        });
    }
    return changes(newObj, origObj);
}

/**
 * Mixes data about to-be-created documents from multiple workers into a single structure
 * @param threadResults {Array<{toDelete: Map<string, Set<number>>, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}>}
 * @returns {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
 */
function _mixCreatedDocuments(threadResults) {
    if (_.isEmpty(threadResults)) {
        return {};
    }

    //virtualDocuments are cloned from worker threads, so we can mutate them freely
    const virtualDocumentsList = threadResults.map((threadResult) => threadResult.virtualDocuments);
    const virtualDocuments = virtualDocumentsList[0];
    for (let i = 1; i < virtualDocumentsList.length; i++) {
        const newVirtualDocuments = virtualDocumentsList[i];

        const newMigrationKeys = Object.keys(newVirtualDocuments);
        for (const newMigrationKey of newMigrationKeys) {
            let iterationCache = virtualDocuments[newMigrationKey];
            if (!iterationCache) {
                iterationCache = { knownIDs: new Map(), unknownIDs: new Set() };
                virtualDocuments[newMigrationKey] = iterationCache;
            }

            for (const newDocumentData of newVirtualDocuments[newMigrationKey].unknownIDs) {
                iterationCache.unknownIDs.add(newDocumentData);
            }

            for (const [id, newDocumentData] of newVirtualDocuments[newMigrationKey].knownIDs) {
                if (iterationCache.knownIDs.has(id)) {
                    throw Error(`Creation of document with ID '${id}' has been already declared in the same migration.`);
                }

                iterationCache.knownIDs.set(id, newDocumentData);
            }
        }
    }

    return virtualDocuments;
}

/**
 * Mixes data about to-be-deleted documents from multiple workers into a single structure
 * @param threadResults {Array<{toDelete: Map<string, Set<number>>, virtualDocuments: Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}>}
 * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>}
 * @returns {Map<string, Set<number>>}
 */
function _mixDeletedDocuments(threadResults, virtualDocuments) {
    if (_.isEmpty(threadResults)) {
        return new Map();
    }

    //toDelete are cloned from worker threads, so we can mutate them freely
    const toDeleteList = threadResults.map((threadResult) => threadResult.toDelete);
    const toDelete = toDeleteList[0];
    for (let i = 1; i < toDeleteList.length; i++) {
        for (const [id, iSet] of toDeleteList[i]) {
            for (const i of iSet) {
                utils.deleteDocument(i, id, virtualDocuments, toDelete);
            }
        }
    }

    return toDelete;
}

/**
 * Checks if the migration has access to the workers
 * @param migration {Migration}
 * @returns {boolean}
 */
function _hasWorkers(migration) {
    return ((migration._info.type === SYNCHRONISATION_TYPES.SERIAL) || (migration._info.type === SYNCHRONISATION_TYPES.INDICES) || (migration._info.type === SYNCHRONISATION_TYPES.BULK) || (migration._info.type === SYNCHRONISATION_TYPES.DOCUMENTS));
}
