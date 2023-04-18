'use strict';

const _ = require(`lodash`);
const elastic = require(`./elastic`);
const logger = require(`./logger`);
const optimizations = require(`./optimizations`);
const nconf = require(`../config/config`);

const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;

const SCROLL_TIMEOUT = nconf.get(`es:scrollTimeout`);
const CORRECT_SETTINGS = nconf.get(`es:correctSettings`);
const CHECK_INTERVALS = nconf.get(`es:checkIntervals`);
const STRICT = nconf.get(`options:strict`);
const ALWAYS_NEW_INDICES = nconf.get(`options:optimizations:alwaysNewIndices`);
const RETRIES = nconf.get(`es:retries`);

class Node {
    constructor(migration) {
        /**
         * Node type, corresponds to the migration types
         * @type {string}
         * @private
         */
        this._type = void 0;
        /**
         * List of all migrations in this node
         * @type {Array<Migration>}
         * @private
         */
        this._migrations = [];

        /**
         * Index of this node for BULK-like types; not typed
         * @type {string}
         * @private
         */
        this._index =  void 0;

        /**
         * List of all input indices, includes main index; not typed
         * @type {Array<string>}
         * @private
         */
        this._inputIndices = [];
        /**
         * List of all output indices, includes main index; not typed
         * @type {Array<string>}
         * @private
         */
        this._outputIndices = [];
        /**
         * List of all dependency indices; not typed
         * @type {Array<string>}
         * @private
         */
        this._dependencyIndices = [];

        /**
         * List of all input points === nodes that must be completed before this node can be run
         * @type {Array<Node>}
         * @private
         */
        this._inputPoints = [];
        /**
         * List of all output points === nodes that cannot run until this node is completed
         * @type {Array<Node>}
         * @private
         */
        this._outputPoints = [];

        /**
         * Is node finished?
         * @type {boolean}
         * @private
         */
        this._finished = false;
        /**
         * When node is running, this is the running promise
         * @type {Promise<Node>}
         * @private
         */
        this._promise = void 0;

        if (migration) {
            this.addMigration(migration);
        }
    }

    /**
     * Adds migration into this node
     * @param migration {Migration}
     */
    addMigration(migration) {
        if (!this._type) {
            this._type = migration._info.type;
        } else if (this._type === SYNCHRONISATION_TYPES.STOP || this._type === SYNCHRONISATION_TYPES.SERIAL || this._type === SYNCHRONISATION_TYPES.INDICES) {
            throw Error(`'${migration.version}': this type cannot be merged.`);
        }

        //Fallback from other types to SCRIPT or BULK
        if (this._type !== migration._info.type) {
            if ((this._type === SYNCHRONISATION_TYPES.PUT && migration._info.type === SYNCHRONISATION_TYPES.SCRIPT) ||
                (this._type === SYNCHRONISATION_TYPES.SCRIPT && migration._info.type === SYNCHRONISATION_TYPES.PUT)) {
                //Only PUT and SCRIPT are presented so far -> SCRIPT with reindex can make it
                this._type = SYNCHRONISATION_TYPES.SCRIPT;
            } else {
                //Other types -> BULK
                this._type = SYNCHRONISATION_TYPES.BULK;
            }
        }

        this._migrations.push(migration);
        if (migration._info.isProcessed) {
            throw Error(`Migration '${migration.version}' has been already processed.`);
        } else {
            migration._info.isProcessed = true;
        }

        const migrationInfo = migration._info;
        if (migrationInfo.index) {
            if (!_.isEmpty(this._index) && this._index !== migrationInfo.index) {
                throw Error(`Merging wrong main indices: ${this._index} and ${migrationInfo.index} from migration '${migration.version}'.`);
            } else {
                this._index = migrationInfo.index;
            }

            this._inputIndices.push(migrationInfo.index);
            this._outputIndices.push(migrationInfo.index);
        }
        if (migrationInfo.inputIndices) {
            this._inputIndices.push(...migrationInfo.inputIndices);
        }
        if (migrationInfo.outputIndices) {
            this._outputIndices.push(...migrationInfo.outputIndices);
        }
        if (migrationInfo.dependencyIndices) {
            this._dependencyIndices.push(...migrationInfo.dependencyIndices);
        }

        this._inputIndices = _.uniq(this._inputIndices);
        this._outputIndices = _.uniq(this._outputIndices);
        this._dependencyIndices = _.uniq(this._dependencyIndices);
    }

    /**
     * Connects this node to another one
     * @param node {Node}
     */
    connectTo(node) {
        if (!this._outputPoints.includes(node)) {
            this._outputPoints.push(node);
            node._inputPoints.push(this);
        }
    }

    /**
     * Checks if this node can run
     * @returns {boolean}
     */
    canRun() {
        if (this._finished) {
            return false;
        } else {
            return this._inputPoints.every((inputPoint) => inputPoint._finished);
        }
    }

    /**
     * Runs this node and adds its promise to an array
     * @param runningNodes {Array<Promise<Node>>}
     */
    run(runningNodes) {
        this._promise = this._run();
        runningNodes.push(this._promise);
    }

    /**
     * Finishes node running, must be called once the run promise is finished
     * Removes the promise from an array and returns new nodes that may newly run (if any)
     * @param runningNodes {Array<Promise<Node>>}
     * @returns {Array<Node>}
     */
    finish(runningNodes) {
        this._finished = true;
        const migrationArrayIndex = runningNodes.indexOf(this._promise);
        runningNodes.splice(migrationArrayIndex, 1);
        delete this._promise;

        return this._outputPoints.filter((outputPoint) => outputPoint.canRun());
    }

    /**
     * Run function of the node
     * @returns {Promise<Node>}
     * @private
     */
    async _run() {
        if (_.isNil(this._type)) {
            //Starting (dummy) node
            return this;
        }

        logger.info(`Starting node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}].`);
        const start = process.hrtime();

        try {
            this._restrictOutputOdmRefreshes();

            await elastic.openIndices(this);

            switch (this._type) {
                case SYNCHRONISATION_TYPES.STOP: {
                    break;
                }

                case SYNCHRONISATION_TYPES.SERIAL:
                case SYNCHRONISATION_TYPES.INDICES: {
                    //These are easy
                    await this._migrations[0].migrate();
                    break;
                }

                case SYNCHRONISATION_TYPES.BULK: {
                    const MainOdm = elastic.getRestrictedOdm(this._index, this._migrations[0]._versionNumbers);
                    const usedTypedOdms = await this.getOdmTypes(MainOdm);

                    //New documents to be created
                    const virtualDocuments = {};
                    //Documents to be deleted at the end
                    const toDelete = {};

                    //Create new indices if necessary and create input-output index mapping
                    const indexMapping = await this._createIndexMapping(usedTypedOdms);

                    //Call beforeAll
                    await this._prepareMigrations(MainOdm, indexMapping);

                    for (const UsedTypedOdm of usedTypedOdms) {
                        //migrate already existing documents, skip not used aliases
                        const body = {
                            query: {
                                match_all: {}
                            }
                        };
                        await this._migrateExistingDocuments(body, UsedTypedOdm, indexMapping, virtualDocuments, toDelete);
                    }

                    //Migrate to-be-created documents + afterAll
                    await this._migrateVirtualDocuments(MainOdm, indexMapping, virtualDocuments, toDelete);

                    //Delete old indices OR delete records
                    await this._cleanIndices(indexMapping, virtualDocuments, toDelete);

                    break;
                }

                case SYNCHRONISATION_TYPES.DOCUMENTS: {
                    //Very similar to BULK type
                    const MainOdm = elastic.getRestrictedOdm(this._index, this._migrations[0]._versionNumbers);
                    const usedTypedOdms = await this.getOdmTypes(MainOdm);

                    //New documents to be created
                    const virtualDocuments = {};
                    //Documents to be deleted at the end
                    const toDelete = {};

                    //Create new indices if necessary and create input-output index mapping
                    const indexMapping = await this._createIndexMapping(usedTypedOdms);

                    //Call migrate
                    await this._prepareMigrations(MainOdm, indexMapping);

                    //Check which documents we have to download
                    const toDownload = {};
                    for (const migration of this._migrations) {
                        if (_.isArray(migration.__updatedDocuments)) {
                            for (const update of migration.__updatedDocuments) {
                                const alias = update.alias;
                                if (!toDownload[alias]) {
                                    toDownload[alias] = [];
                                }

                                toDownload[alias].push(update.id);
                            }
                        }
                    }

                    for (const UsedTypedOdm of usedTypedOdms) {
                        //Download and migrate already existing documents
                        const ids = toDownload[UsedTypedOdm._alias];
                        if (_.isEmpty(ids)) {
                            continue;
                        }

                        const body = {
                            query: {
                                ids: {
                                    values: _.uniq(ids)
                                }
                            }
                        };
                        await this._migrateExistingDocuments(body, UsedTypedOdm, indexMapping, virtualDocuments, toDelete);

                    }

                    //Migrate to-be-created documents + afterAll
                    await this._migrateVirtualDocuments(MainOdm, indexMapping, virtualDocuments, toDelete);

                    //Delete old indices OR delete records
                    await this._cleanIndices(indexMapping, virtualDocuments, toDelete);

                    break;
                }

                case SYNCHRONISATION_TYPES.PUT: {
                    const MainOdm = elastic.getRestrictedOdm(this._index, this._migrations[0]._versionNumbers);
                    const usedTypedOdms = await this.getOdmTypes(MainOdm);

                    for (const UsedTypedOdm of usedTypedOdms) {
                        let myMapping = void 0;
                        let mySettings = void 0;
                        for (const migration of this._migrations) {
                            if (migration.isTypeOf(UsedTypedOdm._type)) {
                                const newMapping = await migration.putMapping((UsedTypedOdm._type) ? UsedTypedOdm._type : void 0);
                                if (!_.isEmpty(newMapping)) {
                                    if (_.isEmpty(myMapping)) {
                                        myMapping = {};
                                    }

                                    _.mergeWith(myMapping, newMapping, (a, b) => _.isArray(b) ? b : undefined);
                                }

                                const newSettings = await migration.putSettings((UsedTypedOdm._type) ? UsedTypedOdm._type : void 0);
                                if (!_.isEmpty(newSettings)) {
                                    if (_.isEmpty(mySettings)) {
                                        mySettings = {};
                                    }

                                    _correctSettings(newSettings);
                                    _.mergeWith(mySettings, newSettings, (a, b) => _.isArray(b) ? b : undefined);
                                }
                            }
                        }

                        if (!_.isEmpty(myMapping)) {
                            await UsedTypedOdm.putMapping(myMapping);
                        }
                        if (!_.isEmpty(mySettings)) {
                            await UsedTypedOdm.putSettings(mySettings);
                        }
                    }

                    break;
                }

                case SYNCHRONISATION_TYPES.SCRIPT: {
                    const MainOdm = elastic.getRestrictedOdm(this._index, this._migrations[0]._versionNumbers);
                    const usedTypedOdms = await this.getOdmTypes(MainOdm);

                    //Create new indices if necessary and create input-output index mapping
                    const indexMapping = await this._createIndexMapping(usedTypedOdms);

                    for (const UsedTypedOdm of usedTypedOdms) {
                        //Compile code
                        let codeCompiled = false;
                        let updateScript = `def root = ctx._source;`;
                        for (const migration of this._migrations) {
                            if (migration.isTypeOf(UsedTypedOdm._type)) {
                                const migrationScript = await migration.runMigrate(void 0, UsedTypedOdm._alias, _.isEmpty(UsedTypedOdm._type) ? void 0 : UsedTypedOdm._type);

                                if (!_.isEmpty(migrationScript)) {
                                    updateScript += migrationScript;
                                    codeCompiled = true;
                                }
                            }
                        }

                        //Run task
                        let retryCounter = 0;
                        do {
                            let task;
                            const requiresReindex = indexMapping[UsedTypedOdm._alias].cloned;
                            if (requiresReindex) {
                                //We need to make reindex
                                const myUpdateScript = (codeCompiled) ? `${updateScript} ctx._version++;` : void 0;
                                task = await UsedTypedOdm.reindex(indexMapping[UsedTypedOdm._alias].outputIndex, myUpdateScript, await optimizations.getBulkSize(UsedTypedOdm), false);

                            } else if (codeCompiled) {
                                //Just make updateByQuery
                                task = await UsedTypedOdm.updateByQuery({
                                    query: {
                                        match_all: {}
                                    },
                                    script: {
                                        source: updateScript,
                                        lang: `painless`
                                    }
                                }, await optimizations.getBulkSize(UsedTypedOdm), false);

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
                    }

                    //Delete old indices
                    await this._cleanIndices(indexMapping);

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
        logger.info(`Finished node with migrations [${this._migrations.map((migration) => migration.version).join(`, `)}] in ${end} s.`);
        return this;
    }

    /**
     * Disables ES refresh on output-only ODMs
     * @private
     */
    _restrictOutputOdmRefreshes() {
        //Do not do refreshes on output only indices
        const outputOnlyIndices = _.difference(this._outputIndices, this._inputIndices);
        if (outputOnlyIndices.length <= 0) {
            return;
        }

        for (const migration of this._migrations) {
            const affectedIndices = _.intersection(outputOnlyIndices, migration._info.outputIndices);

            affectedIndices.forEach((affectedIndex) => {
                //This index is output only -> use refresh restricted version

                const typedIndices = Object.keys(elastic._indicesMap).filter((key) => elastic._indicesMap[key] === affectedIndex);
                //In info object are only not typed indices -> get all possible types (including not typed)
                for (const typedIndex of typedIndices) {
                    if (migration.ODM[typedIndex]) {
                        migration.ODM[typedIndex] = elastic.getRestrictedOdm(typedIndex, this._migrations[0]._versionNumbers);
                    }
                }
            });
        }
    }

    /**
     * Returns all used ODM types, checks types specified in migrations exist
     * @param MainOdm {{}}
     * @returns {Promise<Array<{}>>}
     */
    async getOdmTypes(MainOdm) {
        let existingOdms = [MainOdm];
        if (!MainOdm.hasTypes()) {
            //We don't have to check anything, index doesn't have types
            return existingOdms;
        }

        //Find all existing types
        existingOdms = await MainOdm.getTypes();
        const usedOdms = [];

        //Check all migrations
        for (const migration of this._migrations) {
            //And ensure that it matches at least one of existing types
            let specifiedTypeExist = false;
            for (const ExistingOdm of existingOdms) {
                const myType = ExistingOdm._type;
                if (migration.isTypeOf(myType)) {
                    usedOdms.push(ExistingOdm);
                    specifiedTypeExist = true;
                }
            }

            if (!specifiedTypeExist) {
                throw Error(`'${migration.version}': For requested ${(migration._info.inclusiveIndexType) ? `inclusive` : `exclusive`} index type '${migration._info.indexType}' no index was found.`);
            }
        }

        return _.uniq(usedOdms);
    }

    /**
     * Creates index mapping object
     * When reindex should be performed, it creates new indices
     * @param usedTypedOdms {Array<{}>}
     * @returns {Promise<{}>}
     * @private
     */
    async _createIndexMapping(usedTypedOdms) {
        const indexMapping = {};

        for (const MyTypedOdm of usedTypedOdms) {
            let outputIndex = MyTypedOdm._alias;
            let requiresReindex = ((this._type === SYNCHRONISATION_TYPES.BULK && ALWAYS_NEW_INDICES.bulk) ||
                (this._type === SYNCHRONISATION_TYPES.SCRIPT && ALWAYS_NEW_INDICES.script));
            let myMapping = void 0;
            let originalSettings = void 0;
            let mySettings = void 0;

            //Check if type requires reindex, create new mapping if so
            for (const migration of this._migrations) {
                if (migration.isTypeOf(MyTypedOdm._type)) {
                    if (_.isEmpty(myMapping) && (requiresReindex || migration._info.reindex || (migration._info.type === SYNCHRONISATION_TYPES.PUT))) {
                        const mapping = await MyTypedOdm.getMapping();
                        myMapping = Object.values(mapping)[0].mappings;

                        const settings = await MyTypedOdm.getSettings();
                        mySettings = Object.values(settings)[0].settings;
                        if (!_.isNil(mySettings?.index?.soft_deletes)) {
                            //When index comes from ES7 snapshot, there will be "soft_deletes" property
                            delete mySettings.index.soft_deletes;
                        }
                        originalSettings = _.cloneDeep(mySettings);
                    }

                    if (migration._info.reindex) {
                        await migration.reindex(myMapping, mySettings, (MyTypedOdm._type) ? MyTypedOdm._type : void 0);
                        _correctSettings(mySettings);
                        requiresReindex = true;

                    } else if (migration._info.type === SYNCHRONISATION_TYPES.PUT) {
                        const newMapping = await migration.putMapping((MyTypedOdm._type) ? MyTypedOdm._type : void 0);
                        if (!_.isEmpty(newMapping)) {
                            _.mergeWith(myMapping, newMapping, (a, b) => _.isArray(b) ? b : undefined);
                            requiresReindex = true;
                        }

                        const newSettings = await migration.putSettings((MyTypedOdm._type) ? MyTypedOdm._type : void 0);
                        if (!_.isEmpty(newSettings)) {
                            _correctSettings(newSettings);
                            _.mergeWith(mySettings, newSettings, (a, b) => _.isArray(b) ? b : undefined);
                            requiresReindex = true;
                        }
                    }
                }
            }

            if (requiresReindex) {
                const updatedSettings = _difference(mySettings, originalSettings);
                _.mergeWith(mySettings, updatedSettings, (a, b) => _.isArray(b) ? b : undefined);
                outputIndex = await _safeCreateIndex(MyTypedOdm, mySettings, updatedSettings, myMapping);
            }

            //Fill in mapping structure
            indexMapping[MyTypedOdm._alias] = { resource: MyTypedOdm, outputIndex: outputIndex, cloned: requiresReindex };
        }

        return indexMapping;
    }

    /**
     * Calls initial function for all of the migrations
     * For BULK type this is the beforeAll function
     * For DOCUMENTS type this is the migrate function
     * Also prepares the data for updated documents
     * @param MainOdm
     * @param indexMapping {{}}
     * @returns {Promise<void>}
     * @private
     */
    async _prepareMigrations(MainOdm, indexMapping) {
        const availableTypes = MainOdm.hasTypes() ? Object.keys(indexMapping).map((inputIndex) => MainOdm._parseIndex(inputIndex).type) : [];

        for (const migration of this._migrations) {
            await migration.runBeforeAll();
        }

        //Check if we have to update some documents
        for (const migration of this._migrations) {
            if (_.isArray(migration.__updatedDocuments)) {
                //Go from the end so we can append new entries
                for (let i = migration.__updatedDocuments.length - 1; i >= 0; i--) {
                    const updatedDocument = migration.__updatedDocuments[i];

                    if (MainOdm.hasTypes()) {
                        if (_.isEmpty(updatedDocument.type)) {
                            //Multiple types and nothing specified

                            const usedTypes = [];
                            //Go through available types
                            for (let j = 0; j < availableTypes.length; j++) {
                                const currentType = availableTypes[j];
                                if (migration.isTypeOf(currentType)) {
                                    //Migration uses current type
                                    if (_.isEmpty(usedTypes)) {
                                        //Update current record to use the type
                                        updatedDocument.type = currentType;
                                        updatedDocument.alias = _getTypedAlias(MainOdm, currentType);
                                        usedTypes.push(currentType);

                                    } else {
                                        //Create new records for the rest of the types
                                        const shallowClone = { ...updatedDocument };
                                        shallowClone.type = currentType;
                                        shallowClone.alias = _getTypedAlias(MainOdm, currentType);
                                        migration.__updatedDocuments.push(shallowClone);
                                        usedTypes.push(currentType);
                                    }
                                }
                            }

                            if (!migration._info.inclusiveIndexType || [`*`, `?`].some((wildcard) => migration._info.indexType.includes(wildcard))) {
                                if (STRICT.types) {
                                    throw Error(`'${migration.version}': You have to specify index type for ${(updatedDocument.force ? `forceCreateDocument` : `updateDocument`)} function - ID: '${updatedDocument.id}'.`);
                                } else {
                                    logger.warn(`'${migration.version}': Type has not been specified in ${(updatedDocument.force ? `forceCreateDocument` : `updateDocument`)} function, using for all possible types - ID: '${updatedDocument.id}', used types: '${usedTypes.join(`,`)}'.`);
                                }
                            }

                        } else {
                            //Multiple types and type specified
                            if (!availableTypes.includes(updatedDocument.type)) {
                                throw Error(`'${migration.version}': You specified not existing index type '${updatedDocument.type}' in ${(updatedDocument.force ? `forceCreateDocument` : `updateDocument`)} function - ID: '${updatedDocument.id}'.`);
                            } else if (!migration.isTypeOf(updatedDocument.type)) {
                                throw Error(`'${migration.version}': You specified incorrect type '${updatedDocument.type}' in ${(updatedDocument.force ? `forceCreateDocument` : `updateDocument`)} function - ID: '${updatedDocument.id}'. This type is not usable with selected ${(migration._info.inclusiveIndexType) ? `inclusive` : `exclusive`} index type '${migration._info.indexType}'.`);
                            }

                            updatedDocument.alias = _getTypedAlias(MainOdm, updatedDocument.type);
                        }
                    } else {
                        if (_.isEmpty(updatedDocument.type)) {
                            //No types and not specified
                            updatedDocument.alias = MainOdm._alias;
                        } else {
                            //No types and type specified
                            throw Error(`'${migration.version}': You cannot specify type in ${(updatedDocument.force ? `forceCreateDocument` : `updateDocument`)} function - ID: '${updatedDocument.id}'.`);
                        }
                    }
                }

                //Check if there aren't updates on the same documents
                const duplicities = [];
                for (const updatedDocument of migration.__updatedDocuments) {
                    const counts = migration.__updatedDocuments.filter((doc) => (doc.alias === updatedDocument.alias && doc.id === updatedDocument.id)).length;
                    if (counts > 1) {
                        duplicities.push(`${updatedDocument.alias}:${updatedDocument.id}`);
                    }
                }
                if (duplicities.length > 0) {
                    throw Error(`'${migration.version}': You have specified multiple updateDocument or forceCreateDocument functions for the same document: ${_.uniq(duplicities).join(`,`)}`);
                }
            }
        }
    }

    /**
     * Searches ES for existing data and runs migrations over them
     * @param body {{}}    Query to be searched - matchAll for BULK type, specific IDs for DOCUMENT type
     * @param TypedOdm
     * @param indexMapping {{}}
     * @param virtualDocuments {{}}
     * @param toDelete {{}}
     * @returns {Promise<void>}
     * @private
     */
    async _migrateExistingDocuments(body, TypedOdm, indexMapping, virtualDocuments, toDelete) {
        let esResult;
        try {
            //Initial search
            esResult = await elastic.callEs(`search`, {
                ...body,
                index: TypedOdm._alias,
                from: 0,
                size: await optimizations.getBulkSize(TypedOdm),
                scroll: SCROLL_TIMEOUT,
                version: true
            }, {
                asStream: true
            });

            do {
                const documentBulk = esResult.hits.hits;

                if (!_.isEmpty(esResult._shards?.failures)) {
                    throw new elastic._esErrors.ResponseError({ body: esResult._shards.failures });
                } else if (documentBulk.length <= 0) {
                    break;
                }

                //Migrate bulk
                const toSave = await this._runMigrationsOverBulk(documentBulk, TypedOdm, virtualDocuments, toDelete);
                if (toSave.length > 0) {
                    //And save result
                    const bulkSave = [];
                    toSave.forEach((document) => {
                        bulkSave.push({
                            index: {
                                _index: indexMapping[document._alias].outputIndex,
                                _id: document._id,
                                version: document._version + 1,
                                version_type: `external`
                            }
                        });
                        bulkSave.push(document);
                    });
                    await elastic.sendBulk(bulkSave, `Failed when re-saving existing documents`, TypedOdm);
                }

                //Fetch new results
                esResult = await elastic.callEs(`scroll`, {
                    scroll: SCROLL_TIMEOUT,
                    scroll_id: esResult._scroll_id
                }, {
                    asStream: true
                });

            } while (esResult.hits.hits.length > 0);

        } finally {
            if (!_.isEmpty(esResult?._scroll_id)) {
                await elastic.callEs(`clearScroll`, {
                    scroll_id: esResult._scroll_id
                });
            }
        }
    }

    /**
     * Migrates virtual documents. Virtual documents are the documents that don't exist in ES yet, but have been specified to be created.
     * @param MainOdm
     * @param indexMapping {{}}
     * @param virtualDocuments {{}}
     * @param toDelete {{}}
     * @returns {Promise<void>}
     * @private
     */
    async _migrateVirtualDocuments(MainOdm, indexMapping, virtualDocuments, toDelete) {
        const availableTypes = MainOdm.hasTypes() ? Object.keys(indexMapping).map((inputIndex) => MainOdm._parseIndex(inputIndex).type) : [];

        //Migrate virtual (not yet saved) documents
        for (let i = 0; i < this._migrations.length; i++) {
            //Go through all migrations one-by-one
            const migration = this._migrations[i];

            //At first run afterAll function
            if (migration._info.hasAfterAll) {
                await migration.afterAll();
            }

            //Check not used update requests
            if (_.isArray(migration.__updatedDocuments)) {
                //When fallback is available, create request to create a new document
                const sourceFallback = migration.__updatedDocuments.filter((updatedDocument) => (!updatedDocument.used && !!updatedDocument.fallbackSource));
                for (const fallback of sourceFallback) {
                    migration.createDocument(fallback.fallbackSource, fallback.id, fallback.type);
                }

                //Check not used update functions
                const notUsed = migration.__updatedDocuments.filter((updatedDocument) => (!updatedDocument.used && !updatedDocument.force && !updatedDocument.fallbackSource));
                if (notUsed.length > 0) {
                    if (STRICT.updates) {
                        throw Error(`'${migration.version}': Following documents were not presented to be updated by specified update function: ${notUsed.map(({ alias, id }) => `${alias}:${id}`).join(`,`)}`);
                    } else {
                        logger.warn(`'${migration.version}': Following documents were not presented to be updated by specified update function: ${notUsed.map(({ alias, id }) => `${alias}:${id}`).join(`,`)}`);
                    }
                }
            }

            //Now prepare all the document objects which accumulated in the migration
            if (_.isArray(migration.__createdDocuments)) {
                for (const createDocument of migration.__createdDocuments) {
                    if (MainOdm.hasTypes()) {
                        if (_.isEmpty(createDocument.type)) {
                            //Multiple types and type not specified

                            const usedTypes = [];
                            for (const availableType of availableTypes) {
                                if (migration.isTypeOf(availableType)) {
                                    const myAlias = _getTypedAlias(MainOdm, availableType);
                                    _createDocument(i, myAlias, createDocument.id, createDocument.source, virtualDocuments);
                                    usedTypes.push(availableType);
                                }
                            }

                            if (!migration._info.inclusiveIndexType || [`*`, `?`].some((wildcard) => migration._info.indexType.includes(wildcard))) {
                                if (STRICT.types) {
                                    throw Error(`'${migration.version}': You have to specify index type for createDocument function.`);
                                } else {
                                    if (!createDocument.force) {
                                        //Warning for force create documents is already logged in from update function
                                        logger.warn(`'${migration.version}': No type has been specified in createDocument function, using for all available types: '${usedTypes.join(`,`)}'.`);
                                    }
                                }
                            }
                        } else {
                            //Multiple types and type specified
                            if (!availableTypes.includes(createDocument.type)) {
                                throw Error(`'${migration.version}': You specified not existing index type '${createDocument.type}' when creating new document.`);
                            } else if (!migration.isTypeOf(createDocument.type)) {
                                throw Error(`'${migration.version}': You specified incorrect type '${createDocument.type}' when creating new document. This type is not usable with selected ${(migration._info.inclusiveIndexType) ? `inclusive` : `exclusive`} index type '${migration._info.indexType}'.`);
                            }

                            const myAlias = _getTypedAlias(MainOdm, createDocument.type);
                            _createDocument(i, myAlias, createDocument.id, createDocument.source, virtualDocuments);
                        }
                    } else {
                        if (_.isEmpty(createDocument.type)) {
                            //No types and type not specified
                            _createDocument(i, MainOdm._alias, createDocument.id, createDocument.source, virtualDocuments);
                        } else {
                            //Not types and type specified
                            throw Error(`'${migration.version}': You cannot specify type of newly created document.`);
                        }
                    }
                }
            }

            //Check if new documents should be created and migrate them
            if (!_.isEmpty(virtualDocuments[`${i}`])) {

                //Send by index types (not mixed)
                const grouped = _.groupBy(virtualDocuments[`${i}`], (doc) => doc.alias);
                for (const [alias, aliasDocuments] of Object.entries(grouped)) {
                    const pseudoBulk = [...await this._prepareVirtuals(MainOdm, aliasDocuments, virtualDocuments, i, toDelete), ...await this._allocateVirtuals(MainOdm, aliasDocuments)];

                    let TypedOdm = MainOdm;
                    if (MainOdm.hasTypes()) {
                        const aliasInfo = MainOdm._parseIndex(alias);
                        TypedOdm = MainOdm.type(aliasInfo.type);
                    }

                    let processed = 0;
                    const maxBulkSize = await optimizations.getBulkSize(TypedOdm);
                    const isLastMigration = (i === (this._migrations.length - 1));
                    while (processed < pseudoBulk.length) {
                        //Ensure pseudoBulk size doesn't exceed max allowed size
                        const processedArray = (maxBulkSize >= pseudoBulk.length) ? pseudoBulk : pseudoBulk.slice(processed, processed + maxBulkSize);
                        processed += maxBulkSize;

                        const toSave = (isLastMigration) ?
                            processedArray :
                            await this._runMigrationsOverBulk(processedArray, MainOdm, virtualDocuments, toDelete, i + 1);

                        if (toSave.length > 0) {
                            //And save result
                            const bulkSave = [];
                            toSave.forEach((document) => {
                                bulkSave.push({
                                    index: {
                                        _index: indexMapping[(isLastMigration) ? document._index : document._alias].outputIndex,
                                        _id: document._id
                                    }
                                });
                                bulkSave.push((isLastMigration) ? document._source : document);
                            });

                            await elastic.sendBulk(bulkSave, `Failed when saving newly created documents`, TypedOdm);
                        }
                    }
                }
            }
        }
    }

    /**
     * Cleans indices after the migration process. Deletes old index in case of reindexing or deletes marked documents otherwise
     * @param indexMapping {{}}
     * @param virtualDocuments {{}}
     * @param toDelete {{}}
     * @returns {Promise<void>}
     * @private
     */
    async _cleanIndices(indexMapping, virtualDocuments = {}, toDelete = {}) {
        const notClonedAliases = [];
        for (const value of Object.values(indexMapping)) {
            if (value.cloned) {
                //For cloned indices, just delete the original index
                await value.resource.deleteIndex();
                await value.resource.aliasIndex(value.outputIndex);

            } else {
                //Note not cloned indices
                notClonedAliases.push(value.outputIndex);
            }
        }

        //At first, we have to check if marked documents should really be deleted
        const documentsToCheck = [];
        for (const [alias, ids] of Object.entries(toDelete)) {
            if (notClonedAliases.includes(alias)) {
                documentsToCheck.push(...ids.map((id) => {
                    return { alias, id };
                }));
            }
        }

        //Check if marked documents are mentioned in the list of virtual documents
        const documentsToDelete = [];
        for (let i = documentsToCheck.length - 1; i >= 0; i--) {
            //Check documents from latest (so we can delete them from the list easily)
            const { alias, id } = documentsToCheck[i];

            for (let j = this._migrations.length - 1; j >= 0; j--) {
                //Go from the latest migration

                const checkedIteration = virtualDocuments[`${j}`];
                if (_.isEmpty(checkedIteration)) {
                    continue;
                }

                const existing = checkedIteration.find((doc) => (doc.alias === alias && doc.id === id));
                if (existing) {
                    //Document mentioned
                    if (existing.isDeleted) {
                        //And marked to be deleted -> delete
                        documentsToDelete.push(documentsToCheck[i]);
                    }
                    //Otherwise not marked -> do not delete
                    documentsToCheck.splice(i, 1);
                    break;
                }
            }
        }
        //Delete not mentioned documents as well
        documentsToDelete.push(...documentsToCheck);

        if (documentsToDelete.length > 0) {
            //Finally delete the documents
            const deleteBulk = documentsToDelete.map(({ alias, id }) => {
                return {
                    delete: {
                        _index: alias,
                        _id: id
                    }
                };
            });
            await elastic.sendBulk(deleteBulk);
        }
    }

    /**
     * Migrates single ES bulk. Runs all the beforeAll and migrate functions of all the migrations, takes care if document shouldn't be deleted, etc.
     * @param documents {Array<{}>} Input ES bulk
     * @param Odm
     * @param virtualDocuments {{}}
     * @param toDelete {{}}
     * @param firstMigration {number}
     * @returns {Promise<Array<{}>>} ES bulk after all the migrations have been processed
     * @private
     */
    async _runMigrationsOverBulk(documents, Odm, virtualDocuments, toDelete, firstMigration = 0) {
        //Prepare bulk in our format
        let myBulk = documents.map((document) => {
            //Shallow copy is necessary, as there may be some not-configurable values
            const myDocument = { ...document._source };
            const parsedIndex = Odm._parseIndex(document._index);
            Object.defineProperty(myDocument, `_id`, {  //Writable by default
                value: document._id,
                writable: true,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_alias`, {   //Not writable
                value: parsedIndex.alias,
                writable: false,
                enumerable: false,
                configurable: true
            });
            Object.defineProperty(myDocument, `_type`, {   //Not writable
                value: parsedIndex.type,
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

            return myDocument;
        });
        let newBulk = [];

        //Go through migrations
        for (let i = firstMigration; i < this._migrations.length; i++) {
            const migration = this._migrations[i];

            //Filter documents based on index type requested in migration
            const runBulk = [];
            myBulk.forEach((myDocument) => {
                if (migration.isTypeOf(myDocument._type)) {
                    //Correct type -> run migration
                    runBulk.push(myDocument);
                } else {
                    //Incorrect type -> just pass
                    newBulk.push(myDocument);
                }
            });

            const updatesCount = migration.__updatedDocuments?.length;

            if (!_.isEmpty(runBulk) && migration._info.hasBeforeBulk) {
                //In case of beforeBulk lock _id writability
                runBulk.forEach((myDocument) => Object.defineProperty(myDocument, `_id`, {
                    value: myDocument._id,
                    writable: false,
                    enumerable: false,
                    configurable: true
                }));

                await migration.beforeBulk(runBulk);

                //And unlock afterwards
                runBulk.forEach((myDocument) => Object.defineProperty(myDocument, `_id`, {
                    value: myDocument._id,
                    writable: true,
                    enumerable: false,
                    configurable: true
                }));
            }

            for (const myDocument of runBulk) {
                //Go through bulk and migrate

                const originalId = myDocument._id;
                await migration.runMigrate(myDocument, myDocument._alias, myDocument._type);
                const newId = myDocument._id;

                //Now check if ID has changed
                if (originalId === newId) {
                    //No change -> pass document to the next round
                    newBulk.push(myDocument);

                } else if (_.isNull(newId)) {
                    //Null -> delete document
                    _deleteDocument(i, myDocument._alias, originalId, virtualDocuments, toDelete);

                } else if (_.isUndefined(newId)) {
                    //Undefined -> delete original document and re-save with auto generated ID
                    _deleteDocument(i, myDocument._alias, originalId, virtualDocuments, toDelete);
                    _createDocument(i, myDocument._alias, void 0, myDocument, virtualDocuments);

                } else if (_.isString(newId) && !_.isEmpty(newId)) {
                    //Changed -> delete original document and re-save with new ID
                    _deleteDocument(i, myDocument._alias, originalId, virtualDocuments, toDelete);
                    _createDocument(i, myDocument._alias, newId, myDocument, virtualDocuments);

                } else {
                    throw Error(`'${migration.version}': Invalid ID specified - '${newId}'.`);
                }
            }

            //For DOCUMENTS type - check for some user hacks which wouldn't work
            if (updatesCount && updatesCount !== migration.__updatedDocuments.length) {
                throw Error(`'${migration.version}': updateDocument function cannot be nested in another updateDocument function.`);
            }

            //Switch arrays
            myBulk = newBulk;
            newBulk = [];
        }

        return myBulk;
    }

    /**
     * Checks which documents should be created after specific migration, ensures they don't exist in ES yet and prepares them.
     * @param Odm
     * @param aliasDocuments {[{}]}
     * @param virtualDocuments {{}}
     * @param iteration {number} Index of migration on which behalf we create the documents
     * @param toDelete {{}}
     * @returns {Promise<Array<{}>>} List of prepared virtual documents, mimics ES search response
     * @private
     */
    async _prepareVirtuals(Odm, aliasDocuments, virtualDocuments, iteration, toDelete) {
        const response = [];

        const iterationCache = aliasDocuments;
        if (!_.isArray(iterationCache) || _.isEmpty(iterationCache)) {
            return response;
        }

        const toPrepareVirtuals = [];
        for (let i = iterationCache.length - 1; i >= 0; i--) {
            const virtualDocument = iterationCache[i];
            if (!_.isEmpty(virtualDocument.id)) {
                toPrepareVirtuals.push(virtualDocument);   //Just note
            }
        }

        //Now check if documents with real IDs don't already exist
        const notExistingDocuments = [];
        for (const toPrepareVirtual of toPrepareVirtuals) {
            let isDeleted = false;
            //At first check if document wasn't already virtually created
            for (let j = iteration - 1; j >= 0; j--) {
                //Check previous migrations only
                const checkedIteration = virtualDocuments[`${j}`];
                if (_.isEmpty(checkedIteration)) {
                    continue;
                }

                const existingVirtual = checkedIteration.find((virtualDocument) => (virtualDocument.alias === toPrepareVirtual.alias && virtualDocument.id === toPrepareVirtual.id));
                if (existingVirtual) {
                    if (existingVirtual.isDeleted) {
                        //Was created but was also deleted -> that's OK
                        isDeleted = true;
                        notExistingDocuments.push(toPrepareVirtual);
                        break;
                    } else {
                        //Was created at still exists
                        throw Error(`'${this._migrations[iteration].version}': Document at alias '${toPrepareVirtual.alias}' with ID '${toPrepareVirtual.id}' already exists.`);
                    }
                }
            }

            if (!isDeleted) {
                //Document not mentioned in virtuals -> check if marked on to be deleted list
                if (toDelete[toPrepareVirtual.alias] && toDelete[toPrepareVirtual.alias].includes(toPrepareVirtual.id)) {
                    //Mentioned, that's OK
                    notExistingDocuments.push(toPrepareVirtual);
                }
            }
        }

        //Prepare list of documents we have actually check in ES if they already exist
        let toCheckDocuments = _.difference(toPrepareVirtuals, notExistingDocuments);
        if (toCheckDocuments.length > 0) {
            //Transform to better structure
            toCheckDocuments = toCheckDocuments.reduce((sum, doc) => {
                if (!sum[doc.alias]) {
                    sum[doc.alias] = [];
                }
                sum[doc.alias].push(doc.id);
                return sum;
            }, {});

            //Send request to ES
            const checkPromises = Object.entries(toCheckDocuments).map(([alias, ids]) => {
                return elastic.callEs(`mget`, {
                    index: alias,
                    ids: ids,
                    _source: false
                });
            });
            const checkResults = await Promise.all(checkPromises);

            const alreadyExisting = [];
            for (const checkResult of checkResults) {
                if (checkResult.docs) {
                    for (const checkDoc of checkResult.docs) {
                        if (checkDoc.found) {
                            alreadyExisting.push({ index: checkDoc._index, id: checkDoc._id });
                        }
                    }
                }
            }
            if (alreadyExisting.length > 0) {
                //We found existing document/s
                throw Error(`'${this._migrations[iteration].version}': You try to create documents which already exist: ${alreadyExisting.map(({ index, id }) => `${index}:${id}`).join(`,`)}`);
            }
        }

        for (let i = 0; i < toPrepareVirtuals.length; i++) {
            const preparedVirtual = toPrepareVirtuals[i];

            //Mimic ES search response
            response.push({
                _index: preparedVirtual.alias,
                _id: preparedVirtual.id,
                _version: 1,
                _source: preparedVirtual.source,
            });
        }
        return response;
    }

    /**
     * Creates an empty document in ES for virtual documents which don't specify an ID. Prepares and returns virtual documents.
     * @param Odm
     * @param aliasDocuments {[{}]}
     * @returns {Promise<Array<{}>>} List of prepared virtual documents, mimics ES search response
     * @private
     */
    async _allocateVirtuals(Odm, aliasDocuments) {
        const response = [];

        const iterationCache = aliasDocuments;
        if (!_.isArray(iterationCache) || _.isEmpty(iterationCache)) {
            return response;
        }

        const toAllocateVirtuals = [];
        for (let i = iterationCache.length - 1; i >= 0; i--) {
            const virtualDocument = iterationCache[i];
            if (_.isEmpty(virtualDocument.id)) {
                toAllocateVirtuals.push(...iterationCache.splice(i, 1));   //Note and remove
            }
        }

        if (toAllocateVirtuals.length > 0) {
            //Allocate in ES documents without ID (=== obtain ID)
            const bulkSave = [];
            toAllocateVirtuals.forEach((item) => {
                bulkSave.push({
                    index: {
                        _index: item.alias, //Send to original index intentionally
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
                    _index: allocatedVirtual.alias,
                    _id: responseItems[i].index._id,
                    _version: responseItems[i].index._version,
                    _source: allocatedVirtual.source,
                });
            }
        }

        return response;
    }
}

/**
 * Tries to create new ES index, automatically removes not allowed settings parameters
 * @param TypedOdm
 * @param mySettings {{}} Original settings object
 * @param updatedSettings {{}} User changes on settings
 * @param myMapping {{}} Mapping object
 * @param iteration {number} How many times we tried to strip the invalid parameters
 * @param indexRetry {number} How many times we have to delete incorrect index (see note in code)
 * @returns {Promise<string>} Output index in case this was successful
 * @private
 */
async function _safeCreateIndex(TypedOdm, mySettings, updatedSettings, myMapping, iteration = 0, indexRetry = 0) {
    try {
        //Prepare new index; do NOT set alias yet
        return await TypedOdm.createIndex({
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
                await optimizations.error429(indexRetry);
                indexRetry++;

                await elastic.callEs(`indices.delete`, {
                    index: esError.meta.body.error.root_cause[0].index
                });
                return _safeCreateIndex(TypedOdm, mySettings, updatedSettings, myMapping, 0, indexRetry);
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

        _.mergeWith(mySettings, updatedSettings, (a, b) => _.isArray(b) ? b : undefined);

        return _safeCreateIndex(TypedOdm, mySettings, updatedSettings, myMapping, iteration + 1, indexRetry);
    }
}

/**
 * Waits for single task to finish
 * @param taskId {string}
 * @param retryCounter {number}
 * @returns {Promise<boolean>}
 * @private
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
                    await optimizations.error429(retryCounter);
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
 * @private
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
 * @private
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
 * Marks new document to be created -> virtual document
 * @param i {number} Migration number on which behalf the document should be created
 * @param alias {string} ES index alias where to create the document
 * @param id {string} Optional ES ID of the document
 * @param source {{}} Source object of the document
 * @param virtualDocuments {{}} Map of all virtual documents
 * @private
 */
function _createDocument(i, alias, id = void 0, source, virtualDocuments) {
    let iterationCache = virtualDocuments[`${i}`];
    if (!iterationCache) {
        iterationCache = [];
        virtualDocuments[`${i}`] = iterationCache;
    }

    const existing = iterationCache.find((doc) => (doc.alias === alias && id && doc.id === id));
    if (existing) {
        throw Error(`Creation of document in index '${alias}' with ID '${id}' has been already declared in the same migration.`);
    }

    iterationCache.push({ alias, id, source, isDeleted: false });
}

/**
 * Marks the document as deleted / to be deleted
 * @param i {number} Migration number on which behalf the document should be deleted
 * @param alias ES index alias where to delete the document
 * @param id {string} ES ID of the document
 * @param virtualDocuments {{}} Map of virtual documents
 * @param toDelete {{}} Map of the documents to be deleted
 * @private
 */
function _deleteDocument(i, alias, id, virtualDocuments, toDelete) {
    let deleted = false;
    for (let j = i - 1; j >= 0; j--) {
        //Mark as deleted in not yet saved documents
        const iterationCache = virtualDocuments?.[`${j}`];
        if (!iterationCache) {
            continue;
        }

        const existing = iterationCache.find((doc) => (doc.alias === alias && doc.id === id));
        if (existing) {
            existing.isDeleted = true;
            deleted = true;
            break;
        }
    }

    if (!deleted) {
        //Delete in ES
        if (!toDelete[alias]) {
            toDelete[alias] = [];
        }
        if (!toDelete[alias].includes(id)) {
            toDelete[alias].push(id);
        }
    }
}

/**
 * Quick hack to obtain a typed alias without creating a new ODM type
 * @param Odm
 * @param type {string} Type to use
 * @returns {string} Alias with given type
 * @private
 */
function _getTypedAlias(Odm, type) {
    return `${Odm._tenant}_${Odm._name}_${type}`;
}

module.exports = Node;