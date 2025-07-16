'use strict';

const _ = require(`lodash`);
const ScriptWrapper = require(`./ScriptWrapper`);
const elastic = require(`./elastic`);
const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;
const INDICES = require(`./elastic`)._indicesMap;

class Migration {
    /**
     * Main info about the migration
     * @returns {{}}
     */
    static get INFO() {
        /*
         * TYPE
         * INDEX
         * DEPENDS_ON
         * INDICES
         * INPUT_INDICES
         * MESSAGE
         * SINGLE_WORKER
         */
        throw Error(`'INFO' object not overridden!`);
    }

    /**
     * Synchronisation types
     * @returns {Record<string, string>}
     */
    static get TYPE() {
        return SYNCHRONISATION_TYPES;
    }

    /**
     * Map of known ES indices
     * @returns {Record<string, string>}
     */
    static get INDICES() {
        return INDICES;
    }

    /**
     * Return better name for the class
     * @returns {string}
     */
    static toString() {
        return this.name;
    }

    /**
     * Creates a new migration
     * @param version {string}
     * @param versionNumbers {string}
     * @param position {string | number}
     */
    constructor(version, versionNumbers, position) {
        /**
         * Migration version, either in '<major>.<minor>.<patch>[:<position>]' format, or with 'pre' and 'post' strings
         * @type {string}
         */
        Object.defineProperty(this, `version`, {
            value: (position) ? `${version}:${position}` : version,
            writable: false
        });

        /**
         * Migration version always in '<major>.<minor>.<patch>[:<position>]' format, for internal use
         * @type {string}
         */
        Object.defineProperty(this, `_versionNumbers`, {
            value: (position) ? `${versionNumbers}:${position}` : versionNumbers,
            writable: false
        });

        /**
         * Object with internal info
         * @type {Record<string, *>}
         */
        Object.defineProperty(this, `_info`, {
            value: {},
            writable: true
        });

        /**
         * Map with ODMs
         * @type {Record<string, {}>}
         */
        this.ODM = {};

        /**
         * Map with dependency data
         * @type {Record<string, {}>}
         */
        this.DEPENDENCY = {};

        /**
         * Handy utils
         * @type {Record<string, {}>}
         */
        this.UTILS = void 0;

        /**
         * Internal buffer for documents to be created
         * @type {boolean}
         */
        Object.defineProperty(this, `__isInitialized`, {
            value: false,
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal buffer for documents to be created
         * @type {Array<{source: {}, id: string, isForced: boolean}>}
         */
        Object.defineProperty(this, `__createdDocuments`, {
            value: [],
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal buffer for documents to be updated
         * @type {Map<string, {functionName: string, fallbackSource: {}, isUsed: boolean, isForced: boolean}>}
         */
        Object.defineProperty(this, `__updatedDocuments`, {
            value: new Map(),
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal cache for the finished migration data that may be passed to another processes (master + workers)
         * @type {*}
         */
        Object.defineProperty(this, `__finalMigrationData`, {
            value: void 0,
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal cache for documents script
         * @type {string | Function}
         */
        Object.defineProperty(this, `__scriptCache`, {
            value: void 0,
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Migration filepath
         * @type {string}
         */
        Object.defineProperty(this, `__filepath`, {
            value: void 0,
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Migration index in exported array
         * @type {number}
         */
        Object.defineProperty(this, `__classIndex`, {
            value: void 0,
            writable: true,
            enumerable: false,
            configurable: false
        });

        //This is instance code, to get user specified methods we have to use its prototype === __proto__
        const prototype = Object.getPrototypeOf(this);
        if (prototype.hasOwnProperty(`_initialize`)) {
            throw Error(`You can't specify method "_initialize".`);
        } else if (prototype.hasOwnProperty(`_parseBulkLikeInfo`)) {
            throw Error(`You can't specify method "_parseBulkLikeInfo".`);
        } else if (prototype.hasOwnProperty(`_parseGeneralInfo`)) {
            throw Error(`You can't specify method "_parseGeneralInfo".`);
        } else if (prototype.hasOwnProperty(`__internalDeleteDocument`)) {
            throw Error(`You can't specify method "__internalDeleteDocument".`);
        } else if (prototype.hasOwnProperty(`_runInitialize`)) {
            throw Error(`You can't specify method "_runInitialize".`);
        } else if (prototype.hasOwnProperty(`_runBeforeBulk`)) {
            throw Error(`You can't specify method "_runBeforeBulk".`);
        } else if (prototype.hasOwnProperty(`_runMigrate`)) {
            throw Error(`You can't specify method "_runMigrate".`);
        } else if (prototype.hasOwnProperty(`_runFinalize`)) {
            throw Error(`You can't specify method "_runFinalize".`);

        } else if (prototype.hasOwnProperty(`version`)) {
            throw Error(`You can't specify method "version".`);
        } else if (prototype.hasOwnProperty(`_versionNumbers`)) {
            throw Error(`You can't specify method "_versionNumbers".`);
        } else if (prototype.hasOwnProperty(`_info`)) {
            throw Error(`You can't specify method "_info".`);
        } else if (prototype.hasOwnProperty(`utils`)) {
            throw Error(`You can't specify method "utils".`);
        } else if (prototype.hasOwnProperty(`ODM`)) {
            throw Error(`You can't specify method "ODM".`);
        } else if (prototype.hasOwnProperty(`DEPENDENCY`)) {
            throw Error(`You can't specify method "DEPENDENCY".`);
        } else if (prototype.hasOwnProperty(`__isInitialized`)) {
            throw Error(`You can't specify method "__isInitialized".`);
        } else if (prototype.hasOwnProperty(`__createdDocuments`)) {
            throw Error(`You can't specify method "__createdDocuments".`);
        } else if (prototype.hasOwnProperty(`__updatedDocuments`)) {
            throw Error(`You can't specify method "__updatedDocuments".`);
        } else if (prototype.hasOwnProperty(`__finalMigrationData`)) {
            throw Error(`You can't specify method "__finalMigrationData".`);
        } else if (prototype.hasOwnProperty(`__scriptCache`)) {
            throw Error(`You can't specify method "__scriptCache".`);
        } else if (prototype.hasOwnProperty(`__filepath`)) {
            throw Error(`You can't specify method "__filepath".`);
        } else if (prototype.hasOwnProperty(`__classIndex`)) {
            throw Error(`You can't specify method "__classIndex".`);
        }

        this._initialize(prototype);
    }

    // ================================================== INTERNAL ==================================================

    /**
     * Initializes the migration
     * @param prototype {{}} Instance __proto__ object
     */
    _initialize(prototype) {
        const info = this.constructor.INFO;
        if (_.isEmpty(info)) {
            throw Error(`No info has been specified.`);
        }

        const result = {
            type: info.TYPE,
            index: void 0,
            inputIndices: [],
            outputIndices: [],
            dependencyIndices: [],
            dependencyMigrations: [],
            reindex: false,
            isProcessed: false,
            message: void 0,
            singleWorker: false
        };

        const infoCopy = _.clone(info);
        delete infoCopy.TYPE;

        switch (info.TYPE) {
            case SYNCHRONISATION_TYPES.STOP: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You can't specify the "migration" method.`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                }

                if (!_.isEmpty(info.MESSAGE) && _.isString(info.MESSAGE)) {
                    result.message = info.MESSAGE;
                    delete infoCopy.MESSAGE;
                } else {
                    throw Error(`You have to specify a stop "MESSAGE".`);
                }

                break;
            }
            case SYNCHRONISATION_TYPES.SERIAL: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                if (!_.isNil(info.SINGLE_WORKER)) {
                    if (_.isBoolean(info.SINGLE_WORKER)) {
                        result.singleWorker = (info.SINGLE_WORKER === true);
                        delete infoCopy.SINGLE_WORKER;
                    } else {
                        throw Error(`Property "SINGLE_WORKER" must be a boolean value.`);
                    }
                }

                const validIndices = elastic.getAllowedIndices(this._versionNumbers, _.uniq(Object.keys(elastic._indicesMap)), false);
                result.inputIndices = validIndices;
                result.outputIndices = validIndices;

                result.inputIndices.forEach((index) => this.ODM[index] = elastic.odms[index]);
                result.outputIndices.forEach((index) => this.ODM[index] = elastic.odms[index]);

                break;
            }
            case SYNCHRONISATION_TYPES.INDICES: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                if (!_.isNil(info.SINGLE_WORKER)) {
                    if (_.isBoolean(info.SINGLE_WORKER)) {
                        result.singleWorker = (info.SINGLE_WORKER === true);
                        delete infoCopy.SINGLE_WORKER;
                    } else {
                        throw Error(`Property "SINGLE_WORKER" must be a boolean value.`);
                    }
                }

                this._parseGeneralInfo(info, infoCopy, result);

                if (_.isEmpty(result.inputIndices) && _.isEmpty(result.outputIndices)) {
                    throw Error(`You have to specify at least one index.`);
                }

                break;
            }
            case SYNCHRONISATION_TYPES.BULK: {
                if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                }

                if (!_.isNil(info.SINGLE_WORKER)) {
                    if (_.isBoolean(info.SINGLE_WORKER)) {
                        result.singleWorker = (info.SINGLE_WORKER === true);
                        delete infoCopy.SINGLE_WORKER;
                    } else {
                        throw Error(`Property "SINGLE_WORKER" must be a boolean value.`);
                    }
                }

                this._parseBulkLikeInfo(info, infoCopy, result);

                if (prototype.hasOwnProperty(`reindex`)) {
                    result.reindex = true;
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                break;
            }
            case SYNCHRONISATION_TYPES.DOCUMENTS: {
                if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You can't specify method "migrate".`);
                } else if (!prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You must specify the "beforeAll" method.`);
                }

                if (!_.isNil(info.SINGLE_WORKER)) {
                    if (_.isBoolean(info.SINGLE_WORKER)) {
                        result.singleWorker = (info.SINGLE_WORKER === true);
                        delete infoCopy.SINGLE_WORKER;
                    } else {
                        throw Error(`Property "SINGLE_WORKER" must be a boolean value.`);
                    }
                }

                this._parseBulkLikeInfo(info, infoCopy, result);

                break;
            }
            case SYNCHRONISATION_TYPES.PUT: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You cannot specify the "migration" method.`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (!prototype.hasOwnProperty(`putMapping`) && !prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You have to specify at least one of "putMapping" and "putSettings" methods.`);
                }

                this._parseBulkLikeInfo(info, infoCopy, result);

                break;
            }
            case SYNCHRONISATION_TYPES.SCRIPT: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                }

                this._parseBulkLikeInfo(info, infoCopy, result);

                if (prototype.hasOwnProperty(`reindex`)) {
                    result.reindex = true;
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                break;
            }
            default: {
                throw Error(`Unknown migration type.`);
            }
        }

        //Ban everything other
        const keys = Object.keys(infoCopy);
        if (!_.isEmpty(keys)) {
            throw Error(`You can't specify info properties [${keys.join(`,`)}].`);
        }

        this._info = result;
    }

    /**
     * Parses BULK-like types = BULK, DOCUMENTS, SCRIPT, PUT
     * @param info {{}}
     * @param infoCopy {{}}
     * @param result {{}}
     */
    _parseBulkLikeInfo(info, infoCopy, result) {
        if (_.isEmpty(info.INDEX) || !_.isString(info.INDEX)) {
            throw Error(`You have to specify an "INDEX" property.`);
        } else {
            result.index = elastic.getAllowedIndices(this._versionNumbers, elastic._indicesMap[info.INDEX], true);
            delete infoCopy.INDEX;
        }

        this._parseGeneralInfo(info, infoCopy, result);

        if (result.inputIndices.includes(result.index)) {
            throw Error(`You cannot manually read from main index.`);
        } else if (result.outputIndices.includes(result.index)) {
            throw Error(`You cannot manually write to main index.`);
        }
    }

    /**
     * Parses general info
     * @param info {{}}
     * @param infoCopy {{}}
     * @param result {{}}
     */
    _parseGeneralInfo(info, infoCopy, result) {
        if (info.INDICES) {
            const myIndices = _.castArray(info.INDICES);
            if (myIndices.some((index) => (typeof index !== `string`))) {
                throw Error(`INDICES contains not string value, index mapping may be missing.`);
            }

            result.inputIndices.push(...myIndices);
            result.outputIndices.push(...myIndices);
            delete infoCopy.INDICES;
        } else if (info.hasOwnProperty(`INDICES`)) {
            throw Error(`INDICES property is specified to nill value, index mapping may be missing.`);
        }

        if (info.INPUT_INDICES) {
            const myIndices = _.castArray(info.INPUT_INDICES);
            if (myIndices.some((index) => (typeof index !== `string`))) {
                throw Error(`INPUT_INDICES contains not string value, index mapping may be missing.`);
            }

            result.inputIndices.push(...myIndices);
            delete infoCopy.INPUT_INDICES;
        } else if (info.hasOwnProperty(`INPUT_INDICES`)) {
            throw Error(`INPUT_INDICES property is specified to nill value, index mapping may be missing.`);
        }

        if (info.DEPENDS_ON) {
            const myDependentMigrations = _.castArray(info.DEPENDS_ON);
            let affectedIndices = [];
            for (const myDependentMigration of myDependentMigrations) {
                if (Object.getPrototypeOf(myDependentMigration) !== Migration) {
                    throw Error(`DEPENDS_ON must contain references to other migrations.`);
                }

                if (!_.isNil(myDependentMigration.INFO.INDEX)) {
                    affectedIndices.push(myDependentMigration.INFO.INDEX);
                }
                if (!_.isNil(myDependentMigration.INFO.INDICES)) {
                    affectedIndices.push(..._.castArray(myDependentMigration.INFO.INDICES));
                }
                if (!_.isNil(myDependentMigration.INFO.INPUT_INDICES)) {
                    affectedIndices.push(..._.castArray(myDependentMigration.INFO.INPUT_INDICES));
                }
                affectedIndices = _.uniq(_.compact(affectedIndices));
                if (affectedIndices.some((index) => (typeof index !== `string`))) {
                    throw Error(`DEPENDS_ON results into wrong data.`);
                }
            }

            result.dependencyIndices.push(...affectedIndices);
            result.dependencyMigrations.push(...myDependentMigrations);
            delete infoCopy.DEPENDS_ON;
        } else if (info.hasOwnProperty(`DEPENDS_ON`)) {
            throw Error(`DEPENDS_ON property is specified to nill value, index mapping may be missing.`);
        }

        //Remove duplicities
        result.inputIndices = elastic.getAllowedIndices(this._versionNumbers, _.uniq(_.compact(result.inputIndices)), true);
        result.outputIndices = elastic.getAllowedIndices(this._versionNumbers, _.uniq(_.compact(result.outputIndices)), true);
        result.dependencyIndices = _.uniq(_.compact(result.dependencyIndices));

        //Set ODMs
        result.inputIndices.forEach((index) => this.ODM[index] = (elastic.odms[index]));
        result.outputIndices.forEach((index) => this.ODM[index] = (elastic.odms[index]));
        //No dependencyIndices
    }

    /**
     * Internal initialize, calls the correct function
     * @param alias {string} Index alias
     * @param isJS {boolean} In case of SCRIPT type, should JS code be generated?
     * @returns {Promise<void>}
     */
    async _runInitialize(alias = void 0, isJS = void 0) {
        if ((this._info.type === SYNCHRONISATION_TYPES.SERIAL) || (this._info.type === SYNCHRONISATION_TYPES.INDICES)) {
            this.__finalMigrationData = await this.migrate();

        } else if ((this._info.type === SYNCHRONISATION_TYPES.BULK) || (this._info.type === SYNCHRONISATION_TYPES.DOCUMENTS)) {
            await this.beforeAll();

        } else if (this._info.type === SYNCHRONISATION_TYPES.SCRIPT) {
            const scriptWrapper = new ScriptWrapper({
                isJavascript: isJS,
                alias: alias
            });
            this.__finalMigrationData = await this.migrate(scriptWrapper);
            this.__scriptCache = scriptWrapper._getResult();

        } else {
            //PUT and STOP do not have initialize method
        }

        this.__isInitialized = true;
    }

    /**
     * Internal beforeBulk, calls the correct function
     * @param bulk {Array<{}>}
     * @returns {Promise<void>}
     */
    async _runBeforeBulk(bulk) {
        if (this._info.type === SYNCHRONISATION_TYPES.BULK) {
            let toProcess = bulk;
            if (this.__updatedDocuments.size > 0) {
                //Filter out the documents with a custom update function
                toProcess = bulk.filter((item) => (!this.__updatedDocuments.has(item._id)));
            }

            await this.beforeBulk(toProcess);

        } else {
            //Do nothing
        }
    }

    /**
     * Internal migrate function, calls the correct function
     * @param document {{}}
     * @returns {Promise<void>}
     */
    async _runMigrate(document = void 0) {
        if (this._info.type === SYNCHRONISATION_TYPES.BULK) {
            //Find a document to be updated
            const updateRecord = this.__updatedDocuments.get(document._id);
            if (updateRecord) {
                //Run custom update function
                await this[updateRecord.functionName](document);
                updateRecord.isUsed = true;
            } else {
                await this.migrate(document);
            }

        } else if (this._info.type === SYNCHRONISATION_TYPES.DOCUMENTS) {
            //Find a document to be updated
            const updateRecord = this.__updatedDocuments.get(document._id);
            if (updateRecord) {
                await this[updateRecord.functionName](document);
                updateRecord.isUsed = true;
            }

        } else if (this._info.type === SYNCHRONISATION_TYPES.SCRIPT) {
            this.__scriptCache(document);

        } else {
            //Do nothing
        }
    }

    /**
     * Internal finalize, calls the correct function
     * @returns {Promise<void>}
     */
    async _runFinalize() {
        if ((this._info.type === SYNCHRONISATION_TYPES.BULK) || (this._info.type === SYNCHRONISATION_TYPES.DOCUMENTS)) {
            this.__finalMigrationData = await this.afterAll();

        } else {
            //Do nothing
        }
    }

    /**
     * Internal function to delete a document
     * @param document {*}
     */
    __internalDeleteDocument(document) {
        document._id = null;
    }

    // ================================================== REWRITABLE ==================================================

    /**
     * Puts mapping to the index
     */
    async putMapping() {}

    /**
     * Puts settings to the index
     */
    async putSettings() {}

    /**
     * Reindex function, use it to alter the index mapping and setting
     * @param mapping {{}} Original mapping, alter it the way you need it
     * @param settings {{}} Original settings, alter it the way you need it
     * @returns {Promise<void>}
     */
    async reindex(mapping, settings) {}

    /**
     * Initialize function, called once at the start of the migration.
     * @returns {Promise<void>}
     */
    async beforeAll() {}

    /**
     * BeforeBulk function, called for every ES bulk response from worker thread. Read only, do not change the data here!
     * @param bulk {Array<{}>} ES bulk
     * @returns {Promise<void>}
     */
    async beforeBulk(bulk) {}

    /**
     * Migration function.
     * @param document {{} | ScriptWrapper | void}
     * @returns {Promise<void>}
     */
    async migrate(document = void 0) {}

    /**
     * Finalize function. Called once at the end.
     * @returns {Promise<*>}
     */
    async afterAll() {}
}

module.exports = Migration;
