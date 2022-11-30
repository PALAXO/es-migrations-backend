'use strict';

const _ = require(`lodash`);
const ScriptWrapper = require(`./ScriptWrapper`);
const elastic = require(`./elastic`);
const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;
const INDICES = require(`./elastic`)._indicesMap;

global.MESSAGES = {};    //Global message cache

class Migration {
    constructor(version, versionNumbers, position) {
        /**
         * Migration version, either in '<major>.<minor>.<patch>[:<position>]' format, or with 'pre' and 'post' strings
         * @type {string}
         * @private
         */
        Object.defineProperty(this, `version`, {
            value: (position) ? `${version}:${position}` : version
        });

        /**
         * Migration version always in '<major>.<minor>.<patch>[:<position>]' format, for internal use
         * @type {string}
         * @private
         */
        Object.defineProperty(this, `_versionNumbers`, {
            value: (position) ? `${versionNumbers}:${position}` : versionNumbers
        });

        /**
         * Object with internal info
         * @type {{}}
         * @private
         */
        Object.defineProperty(this, `_info`, {
            value: {},
            writable: true
        });

        /**
         * Handy ES utils
         * @type {{}}
         * @private
         */
        this.utils = elastic.utils;

        /**
         * Map with ODMs
         * @type {{}}
         * @private
         */
        this.ODM = {};

        /**
         * Internal buffer for documents to be created
         */
        Object.defineProperty(this, `__createdDocuments`, {
            value: [],
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal buffer for documents to be updated
         */
        Object.defineProperty(this, `__updatedDocuments`, {
            value: [],
            writable: true,
            enumerable: false,
            configurable: false
        });

        /**
         * Internal buffer for documents scripts
         */
        Object.defineProperty(this, `__scriptCache`, {
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
        } else if (prototype.hasOwnProperty(`hasUnsafeOperation`)) {
            throw Error(`You can't specify method "hasUnsafeOperation".`);
        } else if (prototype.hasOwnProperty(`runBeforeAll`)) {
            throw Error(`You can't specify method "runBeforeAll".`);
        } else if (prototype.hasOwnProperty(`runMigrate`)) {
            throw Error(`You can't specify method "runMigrate".`);
        } else if (prototype.hasOwnProperty(`note`)) {
            throw Error(`You can't specify method "note".`);
        } else if (prototype.hasOwnProperty(`createDocument`)) {
            throw Error(`You can't specify method "createDocument".`);
        } else if (prototype.hasOwnProperty(`updateDocument`)) {
            throw Error(`You can't specify method "updateDocument".`);
        } else if (prototype.hasOwnProperty(`forceCreateDocument`)) {
            throw Error(`You can't specify method "forceCreateDocument".`);
        }

        this._initialize(prototype);
    }

    /**
     * Initializes the migration
     * @param prototype {{}} Instance __proto__ object
     * @private
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
            reindex: false,
            hasBeforeAll: false,
            hasBeforeBulk: false,
            hasAfterAll: false,
            isProcessed: false
        };

        const infoCopy = _.clone(info);
        delete infoCopy.TYPE;

        switch (info.TYPE) {
            case SYNCHRONISATION_TYPES.STOP: {
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
                } else if (prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You can't specify the "migration" method.`);
                }

                if (!_.isEmpty(info.MESSAGE) && _.isString(info.MESSAGE)) {
                    this.note(info.MESSAGE);
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

                result.inputIndices = _.uniq(Object.keys(elastic._indicesMap));
                result.outputIndices = _.uniq(Object.keys(elastic._indicesMap));

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

                this._parseBulkLikeInfo(info, infoCopy, result);

                if (prototype.hasOwnProperty(`reindex`)) {
                    result.reindex = true;
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                if (prototype.hasOwnProperty(`beforeAll`)) {
                    result.hasBeforeAll = true;
                }
                if (prototype.hasOwnProperty(`beforeBulk`)) {
                    result.hasBeforeBulk = true;
                }
                if (prototype.hasOwnProperty(`afterAll`)) {
                    result.hasAfterAll = true;
                }

                break;
            }
            case SYNCHRONISATION_TYPES.DOCUMENTS: {
                if (prototype.hasOwnProperty(`putMapping`)) {
                    throw Error(`You can't specify method "putMapping".`);
                } else if (prototype.hasOwnProperty(`putSettings`)) {
                    throw Error(`You can't specify method "putSettings".`);
                } else if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (!prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You must specify the "migrate" method.`);
                }

                this._parseBulkLikeInfo(info, infoCopy, result);

                break;
            }
            case SYNCHRONISATION_TYPES.PUT: {
                if (prototype.hasOwnProperty(`beforeAll`)) {
                    throw Error(`You can't specify method "beforeAll".`);
                } else if (prototype.hasOwnProperty(`beforeBulk`)) {
                    throw Error(`You can't specify method "beforeBulk".`);
                } else if (prototype.hasOwnProperty(`afterAll`)) {
                    throw Error(`You can't specify method "afterAll".`);
                } else if (prototype.hasOwnProperty(`reindex`)) {
                    throw Error(`You can't specify method "reindex".`);
                } else if (prototype.hasOwnProperty(`migrate`)) {
                    throw Error(`You cannot specify the "migration" method.`);
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
     * @param info
     * @param infoCopy
     * @param result
     * @private
     */
    _parseBulkLikeInfo(info, infoCopy, result) {
        if (_.isEmpty(info.INDEX) || !_.isString(info.INDEX)) {
            throw Error(`You have to specify an "INDEX" property.`);
        } else {
            result.index = elastic._indicesMap[info.INDEX];
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
     * @private
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

        if (info.OUTPUT_INDICES) {
            const myIndices = _.castArray(info.OUTPUT_INDICES);
            if (myIndices.some((index) => (typeof index !== `string`))) {
                throw Error(`OUTPUT_INDICES contains not string value, index mapping may be missing.`);
            }

            result.outputIndices.push(...myIndices);
            delete infoCopy.OUTPUT_INDICES;
        } else if (info.hasOwnProperty(`OUTPUT_INDICES`)) {
            throw Error(`OUTPUT_INDICES property is specified to nill value, index mapping may be missing.`);
        }

        if (info.DEPENDS_ON) {
            const myDataFrom = _.castArray(info.DEPENDS_ON);
            if (myDataFrom.some((index) => (typeof index !== `string`))) {
                throw Error(`DEPENDS_ON contains not string value, index mapping may be missing.`);
            }

            result.dependencyIndices.push(...myDataFrom);
            delete infoCopy.DEPENDS_ON;
        } else if (info.hasOwnProperty(`DEPENDS_ON`)) {
            throw Error(`DEPENDS_ON property is specified to nill value, index mapping may be missing.`);
        }

        //Remove duplicities
        result.inputIndices = _.uniq(_.compact(result.inputIndices));
        result.outputIndices = _.uniq(_.compact(result.outputIndices));
        result.dependencyIndices = _.uniq(_.compact(result.dependencyIndices));

        //Set ODMs
        result.inputIndices.forEach((index) => this.ODM[index] = (elastic.odms[index]));
        result.outputIndices.forEach((index) => this.ODM[index] = (elastic.odms[index]));
        //No dependencyIndices
    }

    /**
     * Checks if migration contains ES unsafe operation
     * @returns {boolean}
     */
    hasUnsafeOperation() {
        return this.constructor.toString().includes(`_callEs`);
    }

    /**
     * Internal beforeAll, calls the correct function
     * @returns {Promise<void>}
     */
    async runBeforeAll() {
        if (this._info.type === SYNCHRONISATION_TYPES.BULK && this._info.hasBeforeAll) {
            return this.beforeAll();

        } else if (this._info.type === SYNCHRONISATION_TYPES.DOCUMENTS) {
            return this.migrate();

        } else {
            //Do nothing
        }
    }

    /**
     * Internal migrate function, calls the correct function
     * @param document {{}}
     * @param alias {string} Index alias
     * @returns {Promise<undefined|string>}
     */
    async runMigrate(document = void 0, alias) {
        if (this._info.type === SYNCHRONISATION_TYPES.SCRIPT) {
            if (!this.__scriptCache) {
                const scriptWrapper = new ScriptWrapper({
                    isJavascript: !!document,
                    alias: alias
                });
                await this.migrate(scriptWrapper);
                this.__scriptCache = scriptWrapper._getResult();
            }

            if (!document) {
                return this.__scriptCache;
            } else {
                this.__scriptCache(document);
            }

        } else if (this._info.type === SYNCHRONISATION_TYPES.PUT) {
            //Do nothing

        } else if (this._info.type === SYNCHRONISATION_TYPES.DOCUMENTS) {
            //Find document to be updated
            const updateRecords = this.__updatedDocuments.filter((updatedDocument) => (updatedDocument.id === document._id && updatedDocument.alias === document._alias));
            if (updateRecords.length > 1) {
                throw Error(`Found multiple updateDocument functions for document ${document._alias}:${document._id}.`);

            } else if (updateRecords.length === 1) {
                const updateRecord = updateRecords[0];
                await updateRecord.func(document);
                updateRecord.used = true;
            }

        } else if (this._info.type === SYNCHRONISATION_TYPES.STOP) {
            throw Error(`Internal error - running migration method on STOP type.`);

        } else {
            //BULK / INDICES / SERIAL
            return this.migrate(document);
        }
    }

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
         * OUTPUT_INDICES
         * MESSAGE
         */
        throw Error(`'INFO' object not overridden!`);
    }

    /**
     * Synchronisation types
     * @returns {Readonly<{STOP: symbol, SERIAL: symbol, INDICES: symbol, PUT: symbol, SCRIPT: symbol, BULK: symbol, DOCUMENTS: symbol}>}
     */
    static get TYPE() {
        return SYNCHRONISATION_TYPES;
    }

    /**
     * Map of known ES indices
     * @returns {{}}
     */
    static get INDICES() {
        return INDICES;
    }

    /**
     * Function to make ES requests, when you have to use it and you know what you are doing...
     * @param path {string}
     * @param body {{}}
     * @param options {{}}
     * @returns {Promise<*>}
     * @private
     */
    async _callEs(path, body, options = void 0) {
        return elastic.callEs(path, body, options);
    }

    /**
     * Note function. Used message will be shown to the user at the end
     * @param message {string} Message to be shown
     */
    note(message) {
        if (!global.MESSAGES[this._versionNumbers]) {
            global.MESSAGES[this._versionNumbers] = {
                version: this.version,
                messages: []
            };
        }

        global.MESSAGES[this._versionNumbers].messages.push(message);
    }

    /**
     * Creates new document, if ID is specified, is must not exist yet
     * @param source {{}} Document object
     * @param id {string} Optional ID
     */
    createDocument(source, id = void 0) {
        if (this._info.type !== SYNCHRONISATION_TYPES.BULK && this._info.type !== SYNCHRONISATION_TYPES.DOCUMENTS) {
            throw Error(`'${this.version}': 'createDocument' function cannot be called from current synchronisation type!`);
        } else if (_.isNil(source) || !_.isObject(source) || _.isFunction(source)) {
            throw Error(`'${this.version}': Source of the new document in 'createDocument' function has to be an object.`);
        }

        this.__createdDocuments.push({ source, id, force: false });
    }

    /**
     * Updates existing document, may create new document when not exist
     * @param func {Function} Update function
     * @param fallbackSource {{}} Optional document object; when specified and document not exists yet, it will be used to create a anew one
     * @param id {string} Document ID
     */
    updateDocument(func, fallbackSource = void 0, id) {
        if (this._info.type !== SYNCHRONISATION_TYPES.DOCUMENTS) {
            throw Error(`'${this.version}': 'updateDocument' function cannot be called from current synchronisation type!`);
        } else if (_.isNil(func) || !_.isFunction(func)) {
            throw Error(`'${this.version}': You have to specify the update function for 'updateDocument' function.`);
        } else if (_.isNil(id) || !_.isString(id)) {
            throw Error(`'${this.version}': You have to specify the document ID for 'updateDocument' function.`);
        } else if (fallbackSource && !_.isObject(fallbackSource) || _.isFunction(fallbackSource)) {
            throw Error(`'${this.version}': Fallback source has to be a nill value or an object in 'updateDocument' function.`);
        }

        this.__updatedDocuments.push({ func, fallbackSource, id, alias: void 0, used: false, force: false });
    }

    /**
     * Creates new document, Rewrites the original one, when exists
     * @param source {{}} Document object
     * @param id {string} Document ID
     */
    forceCreateDocument(source, id) {
        if (this._info.type !== SYNCHRONISATION_TYPES.DOCUMENTS) {
            throw Error(`'${this.version}': 'forceCreateDocument' function cannot be called from current synchronisation type!`);
        } else if (_.isNil(source) || !_.isObject(source) || _.isFunction(source)) {
            throw Error(`'${this.version}': Source of the new document in 'forceCreateDocument' function has to be an object.`);
        } else if (_.isNil(id) || !_.isString(id)) {
            throw Error(`'${this.version}': You have to specify the document ID for 'forceCreateDocument' function.`);
        }

        this.__createdDocuments.push({ source, id, force: true });
        this.__updatedDocuments.push({
            func: (document) => {
                document._id = null;
            }, fallbackSource: void 0, id, alias: void 0, used: false, force: true });
    }

    /**
     * Puts mapping to the index. When multiple types are possible, you can use 'type' parameter to distinguish between them
     */
    async putMapping() {
        return void 0;
    }

    /**
     * Puts settings to the index. When multiple types are possible, you can use 'type' parameter to distinguish between them
     */
    async putSettings() {
        return void 0;
    }

    /**
     * Reindex function, use it to alter the index mapping and setting. In case of multiple index types, use 'type' parameter to distinguish them.
     * @param mapping {{}} Original mapping, alter it the way you need it
     * @param settings {{}} Original settings, alter it the way you need it
     * @returns {Promise<void>}
     */
    async reindex(mapping, settings) {
        throw Error(`'${this.version}': 'reindex' function not overridden!`);
    }

    /**
     * BeforeAll function, called once at the start of the migration.
     * @returns {Promise<void>}
     */
    async beforeAll() {
        throw Error(`'${this.version}': 'beforeAll' function not overridden!`);
    }

    /**
     * BeforeBulk function, called for every ES bulk response. Read only, do not change the data here!
     * @param bulk {Array<{}>} ES bulk
     * @returns {Promise<void>}
     */
    async beforeBulk(bulk) {
        throw Error(`'${this.version}': 'beforeBulk' function not overridden!`);
    }

    /**
     * Migration function. For most synchronisation types, you have to alter the document parameter in the way you need it.
     * @param document {{} | ScriptWrapper}
     * @returns {Promise<void>}
     */
    async migrate(document = void 0) {
        //Dummy
    }

    /**
     * AfterAll function. Called once at the end.
     * @returns {Promise<void>}
     */
    async afterAll() {
        throw Error(`'${this.version}': 'afterAll' function not overridden!`);
    }
}

module.exports = Migration;