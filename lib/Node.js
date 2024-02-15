'use strict';

const _ = require(`lodash`);
const { fork } = require(`node:child_process`);
const path = require(`path`);

const elastic = require(`./elastic`);
const optimizations = require(`./optimizations`);
const utils = require(`./utils`);
const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;

class Node {
    constructor(migration) {
        /**
         * Node type, corresponds to the migration types
         * @type {string}
         */
        this._type = void 0;

        /**
         * List of all migrations in this node
         * @type {Array<Migration>}
         */
        this._migrations = [];

        /**
         * Index of this node for BULK-like types
         * @type {string}
         */
        this._index =  void 0;

        /**
         * List of all input indices, includes main index
         * @type {Array<string>}
         */
        this._inputIndices = [];

        /**
         * List of all output indices, includes main index
         * @type {Array<string>}
         */
        this._outputIndices = [];

        /**
         * List of all dependency indices
         * @type {Array<string>}
         */
        this._dependencyIndices = [];

        /**
         * List of all input points === nodes that must be completed before this node can be run
         * @type {Array<Node>}
         */
        this._inputPoints = [];

        /**
         * List of all output points === nodes that cannot run until this node is completed
         * @type {Array<Node>}
         */
        this._outputPoints = [];

        /**
         * Is node finished?
         * @type {boolean}
         */
        this._finished = false;

        /**
         * When node is running, this is the running promise
         * @type {Promise<Node>}
         */
        this._promise = void 0;

        /**
         * Child process
         * @type {ChildProcess}
         */
        this._process = void 0;

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
     * Runs this node as a new process and adds its promise to an array
     * @param runningNodes {Array<Promise<Node>>}
     * @param globalMessages {Record<string, {version: string, messages: Array<string>}>}
     * @param metadata {{tenant: string, esHost: string, indicesInfo: {INDICES: Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}, migrationPath: string, migrationsConfig: {}, elasticsearchConfig: {}}}
     * @param hostWorkers {Map<string, number>}
     * @param workerNumbers {[number, number]}
     */
    run(runningNodes, globalMessages, metadata, hostWorkers, workerNumbers) {
        if (_.isNil(this._type)) {
            //Starting (dummy) node
            this._promise = Promise.resolve(this);
            runningNodes.push(this._promise);

        } else {
            const myProcess = new fork(path.join(__dirname, `process.js`), process.argv.slice(2), {
                serialization: `advanced`
            });
            myProcess._promise = void 0;
            myProcess._sendMessage = function (data) {
                let myResolve, myReject;
                const promise = new Promise((resolve, reject) => {
                    myResolve = resolve;
                    myReject = reject;
                });
                this._promise = { promise, resolve: myResolve, reject: myReject };
                this.send(data);
                return promise;
            };

            myProcess.on(`message`, (message) => {
                switch (message.type) {
                    case `__done`: {
                        myProcess._promise.resolve(message.returnValue ?? {});
                        break;
                    }

                    case `__error`: {
                        const errorMessage = message.returnValue?.message ?? `Unknown error`;
                        try {
                            myProcess._promise.reject(errorMessage);
                        } catch (e) {
                            throw Error(errorMessage);
                        }
                        break;
                    }

                    default: {
                        const errorMessage = `Unexpected process message '${message}'`;
                        try {
                            myProcess._promise.reject(errorMessage);
                        } catch (e) {
                            throw Error(errorMessage);
                        }
                    }
                }
            });

            this._process = myProcess;
            this._promise = this._run(globalMessages, metadata, hostWorkers, workerNumbers);
            this._promise.terminate = () => {
                try {
                    this._process.send({
                        type: `__terminate`
                    });
                } catch (e) {
                    //Nothing
                }
            };
            runningNodes.push(this._promise);
        }
    }

    /**
     * Finishes node running, must be called once the run promise is finished
     * Removes the promise from an array and returns new nodes that may run (if any)
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
     * Runs migration in a new process
     * @param globalMessages {Record<string, {version: string, messages: Array<string>}>}
     * @param metadata {{tenant: string, esHost: string, indicesInfo: {INDICES: Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}, migrationPath: string, migrationsConfig: {}, elasticsearchConfig: {}}}
     * @param hostWorkers {Map<string, number>}
     * @param workerNumbers {[number, number]}
     * @returns {Promise<Node>}
     */
    async _run(globalMessages, metadata, hostWorkers, workerNumbers) {
        const allAverageDocumentSizes = optimizations.getAverageDocumentSizes();
        const myAverageDocumentSizes = {};

        const allRestrictions = optimizations.getRestrictions();
        const myRestrictions = {
            enabled: allRestrictions.enabled,
            fields: allRestrictions.fields,
            data: {}
        };

        const RandomOdm = Object.values(elastic.odms)[0];

        const allIndices = _.compact(_.uniq([...this._inputIndices, ...this._outputIndices]));
        for (const index of allIndices) {
            const alias = elastic.odms[index].alias;

            const indexAverageDocumentSize = allAverageDocumentSizes?.[alias];
            if (indexAverageDocumentSize && indexAverageDocumentSize >= 1) {
                myAverageDocumentSizes[alias] = indexAverageDocumentSize;
            }

            for (const [index, data] of Object.entries(allRestrictions.data)) {
                const dataAlias = RandomOdm._parseIndex(index).alias;
                if (alias === dataAlias) {
                    myRestrictions.data[index] = data;
                }
            }
        }

        const returnValue = await this._process._sendMessage({
            type: `__run`,
            data: {
                metadata: metadata,
                hostWorkers: hostWorkers,
                workerNumbers: workerNumbers,

                restrictions: myRestrictions,
                averageDocumentSizes: myAverageDocumentSizes,

                type: this._type,
                index: this._index,
                inputIndices: this._inputIndices,
                outputIndices: this._outputIndices,
                migrations: this._migrations.map((migration) => {
                    return {
                        filepath: migration.__filepath,
                        classIndex: migration.__classIndex,
                        version: migration.version,
                        versionNumbers: migration._versionNumbers,
                    };
                }),
                backgroundToProcessData: this._migrations.map((migration) =>
                    migration._info.dependencyMigrations.map((dependencyMigration) =>
                        dependencyMigration.__finalMigrationData
                    )
                )
            }
        });
        utils.mergeNotes(globalMessages, returnValue.messages);
        for (let i = 0; i < this._migrations.length; i++) {
            this._migrations[i].__finalMigrationData = returnValue.processToBackgroundData[i];
        }

        for (const outputIndex of this._outputIndices) {
            const alias = elastic.odms[outputIndex].alias;
            const newAverageDocumentSizes = returnValue.averageDocumentSizes?.[alias];
            if (newAverageDocumentSizes && newAverageDocumentSizes >= 1) {
                optimizations.updateAverageDocumentSizes(alias, newAverageDocumentSizes);
            }

            const newRestrictionsData = returnValue.restrictions?.data;
            if (newRestrictionsData) {
                for (const [index, data] of Object.entries(newRestrictionsData)) {
                    const dataAlias = RandomOdm._parseIndex(index).alias;
                    if (alias === dataAlias) {
                        optimizations.updateRestrictions(index, data);
                    }
                }
            }
        }

        return this;
    }
}

module.exports = Node;