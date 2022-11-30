'use strict';

const _ = require(`lodash`);
const nconf = require(`../config/config`);
const elastic = require(`./elastic`);
const logger = require(`./logger`);
const Node = require(`./Node`);
const optimizations = require(`./optimizations`);

const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;

const MAX_SCRIPT_CHAINS = nconf.get(`options:limits:maxScriptChains`);
const MAX_INDEX_DELETES = nconf.get(`options:limits:maxIndexDeletes`);
const CHECK_INTERVALS = nconf.get(`es:checkIntervals`);
const BACKUP = nconf.get(`backup`);
const SNAPSHOT_PREFIX = BACKUP.snapshotPrefix;

class Migrate {
    constructor(repository) {
        this._repository = (!_.isNil(repository)) ? repository : void 0;
        this._isInitialized = false;
        this._tenant = void 0;
        this._snapshot = void 0;
    }

    /**
     * Initializes migrate instance, returns Migration class
     * @param tenant {string}
     * @param esHost {string}
     * @param indicesInfo {{}}
     * @returns {Promise<Migration>}
     */
    async initialize(tenant, esHost, indicesInfo)  {
        await elastic.createElastic(tenant, esHost, indicesInfo);
        this._isInitialized = true;
        this._tenant = tenant;
        return require(`./Migration`);
    }

    /**
     * Returns checkMigration function
     * @returns {Function}
     */
    getCheckMigrationFunction() {
        const checkMigration = function (migrationSource) {
            if (!this._isInitialized) {
                throw Error(`Not initialized.`);
            }

            return {
                isStop: (migrationSource.INFO?.TYPE === SYNCHRONISATION_TYPES.STOP)
            };
        };
        return checkMigration.bind(this);
    }

    /**
     * Clears all existing snapshots
     * @returns {Promise<void>}
     */
    async clear() {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        await deleteSnapshots(this);
        this._snapshot = void 0;
    }

    /**
     * Restores latest snapshot
     * @returns {Promise<boolean>}
     */
    async restore() {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        const snapshot = await restoreSnapshot(this);
        if (snapshot) {
            this._snapshot = snapshot;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates snapshot of whole tenant
     * @returns {Promise<void>}
     */
    async backup() {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        if (!this._snapshot) {
            await _backUp(this);
        }
    }

    /**
     * Migrates ES using specified migrations
     * @param migrations {Array<Migration>}
     * @param currentVersionString {string}
     * @param targetMeta {{}}
     * @returns {Promise<{messages: Array<string>, isSuccess: boolean}>}
     */
    async migrate(migrations, currentVersionString, targetMeta) {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        await elastic.migrateToAliases();

        const nodes = createNodes(migrations);
        const isSuccess = await migrate(this, nodes, targetMeta);
        const messages = getMessages();

        return { isSuccess, messages };
    }
}

module.exports = Migrate;

/**
 * Deletes all snapshots
 * @param self {Migrate}
 * @returns {Promise<void>}
 */
async function deleteSnapshots(self) {
    logger.debug(`Deleting all snapshots...`);
    const myRepository = (!_.isNil(self._repository)) ? self._repository : BACKUP.repository;

    try {
        await elastic.callEs(`snapshot.delete`, {
            repository: myRepository,
            snapshot: `${SNAPSHOT_PREFIX}_${self._tenant}_*`
        });
    } catch (e) {
        //OK
    }

    logger.debug(`All snapshots were deleted.`);
}

/**
 * Restores latest snapshot, if available
 * @param self {Migrate}
 * @returns {Promise<string>}
 */
async function restoreSnapshot(self) {
    logger.debug(`Trying to restore the latest snapshot...`);
    const tenant = self._tenant;
    const myRepository = (!_.isNil(self._repository)) ? self._repository : BACKUP.repository;

    //Find existing snapshots
    let repo;
    try {
        repo = await elastic.callEs(`snapshot.get`, {
            repository: myRepository,
            snapshot: `${SNAPSHOT_PREFIX}_${tenant}_*`
        });
    } catch (e) {
        logger.debug(`Snapshot repository doesn't exist.`);
        return void 0;
    }

    const allSnapshots = repo.snapshots;
    if (_.isEmpty(allSnapshots)) {
        logger.debug(`No snapshot found.`);
        return void 0;
    }

    //To be super sure, check if snapshot name matches
    const mySnapshots = allSnapshots.filter((snap) => snap.snapshot.split(`_`).length === 3
        && snap.snapshot.split(`_`)[0] === SNAPSHOT_PREFIX && snap.snapshot.split(`_`)[1] === tenant
        && _.isFinite(parseInt(snap.snapshot.split(`_`)[2], 10)));
    if (_.isEmpty(mySnapshots)) {
        logger.debug(`No matching snapshot found.`);
        return void 0;
    }

    //Sort by date
    mySnapshots.sort((a, b) => {
        //In theory we could use fields like 'start_time' or 'end_time', but...
        const aTime = parseInt(a.snapshot.split(`_`)[2], 10);
        const bTime = parseInt(b.snapshot.split(`_`)[2], 10);
        return aTime - bTime;
    });
    const latestSnapshot = mySnapshots.pop();

    //Delete indices
    await elastic.callEs(`indices.delete`, {
        index: `${tenant}_*`
    });

    //Restore dev
    await _restoreSnapshot(self, latestSnapshot.snapshot);

    logger.info(`Latest snapshot has been restored.`);

    return latestSnapshot.snapshot;
}

/**
 * Creates nodes from migrations
 * @param migrations {Array<Migration>} Loaded migrations
 * @returns {Array<Node>} Migration nodes
 */
function createNodes(migrations) {
    logger.debug(`Transforming migrations...`);

    const nodes = [];
    const migrationBulks = _splitMigrations(migrations);
    migrationBulks.forEach((migrationBulk) => _createNodes(migrationBulk, nodes));

    logger.info(`Migrations transformed into ${nodes.length} migration nodes.`);

    return nodes;
}

/**
 * Migrates the ES data
 * @param self {Migrate} This context
 * @param nodes {Array<Node>}
 * @param targetMeta {{}}
 * @returns {Promise<boolean>} Has been the migration successful?
 */
async function migrate(self, nodes, targetMeta) {
    await elastic.lockTenant(self._tenant);

    let backupInfo;
    try {
        backupInfo = await _backUp(self, nodes);
    } catch (e) {
        await elastic.unlockTenant(self._tenant);
        logger.fatal(`Unable to make a backup: ${e}`);
        process.exit(1);
    }

    let isSuccess = true;
    try {
        const graph = _connectNodes(nodes);

        //Load meta
        const metaMapping = await elastic._metaOdm.getMapping();
        const sourceMeta = Object.values(metaMapping)[0]?.mappings?._meta ?? {};
        await elastic.checkExistingIndices(self._tenant, sourceMeta);
        optimizations.createCache(sourceMeta?._optimizations);

        //Restrict refreshes and replicas
        await elastic.restrictIndices(self._tenant, nodes);

        //Run monitoring function
        elastic.runMonitor();

        const start = process.hrtime();
        await _runGraph(graph);
        const end = process.hrtime(start);
        logger.info(`All migrations successfully processed in ${end} s.`);

        //Stop monitoring function
        elastic.stopMonitor();

        //Restore refreshes and replicas
        await elastic.releaseIndices();

        //Update meta
        targetMeta._optimizations = optimizations.getCache();
        await elastic.saveNewIndices(self._tenant, targetMeta);
        await elastic._metaOdm.putMapping({
            _meta: targetMeta
        });

    } catch (e) {
        isSuccess = false;
        logger.fatal(`Migration failed, main reason: ${e}`);
    }

    await _cleanUp(self, isSuccess, backupInfo);
    await elastic.unlockTenant(self._tenant);

    return isSuccess;
}

/**
 * Returns migration messages
 * @returns {Array<string>}
 */
function getMessages() {
    const myMessages = [];

    const messages = global.MESSAGES;
    if (!_.isEmpty(messages)) {
        //Message was noted

        //Make sure they will be shown in the right order
        const messageArray = Object.entries(messages).map(([key, values]) => {
            return {
                key, values
            };
        });
        messageArray.sort((a, b) => {
            const aVersions = a.key.split(/[.:]/);
            const bVersions = b.key.split(/[.:]/);
            if (aVersions[0] !== bVersions[0]) {
                return parseInt(aVersions[0], 10) - parseInt(bVersions[0], 10);
            } else if (aVersions[1] !== bVersions[1]) {
                return parseInt(aVersions[1], 10) - parseInt(bVersions[1], 10);
            } else if (aVersions[2] !== bVersions[2]) {
                return parseInt(aVersions[2], 10) - parseInt(bVersions[2], 10);
            } else if (aVersions[3] && bVersions[3]) {
                return parseInt(aVersions[3], 10) - parseInt(bVersions[3], 10);
            } else {
                return 0;
            }
        });

        messageArray.forEach((messages) => {
            messages.values.messages.forEach((message) => {
                myMessages.push(`'${messages.values.version}': ${message}`);
            });
        });
    }

    return myMessages;
}


//================================================== TRANSFORMATIONS ==================================================

/**
 * Splits migrations by SERIAL synchronisation type
 * @param migrations {Array<Migration>}
 * @returns {Array<Migration> | Array<Array<Migration>>}
 * @private
 */
function _splitMigrations(migrations) {
    logger.debug(`Splitting migrations...`);

    const migrationBulks = [];
    let migrationBuffer = [];
    for (const migration of migrations) {
        if (migration._info.type === SYNCHRONISATION_TYPES.SERIAL) {
            //We have got a SERIAL type
            if (!_.isEmpty(migrationBuffer)) {
                //If buffer is not empty, flush to bulks
                migrationBulks.push(migrationBuffer);
                migrationBuffer = [];
            }

            //And push SERIAL migration as a new chunk
            migrationBulks.push(migration);
        } else {
            //Else just buffer migration
            migrationBuffer.push(migration);
        }
    }
    //At the end, flush buffered migrations
    if (!_.isEmpty(migrationBuffer)) {
        migrationBulks.push(migrationBuffer);
    }
    logger.debug(`Migrations split up.`);

    return migrationBulks;
}

/**
 * Transforms migrations into nodes. Several migrations may be merged into one node.
 * @param migrationBulk {Array<Migration> | Migration}
 * @param nodes {Array<Node>}
 * @private
 */
function _createNodes(migrationBulk, nodes) {
    logger.debug(`Creating migration nodes from migration bulk...`);

    if (!_.isArray(migrationBulk)) {
        //It is a SERIAL type
        nodes.push(new Node(migrationBulk));
    }

    for (let i = 0; i < migrationBulk.length; i++) {
        const migration = migrationBulk[i];

        if (migration._info.type === SYNCHRONISATION_TYPES.INDICES || migration._info.type === SYNCHRONISATION_TYPES.STOP) {
            //We can't merge INDICES / STOP type
            nodes.push(new Node(migration));

        } else {
            //BULK / DOCUMENTS / PUT / SCRIPT
            if (migration._info.isProcessed) {
                //Already processed
                continue;
            }

            /*
             * Go through the next migrations and buffer them
             *
             * Algorithm:
             * if (
             *   noIntersection(
             *     migration(index + outputIndices)                 //A
             *     nextMigration(inputIndices + dependencyIndices), //1
             *   ),
             *   noIntersection(
             *     migration(index + inputIndices + outputIndices)  //B
             *     nextMigration(outputIndices),                    //2
             *   ),
             *   noIntersection(
             *     migration(inputIndices + outputIndices)  //C
             *     nextMigration(index),                    //3
             *   )
             * ) {
             *   if (nextMigration(index) === migration(index)) {
             *     //Merge
             *
             *   } else {
             *     //Continue
             *
             *   }
             * } else {
             *   //Break merging
             * }
             */

            const buffer = [migration];
            const index = migration._info.index;
            let bufferIndexOutputIndices = _.uniq([index, ...migration._info.outputIndices]);                           //A
            let bufferAllIndices = _.uniq([index, ...migration._info.inputIndices, ...migration._info.outputIndices]);  //B
            let bufferInputOutputIndices = _.uniq([...migration._info.inputIndices, ...migration._info.outputIndices]); //C

            for (let j = i + 1; j < migrationBulk.length; j++) {
                const nextMigration = migrationBulk[j];

                const nextMigrationInputIndices = _.uniq([...nextMigration._info.inputIndices, ...nextMigration._info.dependencyIndices]);  //1
                const nextMigrationOutputIndices = _.uniq([...nextMigration._info.outputIndices]);                                          //2
                const nextMigrationIndex = nextMigration._info.index ? [nextMigration._info.index] : [];                                        //3

                if (!bufferIndexOutputIndices.some((element) => nextMigrationInputIndices.includes(element)) &&
                    !bufferAllIndices.some((element) => nextMigrationOutputIndices.includes(element)) &&
                    !bufferInputOutputIndices.some((element) => nextMigrationIndex.includes(element))) {

                    if (nextMigration._info.index === index) {
                        //Index matches (we don't care about index types, these are handled in node)
                        buffer.push(nextMigration);

                        //Update buffered indices
                        bufferIndexOutputIndices = _.uniq([...bufferIndexOutputIndices, ...migration._info.outputIndices]);                                     //A
                        bufferAllIndices = _.uniq([...bufferAllIndices, ...migration._info.inputIndices, ...migration._info.outputIndices]);                    //B
                        bufferInputOutputIndices = _.uniq([...bufferInputOutputIndices, ...migration._info.inputIndices, ...migration._info.outputIndices]);    //C

                    } else {
                        continue;
                    }

                } else {
                    break;
                }
            }

            //If BULK is enforced or if any migration in chain is of type BULK, use BULK for all of them
            if (MAX_SCRIPT_CHAINS < 0 || buffer.some((myMigration) => myMigration._info.type === SYNCHRONISATION_TYPES.BULK)) {
                const mergeNode = new Node();
                for (const myMigration of buffer) {
                    mergeNode.addMigration(myMigration);
                }
                nodes.push(mergeNode);

            } else {
                //Else check what is the best way to merge these
                let bulkFallback = false;
                const puts = buffer.filter((myMigration) => myMigration._info.type === SYNCHRONISATION_TYPES.PUT);
                const scripts = buffer.filter((myMigration) => myMigration._info.type === SYNCHRONISATION_TYPES.SCRIPT);
                const documents = buffer.filter((myMigration) => myMigration._info.type === SYNCHRONISATION_TYPES.DOCUMENTS);

                if (!_.isEmpty(documents) && !_.isEmpty(scripts)) {
                    //Filter PUT type out
                    const filteredBuffer = buffer.filter((migration) => (migration._info.type !== SYNCHRONISATION_TYPES.PUT));

                    //And check how many times is the SCRIPT chain interrupted
                    let currentType = filteredBuffer[0]._info.type;
                    let scriptChains = (currentType === SYNCHRONISATION_TYPES.SCRIPT) ? 1 : 0;
                    for (let j = 1; j < filteredBuffer.length && scriptChains <= MAX_SCRIPT_CHAINS; j++) {
                        const newMigration = filteredBuffer[j];

                        if (newMigration._info.type === SYNCHRONISATION_TYPES.SCRIPT && currentType !== SYNCHRONISATION_TYPES.SCRIPT) {
                            currentType = newMigration._info.type;
                            scriptChains++;
                        } else if (newMigration._info.type !== SYNCHRONISATION_TYPES.SCRIPT && currentType === SYNCHRONISATION_TYPES.SCRIPT) {
                            currentType = newMigration._info.type;
                        }
                    }

                    //Have we crossed the border?
                    bulkFallback = (scriptChains > MAX_SCRIPT_CHAINS);
                }

                if (bulkFallback) {
                    //We have crossed the border, fallback to BULK
                    const mergeNode = new Node();
                    for (const myMigration of buffer) {
                        mergeNode.addMigration(myMigration);
                    }
                    nodes.push(mergeNode);

                } else {
                    //We can merge better way
                    if (_.isEmpty(scripts) && !_.isEmpty(puts)) {
                        //No SCRIPT types -> send all PUT and then all DOCUMENTS
                        let mergeNode = new Node();
                        for (const myMigration of puts) {
                            mergeNode.addMigration(myMigration);
                        }
                        nodes.push(mergeNode);

                        if (!_.isEmpty(documents)) {
                            mergeNode = new Node();
                            for (const myMigration of documents) {
                                mergeNode.addMigration(myMigration);
                            }
                            nodes.push(mergeNode);
                        }

                    } else {
                        //Send interrupted chains, allow PUT and SCRIPT merge
                        while (buffer.length > 0) {
                            const sendingType = buffer[0]._info.type;

                            const mergeNode = new Node();
                            while ((buffer.length > 0) &&
                            (buffer[0]._info.type === sendingType ||
                                (buffer[0]._info.type === SYNCHRONISATION_TYPES.PUT && sendingType === SYNCHRONISATION_TYPES.SCRIPT) ||
                                (buffer[0]._info.type === SYNCHRONISATION_TYPES.SCRIPT && sendingType === SYNCHRONISATION_TYPES.PUT))) {
                                mergeNode.addMigration(buffer.shift());
                            }
                            nodes.push(mergeNode);
                        }
                    }
                }
            }
        }
    }

    logger.debug(`Migration bulk transformed to migration node.`);
}


//====================================================== BACK UP ======================================================

/**
 * Back ups the ES data
 * @param self {Migrate} This context
 * @param nodes {Array<Node>} All migration nodes
 * @returns {Promise<{usageMap: {}, snapshot: string}>}
 * @private
 */
async function _backUp(self, nodes = void 0) {
    if (!_.isEmpty(self._snapshot)) {
        //Already snapshoted
        return void 0;

    } else if (_.isEmpty(nodes)) {
        logger.debug(`Running backup...`);

        //Snapshot whole tenant
        self._snapshot = await _snapshot(self);

        logger.info(`Data backed up.`);

    } else {
        logger.debug(`Running backup...`);

        //Backup only affected indices, if possible
        let indicesOnly = BACKUP.onlyUsedIndices;
        if (indicesOnly) {
            //Is it safe to use?
            for (const node of nodes) {
                if (node._migrations.some((migration) => migration.hasUnsafeOperation())) {
                    logger.info(`Some of the migrations contain unsafe operations -> will snapshot whole tenant.`);
                    indicesOnly = false;
                    break;
                }
            }
        }

        const usageMap = (indicesOnly) ? await _getIndexUsage(self._tenant, nodes) : void 0;
        const snapshot = await _snapshot(self, usageMap);
        logger.info(`Data backed up.`);

        return {
            usageMap: usageMap,
            snapshot: snapshot
        };
    }
}

/**
 * Returns map with indices usage info
 * @param tenant {string}
 * @param nodes {Array<Node>} All migration nodes
 * @returns {Promise<{}>}
 * @private
 */
async function _getIndexUsage(tenant, nodes) {
    logger.debug(`Checking used indices...`);

    //Check existing indices
    const stats = await elastic.callEs(`indices.stats`, {
        index: `${tenant}_*`
    });

    //Prepare index map
    const usageMap = Object.keys(stats.indices).reduce((sum, index) => {
        sum[index] = false;
        return sum;
    }, {});

    //Find used indices
    let usedIndices = [];
    for (const node of nodes) {
        if (BACKUP.backupInputIndices) {
            usedIndices.push(...node._inputIndices);
        }
        usedIndices.push(...node._outputIndices);

    }
    usedIndices = _.uniq(_.compact(usedIndices));

    //Get all types of used ODMs
    const allUsedOdms = usedIndices.map((usedIndex) => elastic.odms[usedIndex]);
    const existingUsedOdms = await elastic.getExistingModels(allUsedOdms);

    //Backup used ODMs
    for (const Odm of existingUsedOdms) {
        const realIndex = await Odm.getIndex();
        usageMap[realIndex] = true;
    }

    return usageMap;
}

/**
 * Makes snapshot of tenant or used indices (if usageMap specified)
 * @param self {Migrate}
 * @param usageMap {Object} Optional map with indices usage
 * @returns {Promise<string>} Created snapshot name
 * @private
 */
async function _snapshot(self, usageMap = void 0) {
    logger.debug(`Creating snapshot...`);

    const tenant = self._tenant;

    if (_.isNil(self._repository)) {
        //Create new repository
        await elastic.callEs(`snapshot.createRepository`, {
            name: BACKUP.repository,
            type: `fs`,
            settings: {
                location: BACKUP.location
            }
        });

    } else {
        //Custom repository has been specified
        //Check if repository exists, it will throw otherwise
        await elastic.callEs(`snapshot.getRepository`, {
            name: self._repository
        });
    }

    const indices = (_.isEmpty(usageMap)) ? `${tenant}_*` : Object.keys(usageMap).filter((index) => usageMap[index]);
    if (_.isEmpty(indices)) {
        return void 0;

    } else {
        return _createSnapshot(self, indices);
    }
}


//====================================================== RUNNING ======================================================

/**
 * Connects nodes into the graph, according its dependencies
 * @param nodes {Array<Node>} All nodes to be connected
 * @returns {Node} Input point of graph
 * @private
 */
function _connectNodes(nodes) {
    logger.debug(`Connecting migration nodes...`);

    const SERIAL_TYPE = `_SERIAL_`; //Special (dummy) index for SERIAL type - to be super sure it will really be serial
    const startPoint = new Node();  //Start (dummy) node
    const lockPoints = {};

    //Connect nodes
    for (const node of nodes) {
        //Note indices by its purpose
        const outputIndices = [...node._outputIndices];
        if (node._type === SYNCHRONISATION_TYPES.SERIAL) {
            //For SERIAL type add special (dummy) index
            outputIndices.push(SERIAL_TYPE);
        }
        const inputOnlyIndices = _.difference(_.uniq([...node._inputIndices, ...node._dependencyIndices]), outputIndices);
        const allIndices = [...inputOnlyIndices, ...outputIndices];

        //Create initial lock points for not yet used indices
        allIndices.forEach((myIndex) => {
            if (_.isEmpty(lockPoints[myIndex])) {
                lockPoints[myIndex] = {
                    source: startPoint,     //Latest node that written to the index
                    readingNodes: [],       //Nodes that read from the source node
                };
            }
        });

        for (const inputOnlyIndex of inputOnlyIndices) {
            //Here we want only to read from the index -> connect to original source node and push current node as a reading node
            lockPoints[inputOnlyIndex].source.connectTo(node);
            lockPoints[inputOnlyIndex].readingNodes.push(node);
        }

        for (const outputIndex of outputIndices) {
            //We want to write to (and maybe even read from) the index -> connect necessary nodes and set lock point into this node
            if (lockPoints[outputIndex].readingNodes.length > 0) {
                lockPoints[outputIndex].readingNodes.forEach((readingNode) => {
                    readingNode.connectTo(node);
                });
            } else {
                lockPoints[outputIndex].source.connectTo(node);
            }

            lockPoints[outputIndex].source = node;
            lockPoints[outputIndex].readingNodes = [];
        }

        if (_.isEmpty(inputOnlyIndices) && _.isEmpty(outputIndices)) {
            startPoint.connectTo(node);
        }
    }

    logger.debug(`Migration nodes connected into the graph.`);
    return startPoint;
}

/**
 * Runs graph of nodes, performs migrations in (nearly) optimal way
 * @param graph {Node} Graph input node
 * @returns {Promise<void>}
 * @private
 */
async function _runGraph(graph) {
    logger.info(`Running migration nodes...`);

    const nodesQueue = [];
    const runningNodes = [];

    //Run first (dummy) node
    graph.run(runningNodes);

    try {
        do {
            //Single node finished
            const finishedMigration = await Promise.race(runningNodes);

            //Buffer newly runnable nodes
            nodesQueue.push(...finishedMigration.finish(runningNodes));
            while ((nodesQueue.length > 0) && (runningNodes.length < optimizations.getQueueSize())) {
                //Run node
                const node = nodesQueue.shift();
                node.run(runningNodes);
            }
        } while (!_.isEmpty(runningNodes));

    } catch (e) {
        logger.debug(`Error happened, waiting for all the running migration nodes to settle...`);

        if (runningNodes.length > 0) {
            await Promise.allSettled(runningNodes);
        }
        logger.debug(`All running nodes have settled.`);

        throw e;
    }
}


//====================================================== CLEAN UP ======================================================

/**
 * Performs cleanup after the migration has finished
 * @param self {Migrate} This context
 * @param isSuccess {boolean} Has the migration been successful?
 * @param backupInfo {{}}
 * @returns {Promise<void>}
 * @private
 */
async function _cleanUp(self, isSuccess, backupInfo = void 0) {
    logger.debug(`Running clean up...`);

    if (!isSuccess) {
        const indices = (!_.isEmpty(backupInfo?.usageMap)) ? await _getUsedIndices(self._tenant, backupInfo.usageMap) : `${self._tenant}_*`;
        if (!_.isEmpty(indices)) {
            //Delete snapshoted indices

            if (_.isArray(indices) && (indices.length >= MAX_INDEX_DELETES)) {
                let processed = 0;
                while (processed < indices.length) {
                    //Ensure we don't exceed HTTP line limit
                    const processedArray = indices.slice(processed, processed + MAX_INDEX_DELETES);
                    await elastic.callEs(`indices.delete`, {
                        index: processedArray
                    });
                    processed += MAX_INDEX_DELETES;
                }

            } else {
                await elastic.callEs(`indices.delete`, {
                    index: indices
                });
            }
        }

        const snapshot = self._snapshot || backupInfo.snapshot;
        if (!_.isEmpty(snapshot)) {
            await _restoreSnapshot(self, snapshot);
        }

        logger.info(`Backup restored.`);
    }

    if (!_.isEmpty(backupInfo?.snapshot)) {
        //Delete all snapshots
        await deleteSnapshots(self);
        logger.debug(`Backup cleaned up.`);
    }
}

/**
 * Checks which indices has been used during migration
 * @param tenant {string}
 * @param usageMap {{}}
 * @returns {Promise<Array<string>>}
 * @private
 */
async function _getUsedIndices(tenant, usageMap) {
    //Check existing indices
    const stats = await elastic.callEs(`indices.stats`, {
        index: `${tenant}_*`
    });
    const allIndices = Object.keys(stats.indices);
    const notUsedIndices = Object.keys(usageMap).filter((index) => !usageMap[index]);

    return _.difference(allIndices, notUsedIndices);
}

/**
 * Creates snapshot of given name and with given indices
 * @param self {Migrate}
 * @param indices {Array<string>} Indices to snapshot
 * @returns {Promise<string>}
 * @private
 */
async function _createSnapshot(self, indices) {
    const tenant = self._tenant;
    const myRepository = (!_.isNil(self._repository)) ? self._repository : BACKUP.repository;
    const snapshot = `${SNAPSHOT_PREFIX}_${tenant}_${Date.now()}`;

    //Start creating
    await elastic.callEs(`snapshot.create`, {
        repository: myRepository,
        snapshot: snapshot,
        wait_for_completion: false,
        indices: indices,
        ignore_unavailable: true
    });

    //Wait less time for the first time
    await new Promise((resolve) => {
        setTimeout(resolve, CHECK_INTERVALS.snapshot / 2);
    });

    do {
        //Check if created
        const snapshotResponse = await elastic.callEs(`snapshot.get`, {
            repository: myRepository,
            snapshot: snapshot
        });
        const mySnapshots = snapshotResponse.snapshots;
        if (!_.isArray(mySnapshots) || _.isEmpty(mySnapshots)) {
            throw Error(`Unable to create backup, not found correct snapshot to check the state.`);
        }

        const mySnapshotState = Object.values(mySnapshots)[0]?.state;
        if (mySnapshotState === `FAILED` || mySnapshotState === `PARTIAL`) {
            throw Error(`Unable to create backup, snapshot results to state '${mySnapshotState}'.`);
        }

        if (mySnapshotState === `SUCCESS`) {
            //Finished
            break;

        } else if (mySnapshotState === `IN_PROGRESS`) {
            await new Promise((resolve) => {
                setTimeout(resolve, CHECK_INTERVALS.snapshot);
            });

        } else {
            throw Error(`Unable to create backup, unknown snapshot state '${mySnapshotState}'.`);
        }

        // eslint-disable-next-line no-constant-condition
    } while (true);

    return snapshot;
}

/**
 * Restores snapshot of given name
 * @param self {Migrate}
 * @param snapshot {string}
 * @returns {Promise<void>}
 * @private
 */
async function _restoreSnapshot(self, snapshot) {
    const myRepository = (!_.isNil(self._repository)) ? self._repository : BACKUP.repository;

    //Get indices in snapshot
    const details = await elastic.callEs(`snapshot.get`, {
        repository: myRepository,
        snapshot: snapshot
    });
    const indices = details.snapshots?.[0]?.indices ?? [];

    //Run restore
    await elastic.callEs(`snapshot.restore`, {
        repository: myRepository,
        snapshot: snapshot,
        wait_for_completion: false,
        include_aliases: true,
        include_global_state: true
    });

    //Wait less time for the first time
    await new Promise((resolve) => {
        setTimeout(resolve, CHECK_INTERVALS.restore / 2);
    });

    //Check if restore is completed
    for (const index of indices) {
        do {
            //Gent index progress
            const result = await elastic.callEs(`indices.recovery`, {
                index: index
            });
            const shards = Object.values(result)?.[0]?.shards ?? [];
            const snapshotShards = shards.filter((shard) => (shard.type === `SNAPSHOT`));

            if (snapshotShards.length > 0 && snapshotShards.every((shard) => (shard.stage === `DONE`))) {
                //Index restored
                break;

            } else {
                //Index not restored yet
                await new Promise((resolve) => {
                    setTimeout(resolve, CHECK_INTERVALS.restore);
                });
            }

            // eslint-disable-next-line no-constant-condition
        } while (true);
    }
}
