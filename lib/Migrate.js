'use strict';

const _ = require(`lodash`);
const os = require(`os`);
const nconf = require(`../config/config`);
const elastic = require(`./elastic`);
const logger = require(`./logger`);
const Node = require(`./Node`);
const optimizations = require(`./optimizations`);
const utils = require(`./utils`);

const SYNCHRONISATION_TYPES = require(`./synchronisationTypes`).synchronisationTypes;

const PARALLELIZATION = nconf.get(`options:parallelization`);
const PROCESS = PARALLELIZATION.process;
const MAX_SCRIPT_CHAINS = nconf.get(`options:limits:maxScriptChains`);
const MAX_INDEX_DELETES = nconf.get(`options:limits:maxIndexDeletes`);
const CHECK_INTERVALS = nconf.get(`es:checkIntervals`);
const RESTORE = nconf.get(`es:restore`);
const REPORT_SECONDS = nconf.get(`options:reportSeconds`);
const BACKUP = nconf.get(`backup`);
const MAX_SNAPSHOT_BYTES_PER_SEC = nconf.get(`options:maxSnapshotBytesPerSec`);
const SNAPSHOT_PREFIX = BACKUP.snapshotPrefix;


class Migrate {
    /**
     * @param repository {string}
     */
    constructor(repository) {
        /** @type {string} */
        this._repository = (!_.isNil(repository)) ? repository : void 0;
        /** @type {{tenant: string, esHost: string, indicesInfo: {INDICES: Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}, migrationPath: string, migrationsConfig: {}, elasticsearchConfig: {}, options: { loggerUid: string }}} */
        this._metadata = void 0;
        /** @type {boolean} */
        this._isInitialized = false;
        /** @type {string} */
        this._tenant = void 0;
        /** @type {string} */
        this._fullSnapshot = void 0;

        /** @type {Record<string, {version: string, messages: Array<string>}>} */
        this._messages = {};

        /** @type {number} */
        this._processes = PARALLELIZATION.processes;
        /** @type {[number, number]} */
        this._workerNumbers = [void 0, void 0];
    }

    /**
     * Initializes migrate instance, returns Migration class
     * @param tenant {string}
     * @param esHost {string}
     * @param indicesInfo {{INDICES: Record<string, {name: string, allowed: {initial: boolean, versions: Array<{from: string, allowed: boolean}>}, types: {initial: boolean, versions: Array<{from: string, types: boolean}>}, maxBulkSize: number}>}}
     * @param migrationPath {string}
     * @param migrationsConfig {{}}
     * @param elasticsearchConfig {{}}
     * @param options {{ loggerUid: string }}
     * @returns {Promise<Migration>}
     */
    async initialize(tenant, esHost, indicesInfo, migrationPath, migrationsConfig = void 0, elasticsearchConfig = void 0, options = void 0)  {
        if (migrationsConfig) {
            logger.setLoggerConfig(migrationsConfig, options?.loggerUid);
        }

        this._metadata = {
            tenant, esHost, indicesInfo, migrationPath, migrationsConfig, elasticsearchConfig, options
        };
        await elastic.createElastic(tenant, esHost, indicesInfo, elasticsearchConfig, options?.loggerUid);
        this._isInitialized = true;
        this._tenant = tenant;
        return require(`./Migration`);
    }

    /**
     * Returns "checkMigration" function
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
     * Migrates ES using specified migrations
     * @param migrations {Array<Migration>}
     * @param currentVersionString {string}
     * @param targetMeta {{}}
     * @param additional {{processInfo: [number, number, number]=, disableRepositories: boolean=}}
     * @returns {Promise<{messages: Array<string>, isSuccess: boolean}>}
     */
    async migrate(migrations, currentVersionString, targetMeta, additional = {}) {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        //Rewrite number of processes / threads
        if (_.isInteger(additional?.processInfo?.[0])) {
            this._processes = additional.processInfo[0];
        }
        if (_.isInteger(additional?.processInfo?.[1])) {
            this._workerNumbers[0] = additional.processInfo[1];
        }
        if (_.isInteger(additional?.processInfo?.[2])) {
            this._workerNumbers[1] = additional.processInfo[2];
        }

        const disabledRepositories = await this._disableRepositories((additional?.disableRepositories === true));

        await elastic.migrateToAliases();

        _identifyDependencies(migrations);

        logger.debug(`Transforming migrations...`);
        const nodes = [];
        const migrationBulks = _splitMigrations(migrations);
        migrationBulks.forEach((migrationBulk) => _createNodes(migrationBulk, nodes));
        _sortNodes(nodes);
        logger.info(`Migrations transformed into ${nodes.length} migration nodes.`);

        try {
            const isSuccess = await this._migrate(nodes, targetMeta);
            const messages = this._getMessages();
            return { isSuccess, messages };

        } catch (e) {
            logger.debug(e);
            return { isSuccess: false, messages: [] };

        } finally {
            try {
                await this._enableRepositories(disabledRepositories);
            } catch (e) {
                if (!_.isEmpty(disabledRepositories)) {
                    console.log(`!!! Unable to re-enable snapshot repositories. !!!`);
                    console.log(`Following snapshot repositories have "settings.readonly" property altered to "true": ${Object.keys(disabledRepositories).join(`, `)}`);
                    console.log(`In these snapshot repositories, the property "settings.readonly" must be manually unset.`);
                }
            }
        }
    }

    /**
     * Lists all existing snapshots
     * @returns {Promise<Array<string>>}
     */
    async listExistingSnapshots() {
        return this._getExistingSnapshots();
    }

    //Snapshots
    /**
     * Clears all existing snapshots
     * @returns {Promise<void>}
     */
    async clearSnapshots() {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        await this._deleteSnapshots();
    }

    /**
     * Restores snapshot (the latest by default)
     * @param snapshotName {string=} Optional snapshot name
     * @returns {Promise<boolean>}
     */
    async restoreSnapshot(snapshotName = void 0) {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        logger.info(`Restoring backup...`);
        let toRestoreSnapshot;
        if (_.isEmpty(snapshotName)) {
            const mySnapshots = await this._getExistingSnapshots();
            if (!_.isEmpty(mySnapshots)) {
                toRestoreSnapshot = mySnapshots.pop();
            }
        } else {
            toRestoreSnapshot = snapshotName;
        }

        if (toRestoreSnapshot) {
            await this._restoreSnapshot(toRestoreSnapshot);

            const nameParts = toRestoreSnapshot.split(`_`);
            if (nameParts[2] === `full`) {
                this._fullSnapshot = toRestoreSnapshot;
            }

            logger.info(`Backup restored.`);
            return true;
        } else {
            logger.debug(`Backup not restored.`);
            return false;
        }
    }

    /**
     * Creates snapshot of whole tenant
     * @returns {Promise<void>}
     */
    async createSnapshot() {
        if (!this._isInitialized) {
            throw Error(`Not initialized.`);
        }

        if (!this._fullSnapshot) {
            await this._backUp();
        }
    }

    //==================================================== INTERNAL ====================================================

    /**
     * Deletes all snapshots
     * @returns {Promise<void>}
     */
    async _deleteSnapshots() {
        logger.debug(`Deleting all snapshots...`);
        const myRepository = (!_.isNil(this._repository)) ? this._repository : BACKUP.repository;

        try {
            await elastic.callEs(`snapshot.delete`, {
                repository: myRepository,
                snapshot: `${SNAPSHOT_PREFIX}_${this._tenant}_*`
            });
            logger.debug(`All snapshots were deleted.`);
        } catch (e) {
            logger.debug(`Error when deleting snapshots - ${e}.`);
        }

        this._fullSnapshot = void 0;
    }

    /**
     * Returns list of existing snapshots
     * @returns {Promise<Array<string>>}
     */
    async _getExistingSnapshots() {
        const tenant = this._tenant;
        const myRepository = (!_.isNil(this._repository)) ? this._repository : BACKUP.repository;

        //Find existing snapshots
        let repo;
        try {
            repo = await elastic.callEs(`snapshot.get`, {
                repository: myRepository,
                snapshot: `${SNAPSHOT_PREFIX}_${tenant}_*`
            });
        } catch (e) {
            logger.debug(`Snapshot repository doesn't exist.`);
            return [];
        }

        let allSnapshots = repo.snapshots;
        if (_.isEmpty(allSnapshots)) {
            logger.debug(`No snapshot found.`);
            return [];
        }
        allSnapshots = allSnapshots.map((mySnapshot) => mySnapshot.snapshot);

        //To be super sure, check if snapshot name matches
        const mySnapshots = allSnapshots.filter((snap) => snap.startsWith(`${SNAPSHOT_PREFIX}_${tenant}_`));
        if (_.isEmpty(mySnapshots)) {
            logger.debug(`No matching snapshot found.`);
            return [];
        }

        //Sort by date
        mySnapshots.sort((a, b) => {
            //In theory, we could use fields like 'start_time' or 'end_time', but...
            const aParts = a.split(`_`);
            const aTime = parseInt(aParts[aParts.length - 1], 10);
            const bParts = b.split(`_`);
            const bTime = parseInt(bParts[bParts.length - 1], 10);
            return aTime - bTime;
        });

        return mySnapshots;
    }

    /**
     * Migrates the ES data
     * @param nodes {Array<Node>}
     * @param targetMeta {{}}
     * @returns {Promise<boolean>} Has been the migration successful?
     */
    async _migrate(nodes, targetMeta) {
        await elastic.lockTenant(this._tenant);

        let partialSnapshot;
        try {
            partialSnapshot = await this._backUp(nodes);
        } catch (e) {
            await elastic.unlockTenant(this._tenant);
            logger.fatal(`Unable to make a backup: ${e}`);

            if (e.toString().includes(`the contents of the repository do not match its expected state`) && _.isNil(this._repository)) {
                //There is some kind of corruption in migration repo... This sometimes happen
                console.log(`This error means there is some kind of data mismatch in the snapshot (backup) repository '${BACKUP.repository}'.`);
                console.log(`This repository serves only for migration purposes. If there are no saved backups you may need (e.g. from other tenants), feel free to delete this repository, this should solve the problem.`);
                console.log(`To do so, make following request: 'DELETE <elastic_url>/_snapshot/${BACKUP.repository}'. Then run the migration script again.`);
            }
            throw e;
        }

        let isSuccess = true;
        try {
            const graph = _connectNodes(nodes);

            //Load meta
            const metaMapping = await elastic._metaOdm.getMapping();
            const sourceMeta = Object.values(metaMapping)[0]?.mappings?._meta ?? {};
            await elastic.checkExistingIndices(this._tenant, sourceMeta);
            optimizations.setAverageDocumentSizes(sourceMeta?._optimizations?.averageDocumentSizes);

            //Save in-progress state
            const progressMeta = Object.assign({}, sourceMeta);
            progressMeta.inProgress = true;
            await elastic._metaOdm.putMapping({
                _meta: progressMeta
            });

            //Restrict refreshes and replicas
            await elastic.restrictIndices(this._tenant, nodes);

            const hostWorkers = await _computeHostsWorkers();

            const start = process.hrtime();
            await this._runGraph(graph, hostWorkers);
            const end = process.hrtime(start);
            logger.info(`All migrations successfully processed in ${utils.formatHRTime(end)} s.`);

            //Restore refreshes and replicas
            await elastic.releaseIndices();

            //Update meta
            const finalMeta = Object.assign({}, sourceMeta, targetMeta);
            if (!finalMeta._optimizations) {
                finalMeta._optimizations = {};
            }
            finalMeta._optimizations.averageDocumentSizes = optimizations.getAverageDocumentSizes();
            await elastic.saveNewIndices(this._tenant, finalMeta);
            delete finalMeta.inProgress;
            await elastic._metaOdm.putMapping({
                _meta: finalMeta
            });

        } catch (e) {
            isSuccess = false;
            logger.fatal(`Migration failed, main reason: ${e}`);
        }

        await this._cleanUp(isSuccess, partialSnapshot);

        return isSuccess;
    }

    //==================================================== BACK UP ====================================================

    /**
     * Disables not used repositories when allowed
     * @param disableRepositories {boolean =}
     * @returns {Promise<{}>}
     */
    async _disableRepositories(disableRepositories = false) {
        if (!disableRepositories) {
            return {};
        }

        const myRepository = (!_.isNil(this._repository)) ? this._repository : BACKUP.repository;
        const existingRepositories = await elastic.callEs(`snapshot.getRepository`, {});

        const toDisableRepositoryNames = [];
        Object.keys(existingRepositories).forEach((repoName) => {
            if ((repoName !== myRepository) && (_.get(existingRepositories[repoName], `settings.readonly`) !== `true`)) {
                toDisableRepositoryNames.push(repoName);
            }
        });

        const disabledRepositories = {};
        if (toDisableRepositoryNames.length > 0) {
            logger.info(`Disabling following snapshot repositories: ${toDisableRepositoryNames.join(`, `)}`);
            if (!_.isNil(this._repository)) {
                logger.warn(`Custom specified repository '${this._repository}' wasn't disabled on purpose.`);
            }

            for (const toDisableRepositoryName of toDisableRepositoryNames) {
                const originalRepo = existingRepositories[toDisableRepositoryName];
                _.set(originalRepo, `name`, toDisableRepositoryName);
                delete originalRepo.uuid;
                disabledRepositories[toDisableRepositoryName] = originalRepo;

                const alteredRepo = _.cloneDeep(originalRepo);
                _.set(alteredRepo, `settings.readonly`, true);

                await elastic.callEs(`snapshot.createRepository`, alteredRepo);
            }
        }

        return disabledRepositories;
    }

    /**
     * Backups the ES data
     * @param nodes {Array<Node>} All migration nodes
     * @returns {Promise<string>}
     */
    async _backUp(nodes = void 0) {
        if (!_.isEmpty(this._fullSnapshot)) {
            //Already snapshoted
            return void 0;

        } else if (_.isEmpty(nodes)) {
            logger.info(`Running backup...`);

            //Snapshot whole tenant
            this._fullSnapshot = await this._makeSnapshot();

            logger.info(`Data backed up.`);

        } else {
            logger.info(`Running backup...`);

            //Backup only affected indices, if possible
            let usageMap = void 0;
            let snapshotIndices = void 0;
            if (BACKUP.onlyUsedIndices) {
                const result = await _getIndexUsage(this._tenant, nodes);
                usageMap = result.usageMap;
                snapshotIndices = result.snapshotIndices;
            }
            const partialSnapshot = await this._makeSnapshot(usageMap, snapshotIndices);
            logger.info(`Data backed up.`);

            return partialSnapshot;
        }
    }

    /**
     * Makes snapshot of tenant or used indices (if usageMap specified)
     * @param usageMap {Record<string, boolean>} Optional map with indices usage
     * @param snapshotIndices {Array<string>} Names of indices that are snapshot
     * @returns {Promise<string>} Created snapshot name
     */
    async _makeSnapshot(usageMap = void 0, snapshotIndices = void 0) {
        logger.debug(`Creating backup...`);

        const tenant = this._tenant;

        if (_.isNil(this._repository)) {
            //Create new repository
            await elastic.callEs(`snapshot.createRepository`, {
                name: BACKUP.repository,
                type: `fs`,
                settings: {
                    location: BACKUP.location,
                    max_snapshot_bytes_per_sec: MAX_SNAPSHOT_BYTES_PER_SEC
                }
            });

        } else {
            //Custom repository has been specified
            //Check if repository exists, it will throw otherwise
            await elastic.callEs(`snapshot.getRepository`, {
                name: this._repository
            });
        }

        const indices = (_.isEmpty(usageMap)) ? `${tenant}_*` : Object.keys(usageMap).filter((index) => usageMap[index]);
        if (_.isEmpty(indices)) {
            return void 0;

        } else {
            return this._createSnapshot(indices, snapshotIndices);
        }
    }

    //==================================================== RUNNING ====================================================

    /**
     * Runs graph of nodes, performs migrations in (nearly) optimal way
     * @param graph {Node} Graph input node
     * @param hostWorkers {Map<string, number>}
     * @returns {Promise<void>}
     */
    async _runGraph(graph, hostWorkers) {
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
                while ((nodesQueue.length > 0) && (runningNodes.length < this._processes)) {
                    //Run node
                    const node = nodesQueue.shift();
                    node.run(runningNodes, this._messages, this._metadata, hostWorkers, this._workerNumbers);
                }
            } while (!_.isEmpty(runningNodes));

        } catch (e) {
            logger.debug(`Error happened, waiting for all the running nodes to settle...`);

            if (runningNodes.length > 0) {
                runningNodes.forEach((runningNode) => {
                    if (runningNode?.terminate) {
                        runningNode.terminate();
                    }
                });
                await Promise.allSettled(runningNodes);
            }
            logger.debug(`All running nodes have settled.`);

            throw e;
        }
    }

    /**
     * Returns migration messages
     * @returns {Array<string>}
     */
    _getMessages() {
        const myMessages = [];

        if (!_.isEmpty(this._messages)) {
            //Message was noted

            //Make sure they will be shown in the right order
            const messageArray = Object.entries(this._messages).map(([key, values]) => {
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

    //==================================================== CLEAN UP ====================================================

    /**
     * Restores repository original states
     * @param disabledRepositories {{}} Original repository states
     * @returns {Promise<void>}
     */
    async _enableRepositories(disabledRepositories = {}) {
        const disabledRepositoryNames = Object.keys(disabledRepositories);
        if (disabledRepositoryNames.length > 0) {
            logger.info(`Enabling following snapshot repositories: ${disabledRepositoryNames.join(`, `)}`);
            for (const disabledRepositoryName of disabledRepositoryNames) {
                await elastic.callEs(`snapshot.createRepository`, disabledRepositories[disabledRepositoryName]);
            }
        }
    }

    /**
     * Performs cleanup after the migration has finished
     * @param isSuccess {boolean} Has the migration been successful?
     * @param partialSnapshot {string}
     * @returns {Promise<void>}
     */
    async _cleanUp(isSuccess, partialSnapshot = void 0) {
        if (!isSuccess) {
            let counter = 0;
            let isRestored = false;
            const mySnapshot = this._fullSnapshot || partialSnapshot;
            if (_.isEmpty(mySnapshot)) {
                throw Error(`No backup to restore.`);
            }

            do {
                try {
                    logger.info(`Restoring backup...`);
                    await this._restoreSnapshot(mySnapshot);
                    isRestored = true;
                    logger.info(`Backup restored.`);

                } catch (e) {
                    const retry = (counter++ < RESTORE.maxRetries);

                    if (retry && !e.skipRetry) {
                        logger.error(`Unable to restore the backup - ${e}!`);
                        logger.debug(e);
                        logger.info(`Waiting before retry (try ${counter} of ${RESTORE.maxRetries})`);
                        await new Promise((resolve) => { setTimeout(resolve, RESTORE.waitTime); });
                    } else {
                        logger.fatal(`Unable to restore the backup.`);
                        logger.fatal(`Crashing, the backup will not be removed and the tenant will remain locked.`);
                        throw e;
                    }
                }
            } while (!isRestored);
        }

        if (!_.isEmpty(partialSnapshot)) {
            logger.info(`Retaining the backup.`);
        }

        await elastic.unlockTenant(this._tenant);
    }

    /**
     * Creates snapshot of given name and with given indices
     * @param indices {string | Array<string>} Indices to snapshot
     * @param snapshotIndices {Array<string>} Array of indices that are snapshot
     * @returns {Promise<string>}
     */
    async _createSnapshot(indices, snapshotIndices = void 0) {
        const tenant = this._tenant;
        const myRepository = (!_.isNil(this._repository)) ? this._repository : BACKUP.repository;
        const isFull = (!_.isArray(indices));
        if (isFull) {
            snapshotIndices = void 0;
        }
        const snapshot = (isFull) ? `${SNAPSHOT_PREFIX}_${tenant}_full_${Date.now()}` : `${SNAPSHOT_PREFIX}_${tenant}_${Date.now()}`;
        let nextReport = Date.now() + (REPORT_SECONDS * 1000);

        //Start creating
        await elastic.callEs(`snapshot.create`, {
            repository: myRepository,
            snapshot: snapshot,
            wait_for_completion: false,
            indices: indices,
            ignore_unavailable: true,
            metadata: {
                snapshotIndices: snapshotIndices
            }
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
                if (nextReport < Date.now()) {
                    nextReport = Date.now() + (REPORT_SECONDS * 1000);
                    logger.info(`Waiting for ES to create a backup...`);
                }

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
     * @param snapshot {string}
     * @returns {Promise<void>}
     */
    async _restoreSnapshot(snapshot) {
        const clusterStatus = await elastic.callEs(`cluster.health`);
        if (clusterStatus?.status === `red` || clusterStatus?.status === `RED`) {
            console.log(`!!! The ES cluster is in the RED state. The snapshot cannot be restored. !!!`);
            console.log(`If you wish the migration script to restore the snapshot, please fix the ES cluster to GREEN or YELLOW status and run the migration script again.`);
            const err = Error(`The ES cluster is in the RED state, backup cannot be restored.`);
            err.skipRetry = true;
            throw err;
        }

        const myRepository = (!_.isNil(this._repository)) ? this._repository : BACKUP.repository;
        let nextReport = Date.now() + (REPORT_SECONDS * 1000);

        //Get indices in snapshot
        const details = await elastic.callEs(`snapshot.get`, {
            repository: myRepository,
            snapshot: snapshot
        });
        const indices = details.snapshots?.[0]?.indices ?? [];
        const snapshotIndices = details.snapshots?.[0]?.metadata?.snapshotIndices ?? null;

        await this._deleteExistingIndices(snapshotIndices);

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
                    if (nextReport < Date.now()) {
                        nextReport = Date.now() + (REPORT_SECONDS * 1000);
                        logger.info(`Waiting for ES to restore the backup...`);
                    }

                    await new Promise((resolve) => {
                        setTimeout(resolve, CHECK_INTERVALS.restore);
                    });
                }

                // eslint-disable-next-line no-constant-condition
            } while (true);
        }

        //Restored, remove in-progress status
        const metaMapping = await elastic._metaOdm.getMapping();
        const sourceMeta = Object.values(metaMapping)[0]?.mappings?._meta ?? {};
        delete sourceMeta.inProgress;
        await elastic._metaOdm.putMapping({
            _meta: sourceMeta
        });
    }

    /**
     * Deletes given indices and other uuid versions
     * @param snapshotIndices {Array<string>}
     * @returns {Promise<void>}
     */
    async _deleteExistingIndices(snapshotIndices) {
        const stats = await elastic.callEs(`indices.stats`, {
            index: `${this._tenant}_*`
        });
        const existingIndices = Object.keys(stats.indices);
        const tenantIndices = existingIndices.filter((existingIndex) => existingIndex.startsWith(`${this._tenant}_`));

        let toDeleteIndices = [];
        if (!_.isArray(snapshotIndices)) {
            toDeleteIndices = tenantIndices;
        } else {
            for (const tenantIndex of tenantIndices) {
                const parts = tenantIndex.split(`-`)[0].split(`_`);
                parts.shift();
                const mainPart = parts.join(`_`);
                const key = elastic.nameToKey[mainPart];
                if (snapshotIndices.includes(key)) {
                    toDeleteIndices.push(tenantIndex);
                }
            }
        }

        toDeleteIndices = _.uniq(_.compact(toDeleteIndices));
        if (toDeleteIndices.length > 0) {
            if (_.isArray(toDeleteIndices) && (toDeleteIndices.length >= MAX_INDEX_DELETES)) {
                let processed = 0;
                while (processed < toDeleteIndices.length) {
                    //Ensure we don't exceed HTTP line limit
                    const processedArray = toDeleteIndices.slice(processed, processed + MAX_INDEX_DELETES);
                    await elastic.callEs(`indices.delete`, {
                        index: processedArray
                    });
                    processed += MAX_INDEX_DELETES;
                }

            } else {
                await elastic.callEs(`indices.delete`, {
                    index: toDeleteIndices
                });
            }
        }
    }
}

/**
 * Identifies migrations dependencies and replaces the references with instances
 * @param migrations {Array<Migration>}
 */
function _identifyDependencies(migrations) {
    for (const migration of migrations) {
        if (!_.isEmpty(migration._info.dependencyMigrations)) {
            for (let i = 0; i < migration._info.dependencyMigrations.length; i++) {
                const DependentMigration = migration._info.dependencyMigrations[i];

                let isFound = false;
                for (const innerMigration of migrations) {
                    if (innerMigration.constructor === DependentMigration) {
                        if (migration.__filepath !== innerMigration.__filepath) {
                            throw Error(`Migration '${migration.version}' from file '${migration.__filepath}' cannot depend on migration from another file (${innerMigration.__filepath})`);
                        } else if (migration.__classIndex <= innerMigration.__classIndex) {
                            throw Error(`Migration '${migration.version}' cannot depend on migration that is exported after this one.`);
                        }
                        migration._info.dependencyMigrations[i] = innerMigration;
                        isFound = true;
                        break;
                    }
                }
                if (!isFound) {
                    throw Error(`Unable to identify all the dependencies`);
                }
            }
        }
    }
}

/**
 * Returns map with indices usage info
 * @param tenant {string}
 * @param nodes {Array<Node>} All migration nodes
 * @returns {Promise<{usageMap: Record<string, boolean>, snapshotIndices: Array<string>}>}
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

    return {
        usageMap: usageMap,
        snapshotIndices: usedIndices
    };
}

//================================================== TRANSFORMATIONS ==================================================

/**
 * Splits migrations by SERIAL synchronisation type
 * @param migrations {Array<Migration>}
 * @returns {Array<Migration> | Array<Array<Migration>>}
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
                        //Index matches
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

/**
 * Sort migration nodes using its latest migration
 * @param nodes {Array<Node>}
 */
function _sortNodes(nodes) {
    nodes.sort((a, b) => {
        if (a._migrations.length <= 0 || b._migrations.length <= 0) {
            return 0;
        }

        //Versions of the last migrations in compared nodes
        const aVersions = a._migrations[a._migrations.length - 1]._versionNumbers.split(/[.:]/).map((version) => parseInt(version, 10));
        const bVersions = b._migrations[b._migrations.length - 1]._versionNumbers.split(/[.:]/).map((version) => parseInt(version, 10));

        const count = (aVersions.length === 4 && bVersions.length === 4) ? 4 : 3;
        for (let i = 0; i < count; i++) {
            if (aVersions[i] < bVersions[i]) {
                return -1;
            } else if (aVersions[i] > bVersions[i]) {
                return 1;
            }
        }

        return 0;
    });
}

//====================================================== RUNNING ======================================================

/**
 * Connects nodes into the graph, according its dependencies
 * @param nodes {Array<Node>} All nodes to be connected
 * @returns {Node} Input point of graph
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
 * Computes optimal worker count for each available ES host and returns map (<hostname> -> <workerCount>)
 * @returns {Promise<Map<string, number>>}
 */
async function _computeHostsWorkers() {
    const hostWorkers = new Map();

    const nodesInfo = await elastic.callEs(`nodes.info`);
    if (nodesInfo.nodes) {
        for (const node of Object.values(nodesInfo.nodes)) {
            if (_.isArray(node?.roles) && node.roles.some((role) => role.includes(`data`))) {
                //Check only data-typed roles
                if (_.isString(node?.name) && _.isInteger(node?.jvm?.mem?.heap_max_in_bytes) && _.isInteger(node?.thread_pool?.write?.size)) {
                    const targetWorkers = _getWorkerCountForHost(node.thread_pool.write.size, node.jvm.mem.heap_max_in_bytes);

                    if ((!hostWorkers.has(node.name)) || (hostWorkers.get(node.name) > targetWorkers)) {
                        hostWorkers.set(node.name, targetWorkers);
                    }
                }
            }
        }
    }

    //Also set some default value
    hostWorkers.set(`*default*`, _getWorkerCountForHost(PROCESS.defaultNodeThreads, PROCESS.defaultNodeRamGiB * 1024 * 1024 * 1024));
    const localHostname = os.hostname();
    if (!hostWorkers.has(localHostname)) {
        //Ensure localhost is set
        hostWorkers.set(localHostname, _getWorkerCountForHost(os.cpus().length, (os.totalmem() / 2)));
    }

    return hostWorkers;
}

/**
 * Returns optimal worker count based on ES node write thread count and heap
 * @param threads {number}
 * @param heap {number}
 * @returns {number}
 */
function _getWorkerCountForHost(threads, heap) {
    const targetThreads = (threads ** (1 - PROCESS.hostLoadExponent)) * PROCESS.workersPerESThread * PROCESS.maximumESLoad;
    const heapLimit = Math.floor(heap / (PROCESS.ramPerESThreadGiB * 1024 * 1024 * 1024));
    return Math.max(1, Math.min(targetThreads, heapLimit));
}


module.exports = Migrate;
