'use strict';

const _ = require(`lodash`);

/**
 * Marks new document to be created -> virtual document
 * @param migrationIndex {number} Migration index on which behalf the document should be created
 * @param id {string} Optional ES ID of the document
 * @param source {{}} Source object of the document
 * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>} Map of all virtual documents
 */
function createDocument(migrationIndex, id = void 0, source, virtualDocuments) {
    let skipCheck = false;
    let iterationCache = virtualDocuments[`${migrationIndex}`];
    if (!iterationCache) {
        skipCheck = true;
        iterationCache = { knownIDs: new Map(), unknownIDs: new Set() };
        virtualDocuments[`${migrationIndex}`] = iterationCache;
    }

    if (!skipCheck && id && iterationCache.knownIDs.has(id)) {
        throw Error(`Creation of document with ID '${id}' has been already declared in the same migration.`);
    } else {
        if (id) {
            iterationCache.knownIDs.set(id, { source, isDeleted: false });
        } else {
            iterationCache.unknownIDs.add({ source });
        }
    }
}

/**
 * Marks the document as deleted / to be deleted
 * @param migrationIndex {number} Migration index on which behalf the document should be deleted
 * @param id {string} ES ID of the document
 * @param virtualDocuments {Record<string, {knownIDs: Map<string, {source: {}, isDeleted: boolean}>, unknownIDs: Set<{source: {}}>}>} Map of virtual documents
 * @param toDelete {Map<string, Set<number>>} Map of the documents to be deleted
 */
function deleteDocument(migrationIndex, id, virtualDocuments, toDelete) {
    let deleted = false;
    for (let actualMigrationIndex = migrationIndex - 1; actualMigrationIndex >= 0; actualMigrationIndex--) {
        //Mark as deleted in not yet saved documents
        const iterationCache = virtualDocuments[`${actualMigrationIndex}`];
        if (!iterationCache) {
            continue;
        }

        const existingVirtualDocument = iterationCache.knownIDs.get(id);
        if (existingVirtualDocument) {
            existingVirtualDocument.isDeleted = true;
            deleted = true;
            break;
        }
    }

    if (!deleted) {
        //Delete in ES
        let mySet = toDelete.get(id);
        if (mySet) {
            mySet.add(migrationIndex);

        } else {
            mySet = new Set();
            mySet.add(migrationIndex);
            toDelete.set(id, mySet);
        }
    }
}

/**
 * Merge given thread message objects into main messages
 * @param messages {Record<string, {version: string, messages: Array<string>}>}
 * @param threadMessages {Record<string, {version: string, messages: Array<string>}>}
 */
function mergeNotes(messages, threadMessages) {
    if (!_.isEmpty(threadMessages)) {
        Object.entries(threadMessages).forEach(([versionNumbers, data]) => {
            if (!messages[versionNumbers]) {
                messages[versionNumbers] = {
                    version: data.version,
                    messages: []
                };
            }

            messages[versionNumbers].messages.push(...data.messages);
        });
    }
}

/**
 * Parse given HR time to string
 * @param hrTime {[number, number]}
 * @returns {string}
 */
function formatHRTime(hrTime) {
    return `${(hrTime[0] + (hrTime[1] / 1000000000)).toFixed(2)}`;
}

/**
 * Promisifies inter-thread communication
 * @param communicationObject {*}
 * @param onMessage {function}
 */
function promisifyThreadCommunication(communicationObject, onMessage) {
    communicationObject._promiseCounter = 0;
    communicationObject._activePromises = new Map();
    communicationObject._sendMessage = function (data) {
        let myResolve, myReject;
        const promiseIndex = this._promiseCounter++;
        const promise = new Promise((resolve, reject) => {
            myResolve = (result) => {
                this._activePromises.delete(promiseIndex);
                resolve(result);
            };
            myReject = (result) => {
                this._activePromises.delete(promiseIndex);
                reject(result);
            };
        });
        this._activePromises.set(promiseIndex, { promise, resolve: myResolve, reject: myReject });
        data.promiseIndex = promiseIndex;
        this.postMessage(data);
        return promise;
    };


    communicationObject.on(`message`, onMessage);
}

module.exports = {
    createDocument, deleteDocument,
    mergeNotes, formatHRTime,
    promisifyThreadCommunication
};
