# ES Migrations BackEnd

This is the second part of the ES Migration tool responsible for processing the migrations. Following readme is the same for both parts.


# ES Migration tool
This is ES migration tool usable for updating ES data/mapping/settings to the latest version.

This tool connects to the ES instance, checks chosen tenant version (based on "meta" index), loads available versions and use them to update ES to the latest version.

This tools uses ES-ODM library internally and requires its indices naming convention (that means index names like `<tenant>_<name>[_<type>]`).

Only update ("up" operation) is supported, downgrade ("down" operation) is not supported.


## Usage
### Running
To run the tool, you always have to choose the tenant. Other arguments are optional:
`npm run migrate <tenant> [-- [-m <migrationFolder>] [-u <esHost>] [-r <repository>] [-h] [--dev] [--clear]]`
- `tenant` - Tenant which should be updated
- `migrationFolder` - Manually specify migration folder, by default it is read from the configuration
- `esHost` - Manually specify ES host, by default it is read from the configuration
- `repository` - Custom snapshot repository to be used
- `-h` - Show help
- `--dev` - Switch to use the dev mode
- `--clear` - Switch to use the clear mode


- Dev mode
    - Intended to be used when developing. In this mode, the tenant snapshot is restored at the start (if available) and at the end the snapshot back up is not deleted.
        - This allows you to easily develop your migration and made multiple updates in it, changing still the same data.

- Clear mode
    - This just deletes all backed up tenant snapshots. Can be used after dev mode, once the migration is finished.
        - All tenant snapshots are also deleted automatically when running migration tool WITHOUT dev mode (in normal run).


### Migration folder
All migrations are loaded from a migration folder. Default location is set in configuration file and can be also specified manually when running the migrations. For migration folder, these things are mandatory:

#### Indices mapping file
- Migration folder must contain `indices.js` mapping file. This file contains all available indices in ES. It must export object `INDICES` and optionally even object `TYPES`.
    - `INDICES` - Contains info about ALL existing indices (except reserved index `meta`).
        - Each record must contain filed `name` with the main part of index name.
        - In case the index uses multiple types, the record must also specify `types: true`.
        - In case you want to limit the number of records in bulks used for reading/writing data to ES, specify the value for the record as `maxBulkSize: 100`. This number will be used as upper bound.
    - `TYPES` - For indices which uses types. This allows you to create typed indices, so you don't have to specify the type ad-hoc in migration `INFO` property or filter it in code.
        - Keys must not collide with `INDICES` keys.
        - You have to specify the master type as `typeof: INDICES.DOCUMENTS_ALL` (reference to `INDICES` object) and also specify whether your type should be inclusive or exclusive.
            - Use ``inclusiveType`: `myType` `` in order to match the type. You can reference this record everywhere, just like `INDICES` records.
            - Use ``exclusiveType`: `myType` `` in order to NOT match the type (but match all the other ones). You can reference this record only for main `INDEX`, not for manual `INDICES`. This is because you cannot receive exclusively typed ODM model.
            - In both cases you can use ES wildcards (`?`, `*`).

#### Version mapping file
- Migration folder must contain `versions.js` mapping file. This file contains mapping of ES tools.
  This mapping is necessary, as different migrations may be created for different ES version, use different functions, etc. and it is necessary to make them processed with the correct tool.
- This file exports array with objects, each object must contain:
    - `from` - From (starting) version; inclusive.
    - `to` - To (final) version; inclusive.
    - `version` - String with tool to be used. This should match tool alias in `package.json` file.
- Versions cannot overlap.

#### Patch folders
- Migrations are located inside patch folders. This folder must be named with major and minor migration version, e.g. `22.0`.
- Migration files are named with syntax `<patchVersion>[_optionalName].js`.
- Name of the patch folder and the migration file creates the migration version, e.g. `22.0.1`.

### Create migration
- You can create the migration file either manually, or automatically by calling `npm run createMigration [<SyncType = BULK>] [<optionalName>]`.
- Creating the migration using the script also creates timestamp file in order to cause GIT conflicts.

- You can also create special pre-/post- migrations. These are created using word `pre` or `post` in migration name instead of the patch version and can be located inside the patch folder (local usage), or in migration folder (global usage).


## Migration synchronisation types
- Each migration file must export single class or an array of classes. Each class must extends from `Migration` class.
- Each class must specify static getter `INFO` which returns main information about the migration. This info differs depending on the migration type, but always contains property `TYPE` which specifies the migration synchronisation type.
- Follows all possible synchronisation types with brief explanation.


### Serial type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.SERIAL
            // Nothing else is specified
        };
    }
    
    //Called one time only
    async migrate() {
        //const ODMs = this.ODM;
        //Call ODM functions
    }
}

module.exports = [MyMigration];
```
- This is synchronous type and should NOT be used if possible.
- All other preceding types must finish before this type starts and all upcoming types cannot start until this type finishes.
- You specify the code inside `migrate` function.
    - Using `this.ODM` you have access to all defined ODM models (except for typed models with exclusive type).
- In most cases you DON'T want to use this type, as it is too slow.


### Indices type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.INDICES,
            INDICES: [this.INDICES.DOCUMENTS]   //- Shortcut for both INPUT_ and OUTPUT_ INDICES
            //INPUT_INDICES: []  //For indices you only want to read from
            //OUTPUT_INDICES: [] //For indices you only want to write to
            //DEPENDS_ON: []     //For indices you dont want to read (nor write), but it is necessary to be finished before this Migration (e.g. you are caching some data from the index and you will use this cache in this migration)
        };
    }

    //Called one time only
    async migrate() {
        //const ODMs = this.ODM;
        //Call ODM functions
    }
}

module.exports = [MyMigration];
```
- This type can run in parallel with other types, but for most things this is still too slow.
- If possible, try not to use this type, though there may be some cases when usage of this type is necessary.
- You specify the code inside `migrate` function.
    - Use `this.ODM` to access yours defined ODM models (except for typed models with exclusive type).
        - It is necessary to correctly specify input indices (`INPUT_INDICES` - you only read) and output indices (`OUTPUT_INDICES` - you only write). If you need both (or not sure), just specify the index in both, or use `INDICES` as a shortcut.


### Bulk type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.BULK,
            INDEX: this.INDICES.DOCUMENTS    //You have ti specify one main index - on this index all the stuff will happen in background
            //INCLUSIVE_TYPE: `type` or EXCLUSIVE_TYPE: `type` //Ad-hoc way to specify index type for typed indices - only when you neeed it
            //DEPENDS_ON: []  //Same as previsous type
            
            //INDICES: []        //Same as previous types
            //INPUT_INDICES: []
            //OUTPUT_INDICES: []
        };
    }
    
    /*
    //Called once for each existing index type
    async reindex(mapping, settings, type = void 0) {
         //Alter mapping / settings objects
         //e.g.:
         //delete mapping.properties.content;
         //settings.index.number_of_shards = 2;
    }
    */
    
    //Called one time only, at the beginning
    async beforeAll() {
        //Custom initialization
    }
    
    //Called once for each bulk
    async beforeBulk(bulk) {
        //const ODMs = this.ODM;
        
        //Do not alter bulk, read only
    }

    //Called once for each document
    async migrate(document) {
        //Alter document object
        
        //document._alias - read-only
        //document._type - read-only
        //document._id - change to:
        //  - null => delete document
        //  - void 0 => reindex under random ID
        //  - 'newID' => change ID to specified one
    }
    
    //Called one time only
    async afterAll() {
        /*
        e.g. this.createDocument({ /* New document content */ }, ?id, ?type);
        */
    }
}

module.exports = [MyMigration];
```
- This type can run in parallel and can merge with some other types (serves as a fallback for some more specialized types).
- This may be the most used type, very versatile.
- In case you need to reindex (change index mapping/settings), specify `reindex` function.
    - Then just alter the original properties (you receive them as method arguments) as needed.
- This type reads and writes all the index data in bulks.
- You can use method `beforeAll` for initial initialization.
- You can use method `beforeBulk` to fetch some data from current bulk.
    - You shouldn't alter the bulk data here, read only.
- You can use method `migrate` to alter the document record. Whatever changes you made will be saved.
    - You can also alter `_id` property to delete the document / change the ID.
- You can use method `afterAll` to do some stuff after all the bulks have been processed.
- If you specified `INDICES` (or `INPUT_INDICES` or `OUTPUT_INDICES`) you will also receive the ODMs as in previous types.
- In all the methods you can call method `this.createDocument({ /* New document content */ }, ?id, ?type);` to create new document.
    - You specify the document body as first parameter.
    - You MAY specify custom ID as second parameter, or it is automatically generated. It is mandatory this ID must NOT exist.
    - In case if typed index (and not type specified), you can also specify the type as last parameter (or all possible types are used).


### Documents type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.DOCUMENTS,
            INDEX: this.INDICES.DOCUMENTS
            //INCLUSIVE_TYPE: `type` or EXCLUSIVE_TYPE: `type`    //See BULK type for info
            //DEPENDS_ON: []
            
            //INPUT_INDICES: []
            //INDICES: []
            //OUTPUT_INDICES: []
        };
    }

    //Called one time only
    async migrate() {
        /*
        this.forceCreateDocument({ /* New document content */ }, id, ?type);
        
        this.createDocument({ /* New document content */ }, ?id, ?type);
        
        this.updateDocument((document) => { /* Same as bulk migrate function */ }, ?fallbackObject, id, ?type);
        */
    }
}

module.exports = [MyMigration];
```
- This type can run in parallel and can merge with some other types.
- You should use this type when you need to create new documents or alter (or delete) some specific documents (you know the IDs).
- You specify all your code in method `migrate`.
- Again you can use `INDICES` (or `INPUT_INDICES` or `OUTPUT_INDICES`) if you need the ODMs to fetch some data.

- To create new document you can use method `this.createDocument({ /* New document content */ }, ?id, ?type);` (same as in BULK)
    - You specify the document body as first parameter.
    - You MAY specify custom ID as second parameter, or it is automatically generated. It is mandatory this ID must NOT exist.
    - In case if typed index (and not type specified), you can also specify the type as last parameter (or all possible types are used).
- Or you can use method `this.forceCreateDocument({ /* New document content */ }, id, ?type);`
    - Similar to `createDocument`, but requires `id` specified and rewrites existing document.
    - You specify the document body as first parameter.
    - You MUST specify custom ID as second parameter. Existing document is rewritten.
    - In case if typed index (and not type specified), you can also specify the type as last parameter (or all possible types are used).
- To update document you can use method `this.updateDocument((document) => { /* Same as bulk migrate function */ }, ?fallbackObject, id, ?type);`
    - This will update existing document, or MAY create new one if the document doesn't exist.
    - You must specify update function as the first parameter.
        - Very similar to BULK type `migrate` function - you receive document instance and alter as needed.
        - You can alter `_id` to delete/reindex the document.
        - You cannot use nested create/update methods inside this function.
    - You MAY specify fallback object as second parameter. This object will be used when document doesn't exist to create it.
    - You MUST specify document ID.
    - In case if typed index (and not type specified), you can also specify the type as last parameter (or all possible types are used).


### Put type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.PUT,
            INDEX: this.INDICES.DOCUMENTS
            //INCLUSIVE_TYPE: `type` or EXCLUSIVE_TYPE: `type` //See BULK type for info
            //DEPENDS_ON: []
            
            //INPUT_INDICES: []
            //INDICES: []
            //OUTPUT_INDICES: []
        };
    }
    
    //Called once for each existing index type
    async putMapping(type = void 0) {
        return {
            //New mapping properties
        };
    }
    
    //Called once for each existing index type
    async putSettings(type = void 0) {
        return {
            //New settings properties
        };
    }
}

module.exports = [MyMigration];
```
- This type can run in parallel and can merge with some other types.
- You should use this type when you need update mapping/settings, and it doesn't need reindex.
    - ES `putMapping`/`putSettings` functions
- You specify your code in methods `putMapping` or `putSettings`.
- Again you can use `INDICES` (or `INPUT_INDICES` or `OUTPUT_INDICES`) if you need the ODMs to fetch some data.


### Script type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.SCRIPT,
            INDEX: this.INDICES.DOCUMENTS
            //INCLUSIVE_TYPE: `type` or EXCLUSIVE_TYPE: `type` //See BULK type for info
            //DEPENDS_ON: []
            
            //INPUT_INDICES: []
            //INDICES: []
            //OUTPUT_INDICES: []
        };
    }
    
    /*
    //Called once for each existing index type
    async reindex(mapping, settings, type = void 0) {
        //Alter mapping / settings objects
    }
    */

    //Called once for each existing index type
    async migrate(doc, type = void 0) {
        //Call doc functions
    }
}

module.exports = [MyMigration];
```
- This type can run in parallel and can merge with some other types.
- Code in this type is compiled into Painless or JS code.
- You use special ScriptWrapper methods to specify what you want to do.
- Still a bit experimental, but should be usable for basic document editing.
    - You can create/set/delete properties, do if statements, cycles, delete document, reindex, etc.
    - You cannot create new document, specify custom function, create deep copies
    - Take caution when working with numbers, as Painless and JS use different types, there may be a problem with floating point.
        - When working with numbers, it's recommended to always use `explicitCast` function and try it for both Painless and JS.
- Again you can use `INDICES` (or `INPUT_INDICES` or `OUTPUT_INDICES`) if you need the ODMs to fetch some data.
- In case you need to reindex (change index mapping/settings), specify `reindex` function.
    - Then just alter the original properties (you receive them as method arguments) as needed (same as BULK type).
- Main code is in `migrate` method. You use `doc` (first parameter) methods to alter the document.


### Stop type
```
'use strict';

const Migration = require(`Migration`);

class MyMigration extends Migration {
    static get INFO() {
        return {
            TYPE: this.TYPE.STOP,
            MESSAGE: `Migrations have been stopped...`   //Required message
        };
    }
}

module.exports = [MyMigration];
```
- Special type intended to stop the migrations.
- Can be used when you need manually do some stuff (e.q. change ES configuration) and you need to stop at this point.
- You don't specify any code
- Note: Migrations are automatically stopped when another migration tool version should be used. You don't have to (but of course you can) specify this type at that point.


### Common functionality
- In all your code you can also use method `this.note('message')`.
    - This will log the message at the end of the migrations.

- You have access to `this.utils` utility object. Following utilities are provided:
    - `BulkArray` - Access to ODM bulk array

    - `async bulkIterator(Odm, body)` - this function provides you iterator over specified ODM model data.
    ```
    const myBulks = this.utils.bulkIterator(this.ODM.NOTES, {
        query: {
            ... custom query
        }
    });

    for await (const myBulk of myBulks) {
        myBulk.forEach((doc) => {
            //doc is ODM instance
        });
    }
    ```

    - `async getBulkSize(Odm)` - this function returns optimal bulk size for specified ODM model

- You can also use `_callEs()` function to do custom ES calls.
    - Do NOT use it if you don't have to. And you should never have to. This may internally disable some optimizations.
```
    await this._callEs(`indices.stats`, {
        index: MyOdm._alias
    });
```


## Configuration

### Loader configuration
```
"migrationFolder": "migrations",             //Default migration folder
"esHost": "http://localhost:9200",           //Default ES host string
"logs": {                                    //Logs configuration
    ...
}
```

### Migration tool configuration
```
"options": {
    "limits": {                             //Hard limits
        "bulk": {                           //Bulk limits
            "minSize": 10,
            "defaultSize": 1000,
            "maxSize": 10000
        },
        "queue": {                          //Queue limits
            "minSize": 1,
            "defaultSize": 5,
            "maxSize": 20
        },
        "maxScriptChains": 2,               //When fallback from SCRIPT to BULK sync type; < 0 - always, >= 0 - allows given number of SCRIPT / DOCUMENT types interrupted chains
        "maxIndexDeletes": 5                //Maximum number of indices specified in URL when deleting indices (ensures HTTP path < 4096)
    },
    "optimizations": {                      //Optimization settings
        "restrictions": {                   //Index restrictions
            "replicas": true,               //Restrict (disable) replicas?
            "refreshes": true              //Restrict (disable) refreshes?
        },
        "targetBulkSize": {                 //Targeting bulk to given size
            "enabled": true,                //Enabled?
            "minMB": 10,
            "defaultMB": 25,
            "maxMB": 45,
            "lowPassCoefficient": 0.8,
            "sampleSize": 10                //Number of samples for unknown indices
        },
        "dynamic": {                        //Dynamic changes
            "persistent": false,            //Make dynamic changes persistent?
            "limiting": true,               //Allow limiting in case of 413/429?
            "limitingInterval": 3000,       //Time space between two limiting actions
            "monitoring": false,            //Periodic monitor - NOT IMPLEMENTED
            "monitoringInterval": 1000      //Interval for monitor
        },
        "alwaysNewIndices": {               //Should BULK / SCRIPT types always create new indices?
            "bulk": false,
            "script": false
        }
    },
    "strict": {
      "updates": false,                     //Throw when updateDocument function has not been used (=== ID not exist and no fallback)
      "types": false                        //Throw when index type is not specified in create/forceCreate/update function
    },
    "blockIndices": true                    //Use indices blocks?
},
"backup": {
    "onlyUsedIndices": true,                //Backup only really used indices, unless string "_callEs" occures in any of the migrations
    "backupInputIndices": true,             //Backup also indices marked as input only?
    "location": "./es_backup",              //ES snapshot repository location
    "repository": "es-migrations-backup"    //ES snapshot repository name
},
"es": {
    "scrollTimeout": "5m",
    "correctSettings": true,                //Ensure specified ES settings (reindex, putSettings), to be always nested inside "index" object
    "retries":  {                           //Retries in case of ES 429 error
        "base": 2.0,                        //Base of sleep exponential function
        "maxRetries": 8                     //Max retries
    },
    "checkIntervals": {                     //Check intervals (in ms) for not waited calls
        "snapshot": 2500,                   //Check interval for snapshots
        "restore": 2500,                    //Check interval for restore
        "task": 2500                        //Check interval for tasks
    }
},
"logs": {                                   //Logs configuration
    ....
}
```



## Brief technical explanation
### Loader part
At first version mapping file is loaded and current ES version is checked. This is all done by the migration loader, without usage of ES library (so it is version independent).
Then available migration files are checked and migration tool is initialized, which gives us access to base migration class, used as parent when loading all the migrations.
Once migration instances are created, they are passed to main migration tool to process them.

### Migration part
Migration tool then ensures all specified indices in given tenant uses aliases. If not, the indices are "aliased".

Then the migrations are checked up and are transformed into migration nodes. Nodes are created by merging the migrations.
Some migrations cannot be merged (SERIAL, INDICES) and such a node then always contain only one migration. Other migrations MAY be merged, if this is beneficial.

Merging is done by buffering migrations with the same main index (property `INDEX`) till possible. Things that may break the buffering are indices conflicts, e.g. when one migration reads from an index which has been previously written to.
Once buffering is finished, it is decided how many nodes will be created.

As a rule of thumb, when there is a BULK migration, all the buffered migrations fallback to this type.
Otherwise, we have to check if there are both DOCUMENTS and SCRIPT synchronisation types presented in the buffer. If so, we check how many times the DOCUMENTS type breaks the SCRIPT type. If certain value is exceeded, we fall back to the BULK type.
If not, we can create multiple nodes, for each synchronisation type.

Then the tenant is locked, which means blocks to read and write are set and is created the backup in form of snapshot. By default, only affected indices (both input and output) are snapshot.

Then the migration nodes are connected into a graph, based on possible node parallelism. If there are some optimization data from previous run, these are loaded.  Then refreshes and replicas are disabled on affected indices.

Then the migration nodes are processed. We start from an initial (dummy) node and run nodes if possible. To run the node, all its predecessors (input connections in graph) must be already processed.
There is also a limit so that we don't pressure ES too much.

What exactly happens when single node runs depends on included migrations. The common part is, we release ES blocks on the start of the migration for used indices and for output only indices we replace ODM models (if applicable) with restricted versions, that do not do refreshes.
Similarly, once the node is finished, we close (block) indices, if possible.

What happens in between is dependent on included migrations. If node contains only SERIAL or INDICES migration, then the main migration function is just run and for the type STOP nothing happens. Otherwise, multiple steps must be done.

Generally we have to get all affected indices. We know these from the INFO object, but they may contain types. So we have to get all existing types and filter them based on inclusive/exclusive data info.

Easiest is a type PUT, where we only go through all the typed indices and for each we go through all the migrations. We always check that the migration should apply for given typed index and if so, we cache output of specified `putSettings`/`putMapping` methods.
Result of this cache is then used to call single `putSettings` and/or `putMapping` method for each typed index.

For the other synchronisation types, we must create mapping object at first. This object specifies original index and output index. If there is no reindexing, these match.
Otherwise, we have to prepare empty index and this is the output point. These new indices are created based on reindex function and/or `putMapping`/`putSettings` functions, in case of PUT type fallback.

For type SCRIPT we then go through all typed indices and run migrate functions, which creates Painless script. Scripts from all the migrations are concatenated and used in either `updateByQuery` or `reindex` ES functions, depending on whether we have to reindex or not.

Types BULK and DOCUMENTS are processed in a very similar matter. At first, we have to migrate already existing documents. There is the only difference between these types.
In both cases we go through typed indices one-by-one, but in BULK type we process whole index, whereas in type DOCUMENTS we process only certain IDs.

Requested documents are then loaded in bulks and are processed in specified function. This is either main migrate function for BULK type or updateDocument function for type DOCUMENTS. In case of SCRIPT type fallback, we generate JS function.

For each bulk we then process all the migrations possible. In case the document should be deleted or re-indexed, it is not passed to the next migration code, but this information is noted. At the end are then the documents saved into output index.

Now comes the second stage, where we have to migrate "virtual" (not existing yet) documents. These are the documents created by (force)createDocument function or by changing the index.
Migrations are processed one-by-one and in each step we gather such documents and migrate them using all the upcoming migrations. Results are saved to the output index. This may of course lead to more virtual documents created, which will be processed in next rounds.

At the end all the documents are processed, and we have to delete marked documents. In case we indexed into new index, we just delete the original index and switch the alias.
Otherwise, we check whether the document should be really deleted (e.g. if it hasn't been rewritten later) and if so, we delete the document from the original index.

Once all the migration nodes are processed, we then restore original refresh intervals and replica counts, save new version and unblock the indices. Then the snapshot is deleted.

In case any error happened, we wait till all the running operations finish, then we delete all new/affected indices and restore the snapshot.

As a side note, creating snapshot, restoring snapshot and running `updateByQuery` and `reindex` is performed using tasks, instead of waiting for request response, so it shouldn't time out.
