'use strict';

const _ = require(`lodash`);
const { v4: uuid } = require(`uuid`);

class ScriptWrapper {
    /**
     * Creates new script wrapper
     * @param key {string | { isJavascript: boolean, alias: String }}
     * @param context {Object}
     */
    constructor(key, context) {
        if (!context) {
            this.key = `root`;
            this.context = {
                isJavaScript: key.isJavascript,
                alias: key.alias,
                code: ``    //Root element not added intentionally
            };

        } else {
            this.key = key;
            this.context = context;
        }
    }

    /**
     * Returns JS or painless code
     * @returns {string}
     */
    _getResult() {
        if (this.__isJavaScript()) {
            return `const root = doc; ${this.context.code}`;
        } else {
            return this.context.code;
        }
    }

    //==================================================== INTERNAL ====================================================
    /**
     * Writes new code
     * @param newCode {string}
     */
    __addCode(newCode) {
        this.context.code = `${this.context.code}${newCode}`;
    }

    /**
     * Checks if this script is intended for JavaScript od Painless
     * @returns {boolean} True for JavaScript
     */
    __isJavaScript() {
        return this.context.isJavaScript;
    }

    /**
     * Generates new key name
     * @param key {string | number | ScriptWrapper} Name base
     * @returns {string} Generated key
     */
    __generateKeyName(key = void 0) {
        if (_.isString(key) || _.isNumber(key)) {
            return `custom_${key}_${uuid().replaceAll(`-`, ``)}`;
        } else if (key instanceof ScriptWrapper) {
            return `ref_${key.key.slice(key.key.length - 5)}_${uuid().replaceAll(`-`, ``)}`;
        } else {
            return `auto_${uuid().replaceAll(`-`, ``)}`;
        }
    }

    /**
     * Parses value
     * @param value {*}
     * @returns {*}
     */
    __parseValue(value) {
        if (value instanceof ScriptWrapper) {
            return value.key;
        } else if (_.isNull(value)) {
            return `null`;
        } else if (_.isUndefined(value)) {
            return void 0;  //Handled manually
        } else if (_.isSymbol(value) || _.isFunction(value) || typeof value === `bigint`) {
            throw Error(`Not supported type of parameter.`);
        } else if (_.isNumber(value) || _.isBoolean(value)) {
            return value;
        } else if (_.isString(value)) {
            return `'${value}'`;
        } else if (_.isDate(value)) {
            return `'${value.toISOString()}'`;
        } else if (_.isArray(value)) {
            const hlpArray = this.__generateKeyName(`hlpArray`);
            if (this.__isJavaScript()) {
                this.__addCode(`let ${hlpArray} = new Array();`);
                for (const element of value) {
                    const parsedValue = this.__parseValue(element);
                    if (_.isUndefined(parsedValue)) {
                        this.__addCode(`${hlpArray}.push(${this.__parseValue(null)});`);
                    } else {
                        this.__addCode(`${hlpArray}.push(${parsedValue});`);
                    }
                }
            } else {
                this.__addCode(`def ${hlpArray} = new ArrayList();`);
                for (const element of value) {
                    const parsedValue = this.__parseValue(element);
                    if (_.isUndefined(parsedValue)) {
                        this.__addCode(`${hlpArray}.add(${this.__parseValue(null)});`);
                    } else {
                        this.__addCode(`${hlpArray}.add(${parsedValue});`);
                    }
                }
            }
            return hlpArray;
        } else if (_.isObject(value)) {
            const hlpObject = this.__generateKeyName(`hlpObject`);
            if (this.__isJavaScript()) {
                this.__addCode(`let ${hlpObject} = new Object();`);
                for (const key of Object.keys(value)) {
                    const parsedValue = this.__parseValue(value[key]);
                    if (!_.isUndefined(parsedValue)) {
                        this.__addCode(`${hlpObject}['${key.toString()}'] = ${parsedValue};`);
                    }
                }
            } else {
                this.__addCode(`def ${hlpObject} = new HashMap();`);
                for (const key of Object.keys(value)) {
                    const parsedValue = this.__parseValue(value[key]);
                    if (!_.isUndefined(parsedValue)) {
                        this.__addCode(`${hlpObject}.put('${key.toString()}', ${parsedValue});`);
                    }
                }
            }
            return hlpObject;
        } else {
            throw Error(`Unknown type of parameter.`);
        }
    }

    //==================================================== METHODS ====================================================
    //================================= All types =================================
    /**
     * Assigns value to the ScriptWrapper, alters value
     * @param value {ScriptWrapper} Value to assign
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    setValue(value) {
        if (!(value instanceof ScriptWrapper)) {
            throw Error(`Method 'setValue' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        this.__addCode(`${this.key} = ${value.key};`);
        return this;
    }

    /**
     * Shallow clone of the value
     * @returns {ScriptWrapper} New ScriptWrapper instance
     */
    clone() {
        const valueKey = this.__generateKeyName(`clone`);

        const isNull = this.isNull();
        const isNumber = this.isNumber();
        const isBoolean = this.isBoolean();
        const isString = this.isString();
        const isArray = this.isArray();
        const isObject = this.isObject();

        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = null;`);
            this.__addCode(`if (${isNull.key}) { ${valueKey} = null; } else if (${isNumber.key} || ${isBoolean.key} || ${isString.key}) { ${valueKey} = ${this.key}; } else `);
            this.__addCode(`if (${isArray.key}) { ${valueKey} = [ ...${this.key} ]; } else if (${isObject.key}) { ${valueKey} = { ...${this.key} }; } else { throw Error('Unknown data type in "clone" function.'); }`);

        } else {
            this.__addCode(`def ${valueKey} = null;`);
            this.__addCode(`if (${isNull.key}) { ${valueKey} = null; } else if (${isNumber.key} || ${isBoolean.key} || ${isString.key}) { ${valueKey} = ${this.key}; } else `);
            this.__addCode(`if (${isArray.key} || ${isObject.key}) { ${valueKey} = ${this.key}.clone(); } else { throw new Exception('Unknown data type in "clone" function.'); }`);
        }

        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Performs equals comparison, works with multiple types
     * @param compared {ScriptWrapper} ScriptWrapper instance to compare to
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    equals(compared) {
        if (!(compared instanceof ScriptWrapper)) {
            throw Error(`Method 'equals' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const valueKey = this.__generateKeyName(`equals`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} === ${compared.key});`);
        } else {
            this.__addCode(`def ${valueKey} = ( ${this.key}.equals(${compared.key}) );`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= Null =================================
    /**
     * Checks if value is null
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isNull() {
        const valueKey = this.__generateKeyName(`isNull`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} === null);`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} == null);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= String + Array =================================
    /**
     * Checks length of string / array, works with strings and arrays only
     * @returns {ScriptWrapper} New number ScriptWrapper instance
     */
    length() {
        const valueKey = this.__generateKeyName(`length`);
        const isString = this.isString();
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key} && !${isArray.key}) throw Error('Method "length" must be called on string or array.');`);
            this.__addCode(`let ${valueKey} = ${this.key}.length;`);
        } else {
            this.__addCode(`if (!${isString.key} && !${isArray.key}) throw new Exception('Method "length" must be called on string or array.');`);
            this.__addCode(`def ${valueKey} = (${this.key} instanceof String) ? ${this.key}.length() : ${this.key}.size();`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= Object + Array =================================
    /**
     * Returns value of given key, works with arrays and objects only
     * @param key {string | number | ScriptWrapper} Key to be retrieved
     * @returns {ScriptWrapper} New ScriptWrapper instance
     */
    getKey(key) {
        if (!_.isString(key) && !_.isNumber(key) && !(key instanceof ScriptWrapper)) {
            throw Error(`Method 'getKey' requires its parameter to be of type 'string' or 'number' or instance of 'ScriptWrapper'.`);
        }

        const valueKey = this.__generateKeyName(key);
        const isObject = this.isObject();
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw Error('Method "getKey" must be called on object or array.');`);
            if (_.isString(key) || _.isNumber(key)) {
                this.__addCode(`let ${valueKey} = (typeof ${this.key}['${key.toString()}'] !== 'undefined') ? ${this.key}['${key.toString()}'] : null;`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw Error('Key used in method "getKey" must be of type string or number.');`);
                this.__addCode(`let ${valueKey} = (typeof ${this.key}[${key.key}] !== 'undefined') ? ${this.key}[${key.key}] : null;`);
            }
        } else {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw new Exception('Method "getKey" must be called on object or array.');`);
            if (_.isString(key)) {
                this.__addCode(`def ${valueKey} = ${this.key}['${key.toString()}'];`);
            } else if (_.isNumber(key)) {
                this.__addCode(`def ${valueKey} = ${this.key}[${key.toString()}];`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw new Exception('Key used in method "getKey" must be of type string or number.');`);
                this.__addCode(`def ${valueKey} = ${this.key}[${key.key}];`);
            }
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Sets value to given key, works with arrays and objects only
     * @param key {string | number | ScriptWrapper} Key to be set
     * @param value {ScriptWrapper} ScriptWrapper value to be used
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    setKey(key, value) {
        if (!_.isString(key) && !_.isNumber(key) && !(key instanceof ScriptWrapper)) {
            throw Error(`Method 'setKey' requires its first parameter to be of type 'string' or 'number' or instance of 'ScriptWrapper'.`);
        } else if (!(value instanceof ScriptWrapper)) {
            throw Error(`Method 'setKey' requires its second parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isObject = this.isObject();
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw Error('Method "setKey" must be called on object or array.');`);
            if (_.isString(key) || _.isNumber(key)) {
                this.__addCode(`${this.key}['${key.toString()}'] = ${value.key};`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw Error('Key used in method "setKey" must be of type string or number.');`);
                this.__addCode(`${this.key}[${key.key}] = ${value.key};`);
            }
        } else {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw new Exception('Method "setKey" must be called on object or array.');`);
            if (_.isString(key)) {
                this.__addCode(`${this.key}['${key.toString()}'] = ${value.key};`);
            } else if (_.isNumber(key)) {
                this.__addCode(`${this.key}[${key.toString()}] = ${value.key};`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw new Exception('Key used in method "setKey" must be of type string or number.');`);
                this.__addCode(`${this.key}[${key.key}] = ${value.key};`);
            }
        }
        return this;
    }

    /**
     * Removes value at given key, works with arrays and objects only
     * @param key {string | number | ScriptWrapper} Key to be removed
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    removeKey(key) {
        if (!_.isString(key) && !_.isNumber(key) && !(key instanceof ScriptWrapper)) {
            throw Error(`Method 'setKey' requires its first parameter to be of type 'string' or 'number' or instance of 'ScriptWrapper'.`);
        }

        const isObject = this.isObject();
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw Error('Method "removeKey" must be called on object or array.');`);
            if (_.isString(key)) {
                this.__addCode(`delete ${this.key}['${key.toString()}'];`);
            } else if (_.isNumber(key)) {
                this.__addCode(`${this.key}.splice(${key.toString()}, 1);`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw Error('Key used in method "removeKey" must be of type string or number.');`);
                this.__addCode(`if (${isString.key}) delete ${this.key}[${key.key}]; else ${this.key}.splice(${key.key}, 1);`);
            }
        } else {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw new Exception('Method "removeKey" must be called on object or array.');`);
            if (_.isString(key)) {
                this.__addCode(`${this.key}.remove('${key.toString()}');`);
            } else if (_.isNumber(key)) {
                this.__addCode(`${this.key}.remove(${key.toString()});`);
            } else {
                const isString = key.isString();
                const isNumber = key.isNumber();
                this.__addCode(`if (!${isString.key} && !${isNumber.key}) throw new Exception('Key used in method "removeKey" must be of type string or number.');`);
                this.__addCode(`${this.key}.remove(${key.key});`);
            }
        }
        return this;
    }

    /**
     * Removes all values, works with arrays and objects only
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    removeAllKeys() {
        const isObject = this.isObject();
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw Error('Method "removeAllKeys" must be called on object or array.');`);
            this.__addCode(`if (${isArray.key}) ${this.key}.length = 0; else for (const key of Object.keys(${this.key})) { delete ${this.key}[key]; };`);
        } else {
            this.__addCode(`if (!${isObject.key} && !${isArray.key}) throw new Exception('Method "removeAllKeys" must be called on object or array.');`);
            this.__addCode(`${this.key}.clear();`);
        }
        return this;
    }

    //================================= Object =================================
    /**
     * Checks if value is an object; array is NOT an object
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isObject() {
        const valueKey = this.__generateKeyName(`isObject`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (typeof ${this.key} === 'object' && !Array.isArray(${this.key}) && ${this.key} !== null);`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} instanceof Map);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Returns object keys, works with objects only
     * @returns {ScriptWrapper} New array ScriptWrapper instance
     */
    getKeys() {
        const valueKey = this.__generateKeyName(`objectKeys`);
        const isObject = this.isObject();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isObject.key}) throw Error('Method "getKeys" must be called on object.');`);
            this.__addCode(`let ${valueKey} = Object.keys(${this.key});`);
        } else {
            this.__addCode(`if (!${isObject.key}) throw new Exception('Method "getKeys" must be called on object.');`);
            this.__addCode(`def ${valueKey} = Arrays.asList(${this.key}.keySet().toArray());`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= Array =================================
    /**
     * Checks if value is an array
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isArray() {
        const valueKey = this.__generateKeyName(`isArray`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (Array.isArray(${this.key}));`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} instanceof List);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Casts value to an array, alters value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castArray() {
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isArray.key}) ${this.key} = [${this.key}];`);
        } else {
            const valueKey = this.__generateKeyName(`castArray`);
            this.__addCode(`if (!${isArray.key}) { def ${valueKey} = ${this.key}; ${this.key} = new ArrayList(); ${this.key}.add(${valueKey}); }`);
        }
        return this;
    }

    /**
     * Pushes a value to an end of the array, alters value, works with arrays only
     * @param value {ScriptWrapper} ScriptWrapper instance to push
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    push(value) {
        if (!(value instanceof ScriptWrapper)) {
            throw Error(`Method 'setValue' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isArray.key}) throw Error('Method "push" must be called on array.');`);
            this.__addCode(`${this.key}.push(${value.key});`);
        } else {
            this.__addCode(`if (!${isArray.key}) throw new Exception('Method "push" must be called on array.');`);
            this.__addCode(`${this.key}.add(${value.key});`);
        }
        return this;
    }

    /**
     * Pushes a value at the beginning of the array, alters value, works with arrays only
     * @param value {ScriptWrapper} ScriptWrapper instance to unshift
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    unshift(value) {
        if (!(value instanceof ScriptWrapper)) {
            throw Error(`Method 'setValue' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isArray.key}) throw Error('Method "unshift" must be called on array.');`);
            this.__addCode(`${this.key}.unshift(${value.key});`);
        } else {
            this.__addCode(`if (!${isArray.key}) throw new Exception('Method "unshift" must be called on array.');`);
            this.__addCode(`${this.key}.add(0, ${value.key});`);
        }
        return this;
    }

    /**
     * Removes and returns item at the first position, works with arrays only
     * @returns {ScriptWrapper} New ScriptWrapper instance
     */
    shift() {
        const valueKey = this.__generateKeyName(`shift`);
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isArray.key}) throw Error('Method "shift" must be called on array.');`);
            this.__addCode(`let ${valueKey} = ${this.key}.shift();`);
        } else {
            this.__addCode(`if (!${isArray.key}) throw new Exception('Method "shift" must be called on array.');`);
            this.__addCode(`def ${valueKey} = ${this.key}.remove(0);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Removes and returns item at the last position, works with arrays only
     * @returns {ScriptWrapper} New ScriptWrapper instance
     */
    pop() {
        const valueKey = this.__generateKeyName(`pop`);
        const isArray = this.isArray();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isArray.key}) throw Error('Method "pop" must be called on array.');`);
            this.__addCode(`let ${valueKey} = ${this.key}.pop();`);
        } else {
            this.__addCode(`if (!${isArray.key}) throw new Exception('Method "pop" must be called on array.');`);
            this.__addCode(`def ${valueKey} = ${this.key}.remove(${this.key}.size() - 1);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= Number =================================
    /**
     * Checks if value is a number
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isNumber() {
        const valueKey = this.__generateKeyName(`isNumber`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (typeof ${this.key} === 'number');`);
        } else {
            this.__addCode(`def ${valueKey} = ((${this.key} instanceof Integer) || (${this.key} instanceof Long) || (${this.key} instanceof Float) || (${this.key} instanceof Double));`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Parses int value, alters value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castInt() {
        if (this.__isJavaScript()) {
            this.__addCode(`${this.key} = parseInt(${this.key} + '', 10);`);
        } else {
            this.__addCode(`${this.key} = Integer.parseInt(${this.key} + '', 10);`);
        }
        return this;
    }

    /**
     * Parses long value, alters value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castLong() {
        if (this.__isJavaScript()) {
            this.__addCode(`${this.key} = parseInt(${this.key} + '', 10);`);
        } else {
            this.__addCode(`${this.key} = Long.parseLong(${this.key} + '', 10);`);
        }
        return this;
    }

    /**
     * Parses float value, alters value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castFloat() {
        if (this.__isJavaScript()) {
            this.__addCode(`${this.key} = parseFloat(${this.key} + '');`);
        } else {
            this.__addCode(`${this.key} = Float.parseFloat(${this.key} + '');`);
        }
        return this;
    }

    /**
     * Parses double value, alters value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castDouble() {
        if (this.__isJavaScript()) {
            this.__addCode(`${this.key} = parseFloat(${this.key} + '');`);
        } else {
            this.__addCode(`${this.key} = Double.parseDouble(${this.key} + '');`);
        }
        return this;
    }

    /**
     * Casts to selected type, alters value, works with numbers only
     * @param type {string} Type where to cast the number
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    explicitCast(type) {
        if (!_.isString(type)) {
            throw Error(`Method 'explicitCast' requires its parameter to be of type 'string'.`);
        }

        const isNumber = this.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "explicitCast" must be called on number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "explicitCast" must be called on number.');`);
        }

        if (this.__isJavaScript()) {
            this.__addCode(`if ("${type}" === "long" || "${type}" === "int" || "${type}" === "short" || "${type}" === "byte") { ${this.key} = parseInt(${this.key} + '', 10); }`);
        } else {
            this.__addCode(`${this.key} = (${type}) ${this.key};`);
        }
        return this;
    }

    /**
     * Increases number, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use as an addend
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    increase(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'increase' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "increase" must be called on a number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "increase" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "increase" must be called on a number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "increase" requires number argument.');`);
        }

        this.__addCode(`${this.key} += ${number.key};`);
        return this;
    }

    /**
     * Decreases number, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use as a subtrahend
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    decrease(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'decrease' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "decrease" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "decrease" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "decrease" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "decrease" requires number argument.');`);
        }

        this.__addCode(`${this.key} -= ${number.key};`);
        return this;
    }

    /**
     * Multiplies number, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use as a multiplicand
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    multiply(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'multiply' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "multiply" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "multiply" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "multiply" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "multiply" requires number argument.');`);
        }

        this.__addCode(`${this.key} *= ${number.key};`);
        return this;
    }

    /**
     * Divides number, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use as a divisor
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    divide(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'divide' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "divide" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "divide" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "divide" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "divide" requires number argument.');`);
        }

        this.__addCode(`${this.key} /= ${number.key};`);
        return this;
    }

    /**
     * Powers the value using given exponent, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use as an exponent
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    pow(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'pow' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "pow" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "pow" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "pow" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "pow" requires number argument.');`);
        }

        this.__addCode(`${this.key} = Math.pow(${this.key}, ${number.key});`);
        return this;
    }

    /**
     * Returns remainder, alters value, works with numbers only
     * @param number {ScriptWrapper} ScriptWrapper instance to use for mod
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    mod(number) {
        if (!(number instanceof ScriptWrapper)) {
            throw Error(`Method 'mod' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const numberIsNumber = number.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "mod" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw Error('Method "mod" requires number argument.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "mod" must be called on number.');`);
            this.__addCode(`if (!${numberIsNumber.key}) throw new Exception('Method "mod" requires number argument.');`);
        }

        this.__addCode(`${this.key} %= ${number.key};`);
        return this;
    }

    /**
     * Rounds number, alters value, works with numbers only
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    round() {
        const isNumber = this.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "round" must be called on number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "round" must be called on number.');`);
        }

        this.__addCode(`${this.key} = Math.round(${this.key});`);
        return this;
    }

    /**
     * Floors number, alters value, works with numbers only
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    floor() {
        const isNumber = this.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "floor" must be called on number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "floor" must be called on number.');`);
        }

        this.__addCode(`${this.key} = Math.floor(${this.key});`);
        return this;
    }

    /**
     * Ceils number, alters value, works with numbers only
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    ceil() {
        const isNumber = this.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "ceil" must be called on number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "ceil" must be called on number.');`);
        }

        this.__addCode(`${this.key} = Math.ceil(${this.key});`);
        return this;
    }

    /**
     * Performs < comparison, number types only
     * @param compared {ScriptWrapper} ScriptWrapper instance to compare to
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isLowerThan(compared) {
        if (!(compared instanceof ScriptWrapper)) {
            throw Error(`Method 'isLowerThan' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const comparedIsNumber = compared.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "isLowerThan" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw Error('Method "isLowerThan" requires to be compared against number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "isLowerThan" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw new Exception('Method "isLowerThan" requires to be compared against number.');`);
        }

        const valueKey = this.__generateKeyName(`isLowerThan`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} < ${compared.key});`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} < ${compared.key});`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Performs <= comparison, number types only
     * @param compared {ScriptWrapper} ScriptWrapper instance to compare to
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isLowerThanOrEqual(compared) {
        if (!(compared instanceof ScriptWrapper)) {
            throw Error(`Method 'isLowerThanOrEqual' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const comparedIsNumber = compared.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "isLowerThanOrEqual" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw Error('Method "isLowerThanOrEqual" requires to be compared against number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "isLowerThanOrEqual" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw new Exception('Method "isLowerThanOrEqual" requires to be compared against number.');`);
        }

        const valueKey = this.__generateKeyName(`isLowerThanOrEqual`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} <= ${compared.key});`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} <= ${compared.key});`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Performs > comparison, number types only
     * @param compared {ScriptWrapper} ScriptWrapper instance to compare to
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isGreaterThan(compared) {
        if (!(compared instanceof ScriptWrapper)) {
            throw Error(`Method 'isGreaterThan' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const comparedIsNumber = compared.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "isGreaterThan" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw Error('Method "isGreaterThan" requires to be compared against number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "isGreaterThan" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw new Exception('Method "isGreaterThan" requires to be compared against number.');`);
        }

        const valueKey = this.__generateKeyName(`isGreaterThan`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} > ${compared.key});`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} > ${compared.key});`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Performs >= comparison, number types only
     * @param compared {ScriptWrapper} ScriptWrapper instance to compare to
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isGreaterThanOrEqual(compared) {
        if (!(compared instanceof ScriptWrapper)) {
            throw Error(`Method 'isGreaterThanOrEqual' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isNumber = this.isNumber();
        const comparedIsNumber = compared.isNumber();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isNumber.key}) throw Error('Method "isGreaterThanOrEqual" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw Error('Method "isGreaterThanOrEqual" requires to be compared against number.');`);
        } else {
            this.__addCode(`if (!${isNumber.key}) throw new Exception('Method "isGreaterThanOrEqual" must be called on number.');`);
            this.__addCode(`if (!${comparedIsNumber.key}) throw new Exception('Method "isGreaterThanOrEqual" requires to be compared against number.');`);
        }

        const valueKey = this.__generateKeyName(`isGreaterThanOrEqual`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key} >= ${compared.key});`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} >= ${compared.key});`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //================================= Boolean =================================
    /**
     * Checks if value is a boolean
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isBoolean() {
        const valueKey = this.__generateKeyName(`isBoolean`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (typeof ${this.key} === 'boolean');`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} instanceof Boolean);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Casts value to boolean type
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castBoolean() {
        const isBoolean = this.isBoolean();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isBoolean.key}) ${this.key} = (${this.key}.toString().toLowerCase() === 'true');`);
        } else {
            this.__addCode(`if (!${isBoolean.key}) ${this.key} = Boolean.parseBoolean(${this.key} + "");`);
        }
        return this;
    }

    /**
     * Negates boolean, alters value, works with booleans only
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    negate() {
        const isBoolean = this.isBoolean();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isBoolean.key}) throw Error('Method "negate" must be called on boolean.');`);
        } else {
            this.__addCode(`if (!${isBoolean.key}) throw new Exception('Method "negate" must be called on boolean.');`);
        }

        this.__addCode(`${this.key} = !(${this.key});`);
        return this;
    }

    //================================= String =================================
    /**
     * Checks if value is a string
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    isString() {
        const valueKey = this.__generateKeyName(`isString`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (typeof ${this.key} === 'string');`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key} instanceof String);`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Casts value to string type, alters the value
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    castString() {
        const isString = this.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) ${this.key} = ${this.key}.toString();`);
        } else {
            this.__addCode(`if (!${isString.key}) ${this.key} = ${this.key} + "";`);
        }
        return this;
    }

    /**
     * Prepends prefix to the string, alters the value, works with strings only
     * @param prefix {ScriptWrapper} ScriptWrapper instance to prepend
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    prepend(prefix) {
        if (!(prefix instanceof ScriptWrapper)) {
            throw Error(`Method 'prependString' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const prefixIsString = prefix.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "prepend" must be called on string.');`);
            this.__addCode(`if (!${prefixIsString.key}) throw Error('Method "prepend" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "prepend" must be called on string.');`);
            this.__addCode(`if (!${prefixIsString.key}) throw new Exception('Method "prepend" requires string argument.');`);
        }

        this.__addCode(`${this.key} = ${prefix.key} + ${this.key};`);
        return this;
    }

    /**
     * Appends suffix to the string, alters the value, works with strings only
     * @param suffix {ScriptWrapper} ScriptWrapper instance to append
     * @returns {ScriptWrapper} The same, altered, ScriptWrapper === this
     */
    append(suffix) {
        if (!(suffix instanceof ScriptWrapper)) {
            throw Error(`Method 'appendString' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const suffixIsString = suffix.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "append" must be called on string.');`);
            this.__addCode(`if (!${suffixIsString.key}) throw Error('Method "append" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "append" must be called on string.');`);
            this.__addCode(`if (!${suffixIsString.key}) throw new Exception('Method "append" requires string argument.');`);
        }

        this.__addCode(`${this.key} = ${this.key} + ${suffix.key};`);
        return this;
    }

    /**
     * Returns index of a string (or -1), works with strings only
     * @param string {ScriptWrapper} ScriptWrapper string instance to check its index
     * @returns {ScriptWrapper} New number ScriptWrapper instance
     */
    indexOf(string) {
        if (!(string instanceof ScriptWrapper)) {
            throw Error(`Method 'indexOf' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const stringIsString = string.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "indexOf" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw Error('Method "indexOf" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "indexOf" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw new Exception('Method "indexOf" requires string argument.');`);
        }

        const valueKey = this.__generateKeyName(`indexOf`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = ${this.key}.indexOf(${string.key});`);
        } else {
            this.__addCode(`def ${valueKey} = ${this.key}.indexOf(${string.key});`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Checks if property contains given string ScriptWrapper, works with strings only
     * @param string {ScriptWrapper} ScriptWrapper string instance to check
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    includes(string) {
        if (!(string instanceof ScriptWrapper)) {
            throw Error(`Method 'includes' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const stringIsString = string.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "includes" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw Error('Method "includes" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "includes" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw new Exception('Method "includes" requires string argument.');`);
        }

        const valueKey = this.__generateKeyName(`includes`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key}.includes(${string.key}));`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key}.contains(${string.key}));`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Checks if property starts with given string ScriptWrapper, works with strings only
     * @param string {ScriptWrapper} ScriptWrapper string instance to check
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    startsWith(string) {
        if (!(string instanceof ScriptWrapper)) {
            throw Error(`Method 'startsWith' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const stringIsString = string.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "startsWith" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw Error('Method "startsWith" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "startsWith" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw new Exception('Method "startsWith" requires string argument.');`);
        }

        const valueKey = this.__generateKeyName(`startsWith`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key}.startsWith(${string.key}));`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key}.startsWith(${string.key}));`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    /**
     * Checks if property ends with given string ScriptWrapper, works with strings only
     * @param string {ScriptWrapper} ScriptWrapper string instance to check
     * @returns {ScriptWrapper} New boolean ScriptWrapper instance
     */
    endsWith(string) {
        if (!(string instanceof ScriptWrapper)) {
            throw Error(`Method 'endsWith' requires its parameter to be an instance of 'ScriptWrapper'.`);
        }

        const isString = this.isString();
        const stringIsString = string.isString();
        if (this.__isJavaScript()) {
            this.__addCode(`if (!${isString.key}) throw Error('Method "endsWith" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw Error('Method "endsWith" requires string argument.');`);
        } else {
            this.__addCode(`if (!${isString.key}) throw new Exception('Method "endsWith" must be called on string.');`);
            this.__addCode(`if (!${stringIsString.key}) throw new Exception('Method "endsWith" requires string argument.');`);
        }

        const valueKey = this.__generateKeyName(`endsWith`);
        if (this.__isJavaScript()) {
            this.__addCode(`let ${valueKey} = (${this.key}.endsWith(${string.key}));`);
        } else {
            this.__addCode(`def ${valueKey} = (${this.key}.endsWith(${string.key}));`);
        }
        return new ScriptWrapper(valueKey, this.context);
    }

    //=================================================== FUNCTIONS ===================================================
    /**
     * Returns object with ScriptWrapper utility functions/attributes. These work within the context of the whole document, rather than single property.
     * @returns {*}
     */
    get utils() {
        const self = this;

        return {
            /**
             * Returns ScriptWrapper with document _id
             * @returns {ScriptWrapper} New string ScriptWrapper instance
             */
            get _id() {
                const valueKey = self.__generateKeyName(`id`);
                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = root._id;`);
                } else {
                    self.__addCode(`def ${valueKey} = ctx._id;`);
                }
                return new ScriptWrapper(valueKey, self.context);
            },

            /**
             * Returns ScriptWrapper with document index alias
             * @returns {ScriptWrapper} New string ScriptWrapper instance
             */
            get _alias() {
                const valueKey = self.__generateKeyName(`alias`);
                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = '${self.context.alias}';`);
                } else {
                    self.__addCode(`def ${valueKey} = '${self.context.alias}';`);
                }
                return new ScriptWrapper(valueKey, self.context);
            },

            /**
             * Deletes the whole document from ES
             */
            deleteDocument() {
                if (self.__isJavaScript()) {
                    self.__addCode(`root._id = null;`);
                } else {
                    self.__addCode(`ctx.op = 'delete';`);
                }
            },

            /**
             * Creates new ScriptWrapper value
             * @param value {*} Source from which to create
             * @returns {ScriptWrapper} New ScriptWrapper instance
             */
            createValue(value) {
                const valueKey = self.__generateKeyName(`custom`);

                const parsedValue = self.__parseValue(value);
                if (_.isUndefined(parsedValue)) {
                    throw Error(`You cannot create 'undefined' value.`);
                }

                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = ${parsedValue};`);
                } else {
                    self.__addCode(`def ${valueKey} = ${parsedValue};`);
                }

                return new ScriptWrapper(valueKey, self.context);
            },

            /**
             * Returns new random number
             * @returns {ScriptWrapper} New number ScriptWrapper instance
             */
            random() {
                const valueKey = self.__generateKeyName(`random`);
                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = Math.random();`);
                } else {
                    self.__addCode(`def ${valueKey} = Math.random();`);
                }
                return new ScriptWrapper(valueKey, self.context);
            },

            /**
             * Makes if statement
             * @param condition {ScriptWrapper} Reference to boolean ScriptWrapper instance
             * @param _body {Function} Function to be performed when condition is accomplished
             * @param _else {Function} Function to be performed when condition is NOT accomplished
             */
            if(condition, _body, _else = void 0) {
                if (!(condition instanceof ScriptWrapper)) {
                    throw Error(`Function 'if' requires its first parameter (condition) to be an instance of 'ScriptWrapper'.`);
                } else if (!_.isFunction(_body)) {
                    throw Error(`Function 'if' requires its second parameter (_body) to be a function.`);
                } else if (!_.isNil(_else) && !_.isFunction(_else)) {
                    throw Error(`Function 'if' requires its third parameter (_else) to be either undefined or a function.`);
                }

                self.__addCode(`if (${condition.key}) { `);
                _body();
                self.__addCode(` }`);

                if (!_.isNil(_else)) {
                    self.__addCode(` else { `);
                    _else();
                    self.__addCode(` }`);
                }
            },

            /**
             * Makes while loop
             * @param condition {ScriptWrapper} Reference to boolean ScriptWrapper instance
             * @param _body {Function} Function to be performed, must also update "condition" ScriptWrapper accordingly
             */
            while(condition, _body) {
                if (!(condition instanceof ScriptWrapper)) {
                    throw Error(`Function 'while' requires its first parameter (condition) to be an instance of 'ScriptWrapper'.`);
                } else if (!_.isFunction(_body)) {
                    throw Error(`Function 'while' requires its second parameter (_body) to be a function.`);
                }

                self.__addCode(`while (${condition.key}) { `);
                _body();
                self.__addCode(` }`);
            },

            /**
             * Makes do-while loop
             * @param _body {Function} Function to be performed, must also update "condition" ScriptWrapper accordingly
             * @param condition {ScriptWrapper} Reference to boolean ScriptWrapper instance
             */
            doWhile(_body, condition) {
                if (!_.isFunction(_body)) {
                    throw Error(`Function 'doWhile' requires its first parameter (_body) to be a function.`);
                } else if (!(condition instanceof ScriptWrapper)) {
                    throw Error(`Function 'doWhile' requires its second parameter (condition) to be an instance of 'ScriptWrapper'.`);
                }

                self.__addCode(`do { `);
                _body();
                self.__addCode(` } while (${condition.key});`);
            },

            /**
             * Iterates through array elements from right to left
             * @param array {ScriptWrapper} Reference to an array to iterate through
             * @param _body {Function} Function to be performed on each element, receives the element as an argument
             */
            forOf(array, _body) {
                if (!(array instanceof ScriptWrapper)) {
                    throw Error(`Function 'forOf' requires its first parameter (array) to be an instance of 'ScriptWrapper'.`);
                } else if (!_.isFunction(_body)) {
                    throw Error(`Function 'forOf' requires its second parameter (_body) to be a function.`);
                }

                const isArray = array.isArray();
                if (self.__isJavaScript()) {
                    self.__addCode(`if (!${isArray.key}) throw Error('Function "forOf" requires its first parameter to be of type array.');`);
                } else {
                    self.__addCode(`if (!${isArray.key}) throw new Exception('Function "forOf" requires its first parameter to be of type array.');`);
                }

                const arrayLength = array.length();
                const iteratorKey = self.__generateKeyName(`iterator`);
                const iterator = new ScriptWrapper(iteratorKey, self.context);
                const elementKey = self.__generateKeyName(`element`);
                const value = new ScriptWrapper(elementKey, self.context);

                if (self.__isJavaScript()) {
                    self.__addCode(`for (let ${iteratorKey} = (${arrayLength.key} - 1); ${iteratorKey} >= 0; --${iteratorKey}) { `);
                    self.__addCode(`let ${elementKey} = ${array.key}[${iteratorKey}];`);
                    _body(value, iterator);
                    self.__addCode(` }`);

                } else {
                    self.__addCode(`for (def ${iteratorKey} = (${arrayLength.key} - 1); ${iteratorKey} >= 0; --${iteratorKey}) { `);
                    self.__addCode(`def ${elementKey} = ${array.key}[${iteratorKey}];`);
                    _body(value, iterator);
                    self.__addCode(` }`);
                }
            },

            /**
             * Adds "break" statement to the code
             */
            BREAK() {
                self.__addCode(`break;`);
            },

            /**
             * Adds "continue" statement to the code
             */
            CONTINUE() {
                self.__addCode(`continue;`);
            },

            /**
             * Creates AND (&&) boolean ScriptWrapper from all specified ScriptWrapper
             * @param booleans {ScriptWrapper} Multiple boolean ScriptWrapper instances (at least two) to AND
             * @returns {ScriptWrapper} New boolean ScriptWrapper instance
             */
            AND(...booleans) {
                if (booleans.length <= 1) {
                    throw Error(`Function 'AND' requires at least two boolean ScriptWrapper values.`);
                } else if (booleans.some((boolean) => !(boolean instanceof ScriptWrapper))) {
                    throw Error(`Function 'AND' requires all its values to be instances of ScriptWrapper.`);
                }

                for (const boolean of booleans) {
                    const isBoolean = boolean.isBoolean();
                    if (self.__isJavaScript()) {
                        self.__addCode(`if (!${isBoolean.key}) throw Error('Function "AND" must be called with booleans.');`);
                    } else {
                        self.__addCode(`if (!${isBoolean.key}) throw new Exception('Function "AND" must be called with booleans.');`);
                    }
                }

                const valueKey = self.__generateKeyName(`AND`);
                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = (`);
                    self.__addCode(booleans.map((boolean) => boolean.key).join(` && `));
                    self.__addCode(`);`);
                } else {
                    self.__addCode(`def ${valueKey} = (`);
                    self.__addCode(booleans.map((boolean) => boolean.key).join(` && `));
                    self.__addCode(`);`);
                }

                return new ScriptWrapper(valueKey, self.context);
            },

            /**
             * Creates OR (||) boolean ScriptWrapper from all specified ScriptWrapper
             * @param booleans {ScriptWrapper} Multiple boolean ScriptWrapper instances (at least two) to OR
             * @returns {ScriptWrapper} New boolean ScriptWrapper instance
             */
            OR(...booleans) {
                if (booleans.length <= 1) {
                    throw Error(`Function 'OR' requires at least two boolean ScriptWrapper values.`);
                } else if (booleans.some((boolean) => !(boolean instanceof ScriptWrapper))) {
                    throw Error(`Function 'OR' requires all its values to be instances of ScriptWrapper.`);
                }

                for (const boolean of booleans) {
                    const isBoolean = boolean.isBoolean();
                    if (self.__isJavaScript()) {
                        self.__addCode(`if (!${isBoolean.key}) throw Error('Function "OR" must be called with booleans.');`);
                    } else {
                        self.__addCode(`if (!${isBoolean.key}) throw new Exception('Function "OR" must be called with booleans.');`);
                    }
                }

                const valueKey = self.__generateKeyName(`OR`);
                if (self.__isJavaScript()) {
                    self.__addCode(`let ${valueKey} = (`);
                    self.__addCode(booleans.map((boolean) => boolean.key).join(` || `));
                    self.__addCode(`);`);
                } else {
                    self.__addCode(`def ${valueKey} = (`);
                    self.__addCode(booleans.map((boolean) => boolean.key).join(` || `));
                    self.__addCode(`);`);
                }

                return new ScriptWrapper(valueKey, self.context);
            }
        };
    }
}

module.exports = ScriptWrapper;