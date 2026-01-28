/* Copyright (C) 2016 NooBaa */
'use strict';

const crypto = require('crypto'); // Used for Random Number Generation only
const util = require('util');

class ObjectId {
    /**
     * @param {(string|Buffer|number|ObjectId)} [id]
     */
    constructor(id) {
        if (id instanceof ObjectId) {
            // eslint-disable-next-line no-constructor-return
            return id;
        }

        this._bsontype = 'ObjectID';

        if ((typeof id) === 'string') {
            if (id.length !== 24 || !ObjectId.isValid(id)) {
                throw new Error('Argument passed in must be a string of 12 bytes or a string of 24 hex characters');
            }
            this.id = Buffer.from(id, 'hex');
        } else if (Buffer.isBuffer(id)) {
            if (id.length !== 12) {
                throw new Error('Argument passed in must be a Buffer of 12 bytes');
            }
            this.id = id;
        } else if (typeof id === 'number') {
            throw new Error('Argument passed in must be a string of 12 bytes or a string of 24 hex characters');
        } else if (id) {
            throw new Error('Argument passed in must be a string of 12 bytes or a string of 24 hex characters');
        } else {
            // Generates a new ObjectId
            // 4 byte timestamp
            const timestamp = Math.floor(Date.now() / 1000);
            const timeBuffer = Buffer.alloc(4);
            timeBuffer.writeUInt32BE(timestamp, 0);

            // 8 bytes random
            const randomBuffer = crypto.randomBytes(8);

            this.id = Buffer.concat([timeBuffer, randomBuffer]);
        }
    }

    /**
     * @returns {string}
     */
    toHexString() {
        return this.id.toString('hex');
    }

    /**
     * @returns {string}
     */
    toString() {
        return this.toHexString();
    }

    /**
     * @returns {string}
     */
    inspect() {
        return `ObjectId("${this.toHexString()}")`;
    }

    /**
     * @returns {string}
     */
    toJSON() {
        return this.toHexString();
    }

    /**
     * @param {ObjectId} other
     * @returns {boolean}
     */
    equals(other) {
        if (other instanceof ObjectId) {
            return this.id.equals(other.id);
        }
        if (typeof other === 'string') {
            return ObjectId.isValid(other) && this.id.toString('hex') === other;
        }
        return false;
    }

    /**
     * @returns {Date}
     */
    getTimestamp() {
        const timestamp = this.id.readUInt32BE(0);
        return new Date(timestamp * 1000);
    }

    /**
     * @param {string|Number|ObjectId} id
     * @returns {boolean}
     */
    static isValid(id) {
        if (!id) return false;
        if (id instanceof ObjectId) return true;
        if (typeof id === 'string') {
            return id.length === 24 && (/^[0-9a-fA-F]{24}$/).test(id);
        }
        if (Buffer.isBuffer(id)) {
            return id.length === 12;
        }
        return false;
    }

    /**
     * @param {number} time
     * @returns {ObjectId}
     */
    static createFromTime(time) {
        const id = Buffer.alloc(12);
        id.writeUInt32BE(time, 0);
        return new ObjectId(id);
    }
}

// Custom inspect for Node.js
ObjectId.prototype[util.inspect.custom] = ObjectId.prototype.inspect;

class Binary {
    /**
     * @param {Buffer} buffer
     * @param {number} [subType]
     */
    constructor(buffer, subType) {
        this.buffer = buffer;
        this.subType = subType;
        this._bsontype = 'Binary';
    }

    length() {
        return this.buffer.length;
    }

    /**
     * @param {number} length
     */
    read(length) {
        return this.buffer.read(length);
    }

    value() {
        return this.buffer;
    }

    toString(format) {
        return this.buffer.toString(format);
    }
}

module.exports = {
    ObjectId,
    ObjectID: ObjectId,
    Binary
};
