/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const { ObjectId } = require('./objectId_utils');
const util = require('util');

class DBSequence {
    constructor(col) {
        this._collection = col.client.define_collection(col);
    }

    async _create(pool) {
        // The collection is created via define_collection which adds it to tables list
    }

    async nextsequence() {
        // empty query, we maintain a single doc in this collection
        const query = {};
        const update = { $inc: { object_version_seq: 1 } };
        const options = { upsert: true, returnOriginal: false };
        const res = await this._collection.findOneAndUpdate(query, update, options);
        return res.value.object_version_seq;
    }
}

/**
 * make a list of ObjectId unique by indexing their string value
 * this is needed since ObjectId is an object so === comparison is not
 * logically correct for it even for two objects with the same id.
 */
function uniq_ids(docs, doc_path) {
    const map = {};
    _.each(docs, doc => {
        let id = _.get(doc, doc_path);
        if (id) {
            id = id._id || id;
            map[String(id)] = id;
        }
    });
    return _.values(map);
}

/**
 * populate a certain doc path which contains object ids to another collection
 * @template {{}} T
 * @param {T[]} docs
 * @param {string} doc_path
 * @param {Object} collection
 * @param {Object} [fields]
 * @returns {Promise<T[]>}
 */
async function populate(docs, doc_path, collection, fields) {
    const docs_list = _.isArray(docs) ? docs : [docs];
    const ids = uniq_ids(docs_list, doc_path);
    if (!ids.length) return docs;
    const items = await collection.find({ _id: { $in: ids } }, { projection: fields });
    const idmap = _.keyBy(items, '_id');
    _.each(docs_list, doc => {
        const id = _.get(doc, doc_path);
        if (id) {
            const item = idmap[String(id)];
            _.set(doc, doc_path, item);
        }
    });
    return docs;
}

function resolve_object_ids_recursive(idmap, item) {
    _.each(item, (val, key) => {
        if (val instanceof ObjectId) {
            if (key !== '_id') {
                const obj = idmap[val.toHexString()];
                if (obj) {
                    item[key] = obj;
                }
            }
        } else if (_.isObject(val) && !_.isString(val)) {
            resolve_object_ids_recursive(idmap, val);
        }
    });
    return item;
}

function resolve_object_ids_paths(idmap, item, paths, allow_missing) {
    _.each(paths, path => {
        const ref = _.get(item, path);
        if (is_object_id(ref)) {
            const obj = idmap[ref];
            if (obj) {
                _.set(item, path, obj);
            } else if (!allow_missing) {
                throw new Error('resolve_object_ids_paths missing ref to ' +
                    path + ' - ' + ref + ' from item ' + util.inspect(item));
            }
        } else if (!allow_missing) {
            if (!ref || !is_object_id(ref._id)) {
                throw new Error('resolve_object_ids_paths missing ref id to ' +
                    path + ' - ' + ref + ' from item ' + util.inspect(item));
            }
        }
    });
    return item;
}

function is_object_id(id) {
    return (id instanceof ObjectId);
}

/*
 *@param base - the array to subtract from
 *@param values - array of values to subtract from base
 *@out - return an array of string containing values in base which did no appear in values
 */
function obj_ids_difference(base, values) {
    const map_base = {};
    for (let i = 0; i < base.length; ++i) {
        map_base[base[i]] = base[i];
    }
    for (let i = 0; i < values.length; ++i) {
        delete map_base[values[i]];
    }
    return _.values(map_base);
}

module.exports = {
    DBSequence,
    uniq_ids,
    populate,
    resolve_object_ids_recursive,
    resolve_object_ids_paths,
    is_object_id,
    obj_ids_difference
};
