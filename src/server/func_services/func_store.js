/* Copyright (C) 2016 NooBaa */
'use strict';

const crypto = require('crypto');
const mongodb = require('mongodb');

// const dbg = require('../../util/debug_module')(__filename);
const db_client = require('../../util/db_client');
const P = require('../../util/promise');

const buffer_utils = require('../../util/buffer_utils');
const func_schema = require('./func_schema');
const func_indexes = require('./func_indexes');

class FuncStore {

    constructor() {
        this._funcs = db_client.instance().define_collection({
            name: 'funcs',
            schema: func_schema,
            db_indexes: func_indexes,
        });
        // this._func_code = db_client.instance().define_gridfs({
        //     name: 'func_code_gridfs'
        // });
    }

    static instance() {
        if (!FuncStore._instance) FuncStore._instance = new FuncStore();
        return FuncStore._instance;
    }

    make_func_id(id_str) {
        return new mongodb.ObjectId(id_str);
    }

    async create_func(func) {
        try {
            this._funcs.validate(func);
            await this._funcs.insertOne(func);
        } catch (err) {
            db_client.instance().check_duplicate_key_conflict(err, 'func');
        }
        return func;
    }

    async delete_func(func_id) {
        await this._funcs.updateOne({
            _id: func_id,
        }, {
            $set: {
                deleted: new Date()
            }
        });
    }

    update_func(func_id, set_updates) {
        return P.resolve().then(async () => {
            await this._funcs.updateOne({
                _id: func_id,
            }, {
                $set: set_updates
            });
        });
    }

    async read_func(system, name, version) {
        const res = await this._funcs.findOne({
            system: system,
            name: name,
            version: version,
            deleted: null,
        });
        return db_client.instance().check_entity_not_deleted(res, 'func');
    }

    get_by_id_include_deleted(func_id) {
        return P.resolve().then(async () => this._funcs.findOne({
            _id: func_id,
        }));
    }

    async list_funcs(system) {
        const list = await this._funcs.find({
            system: system,
            version: '$LATEST',
            deleted: null,
        });
        return list;
    }

    async list_funcs_by_pool(system, pool) {
        const list = await this._funcs.find({
            system: system,
            pools: pool,
            deleted: null,
        });
        return list;
    }

    async list_func_versions(system, name) {
        const list = await this._funcs.find({
            system: system,
            name: name,
            deleted: null,
        });
        return list;
    }

    create_code(params) {
        const code = params.code;
        const sha256 = crypto.createHash('sha256');
        //this is the base64 size. if we want the code size 
        //if we want the code size it should be between 1 to ~3/4 of that size
        var size = code.length;
        return {
            code,
            sha256: sha256.digest('base64'),
            size: size,
        };
    }

    //TODO LMLM: remove...
    async delete_code_gridfs(id) {
        return this._func_code.gridfs().delete(id);
    }

    // stream_code_gridfs(id) {
    //     return this._func_code.gridfs().openDownloadStream(id);
    // }

    stream_code_gridfs(id) {
        return this._func_code.gridfs().openDownloadStream(id);
    }

    async read_code_gridfs(id) {
        return buffer_utils.read_stream_join(this.stream_code_gridfs(id));
    }

    code_filename(system, name, version) {
        return system + '/' + name + '/' + version;
    }

}

FuncStore._instance = undefined;

// EXPORTS
exports.FuncStore = FuncStore;
exports.instance = FuncStore.instance;
