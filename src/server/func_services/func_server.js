/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const request = require('request');

const P = require('../../util/promise');
const stream = require('stream');
const buffer_utils = require('../../util/buffer_utils');
const { RpcError, RPC_BUFFERS } = require('../../rpc');
const dbg = require('../../util/debug_module')(__filename);
const FuncNode = require('../../agent/func_services/func_node');
const addr_utils = require('../../util/addr_utils');
const Dispatcher = require('../notifications/dispatcher');
const server_rpc = require('../server_rpc');
const func_store = require('./func_store');
const func_stats_store = require('./func_stats_store');
const system_store = require('../system_services/system_store').get_instance();
const auth_server = require('../common_services/auth_server');
const node_allocator = require('../node_services/node_allocator');

const AWS = require('aws-sdk');

const FUNC_CONFIG_FIELDS_MUTABLE = [
    'description',
    'role',
    'handler',
    'runtime',
    'memory_size',
    'timeout',
];
const FUNC_CONFIG_DEFAULTS = {
    handler: 'index',
    runtime: 'nodejs6',
    memory_size: 128,
    timeout: 60,
};
const FUNC_CONFIG_FIELDS_IMMUTABLE = [
    'code_size',
    'code_sha256',
    'resource_name',
];

const FUNC_STATS_DEFAULTS = {
    invoked: 0,
    fulfilled: 0,
    rejected: 0,
    aggr_response_time: 0,
    max_response_time: 0,
    avg_response_time: 0,
};

async function create_func(req) {
    dbg.log0('create_func::', req.params.config);
    const { config: func_config, code: func_code } = req.params;
    const { _id: system, pools_by_name } = req.system;
    const { _id: exec_account } = req.account;
    const { name, pools: pool_names = [] } = func_config;
    const version = '$LATEST';
    const pools = pool_names.map(pool_name => pools_by_name[pool_name]);
    for (const pool of pools) {
        if (pool.cloud_pool_info || pool.mongo_pool_info) {
            throw new RpcError('FORBIDDEN', 'invalid_pool');
        }
    }
    const resource_name = `arn:noobaa:lambda:region:${system}:function:${name}:${version}`;
    const code = await _get_func_code_stream(req, func_code);

    const {
        sha256: code_sha256,
        size: code_size
    } = await func_store.instance()
        .create_code({ system, name, version, code });

    const func_id = func_store.instance().make_func_id();
    await func_store.instance().create_func({
        ...FUNC_CONFIG_DEFAULTS,
        ..._.pick(func_config, FUNC_CONFIG_FIELDS_MUTABLE),
        _id: func_id,
        system,
        exec_account,
        name,
        version,
        last_modified: new Date(),
        last_modifier: exec_account,
        resource_name,
        pools: pools.map(pool => pool._id),
        code,
        code_sha256,
        code_size
    });

    Dispatcher.instance().activity({
        event: 'functions.func_created',
        level: 'info',
        system: req.system._id,
        actor: req.account && req.account._id,
        func: func_id,
        desc: ''
    });

    await _load_func(req);
    return _get_func_info(req.func);
}

async function update_func(req) {
    await _load_func(req);

    const { func, params, system } = req;
    const func_config = params.config;
    const config_updates = _.pick(func_config, FUNC_CONFIG_FIELDS_MUTABLE);
    let code_updated = false;

    if (func_config.pools) {
        config_updates.pools = _.map(
            func_config.pools,
            pool_name => system.pools_by_name[pool_name]._id
        );
    }

    const func_code = params.code;
    //TODO: LMLM: We dont really need this as the code is in the same collection
    if (func_code) {
        await func_store.instance().delete_code_gridfs(func.code_gridfs_id);
        const code_stream = await _get_func_code_stream(req, func_code);
        const res = await func_store.instance().create_code_gridfs({
            system: func.system,
            name: func.name,
            version: func.version,
            code_stream,
        });

        config_updates.code_gridfs_id = res.id;
        config_updates.code_sha256 = res.sha256;
        config_updates.code_size = res.size;

        code_updated = true;
    }

    config_updates.last_modified = new Date();
    config_updates.last_modifier = req.account._id;

    await func_store.instance().update_func(func._id, config_updates);
    await _load_func(req);

    let act = {
        level: 'info',
        system: req.system._id,
        actor: req.account && req.account._id,
        func: req.func._id,
        desc: ''
    };
    if (code_updated) {
        act.event = 'functions.func_code_edit';
    } else {
        act.event = 'functions.func_config_edit';
    }

    Dispatcher.instance().activity(act);
    return _get_func_info(func);
}

async function delete_func(req) {
    dbg.log0('delete_func::', req.params.name);
    await _load_func(req);
    //TODO: LMLM: we might not deleting the record, need to check if we have bg for that.
    // If not we might want to change the code field to an empty one. (or maybe not).
    await func_store.instance().delete_func(req.func._id);
}

async function read_func(req) {
    await _load_func(req);
    const reply = _get_func_info(req.func);
    console.log('LMLM: read_func::', reply);
    if (req.params.read_code) {
        const system = req.system._id;
        const name = req.params.name || _.get(req, 'params.config.name');
        const version = req.params.version || _.get(req, 'params.config.version');
        const func = await func_store.instance().read_func(system, name, version);
        console.log('LMLM: func:', func);
        const zipfile_b64 = func.code;
        console.log('LMLM: zipfile_b64:', zipfile_b64);

        // console.log('LMLM: req.params.read_code::', req.params.read_code);
        // const zipfile = await func_store.instance().read_code_gridfs(req.func.code_gridfs_id);
        // console.log('LMLM: zipfile:', zipfile);
        const type = typeof zipfile_b64;
        console.log('LMLM: typeof zipfile_b64:', type);
        //LMLM: converting the base64 string into a zipfile again.
        const zipfile_stream = new stream.Readable({
            read(size) {
                this.push(Buffer.from(zipfile_b64, 'base64'));
                this.push(null);
            }
        });
        const type_zipfile = typeof zipfile_stream;
        console.log('LMLM: typeof type_zipfile:', type_zipfile);
        const zipfile = await buffer_utils.read_stream_join(zipfile_stream);
        reply[RPC_BUFFERS] = { zipfile };
        console.log('LMLM: reply[RPC_BUFFERS]', reply[RPC_BUFFERS]);
        const typerep = typeof reply[RPC_BUFFERS];
        console.log('LMLM: typeof reply[RPC_BUFFERS]', typerep);
    }
    console.log('LMLM: about to return reply');
    return reply;
}

async function read_func_stats(req) {
    // Load the function into the request.
    await _load_func(req);

    const {
        since,
        till = Date.now(),
        step,
        percentiles = [0.5, 0.9, 0.99],
        max_samples = 10000
    } = req.params;

    if (till <= since) {
        throw new Error('read_func_stats: Invalid time range');
    }

    if (!step || step <= 0) {
        throw new Error('read_func_stats: Invalid step value');
    }

    const normalized_since = Math.floor(since / step) * step;
    const normalized_till = Math.ceil(till / step) * step;
    const by_key = _.fromPairs(await func_stats_store.instance()
        .query_func_stats({
            system: req.system._id,
            func: req.func._id,
            since: new Date(normalized_since),
            till: new Date(normalized_till),
            step,
            percentiles,
            max_samples
        }));

    const emptyPercentilesArray = percentiles.map(percentile => {
        const value = 0;
        return { percentile, value };
    });

    return {
        stats: {
            ...FUNC_STATS_DEFAULTS,
            since: normalized_since,
            till: normalized_till,
            response_percentiles: emptyPercentilesArray,
            ...by_key[-1]
        },
        slices: _.range(
            normalized_since,
            normalized_till,
            step
        ).map(since_time => ({
            ...FUNC_STATS_DEFAULTS,
            since: since_time,
            till: since + step,
            response_percentiles: emptyPercentilesArray,
            ...by_key[since_time]
        }))
    };
}

async function list_funcs(req) {
    const funcs = await func_store.instance().list_funcs(req.system._id);
    return {
        functions: _.map(funcs, _get_func_info)
    };
}

async function list_func_versions(req) {
    const funcs = await func_store.instance().list_func_versions(
        req.system._id,
        req.params.name
    );
    return {
        versions: _.map(funcs, _get_func_info)
    };
}

const server_func_node = new FuncNode({
    rpc_client: server_rpc.client,
    storage_path: '/tmp/',
});

async function invoke_func(req) {
    let res;
    dbg.log0('invoke_func::', req.params.name);
    const time = new Date();
    await _load_func(req);
    check_event_permission(req);
    await P.map(req.func.pools, pool => node_allocator.refresh_pool_alloc(pool));

    const func = req.func;
    const node = node_allocator.allocate_node({ pools: func.pools });
    const params = {
        config: _get_func_info(func).config,
        event: req.params.event,
        aws_config: _make_aws_config(req),
        rpc_options: _make_rpc_options(req),
    };
    if (node) {
        dbg.log0('invoking on node',
            func.name, req.params.event,
            node.name, node.pool);
        res = await server_rpc.client.func_node.invoke_func(params, {
            address: node.rpc_address
        });
    } else {
        dbg.log0('invoking on server', func.name, req.params.event);
        res = await server_func_node.invoke_func({ params });
    }
    await func_stats_store.instance().create_func_stat({
        system: req.func.system,
        func: req.func._id,
        time: time,
        took: Date.now() - time.getTime(),
        error: res.error ? true : undefined,
        error_msg: res.error ? res.error.message : undefined,
    });
    if (!_.isUndefined(res.error)) {
        throw res.error;
    }
    return res;
}

function check_event_permission(req) {
    const event = req.params.event;
    if (event && event.Records) {
        _.forEach(event.Records, record => {
            const bucket_name = record && record.s3 && record.s3.bucket && record.s3.bucket.name;
            if (typeof(bucket_name) === 'string') {
                const bucket = req.system.buckets_by_name && req.system.buckets_by_name[bucket_name];
                if (!bucket || bucket.deleting) throw new RpcError('UNAUTHORIZED', 'No such bucket');
                const account = system_store.data.get_by_id(req.func.exec_account);
                if (!auth_server.has_bucket_permission(bucket, account)) {
                    throw new RpcError('UNAUTHORIZED', 'No bucket permission for trigger');
                }
            }
        });
    }
}

function _get_func_code_stream(req, func_code) { //TODO: LMLM: rename to _get_func_code_string
    if (func_code.zipfile_b64) {
        console.log('LMLM: zipfile is given as base64 string');
        return func_code.zipfile_b64;
    } else if (req.rpc_params[RPC_BUFFERS] && req.rpc_params[RPC_BUFFERS].zipfile) {
        console.log('LMLM: zipfile is given as buffer');
        return req.rpc_params[RPC_BUFFERS].zipfile.toString('base64');
    } else if (func_code.s3_bucket && func_code.s3_key) {
        console.log(`LMLM: reading function code from bucket ${func_code.s3_bucket} and key ${func_code.s3_key}`);
        const account_keys = req.account.access_keys[0];
        const s3_endpoint = new AWS.S3({
            endpoint: 'http://127.0.0.1',
            accessKeyId: account_keys.access_key,
            secretAccessKey: account_keys.secret_key,
        });

        const get_object_req = s3_endpoint.getObject({
            Bucket: func_code.s3_bucket,
            Key: func_code.s3_key,
        });
        return get_object_req.createReadStream(); //TODO: LMLM: change to string base64
    } else if (func_code.url) {
        return new Promise((resolve, reject) => {
            console.log(`LMLM: reading function code from ${func_code.url}`);
            request({
                    url: func_code.url,
                    method: 'GET',
                    encoding: null, // get a buffer
                })
                .once('response', res => {

                    if (res.statusCode !== 200) {
                        return reject(new Error(`failed GET request from ${func_code.url}`));
                    }
                    //TODO LMLM: return a base64 string
                    return resolve(res);
                })
                .once('error', err => reject(err));
        });
    } else {
        throw new Error('Unsupported code');
    }
}

async function _load_func(req) {
    const system = req.system._id;
    const name = req.params.name || _.get(req, 'params.config.name');
    const version = req.params.version || _.get(req, 'params.config.version');
    const func = await func_store.instance().read_func(system, name, version);
    console.log('LMLM: func from _load_func:', func);
    req.func = func;
    func.pools = _.map(func.pools, pool_id => system_store.data.get_by_id(pool_id));
    console.log('LMLM: returning from _load_func');
    return func;
}

function _get_func_info(func) {
    const config = _.pick(func,
        'name',
        'version',
        FUNC_CONFIG_FIELDS_MUTABLE,
        FUNC_CONFIG_FIELDS_IMMUTABLE);
    config.last_modified = func.last_modified.getTime();
    const account = system_store.data.get_by_id(func.last_modifier);
    config.last_modifier = account ? account.email : '';
    config.pools = _.map(func.pools, pool => {
        if (pool.name) return pool.name;
        return system_store.data.get_by_id(pool).name;
    });
    config.exec_account = system_store.data.get_by_id(func.exec_account).email;
    const code_location = {
        url: '',
        repository: ''
    };
    return {
        config,
        code_location
    };
}

function _make_rpc_options(req) {
    // TODO: need to change when we start to run funcs outside the
    // cluster (should use a proper hint based on agent location)
    const base_address = addr_utils.get_base_address(req.system.system_address);
    const account = system_store.data.get_by_id(req.func.exec_account);
    const auth_token = auth_server.make_auth_token({
        system_id: req.system._id,
        account_id: account._id,
        role: 'admin',
    });
    return {
        address: base_address.toString(),
        auth_token: auth_token,
    };
}

function _make_aws_config(req) {
    // TODO: need to change when we start to run funcs outside the
    // cluster (should use a proper hint based on agent location)
    const ep_addr = addr_utils.get_base_address(req.system.system_address, {
        hint: 'INTERNAL',
        service: 's3',
        api: 's3',
        protocol: 'https',
    });
    const account = system_store.data.get_by_id(req.func.exec_account);
    const account_keys = account.access_keys[0];
    return {
        region: 'us-east-1',
        endpoint: ep_addr.href,
        sslEnabled: false,
        s3ForcePathStyle: true,
        accessKeyId: account_keys.access_key.unwrap(),
        secretAccessKey: account_keys.secret_key.unwrap(),
    };
}

exports.create_func = create_func;
exports.update_func = update_func;
exports.delete_func = delete_func;
exports.read_func = read_func;
exports.read_func_stats = read_func_stats;
exports.list_funcs = list_funcs;
exports.list_func_versions = list_func_versions;
exports.invoke_func = invoke_func;
