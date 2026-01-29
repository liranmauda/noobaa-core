/* Copyright (C) 2016 NooBaa */
/* eslint max-lines: ['error', 2500] */
'use strict';

const _ = require('lodash');
const fs = require('fs');
const moment = require('moment');
const net = require('net');

const P = require('../../util/promise');
const dbg = require('../../util/debug_module')(__filename);
const pkg = require('../../../package.json');
const diag = require('../utils/server_diagnostics');
const cutil = require('../utils/clustering_utils');
const config = require('../../../config.js');
const MDStore = require('../object_services/md_store').MDStore;
const fs_utils = require('../../util/fs_utils');
const os_utils = require('../../util/os_utils');
const server_rpc = require('../server_rpc');
const cluster_hb = require('../bg_services/cluster_hb');
const Dispatcher = require('../notifications/dispatcher');
const system_store = require('./system_store').get_instance();
const { RpcError, RPC_BUFFERS } = require('../../rpc');

const VERIFY_RESPONSE = [
    'OKAY',
    'SECRET_MISMATCH',
    'VERSION_MISMATCH',
    'ALREADY_A_MEMBER',
    'HAS_OBJECTS',
    'UNREACHABLE',
    'ADDING_SELF',
    'NO_NTP_SET',
    'CONNECTION_TIMEOUT_ORIGIN',
    'CONNECTION_TIMEOUT_NEW'
];

//
//API
//

//Return new cluster info, if doesn't exists in db
async function new_cluster_info(params) {
    if (system_store.get_local_cluster_info()) {
        return;
    }

    const address = (params && params.address) || os_utils.get_local_ipv4_ips()[0];
    const cluster = {
        _id: system_store.new_system_store_id(),
        debug_level: 0,
        is_clusterized: false,
        owner_secret: system_store.get_server_secret(),
        cluster_id: system_store.get_server_secret(),
        owner_address: address,
        owner_shardname: 'shard1',
        location: 'DC1',
        shards: [{
            shardname: 'shard1',
            servers: [{
                address: address //TODO:: on multiple nics support, fix this
            }],
        }],
        config_servers: [],
    };
    return _attach_server_configuration(cluster);
}

function init_cluster() {
    return cluster_hb.do_heartbeat({ skip_server_monitor: true });
}

//Initiate process of adding a server to the cluster
function verify_join_conditions(req) {
    dbg.log0('Got verify_join_conditions request');
    return P.resolve()
        .then(() => os_utils.os_info())
        .then(os_info => {
            const hostname = os_info.hostname;
            let caller_address;
            if (req.connection && req.connection.url) {
                caller_address = req.connection.url.hostname.includes('ffff') ?
                    req.connection.url.hostname.replace(/^.*:/, '') :
                    req.connection.url.hostname;
            } else {
                dbg.error('No connection on request for verify_join_conditions. Got:', req);
                throw new Error('No connection on request for verify_join_conditions');
            }
            return _verify_join_preconditons(req)
                .catch(err => {
                    dbg.error('verify_join_conditions: HAD ERROR', err, err.message);
                    if (_.includes(VERIFY_RESPONSE, err.message)) return err.message;
                    if (err.message === 'CONNECTION_TIMEOUT') return 'CONNECTION_TIMEOUT_NEW';
                    throw err;
                })
                .then(result => ({
                    result,
                    caller_address,
                    hostname,
                }));
        });
}

function _check_candidate_version(req) {
    dbg.log0('_check_candidate_version for address', req.rpc_params.address);
    return P.resolve(
            server_rpc.client.cluster_internal.get_version(undefined, {
                address: server_rpc.get_base_address(req.rpc_params.address),
                timeout: 60000 //60s
            })
        )
        .then(({ version }) => {
            dbg.log0('_check_candidate_version got version', version);
            if (version !== pkg.version) {
                return {
                    result: 'VERSION_MISMATCH',
                    version
                };
            }
            return {
                result: 'OKAY'
            };
        })
        .catch(err => {
            if (err.rpc_code === 'NO_SUCH_RPC_SERVICE') {
                dbg.warn('_check_candidate_version got NO_SUCH_RPC_SERVICE from a server with an old version');
                // Called server is too old to have this code
                return {
                    result: 'VERSION_MISMATCH'
                };
            }
            throw err;
        });
}

async function verify_candidate_join_conditions(req) {
    try {
        dbg.log0('Got verify_candidate_join_conditions for server secret:', req.rpc_params.secret,
            'address:', req.rpc_params.address);

        if (req.rpc_params.secret === system_store.get_server_secret()) {
            dbg.error('lol trying to add self to cluster - self secret received:', req.rpc_params.secret);
            return {
                result: 'ADDING_SELF'
            };
        }

        const version_check_res = await _check_candidate_version(req);
        if (version_check_res.result !== 'OKAY') return version_check_res;

        const nc_res = await os_utils.exec(`echo -n | nc -w5 ${req.rpc_params.address} ${config.MONGO_DEFAULTS.SHARD_SRV_PORT} 2>&1`, {
            ignore_rc: true,
            return_stdout: true
        });
        if (nc_res.includes('Connection timed out')) {
            dbg.warn(`Could not reach ${req.rpc_params.address}:${config.MONGO_DEFAULTS.SHARD_SRV_PORT}, might be due to a FW blocking`);
            return {
                result: 'CONNECTION_TIMEOUT_ORIGIN'
            };
        }

        const verify_res = await server_rpc.client.cluster_internal.verify_join_conditions({
            secret: req.rpc_params.secret
        }, {
            address: server_rpc.get_base_address(req.rpc_params.address),
            timeout: 60000 //60s
        });
        const version = 'version' in version_check_res ? version_check_res.version : undefined;
        return {
            result: verify_res.result,
            hostname: verify_res.hostname,
            ...(version !== undefined && { version }),
        };

    } catch (err) {
        if (err.rpc_code === 'RPC_CONNECT_TIMEOUT' ||
            err.rpc_code === 'RPC_REQUEST_TIMEOUT') {
            dbg.warn('received', err, ' on verify_candidate_join_conditions');
            return {
                result: 'UNREACHABLE'
            };
        }
        throw err;
    }
}

async function get_version(req) {
    dbg.log0('get_version sending version', pkg.version);
    return {
        version: pkg.version
    };
}

function verify_new_ip(req) {
    const address = server_rpc.get_base_address(req.rpc_params.address);
    console.log('verify_new_ip', address);
    return server_rpc.client.cluster_internal.get_secret({}, {
            address,
            timeout: 20000,
            connect_timeout: 20000,
        })
        .then(res => {
            if (res.secret === req.rpc_params.secret) {
                return {
                    result: 'OKAY'
                };
            } else {
                return {
                    result: 'SECRET_MISMATCH'
                };
            }
        })
        .catch(err => {
            dbg.warn('received', err, ' on verify_new_ip');
            if (err.rpc_code === 'RPC_CONNECT_TIMEOUT' ||
                err.rpc_code === 'RPC_REQUEST_TIMEOUT') {
                return {
                    result: 'UNREACHABLE'
                };
            } else {
                throw err;
            }
        });
}

// Currently only updates server's IP
// This function runs only at the master of cluster
function update_member_of_cluster(req) {
    const topology = cutil.get_topology();
    const is_clusterized = topology.is_clusterized;
    // Shouldn't do anything if there is not cluster
    if (!(is_clusterized && system_store.is_cluster_master)) {
        dbg.log0(`update_member_of_cluster: is_clusterized:${is_clusterized},
            is_master:${system_store.is_cluster_master}`);
        return P.resolve();
    }
    const info = cutil.get_cluster_info();

    return _validate_member_request(_.defaults({
            rpc_params: {
                address: req.rpc_params.new_address,
                new_hostname: req.rpc_params.hostname
            },
            req
        }))
        .then(() => _check_candidate_version(_.defaults({
            rpc_params: {
                address: req.rpc_params.new_address,
                new_hostname: req.rpc_params.hostname
            },
            req
        })))
        .then(version_check_res => {
            if (version_check_res.result !== 'OKAY') throw new Error(`Verify member version check returned ${version_check_res}`);
        })
        .then(() => {
            let shard_index = -1;
            let server_idx = -1;
            for (let i = 0; i < info.shards.length; ++i) {
                server_idx = _.findIndex(info.shards[i].servers,
                    server => server.secret === req.rpc_params.target_secret);
                if (server_idx !== -1) {
                    shard_index = i;
                    break;
                }
            }
            if (shard_index === -1 || server_idx === -1) {
                throw new Error(`could not find address:${req.rpc_params.secret} in any shard`);
            }
            const new_shard = topology.shards[shard_index];
            new_shard.servers[server_idx] = {
                address: req.rpc_params.new_address
            };

        })
        // Update current topology of the server
        .then(() => _update_cluster_info(topology))
        // TODO: solve in a better way
        // added this delay, otherwise the next system_store.load doesn't catch the new servers HB
        .then(() => P.delay(1000))
        .then(function() {
            dbg.log0('Edited member of cluster. New topology',
                cutil.pretty_topology(cutil.get_topology()));
            // reload system_store to update after edited member HB
            return system_store.load();
        })
        // ugly but works. perform first heartbeat after server was edited, so UI will present updated data
        .then(() => cluster_hb.do_heartbeat({ skip_server_monitor: true }))
        .catch(function(err) {
            console.error('Failed edit of member to cluster', req.rpc_params, 'with', err);
            throw new Error('Failed edit of member to cluster');
        })
        .then(() => {
            // do nothing. 
        });
}

function set_debug_level(req) {
    dbg.log0('Recieved set_debug_level req', req.rpc_params);
    const debug_params = req.rpc_params;
    const target_servers = [];
    let audit_activity = {};
    return P.fcall(function() {
            if (debug_params.target_secret) {
                const cluster_server = system_store.data.cluster_by_server[debug_params.target_secret];
                if (!cluster_server) {
                    throw new RpcError('CLUSTER_SERVER_NOT_FOUND',
                        `Server with secret key: ${debug_params.target_secret} was not found`
                    );
                }
                audit_activity = {
                    event: 'dbg.set_server_debug_level',
                    server: {
                        hostname: _.get(cluster_server, 'heartbeat.health.os_info.hostname'),
                        secret: cluster_server.owner_secret
                    }
                };
                target_servers.push(cluster_server);
            } else {
                _.each(system_store.data.clusters, cluster => target_servers.push(cluster));
            }

            return P.map_one_by_one(target_servers, function(server) {
                return server_rpc.client.cluster_internal.apply_set_debug_level(debug_params, {
                    address: server_rpc.get_base_address(server.owner_address),
                    auth_token: req.auth_token
                });
            });
        })
        .then(() => {
            if (!debug_params.target_secret && req.system.debug_level !== debug_params.level) {
                Dispatcher.instance().activity(_.defaults(audit_activity, {
                    event: 'dbg.set_debug_level',
                    level: 'info',
                    system: req.system._id,
                    actor: req.account && req.account._id,
                    desc: `Debug level was set to ${debug_params.level ? 'high' : 'low'}`
                }));
            }
        })
        .then(() => {
            // do nothing. 
        });
}


function apply_set_debug_level(req) {
    dbg.log0('Recieved apply_set_debug_level req', req.rpc_params);
    if (req.rpc_params.target_secret) {
        const cluster_server = system_store.data.cluster_by_server[req.rpc_params.target_secret];
        if (!cluster_server) {
            throw new RpcError('CLUSTER_SERVER_NOT_FOUND', `Server with secret key: ${req.rpc_params.target_secret} was not found`);
        }
        if (cluster_server.debug_level === req.rpc_params.level) {
            dbg.log0('requested to set debug level to the same as current level. skipping..', req.rpc_params);
            return;
        }
    } else if (req.system.debug_level === req.rpc_params.level) {
        dbg.log0('requested to set debug level to the same as current level. skipping..', req.rpc_params);
        return;
    }

    return _set_debug_level_internal(req, req.rpc_params.level)
        .then(() => {
            if (req.rpc_params.level > 0) { //If level was set, remove it after 10m
                P.delay_unblocking(config.DEBUG_MODE_PERIOD) //10m
                    .then(() => _set_debug_level_internal(req, 0));
            }
        })
        .then(() => {
            // do nothing. 
        });
}

function _set_debug_level_internal(req, level) {
    dbg.log0('Recieved _set_debug_level_internal req', req.rpc_params, 'With Level', level);
    return P.resolve()
        .then(() => server_rpc.client.redirector.publish_to_cluster({
            method_api: 'debug_api',
            method_name: 'set_debug_level',
            target: '', // required but irrelevant
            request_params: {
                level: level,
                module: 'core'
            }
        }, {
            auth_token: req.auth_token
        }))
        .then(() => {
            const update_object = {};
            const debug_mode = level > 0 ? Date.now() : undefined;

            if (req.rpc_params.target_secret) {
                const cluster_server = system_store.data.cluster_by_server[req.rpc_params.target_secret];
                if (!cluster_server) {
                    throw new RpcError('CLUSTER_SERVER_NOT_FOUND',
                        `Server with secret key: ${req.rpc_params.target_secret} was not found`);
                }
                if (level > 0) {
                    update_object.clusters = [{
                        _id: cluster_server._id,
                        debug_level: level,
                        debug_mode: debug_mode
                    }];
                } else {
                    update_object.clusters = [{
                        _id: cluster_server._id,
                        $set: {
                            debug_level: level
                        },
                        $unset: {
                            debug_mode: true
                        }
                    }];
                }
            } else if (level > 0) {
                update_object.systems = [{
                    _id: req.system._id,
                    debug_level: level,
                    debug_mode: debug_mode
                }];
            } else {
                update_object.systems = [{
                    _id: req.system._id,
                    $set: {
                        debug_level: level
                    },
                    $unset: {
                        debug_mode: true
                    }
                }];
            }

            return system_store.make_changes({
                update: update_object
            });
        });
}


function diagnose_system(req) {
    const target_servers = [];
    const TMP_WORK_DIR = `/tmp/cluster_diag`;
    const INNER_PATH = `${process.cwd()}/build`;
    const OUT_PATH = '/public/' + req.system.name + '_cluster_diagnostics.tgz';
    const WORKING_PATH = `${INNER_PATH}${OUT_PATH}`;
    if (req.rpc_params.target_secret) {

        const cluster_server = system_store.data.cluster_by_server[req.rpc_params.target_secret];
        if (!cluster_server) {
            throw new RpcError('CLUSTER_SERVER_NOT_FOUND',
                `Server with secret key: ${req.rpc_params.target_secret} was not found`
            );
        }
        target_servers.push(cluster_server);
    } else {
        _.each(system_store.data.clusters, cluster => target_servers.push(cluster));
    }

    //In cases of a single server, address might not be publicly available and so using it might fail
    //This case does not happen when clusterized, but as a WA for a single server, use localhost (see #2803)
    if (!system_store.get_local_cluster_info().is_clusterized) {
        target_servers[0].owner_address = 'localhost';
    }

    Dispatcher.instance().activity({
        event: 'dbg.diagnose_system',
        level: 'info',
        system: req.system._id,
        actor: req.account && req.account._id,
        desc: `${req.system.name} diagnostics package was exported by ${req.account && req.account.email.unwrap()}`,
    });

    return fs_utils.create_fresh_path(`${TMP_WORK_DIR}`)
        .then(() => P.map(target_servers, function(server) {
            return server_rpc.client.cluster_internal.collect_server_diagnostics({}, {
                    address: server_rpc.get_base_address(server.owner_address),
                    auth_token: req.auth_token
                })
                .then(res_data => {
                    const data = (res_data[RPC_BUFFERS] && res_data[RPC_BUFFERS].data) || '';
                    if (!data) dbg.warn('diagnose_system: no diagnostics data from ', server.owner_address);
                    const server_hostname = (server.heartbeat && server.heartbeat.health.os_info.hostname) || 'unknown';
                    // Should never exist since above we delete the root folder
                    return fs_utils.create_fresh_path(`${TMP_WORK_DIR}/${server_hostname}_${server.owner_secret}`)
                        .then(() => fs.promises.writeFile(
                            `${TMP_WORK_DIR}/${server_hostname}_${server.owner_secret}/diagnostics.tgz`,
                            data
                        ));
                });
        }))
        .then(() => os_utils.exec(`find ${TMP_WORK_DIR} -maxdepth 1 -type f -delete`))
        .then(() => diag.pack_diagnostics(WORKING_PATH, TMP_WORK_DIR))
        .then(() => (OUT_PATH));
}


function collect_server_diagnostics(req) {
    const INNER_PATH = `${process.cwd()}/build`;
    return P.resolve()
        .then(() => os_utils.os_info())
        .then(os_info => {
            dbg.log0('Recieved diag req');
            const out_path = '/public/' + os_info.hostname + '_srv_diagnostics.tgz';
            const inner_path = process.cwd() + '/build' + out_path;
            return P.resolve()
                .then(() => diag.collect_server_diagnostics(req))
                .then(() => diag.pack_diagnostics(inner_path))
                .then(res => out_path);
        })
        .then(out_path => {
            dbg.log1('Reading packed file');
            return fs.promises.readFile(`${INNER_PATH}${out_path}`)
                .then(data => ({
                    [RPC_BUFFERS]: { data }
                }))
                .catch(err => {
                    dbg.error('DIAGNOSTICS READ FAILED', err.stack || err);
                    throw new Error('Server Collect Diag Error on reading packges diag file');
                });
        })
        .then(res => {
            Dispatcher.instance().activity({
                event: 'dbg.diagnose_server',
                level: 'info',
                actor: req.account && req.account._id,
                system: req.system._id,
                desc: `Collecting server diagnostics`,
            });
            return res;
        })
        .catch(err => {
            dbg.error('DIAGNOSTICS READ FAILED', err.stack || err);
            return {};
        });
}


function read_server_time(req) {
    const cluster_server = system_store.data.cluster_by_server[req.rpc_params.target_secret];
    if (!cluster_server) {
        throw new RpcError('CLUSTER_SERVER_NOT_FOUND',
            `Server with secret key: ${req.rpc_params.target_secret} was not found`);
    }

    return server_rpc.client.cluster_internal.apply_read_server_time(req.rpc_params, {
        address: server_rpc.get_base_address(cluster_server.owner_address),
    });
}


function apply_read_server_time(req) {
    return moment().unix();
}


function update_server_conf(req) {
    dbg.log0('set_server_conf. params:', req.rpc_params);
    const cluster_server = system_store.data.cluster_by_server[req.rpc_params.target_secret];
    if (!cluster_server) {
        throw new Error('unknown server: ' + req.rpc_params.target_secret);
    }

    let audit_desc = ``;
    const audit_server = {};
    return P.resolve()
        .then(() => {
            audit_server.hostname = _.get(cluster_server, 'heartbeat.health.os_info.hostname');
            audit_server.secret = cluster_server.owner_secret;
            if (req.rpc_params.hostname &&
                req.rpc_params.hostname !== audit_server.hostname) { //hostname supplied and actually changed
                audit_desc += `Hostname changed from ${audit_server.hostname} to ${req.rpc_params.hostname}. `;
                audit_server.hostname = req.rpc_params.hostname;
                if (!os_utils.is_valid_hostname(req.rpc_params.hostname)) throw new Error(`Invalid hostname: ${req.rpc_params.hostname}. See RFC 1123`);
                return server_rpc.client.cluster_internal.set_hostname_internal({
                        hostname: req.rpc_params.hostname,
                    }, {
                        address: server_rpc.get_base_address(cluster_server.owner_address),
                        timeout: 60000 //60s
                    })
                    .then(() => cluster_hb.do_heartbeat({ skip_server_monitor: true })) //We call for HB since the hostname changed
                    .then(() => cluster_server);
            }
            return cluster_server;
        })
        .then(() => {
            if (req.rpc_params.location !== cluster_server.location) { //location supplied and actually changed
                const new_name = req.rpc_params.location ? req.rpc_params.location : "''";
                audit_desc += `Location tag set to ${new_name}.`;
                return system_store.make_changes({
                    update: {
                        clusters: [{
                            _id: cluster_server._id,
                            location: req.rpc_params.location ? req.rpc_params.location : ''
                        }]
                    }
                });
            }
        })
        .then(() => {
            if (!audit_desc) return P.resolve();
            Dispatcher.instance().activity({
                event: 'cluster.set_server_conf',
                level: 'info',
                system: req.system._id,
                actor: req.account && req.account._id,
                server: audit_server,
                desc: audit_desc,
            });
        })
        .then(() => {
            // do nothing. 
        });
}

function set_hostname_internal(req) {
    return os_utils.set_hostname(req.rpc_params.hostname);
}

//
//Internals Cluster Control
//
function _validate_member_request(req) {
    if (!os_utils.is_supervised_env()) {
        console.warn('Environment is not a supervised one, currently not allowing clustering operations');
        throw new Error('Environment is not a supervised one, currently not allowing clustering operations');
    }

    if (req.rpc_params.address && !net.isIPv4(req.rpc_params.address)) {
        throw new Error('Adding new members to cluster is allowed by using IP only');
    }

    if (req.rpc_params.new_hostname && !os_utils.is_valid_hostname(req.rpc_params.new_hostname)) {
        throw new Error(`Invalid hostname: ${req.rpc_params.new_hostname}. See RFC 1123`);
    }

    //Check mongo port 27000
    //TODO on sharding will also need to add verification to the cfg port
    return os_utils.exec(`echo -n | nc -w5 ${req.rpc_params.address} ${config.MONGO_DEFAULTS.SHARD_SRV_PORT} 2>&1`, { ignore_rc: true, return_stdout: true })
        .then(response => {
            if (response.includes('Connection timed out')) {
                throw new Error(`Could not reach ${req.rpc_params.address} at port ${config.MONGO_DEFAULTS.SHARD_SRV_PORT},
                might be due to a firewall blocking`);
            }
        });
}

function get_secret(req) {
    return P.resolve()
        .then(() => {
            dbg.log0('_get_secret');
            return {
                secret: system_store.get_server_secret()
            };
        });
}

function _verify_join_preconditons(req) {
    const caller_address = req.connection.url.hostname.includes('ffff') ?
        req.connection.url.hostname.replace(/^.*:/, '') :
        req.connection.url.hostname;
    return P.resolve()
        .then(() => {
            dbg.log0('_verify_join_preconditons');
            //Verify secrets match
            if (req.rpc_params.secret !== system_store.get_server_secret()) {
                dbg.error('Secrets do not match!');
                return 'SECRET_MISMATCH';
            }
            return os_utils.exec(`echo -n | nc -w5 ${caller_address} ${config.MONGO_DEFAULTS.SHARD_SRV_PORT} 2>&1`, { ignore_rc: true, return_stdout: true })
                .then(response => {
                    if (response.includes('Connection timed out')) {
                        throw new Error('CONNECTION_TIMEOUT');
                    }
                })
                .then(() => {
                    const system = system_store.data.systems[0];
                    if (system) {
                        //Verify we are not already joined to a cluster
                        //TODO:: think how do we want to handle it, if at all
                        if (cutil.get_topology().shards.length !== 1 ||
                            cutil.get_topology().shards[0].servers.length !== 1) {
                            dbg.error('Server already joined to a cluster');
                            throw new Error('ALREADY_A_MEMBER');
                        }

                        // verify there were never objects on the joining system
                        return MDStore.instance().had_any_objects_in_system(system._id)
                            .then(has_objects => {
                                if (has_objects) {
                                    throw new Error('HAS_OBJECTS');
                                }
                            });
                    }
                })
                .then(() => {
                    // If we do not need system in order to add a server to a cluster
                    dbg.log0('_verify_join_preconditons okay. server has no system');
                    return 'OKAY';
                })
                .catch(err => {
                    dbg.warn('failed _verify_join_preconditons on', err.message);
                    throw err;
                });
        });
}

function _update_cluster_info(params) {
    let current_clustering = system_store.get_local_cluster_info();
    return P.resolve()
        .then(() => {
            if (!current_clustering) {
                return new_cluster_info();
            }
        })
        .then(new_clustering => {
            current_clustering = current_clustering || new_clustering;
            const update = _.defaults(_.pick(params, _.keys(current_clustering)), current_clustering);
            update.owner_secret = system_store.get_server_secret(); //Keep original owner_secret
            update.owner_address = params.owner_address || current_clustering.owner_address;
            update._id = current_clustering._id;
            dbg.log0('Updating local cluster info for owner', update.owner_secret, 'previous cluster info',
                cutil.pretty_topology(current_clustering), 'new cluster info', cutil.pretty_topology(update));

            let changes;
            // if we are adding a new cluster info use insert in the changes
            if (new_clustering) {
                changes = {
                    insert: {
                        clusters: [update]
                    }
                };
            } else {
                changes = {
                    update: {
                        clusters: [update]
                    }
                };
            }

            return system_store.make_changes(changes)
                .then(() => {
                    dbg.log0('local cluster info updates successfully');
                })
                .catch(err => {
                    console.error('failed on local cluster info update with', err.message);
                    throw err;
                });
        });
}

async function _attach_server_configuration(cluster_server) {
    cluster_server.timezone = (await os_utils.get_time_config()).timezone;
    return cluster_server;
}

function check_cluster_status() {
    const servers = system_store.data.clusters;
    dbg.log2('check_cluster_status', servers);
    const other_servers = _.filter(servers,
        server => server.owner_secret !== system_store.get_server_secret());
    return P.map(other_servers, server =>
        P.timeout(30000, server_rpc.client.cluster_server.ping({}, {
            address: server_rpc.get_base_address(server.owner_address)
        }))
        .then(res => {
            if (res === "PONG") {
                return {
                    secret: server.owner_secret,
                    status: "OPERATIONAL"
                };
            }
            return {
                secret: server.owner_secret,
                status: "FAULTY"
            };
        })
        .catch(err => {
            dbg.warn(`error while pinging server ${server.owner_secret}: `, err.stack || err);
            return {
                secret: server.owner_secret,
                status: "UNREACHABLE"
            };
        })
    );
}

function ping() {
    return "PONG";
}

// EXPORTS
exports.new_cluster_info = new_cluster_info;
exports.init_cluster = init_cluster;
exports.set_debug_level = set_debug_level;
exports.apply_set_debug_level = apply_set_debug_level;
exports.diagnose_system = diagnose_system;
exports.collect_server_diagnostics = collect_server_diagnostics;
exports.read_server_time = read_server_time;
exports.apply_read_server_time = apply_read_server_time;
exports.check_cluster_status = check_cluster_status;
exports.ping = ping;
exports.verify_candidate_join_conditions = verify_candidate_join_conditions;
exports.verify_join_conditions = verify_join_conditions;
exports.verify_new_ip = verify_new_ip;
exports.update_server_conf = update_server_conf;
exports.set_hostname_internal = set_hostname_internal;
exports.get_version = get_version;
exports.get_secret = get_secret;
exports.update_member_of_cluster = update_member_of_cluster;
