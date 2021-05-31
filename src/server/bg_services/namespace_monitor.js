/* Copyright (C) 2016 NooBaa */
'use strict';

const system_store = require('../system_services/system_store').get_instance();
const azure_storage = require('../../util/azure_storage_wrap');
const auth_server = require('../common_services/auth_server');
const dbg = require('../../util/debug_module')(__filename);
const system_utils = require('../utils/system_utils');
const cloud_utils = require('../../util/cloud_utils');
const config = require('../../../config');
const P = require('../../util/promise');
const AWS = require('aws-sdk');
const _ = require('lodash');

class NamespaceMonitor {

    constructor({ name, client }) {
        this.name = name;
        this.client = client;
        this.nsr_connections_obj = {};
    }

    async run_batch() {
        if (!this._can_run()) return;
        dbg.log1('namespace_monitor: starting monitoring namespace resources');
        try {
            await this.test_namespace_resources_validity();
        } catch (err) {
            dbg.error('namespace_monitor:', err, err.stack);
        }
        return config.NAMESPACE_MONITOR_DELAY;
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('namespace_monitor: system_store did not finish initial load');
            return false;
        }

        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;

        return true;
    }

    // Checking namespace resource integrity
    async test_namespace_resources_validity() {

        await P.map_with_concurrency(10, system_store.data.namespace_resources, async nsr => {
            try {
                if (!this.nsr_connections_obj[nsr._id]) {
                    // Initializing a connection per resource
                    this.init_nsr_connection_to_target(nsr);
                }
                // Deleting object that is not exists
                await this.test_single_namespace_resource_validity(nsr);

            } catch (err) {
                if (err.code !== 'BlobNotFound' && err.code !== 'NoSuchKey') {
                    dbg.error('LMLM Error:', err);
                    if (nsr.nsfs_config) {
                        dbg.error('test_namespace_resource_validity failed:', err, nsr.nsfs_config);
                    } else {
                        const { endpoint, target_bucket } = nsr.connection;
                        dbg.error('test_namespace_resource_validity failed:', err, endpoint, target_bucket);
                    }

                    await this.client.pool.update_issues_report({
                        namespace_resource_id: nsr._id,
                        error_code: err.code,
                        time: Date.now(),
                        monitoring: true
                    }, {
                        auth_token: auth_server.make_auth_token({
                            system_id: system_store.data.systems[0]._id,
                            account_id: system_store.data.systems[0].owner._id,
                            role: 'admin'
                        })
                    });
                    dbg.warn(`unexpected error (code=${err.code}) from test_namespace_resource_validity during test. ignoring..`);
                }
                dbg.log1(`test_namespace_resource_validity: namespace resource ${nsr.name} has error as expected`);
            }
        });
        dbg.log1(`test_namespace_resource_validity finished successfully..`);
    }

    async init_nsr_connection_to_target(nsr) {
        // const admin_email = 'admin@noobaa.io';
        let endpoint;
        let access_key;
        let secret_key;
        let endpoint_type;
        // In the case of Namespace FS resource we will create a connection 
        // to the noobaa admin and treat it as S3 compatible.
        // This will allow us to use s3 operation instead of FS calls. 
        if (nsr.nsfs_config) {
            // const account_info = await this.client.account.read_account({ email: admin_email });
            // dbg.log0('LMLM: account_info:', account_info);
            // access_key = account_info.access_keys[0].access_key;
            // secret_key = account_info.access_keys[0].secret_key;
            // dbg.log0('LMLM: account_info:', account_info);

            // const system_info = await this.client.system.read_system();
            // endpoint = system_info.s3_service.address[0].address;
            // access_key = system_info.owner.access_key[0].access_key;
            // secret_key = system_info.owner.access_key[0].secret_key;
            // try {
            const system_address = _.filter(nsr.system.system_address, { 'api': 's3' });
            dbg.log0('LMLM: system_address:', system_address);
            endpoint = system_address[0].hostname;
            // dbg.log0('LMLM: endpoint:', endpoint);
            access_key = nsr.system.owner.access_keys[0].access_key;
            // dbg.log0('LMLM: access_key:', access_key);
            secret_key = nsr.system.owner.access_keys[0].secret_key;
            // dbg.log0('LMLM: secret_key:', secret_key);
            // } catch (e) {
            //     dbg.log0('LMLM: got error:', e);
            //     throw e;
            // }
            // LMLM: TODO: endpoint = //get the admin endpoint? 
        } else {
            endpoint = nsr.connection.endpoint;
            access_key = nsr.connection.access_key;
            secret_key = nsr.connection.secret_key;
            endpoint_type = nsr.connection.endpoint_type;
        }
        let conn;
        if (_.includes(['AWS', 'S3_COMPATIBLE', 'IBM_COS'], endpoint_type) || nsr.nsfs_config) {
            conn = new AWS.S3({
                endpoint: endpoint,
                credentials: {
                    accessKeyId: access_key.unwrap(),
                    secretAccessKey: secret_key.unwrap()
                },
                s3ForcePathStyle: true,
                sslEnabled: false
            });
        } else if (nsr.connection.endpoint_type === 'AZURE') {
            const conn_string = cloud_utils.get_azure_connection_string({
                endpoint,
                access_key: access_key,
                secret_key: secret_key
            });
            conn = azure_storage.createBlobService(conn_string);
        } else {
            dbg.error('namespace_monitor: invalid endpoint type', nsr.endpoint_type);
        }

        if (conn) {
            this.nsr_connections_obj[nsr._id] = conn;
        }
    }

    async test_single_namespace_resource_validity(nsr_info) {
        let endpoint_type;
        let target_bucket;
        if (!nsr_info.nsfs_config) {
            endpoint_type = nsr_info.connection.endpoint_type;
            target_bucket = nsr_info.connection.target_bucket;
        }
        const block_key = `test-delete-non-existing-key-${Date.now()}`;
        const conn = this.nsr_connections_obj[nsr_info._id];

        if (_.includes(['AWS', 'S3_COMPATIBLE', 'IBM_COS'], endpoint_type)) {
            await conn.deleteObjectTagging({
                Bucket: target_bucket,
                Key: block_key
            }).promise();
        } else if (endpoint_type === 'AZURE') {
            await P.fromCallback(callback =>
                conn.deleteBlob(
                    target_bucket,
                    block_key,
                    callback)
            );
        } else if (nsr_info.nsfs_config) {
            dbg.log0('LMLM: will run deleteObject()');
            target_bucket = 'nsfs'; //LMLM: get it properly
            const del = await conn.deleteObject({
                Bucket: target_bucket,
                Key: block_key
            }).promise();
            dbg.log0('LMLM: del:', del);
        } else {
            dbg.error('namespace_monitor: invalid endpoint type', endpoint_type);
        }
    }
}

exports.NamespaceMonitor = NamespaceMonitor;
