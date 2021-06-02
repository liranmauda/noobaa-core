/* Copyright (C) 2016 NooBaa */
'use strict';

const path = require('path');
const P = require('../../util/promise');
const config = require('../../../config');
const nb_native = require('../../util/nb_native');
const system_utils = require('../utils/system_utils');
const dbg = require('../../util/debug_module')(__filename);
const auth_server = require('../common_services/auth_server');
const system_store = require('../system_services/system_store').get_instance();

class NamespaceFSMonitor {

    constructor({ name, client }) {
        this.name = name;
        this.client = client;
    }

    async run_batch() {
        if (!this._can_run()) return;
        dbg.log1('namespace_fs_monitor: starting monitoring namespace resources');
        try {
            await this.test_namespace_fs_resources_validity();
        } catch (err) {
            dbg.error('namespace_fs_monitor:', err, err.stack);
        }
        return config.NAMESPACE_FS_MONITOR_DELAY;
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('namespace_fs_monitor: system_store did not finish initial load');
            return false;
        }

        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;

        return true;
    }

    async test_namespace_fs_resources_validity() {
        await P.map_with_concurrency(10, system_store.data.namespace_resources, async nsr => {
            if (!nsr.nsfs_config) return;
            try {
                await this.test_single_namespace_fs_resource_validity(nsr);
            } catch (err) {
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
        });
        dbg.log1(`test_namespace_fs_resource_validity finished successfully..`);
    }

    async test_single_namespace_fs_resource_validity(nsr) {
        const file = path.join(nsr.nsfs_config.fs_root_path, 'create_object_upload');
        const buffer = Buffer.from('This is test file');
        try {
            await nb_native().fs.writeFile('', file, buffer);
            await nb_native().fs.unlink('', file, buffer);
        } catch (err) {
            dbg.log1('test_single_namespace_fs_resource_validity got error:', err);
            throw err;
        }
    }

}

exports.NamespaceFSMonitor = NamespaceFSMonitor;
