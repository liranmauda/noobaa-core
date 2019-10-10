/* Copyright (C) 2016 NooBaa */
'use strict';

class HostFunctions {

    constructor(client) {
        this._client = client;
    }

    async get_hosts_list() {
        const list_hosts = await this._client.host.list_hosts({});
        const hosts = list_hosts.hosts;
        console.log(`hosts list is: ${hosts}`);
        return hosts;
    }

    async check_all_hosts_free_space(bucket_name) {
        console.log(`Checking free space in all hosts`);
        try {
            const hosts_free_space = [];
            const hosts = await this.get_hosts_list();
            const space = bucket.data.free;
            console.log(`Free space in bucket ${bucket_name} is ${space / 1024 / 1024} MB}`);
            return space;
        } catch (err) {
            console.error(`FAILED to check free space in bucket`, err);
            throw err;
        }
    }

}

exports.HostFunctions = HostFunctions;
