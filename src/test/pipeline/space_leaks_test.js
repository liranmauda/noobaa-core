/* Copyright (C) 2016 NooBaa */
'use strict';

const api = require('../../api');
const P = require('../../util/promise');
const { S3OPS } = require('../utils/s3ops');
const Report = require('../framework/report');
const argv = require('minimist')(process.argv);
const dbg = require('../../util/debug_module')(__filename);
const { PoolFunctions } = require('../utils/pool_functions');
const { BucketFunctions } = require('../utils/bucket_functions');

const test_name = 'space_leaks_test';
dbg.set_process_name(test_name);

let current_size = 0;
const POOL_NAME = "first-pool";

const {
    mgmt_ip,
    mgmt_port_https,
    s3_ip,
    s3_port,
    agent_number,
    dataset_size = 100, //MB
    max_size = 250, //MB
    min_size = 50, //MB
} = argv;

const s3ops = new S3OPS({ ip: s3_ip, port: s3_port });

function usage() {
    console.log(`
    --mgmt_ip               -   noobaa management ip.
    --mgmt_port_https       -   noobaa server management https port
    --s3_ip                 -   noobaa s3 ip
    --s3_port               -   noobaa s3 port
    --agent_number          -   number of agents to create (default: ${agent_number})
    --dataset_size          -   size uploading data for checking rebuild
    --max_size              -   max size of uploading files
    --min_size              -   min size of uploading files
    --help                  -   show this help.
    `);
}

if (argv.help) {
    usage();
    process.exit(1);
}

const rpc = api.new_rpc_from_base_address(`wss://${mgmt_ip}:${mgmt_port_https}`, 'EXTERNAL');
const client = rpc.new_client({});

let report = new Report();
//Define test cases
const cases = [
    'reclaimed blocks',
];
report.init_reporter({
    suite: test_name,
    conf: {
        dataset_size: dataset_size,
    },
    mongo_report: true,
    cases: cases
});

const pool_functions = new PoolFunctions(client);
const bucket_functions = new BucketFunctions(client);

const base_unit = 1024;
const unit_mapping = {
    KB: {
        data_multiplier: Math.pow(base_unit, 1),
        dataset_multiplier: Math.pow(base_unit, 2)
    },
    MB: {
        data_multiplier: Math.pow(base_unit, 2),
        dataset_multiplier: Math.pow(base_unit, 1)
    },
    GB: {
        data_multiplier: Math.pow(base_unit, 3),
        dataset_multiplier: Math.pow(base_unit, 0)
    }
};

async function _upload_and_verify_files(bucket, dataset) {
    const files = [];
    current_size = 0;
    let { data_multiplier } = unit_mapping.MB;
    console.log(`Writing and deleting data till size amount to grow ${dataset} MB`);
    while (current_size < dataset) {
        try {
            console.log(`Uploading files till data size grow to ${dataset}, current size is ${current_size}`);
            let file_size = set_fileSize();
            let file_name = 'file_part_' + file_size + (Math.floor(Date.now() / 1000));
            files.push(file_name);
            current_size += file_size;
            console.log(`Uploading file with size ${file_size} MB`);
            await s3ops.put_file_with_md5(bucket, file_name, file_size, data_multiplier);
            await s3ops.get_file_check_md5(bucket, file_name);
        } catch (e) {
            console.error(`${mgmt_ip} FAILED verification uploading and reading`, e);
            throw e;
        }
    }
    return files;
}

function set_fileSize() {
    let rand_size = Math.floor((Math.random() * (max_size - min_size)) + min_size);
    if (dataset_size - current_size === 0) {
        rand_size = 1;
        //if we choose file size grater then the remaining space for the dataset,
        //set it to be in the size that complete the dataset size.
    } else if (rand_size > dataset_size - current_size) {
        rand_size = dataset_size - current_size;
    }
    return rand_size;
}

async function _cleanup_bucket(bucket) {
    try {
        console.log(`running clean up files from bucket ${bucket}`);
        await s3ops.delete_all_objects_in_bucket(bucket, true);
    } catch (e) {
        console.error('Errors during deleting', e);
        throw e;
    }
}

async function _available_space({ bucket, data_size = '', target = false }) {
    const base_time = Date.now();
    if (data_size) {
        while (Date.now() - base_time < 360 * 1000) {
            try {
                const available_space = await bucket_functions.checkAvailableSpace(bucket);
                if (target && available_space === data_size) {
                    return available_space;
                } else if (available_space <= data_size) {
                    return available_space;
                } else {
                    await P.delay(15 * 1000);
                }
            } catch (e) {
                console.error(`Something went wrong with checkAvailableSpace`);
                throw e;
            }
        }
    } else {
        const available_space = await bucket_functions.checkAvailableSpace(bucket);
        return available_space;
    }
    throw new Error(`Available space meat it's target by now.`);
}

async function _check_space_leaks(bucket) {
    try {
        const initial_available_space = await _available_space({ bucket });
        await _upload_and_verify_files(bucket, dataset_size);
        await _available_space({ bucket, data_size: dataset_size });
        await _cleanup_bucket(bucket);
        const current_available_space = await _available_space({ bucket, target: true });
        if (initial_available_space !== current_available_space) {
            throw new Error(`space should have beed reclaimed by now`);
        }
    } catch (e) {
        report.fail('reclaimed blocks');
        console.error('_check_space_leaks:: failed', e);
        throw e;
    }
    report.success('reclaimed blocks');
}

async function _check_space_leaks_cycle(agents_num) {
    const bucket = 'reclaim.bucket';
    await pool_functions.create_pool(POOL_NAME, agents_num);
    await bucket_functions.createBucket(bucket);
    // Changing the bucket policy
    await pool_functions.change_tier(POOL_NAME, bucket, 'SPREAD');
    await _check_space_leaks(bucket);
}

async function main() {
    try {
        await client.create_auth_token({
            email: 'demo@noobaa.com',
            password: 'DeMo1',
            system: 'demo'
        });
        await _check_space_leaks_cycle(agent_number);
        console.log('space leaks test was successful!');
        await report.report();
        process.exit(0);
    } catch (e) {
        await report.report();
        console.error('something went wrong', e);
        process.exit(1);
    }
}

main();
