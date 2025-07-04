/* Copyright (C) 2016 NooBaa */
"use strict";

const _ = require('lodash');
const fs = require('fs');
const argv = require('minimist')(process.argv);
const { S3 } = require('@aws-sdk/client-s3');
const P = require('../../util/promise');
const semaphore = require('../../util/semaphore');
const os_utils = require('../../util/os_utils');

const UL_TEST = {
    target: '',
    bucket_name: '',
    access_key: '',
    secret_key: '',
    skip_generation: false,
    skip_cleanup: false,
    file_size: 512,
    num_files: 1000,
    num_threads: 10,
    files_per_dir: 5000,
    total_ul_errors: 0,
    base_dir: '/tmp/test_files_ul',
    files: [],
    measurement: {
        points: 0,
        time: 0,
        mid: [],
    }
};

const s3bucket = new S3({
    endpoint: UL_TEST.target,
    credentials: {
        accessKeyId: UL_TEST.access_key,
        secretAccessKey: UL_TEST.secret_key,
    },
    forcePathStyle: true,
    tls: false,
});

function show_usage() {
    console.log('usage: node test_files_ul.js --ip <S3 IP> --bucket <Bucket Name> --access <ACCESS_KEY> --secret <SECRET>');
    console.log('   example: node node test_files_ul.js --ip 10.0.0.1 --bucket files --access 123 --secret abc');

    console.log('Optional Parameters:');
    console.log('   --filesize - File size to upload, in KB. Default: 512KB');
    console.log('                NOTE: Larger files sizes would take longer to generate');
    console.log('   --numfiles - Number of files to upload. Default: 1000');
    console.log('   --numthreads - Number of concurrent threads to use. Default: 10');
    console.log('   --skip_generation - Skip pre generation of files, use last generated files');
    console.log('   --skip_cleanup - Skip cleanup of files, can be used for another run');
}

function pre_generation() {
    const dirs = Math.ceil(UL_TEST.num_files / UL_TEST.files_per_dir);
    console.log('Creating directory structure');
    return os_utils.exec('mkdir -p ' + UL_TEST.base_dir)
        .then(function() {
            return os_utils.exec('rm -rf ' + UL_TEST.base_dir + '/*');
        })
        .then(async function() {
            try {
                let i = 0;
                while (i < dirs) {
                    i += 1;
                    await os_utils.exec('mkdir -p ' + UL_TEST.base_dir + '/dir' + i);
                }
            } catch (err) {
                console.error('Error creating directory structure', err, err.stack);
                throw new Error('Error creating directory structure');
            }
        })
        .then(async function() {
            try {
                console.log('Generating files (this might take some time) ...');
                let d = 0;
                while (d < dirs) {
                    d += 1;
                    const files = (d === dirs) ? UL_TEST.num_files % UL_TEST.files_per_dir : UL_TEST.files_per_dir;
                    console.log(' generating batch', d, 'of', files, 'files');
                    for (let i = 1; i <= files; ++i) {
                        UL_TEST.files.push(UL_TEST.base_dir + '/dir' + d + '/file_' + i);
                    }
                    await os_utils.exec('for i in `seq 1 ' + files + '` ; do' +
                        ' dd if=/dev/urandom of=' + UL_TEST.base_dir + '/dir' + d +
                        '/file_$i  bs=' + UL_TEST.file_size + 'k count=1 ; done');
                }
            } catch (err) {
                console.error('Error generating files', err, err.stack);
                throw new Error('Error generating files');
            }
        });
}

function upload_test() {

    const upload_semaphore = new semaphore.Semaphore(UL_TEST.num_threads);
    return P.all(_.map(UL_TEST.files, function(f) {
        return upload_semaphore.surround(async function() {
            return await upload_file(f);
        });
    }));
}

async function upload_file(test_file) {
    let start_ts;
    console.log('Called upload_file with param', test_file);
    try {
        const params = {
            Bucket: UL_TEST.bucket_name,
            Key: test_file,
            Body: fs.createReadStream(test_file),
        };
        start_ts = Date.now();
        await s3bucket.putObject(params);
        console.log('Done uploading', test_file);
        //TODO:: Add histogram as well
        UL_TEST.measurement.points += 1;
        UL_TEST.measurement.time += (Date.now() - start_ts) / 1000;
        if (UL_TEST.measurement.points === 1000) { //Save mid results per each 1K files
            UL_TEST.measurement.mid.push(UL_TEST.measurement.time / 1000);
            UL_TEST.measurement.points = 0;
            UL_TEST.measurement.time = 0;
        }
    } catch (err) {
        console.error('Error in upload_file', err);
        UL_TEST.total_ul_errors += 1;
        if (UL_TEST.total_ul_errors > UL_TEST.num_files * 0.1) {
            throw new Error('Failed uploading ' + UL_TEST.total_ul_errors + ' files');
        }
    }
}

function print_summary() {
    console.log('');
    console.log('*********************************************************');

    //if (UL_TEST.skip_generation) {
    //    //TODO: real numbers
    //    console.log('Test Summary', UL_TEST.num_files, 'files, each', UL_TEST.file_size, 'KB', 'with', UL_TEST.numthreads, 'threads');
    //} else {
    console.log('Test Summary', UL_TEST.num_files, 'files, each', UL_TEST.file_size, 'KB', 'with', UL_TEST.numthreads, 'threads');
    //}

    console.log('Test results, breakdown per each 1K uploads:');
    let i = 0;
    _.each(UL_TEST.measurement.mid, function(m) {
        console.log('  for files', (i * 1000) + 1, 'to', (i + 1) * 1000, 'avg ul time', m);
        i += 1;
    });
    console.log('  for files', (i * 1000) + 1, 'to', ((i + 1) * 1000) + UL_TEST.measurement.points, 'avg ul time',
        UL_TEST.measurement.time / UL_TEST.measurement.points);
}

function main() {
    let missing_params = false;

    //Verify Input Parameters
    if (_.isUndefined(argv.ip)) {
        missing_params = true;
        console.error('missing target IP');
    } else if (_.isUndefined(argv.bucket)) {
        missing_params = true;
        console.error('missing bucket name');
    } else if (_.isUndefined(argv.access)) {
        missing_params = true;
        console.error('missing access key');
    } else if (_.isUndefined(argv.secret)) {
        missing_params = true;
        console.error('missing secret key');
    }
    if (missing_params) {
        show_usage();
        process.exit(3);
        return;
    }

    UL_TEST.target = 'http://' + argv.ip + ':80';
    UL_TEST.bucket_name = argv.bucket;
    UL_TEST.access_key = argv.access;
    UL_TEST.secret_key = argv.secret;
    if (!_.isUndefined(argv.filesize)) {
        UL_TEST.file_size = argv.filesize;
    }
    if (!_.isUndefined(argv.numfiles)) {
        UL_TEST.num_files = argv.numfiles;
    }
    if (!_.isUndefined(argv.numthreads)) {
        UL_TEST.num_threads = argv.num_threads;
    }
    if (!_.isUndefined(argv.skip_generation)) {
        UL_TEST.skip_generation = true;
    }
    if (!_.isUndefined(argv.skip_cleanup)) {
        UL_TEST.skip_cleanup = true;
    }

    return P.fcall(function() {
            //Pre generate files, so measurement won't be affected
            if (UL_TEST.skip_generation) {
                console.log('Skipping Pre generation of files');
                //TODO:: fill out UL_TEST.files according to existing files
            } else {
                console.log('Pre generating files');
                return pre_generation();
            }
        })
        .then(function() {
            //U/L files & Measure
            if (!UL_TEST.skip_generation) {
                console.log('Done Pre-generating files');
            }
            console.log('Starting to upload files');
            return upload_test();
        })
        .then(function() {
            print_summary();
            if (!UL_TEST.skip_cleanup) {
                return os_utils.exec('rm -rf /tmp/' + UL_TEST.base_dir);
            }
            console.log('Finished running upload test');
        })
        .catch(function() {
            if (!UL_TEST.skip_cleanup) {
                return os_utils.exec('rm -rf /tmp/' + UL_TEST.base_dir);
            }
        });
}

if (require.main === module) {
    main();
}
