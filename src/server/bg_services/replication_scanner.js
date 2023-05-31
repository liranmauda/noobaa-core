/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const util = require('util'); //LMLM to clean
const system_store = require('../system_services/system_store').get_instance();
const dbg = require('../../util/debug_module')(__filename);
const system_utils = require('../utils/system_utils');
const config = require('../../../config');
const P = require('../../util/promise');
const Semaphore = require('../../util/semaphore');
const replication_store = require('../system_services/replication_store').instance();
const cloud_utils = require('../../util/cloud_utils');
const prom_reporting = require('../analytic_services/prometheus_reporting');
const replication_utils = require('../utils/replication_utils');


const PARTIAL_SINGLE_BUCKET_REPLICATION_DEFAULTS = {
    replication_id: '',
    last_cycle_rule_id: '',
    bucket_name: '',
    last_cycle_src_cont_token: '',
    last_cycle_writes_num: 0,
    last_cycle_writes_size: 0,
    last_cycle_error_writes_num: 0,
    last_cycle_error_writes_size: 0,
};

class ReplicationScanner {

    /**
     * @param {{
     *   name: string;
     *   client: nb.APIClient;
     * }} params
     */
    constructor({ name, client }) {
        this.name = name;
        this.client = client;
        this.scanner_semaphore = new Semaphore(config.REPLICATION_SEMAPHORE_CAP, {
            timeout: config.REPLICATION_SEMAPHORE_TIMEOUT,
            timeout_error_code: 'REPLICATION_ITEM_TIMEOUT',
            verbose: true
        });
        this.noobaa_connection = undefined;
    }

    async run_batch() {
        if (!this._can_run()) return;
        dbg.log0('replication_scanner: starting scanning bucket replications');
        try {
            if (!this.noobaa_connection) {
                this.noobaa_connection = cloud_utils.set_noobaa_s3_connection(system_store.data.systems[0]);
            }
            await this.scan();
        } catch (err) {
            dbg.error('replication_scanner:', err, err.stack);
        }
        return config.BUCKET_REPLICATOR_DELAY;
    }

    _can_run() {
        if (!system_store.is_finished_initial_load) {
            dbg.log0('replication_scanner: system_store did not finish initial load');
            return false;
        }

        const system = system_store.data.systems[0];
        if (!system || system_utils.system_in_maintenance(system._id)) return false;

        return true;
    }

    async scan() {
        if (!this.noobaa_connection) throw new Error('noobaa endpoint connection is not started yet...');
        // find rule for each replication policy that was not updated for the longest period
        const least_recently_replicated_rules = await replication_store.find_rules_updated_longest_time_ago();

        await P.all(_.map(least_recently_replicated_rules, async replication_id_and_rule => {
            const { replication_id, rule } = replication_id_and_rule;
            const status = { last_cycle_start: Date.now() };

            dbg.log0(`LMLM:: replication_id: ${replication_id}, rule: ${inspect(rule)}, status: ${inspect(status)}`);

            const { src_bucket, dst_bucket } = replication_utils.find_src_and_dst_buckets(rule.destination_bucket, replication_id);
            if (!src_bucket || !dst_bucket) {
                dbg.error('replication_scanner: can not find src_bucket or dst_bucket object', src_bucket, dst_bucket);
                return;
            }
            const prefix = (rule.filter && rule.filter.prefix) || '';
            const cur_src_cont_token = (rule.rule_status && rule.rule_status.src_cont_token) || '';
            const cur_dst_cont_token = (rule.rule_status && rule.rule_status.dst_cont_token) || '';

            let src_cont_token;
            let dst_cont_token;
            let keys_sizes_map_to_copy;

            // LMLM per https://github.com/noobaa/noobaa-core/pull/7316#discussion_r1211514520
            // after finalizing the scan function we might be able to use inline conditions
            // if not we should remove this comment.
            if (rule.sync_versions) {
                dbg.log0(`LMLM:: We have sync_versions :)`);
                ({ keys_sizes_map_to_copy, src_cont_token, dst_cont_token } = await this.list_versioned_buckets_and_compare(
                    src_bucket.name, dst_bucket.name, prefix, cur_src_cont_token, cur_dst_cont_token));
            } else {
                ({ keys_sizes_map_to_copy, src_cont_token, dst_cont_token } = await this.list_buckets_and_compare(
                    src_bucket.name, dst_bucket.name, prefix, cur_src_cont_token, cur_dst_cont_token));
            }


            //LMLM from here the flow should probably very between sync_versions and not. 

            dbg.log1('replication_scanner: keys_sizes_map_to_copy: ', keys_sizes_map_to_copy);
            let move_res;
            //const keys_to_copy = Object.keys(keys_sizes_map_to_copy); LMLM
            const keys_to_copy = []; //LMLM 
            if (keys_to_copy.length) {
                const copy_type = replication_utils.get_copy_type();
                move_res = await replication_utils.move_objects(
                    this.scanner_semaphore,
                    this.client,
                    copy_type,
                    src_bucket.name,
                    dst_bucket.name,
                    keys_to_copy,
                );
                dbg.log0(`replication_scanner: scan move_res: ${move_res}`);
            }

            await replication_store.update_replication_status_by_id(replication_id,
                rule.rule_id, {
                    ...status,
                    last_cycle_end: Date.now(),
                    src_cont_token,
                    dst_cont_token
                    // always advance the src cont token, if failures happened - they will eventually will be copied
                });

            const replication_status = this._get_rule_status(rule.rule_id,
                src_cont_token, keys_sizes_map_to_copy, move_res || []);

            this.update_replication_prom_report(src_bucket.name, replication_id, replication_status);
        }));
    }

    async list_buckets_and_compare(src_bucket, dst_bucket, prefix, cur_src_cont_token, cur_dst_cont_token) {

        const src_list = await this.list_objects(src_bucket, prefix, cur_src_cont_token);
        const ans = {
            keys_sizes_map_to_copy: {}, //a map between the key and it size, we need it to later report the size in_get_rule_status
            src_cont_token: src_list.NextContinuationToken || '',
            dst_cont_token: ''
        };

        // edge case 1: src list = [] , nothing to replicate
        if (!src_list.Contents.length) return ans;

        let src_contents_left = src_list.Contents;
        let new_dst_cont_token = cur_dst_cont_token;
        const last_src_key = src_list.Contents[src_list.Contents.length - 1].Key;

        let keep_listing_dst = true;
        while (keep_listing_dst) {
            const dst_list = await this.list_objects(dst_bucket, prefix, new_dst_cont_token);

            // edge case 2: dst list = [] , replicate all src_list
            // edge case 3: all src_keys are lexicographic smaller than the first dst key, replicate all src_list
            if (!dst_list.Contents.length || last_src_key < dst_list.Contents[0].Key) {
                ans.keys_sizes_map_to_copy = src_contents_left.reduce(
                    (acc, content1) => {
                        acc[content1.Key] = content1.Size;
                        return acc;
                    }, { ...ans.keys_sizes_map_to_copy });
                break;
            }

            // find keys to copy
            const diff = await this.get_keys_diff(src_contents_left, dst_list.Contents, dst_list.NextContinuationToken,
                src_bucket, dst_bucket);

            keep_listing_dst = diff.keep_listing_dst;
            src_contents_left = diff.src_contents_left;
            ans.keys_sizes_map_to_copy = { ...ans.keys_sizes_map_to_copy, ...diff.to_replicate_map };

            // advance dst token only when cur dst list could not contains next src list items
            const last_dst_key = dst_list.Contents[dst_list.Contents.length - 1].Key;
            if (last_src_key >= last_dst_key) {
                new_dst_cont_token = dst_list.NextContinuationToken;
            }
        }
        return {
            ...ans,
            // if src_list cont token is empty - dst_list cont token should be empty too
            dst_cont_token: (src_list.NextContinuationToken && new_dst_cont_token) || ''
        };
    }

    async list_versioned_buckets_and_compare(src_bucket, dst_bucket, prefix, cur_src_cont_token, cur_dst_cont_token) {

        // list source bucket
        const src_version_response = await this.list_objects_versions(src_bucket, prefix, cur_src_cont_token);
        dbg.log0(`LMLM src_version_response: ${inspect(src_version_response)}`);

        //LMLM remove
        // eslint-disable-next-line prefer-const
        let src_contents_left = this._object_grouped_by_key_and_omitted(src_version_response);
        dbg.log0(`LMLM src_contents_left: ${inspect(src_contents_left)}`);
        const src_cont_token = this._get_next_key_marker(src_version_response.IsTruncated, src_contents_left);

        const ans = {
            keys_sizes_map_to_copy: {}, //a map between the key and it size, we need it to later report the size in_get_rule_status
            src_cont_token,
            dst_cont_token: ''
        };

        dbg.log0(`LMLM ans: ${inspect(ans)}`);

        // edge case 1: Object.keys(src_contents_left).length = [] , nothing to replicate
        if (!Object.keys(src_contents_left).length) return ans;

        //LMLM remove
        // eslint-disable-next-line prefer-const
        let new_dst_cont_token = cur_dst_cont_token;
        const last_src_key = Object.keys(src_contents_left)[Object.keys(src_contents_left).length - 1];

        let keep_listing_dst = true;
        while (keep_listing_dst) {
            const dst_version_response = await this.list_objects_versions(dst_bucket, prefix, new_dst_cont_token);
            const dst_key_etag = this._get_key_etag(dst_version_response);
            dbg.log0(`LMLM dest_key_etag: ${inspect(dst_key_etag)}`);
            keep_listing_dst = false; //LMLM remove
            // edge case 2: Object.keys(dest_key_etag).length = [] , replicate all src_list
            // edge case 3: all src_keys are lexicographic smaller than the first dst key, replicate all src_list
            if (!Object.keys(dst_key_etag).length || last_src_key < Object.keys(dst_key_etag)[0]) {
                // LMLM need to figure it out... 
                // LMLM It will map all the keys to the size we need it to later report the size in_get_rule_status
                // ans.keys_sizes_map_to_copy = src_contents_left.reduce(
                //     (acc, content1) => {
                //         acc[content1.Key] = content1.Size;
                //         return acc;
                //     }, { ...ans.keys_sizes_map_to_copy });
                ans.keys_sizes_map_to_copy = {
                    ...ans.keys_sizes_map_to_copy,
                    ...src_contents_left
                };
                break;
            }

            //LMLM remove... 
            // eslint-disable-next-line prefer-const
            let dst_cont_token = this._get_next_key_marker(dst_version_response.IsTruncated, dst_key_etag);
            // find keys and version pairs to copy
            const diff = await this.get_keys_version_diff(src_contents_left, dst_key_etag, dst_cont_token, src_bucket, dst_bucket);

            dbg.log0(`LMLM: get_keys_version_diff diff: ${inspect(diff)}`);

            keep_listing_dst = diff.keep_listing_dst;
            src_contents_left = diff.src_contents_left;
            // ans.keys_sizes_map_to_copy = { ...ans.keys_sizes_map_to_copy, ...diff.to_replicate_map };

            // // advance dst token only when cur dst list could not contains next src list items
            // const last_dst_key = dst_list.Contents[dst_list.Contents.length - 1].Key;
            // if (last_src_key >= last_dst_key) {
            //     new_dst_cont_token = dst_list.NextContinuationToken;
            // }
        }
        return {
            ...ans,
            // if src_list cont token is empty - dst_list cont token should be empty too
            dst_cont_token: (src_cont_token && new_dst_cont_token) || ''
        };
    }

    async list_objects(bucket_name, prefix, continuation_token) {
        try {
            dbg.log1('replication_server list_objects: params:', bucket_name, prefix, continuation_token);
            const list = await this.noobaa_connection.listObjectsV2({
                Bucket: bucket_name.unwrap(),
                Prefix: prefix,
                ContinuationToken: continuation_token,
                MaxKeys: Number(process.env.REPLICATION_MAX_KEYS) || 1000
            }).promise();

            dbg.log1('replication_server.list_objects: finished successfully', list.Contents.map(c => c.Key));
            return list;
        } catch (err) {
            dbg.error('replication_server.list_objects: error:', err);
            throw err;
        }
    }

    // list_objects_versions will list all the objects with the versions, continuation_token is the key marker.
    // we prefer keeping the continuation_token term due to re-use of db properties but this is really a KeyMarker
    async list_objects_versions(bucket_name, prefix, continuation_token) {
        try {
            dbg.log1('replication_server list_objects_versions: params:', bucket_name, prefix, continuation_token);
            const list = await this.noobaa_connection.listObjectVersions({
                Bucket: bucket_name.unwrap(),
                Prefix: prefix,
                KeyMarker: continuation_token,
                MaxKeys: Number(process.env.REPLICATION_MAX_KEYS) || 1000 // Max keys are the total of Versions + DeleteMarkers  
            }).promise();

            return list;
        } catch (err) {
            dbg.error('replication_server.list_objects_versions: error:', err);
            throw err;
        }
    }

    // _object_grouped_by_key_and_omitted will return the objects grouped by key.
    // If there is more than one key, it omits the last key from the object,
    // In order to avoid processing incomplete list of object + version
    _object_grouped_by_key_and_omitted(version_list) {
        let grouped_by_key = _.groupBy(version_list.Versions, "Key");
        if (version_list.IsTruncated) {
            const last_key_pos = version_list.Versions.length - 1;
            if (Object.keys(grouped_by_key).length > 1) {
                grouped_by_key = _.omit(grouped_by_key, version_list.Versions[last_key_pos].Key);
            }
        }
        return grouped_by_key;
    }

    // if the list is truncated returns the the next key marker as the last key in the omitted objects list
    _get_next_key_marker(is_truncated, contents_list) {
        return is_truncated ? Object.keys(contents_list)[Object.keys(contents_list).length - 1] : '';
    }


    // _get_key_etag takes a list of object versions, groups them by keys, 
    // and extracts the ETag value for the first entry for each key. 
    // it will return an object that maps keys to their respective ETag values.
    _get_key_etag(version_list) {
        // As we want to iterate over the latest only, it is ok to omit the latest.
        // We still want to do it with listObjectVersions and not listObjectsV2 as we can have a delete marker
        const dst_contents_left = this._object_grouped_by_key_and_omitted(version_list);
        // As we get the entries per keys in newest in the head, and we only interested in the newest entry,
        // regardless if it is the latest or not, we will map it to a new object containing only the key and the entry.
        const dst_contents_first_entries = _.mapValues(dst_contents_left, _.head);
        // We want to return an object containing only a key and etag so we can later compare the Etags of the same keys
        return _.mapValues(dst_contents_first_entries, entry => entry.ETag);
    }

    // get_keys_diff finds the object keys that src bucket contains but dst bucket doesn't
    // iterate all src_keys and for each if:
    // case 1: src_key is lexicographic bigger than last dst_key,
    //         list dst from next cont token if exist, else - replicate all src keys
    // case 2: src_key is lexicographic smaller than first dst_key
    //         replicate src_key and continue the loop
    // case 3: src_key in dst list keys range - 
    // if src_key doesn't exist is dst_list or needs update - replicate, else do nothing; continue
    async get_keys_diff(src_keys, dst_keys, dst_next_cont_token, src_bucket_name, dst_bucket_name) {
        dbg.log1('replication_server.get_keys_diff: src contents', src_keys.map(c => c.Key), 'dst contents', dst_keys.map(c => c.Key));

        const to_replicate_map = {}; //map of keys and size
        const dst_map = _.keyBy(dst_keys, 'Key');

        for (const [i, src_content] of src_keys.entries()) {
            const cur_src_key = src_content.Key;
            dbg.log1('replication_server.get_keys_diff, src_key: ', i, cur_src_key);

            // case 1
            if (cur_src_key > dst_keys[dst_keys.length - 1].Key) {
                // in next iteration we shouldn't iterate again src keys we already passed
                const src_contents_left = src_keys.slice(i);

                const ans = dst_next_cont_token ? { to_replicate_map, keep_listing_dst: true, src_contents_left } : {
                    to_replicate_map: src_contents_left.reduce((acc, cur_obj) => {
                        acc[cur_obj.Key] = cur_obj.Size;
                        return acc;
                    }, { ...to_replicate_map })
                };
                dbg.log1('replication_server.get_keys_diff, case1: ', dst_next_cont_token, ans);
                return ans;
            }
            // case 2
            if (cur_src_key < dst_keys[0].Key) {
                dbg.log1('replication_server.get_keys_diff, case2: ', cur_src_key);
                to_replicate_map[cur_src_key] = src_content.Size;
                continue;
            }
            // case 3: src_key is in range
            const dst_content = dst_map[cur_src_key];
            dbg.log1('replication_server.get_keys_diff, case3: src_content', src_content, 'dst_content:', dst_content);
            if (dst_content) {
                const src_md_info = await replication_utils.get_object_md(this.noobaa_connection, src_bucket_name, cur_src_key);
                const dst_md_info = await replication_utils.get_object_md(this.noobaa_connection, dst_bucket_name, cur_src_key);

                const should_copy = replication_utils.check_data_or_md_changed(src_md_info, dst_md_info);
                if (should_copy) to_replicate_map[cur_src_key] = src_content.Size;
            } else {
                to_replicate_map[cur_src_key] = src_content.Size;
            }
        }
        dbg.log1('replication_server.get_keys_diff result:', to_replicate_map);
        return { to_replicate_map };
    }

    // get_keys_version_diff finds the object keys and versions that the source bucket contains but destination bucket doesn't
    // LMLM ..... 
    // iterate all src_keys and for each if:
    // case 1: src_key is lexicographic bigger than last dst_key,
    //         list dst from next cont token if exist, else - replicate all remaining keys and their versions
    // case 2: src_key is lexicographic smaller than first dst_key
    //         replicate src_key + all it's versions and continue the loop
    // case 3: src_key in dst list keys range - 
    //         compare etags of the key in the dst list, and find the place to replicate from. <-- LMLM not great explanations... 
    async get_keys_version_diff(src_keys, dst_keys, dst_next_cont_token, src_bucket_name, dst_bucket_name) {

        //LMLM probably should remove this or change as there are no maps here ? ? ? 
        dbg.log0(`LMLM replication_server.get_keys_version_diff: src_keys: ${inspect(src_keys)} dst_keys: ${inspect(dst_keys)}`);
        const to_replicate_map = {}; //LMLM should be key, version, size and not only key version.
        // const dst_map = _.keyBy(dst_keys, 'Key');
        const dest_key_array = Object.keys(dst_keys);
        if (!dest_key_array.length) {
            dbg.warn(`LMLM something went wrong, we should never reach get_keys_version_diff with dest_key_array.length === 0`);
        }

        // May-29 11:16:29.038 [BGWorkers/32014]    [L0] core.server.bg_services.replication_scanner:: LMLM replication_server.get_keys_version_diff: src_keys: {
        //     kubeconfig1: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'hDbHYWp7Yuin57lfn2Ax_JgBW0EICGv2', IsLatest: true, LastModified: 2023-05-28T07:18:58.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'Gd4HO4TcU6j8L4CEOpLiIYFA6zyDQh4L', IsLatest: false, LastModified: 2023-05-28T07:18:44.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'vyArVjYFjH4U3HXYiscHGSJRNpCNfRJY', IsLatest: false, LastModified: 2023-05-28T07:18:41.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'G3UrvYDAgYK9hmbJXNtS5mNxDxh9eGqe', IsLatest: false, LastModified: 2023-05-28T07:18:40.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 4 ],
        //     kubeconfig2: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: 'L_nUZafY7kX6ixwFbB_JOtZ3ajQ8aFwO', IsLatest: true, LastModified: 2023-05-28T07:45:42.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: '88VcjijkxiOsFZFOWVrHzwGBCHFBd9f5', IsLatest: false, LastModified: 2023-05-28T07:19:14.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: '1dGK3cEPEukXxy3YimFi.UT0i6sq8qmW', IsLatest: false, LastModified: 2023-05-28T07:19:12.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 3 ],
        //     kubeconfig3: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig3', VersionId: 'wAVByTy0CsldF3McIER79EHEI.Nrf3Nu', IsLatest: true, LastModified: 2023-05-28T07:19:22.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig3', VersionId: 'vfPb0GHPcUjp7sgpgt1RPJDNOcrRYun1', IsLatest: false, LastModified: 2023-05-28T07:19:20.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 2 ],
        //     kubeconfig4: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig4', VersionId: 'YbikTcxfmghqdoIuxlkcT_oIdpaQnKsY', IsLatest: true, LastModified: 2023-05-28T07:19:32.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 1 ]
        //   } dst_keys: { kubeconfig1: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig2: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig3: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig4: '"133d21ca2c9604af034fab10ab6a6853"' }


        // Checking lexicographic order <<--- LMLM not great explanation ¯\_(ツ)_/¯
        for (const cur_src_key of Object.keys(src_keys)) {
            //     const cur_src_key = src_content.Key;
            //     dbg.log1('replication_server.get_keys_diff, src_key: ', i, cur_src_key);

            // We will use src_contents_left to omit keys that we already passed 
            // as in the next iteration we shouldn't iterate again on them
            // as _.omit creates a new object we don't need to deep clone src_keys.
            let src_contents_left = src_keys;
            dbg.log0(`LMLM replication_server.get_keys_version_diff cur_src_key ${cur_src_key}, src_contents_left ${inspect(src_contents_left)}`);
            // case 1: 
            if (cur_src_key > dest_key_array[dest_key_array.length - 1]) {
                const ans = dst_next_cont_token ? { to_replicate_map, keep_listing_dst: true, src_contents_left } : {
                    // to_replicate_map: src_contents_left.reduce((acc, cur_obj) => {
                    //     acc[cur_obj.Key] = cur_obj.Size;
                    //     return acc;
                    // }, { ...to_replicate_map })
                    ...to_replicate_map,
                    ...src_contents_left
                };
                dbg.log1(`replication_server.get_keys_version_diff, case 1: ${dst_next_cont_token}  ans: ${ans}`);
                dbg.log0(`LMLM replication_server.get_keys_version_diff, case 1: ${dst_next_cont_token}  ans: ${ans}`);
                return ans;
            }
            // case 2
            if (cur_src_key < dest_key_array[0]) {
                dbg.log1(`replication_server.get_keys_version_diff, case 2: ${cur_src_key}`);
                dbg.log0(`LMLM replication_server.get_keys_version_diff, case 2: ${cur_src_key}`);
                to_replicate_map[cur_src_key] = src_keys[cur_src_key];
                src_contents_left = _.omit(src_contents_left, cur_src_key);
                continue;
            }

            // case 3: src_key is in range
            dbg.log1(`replication_server.get_keys_version_diff, case 3: src_content ${cur_src_key} dst_content etag: ${dst_keys[cur_src_key]}`);
            dbg.log0(`LMLM replication_server.get_keys_version_diff, case 3: src_content ${cur_src_key} dst_content etag: ${dst_keys[cur_src_key]}`);
            if (dst_keys[cur_src_key]) {
                dbg.log0(`LMLM replication_server.get_keys_version in Range ${dst_keys[cur_src_key]}`);
                continue; //LMLM remove
                // LMLM do the compare .... (need to compare the etags and get the key + the version needed.)
                // const src_md_info = await replication_utils.get_object_md(this.noobaa_connection, src_bucket_name, cur_src_key);
                // const dst_md_info = await replication_utils.get_object_md(this.noobaa_connection, dst_bucket_name, cur_src_key);

                // const should_copy = replication_utils.check_data_or_md_changed(src_md_info, dst_md_info);
                // if (should_copy) to_replicate_map[cur_src_key] = src_content.Size;
            } else {
                to_replicate_map[cur_src_key] = src_keys[cur_src_key];
            }
            src_contents_left = _.omit(src_contents_left, cur_src_key);
        }
        dbg.log1('replication_server.get_keys_version_diff result:', to_replicate_map);
        dbg.log0(`LMLM replication_server.get_keys_version_diff result to_replicate_map: ${inspect(to_replicate_map)}`);
        return { to_replicate_map };
    }

    _get_rule_status(rule, src_cont_token, keys_sizes_map_to_copy, move_res) {
        const keys_to_copy_sizes = Object.values(keys_sizes_map_to_copy);
        const num_keys_to_copy = keys_to_copy_sizes.length || 0;
        const num_bytes_to_copy = num_keys_to_copy > 0 ? _.sum(keys_to_copy_sizes) : 0;

        const num_keys_moved = move_res.length || 0;
        const num_bytes_moved = num_keys_moved > 0 ? _.sumBy(move_res, itm => keys_sizes_map_to_copy[itm]) : 0;

        const status = {
            last_cycle_rule_id: rule,
            last_cycle_src_cont_token: src_cont_token,
            last_cycle_writes_num: num_keys_moved,
            last_cycle_writes_size: num_bytes_moved,
            last_cycle_error_writes_num: num_keys_to_copy - num_keys_moved,
            last_cycle_error_writes_size: num_bytes_to_copy - num_bytes_moved,
        };
        dbg.log0('_get_rule_status: ', status);
        return status;
    }

    update_replication_prom_report(bucket_name, replication_policy_id, replication_status) {
        const core_report = prom_reporting.get_core_report();
        const last_cycle_status = _.defaults({
            ...replication_status,
            bucket_name: bucket_name.unwrap(),
            replication_id: replication_policy_id
        }, PARTIAL_SINGLE_BUCKET_REPLICATION_DEFAULTS);

        core_report.set_replication_status(last_cycle_status);
    }
}

// LMLM: to clean :) 
function inspect(x) {
    return util.inspect(_.omit(x, 'source_stream'), true, 5, true);
}

exports.ReplicationScanner = ReplicationScanner;
