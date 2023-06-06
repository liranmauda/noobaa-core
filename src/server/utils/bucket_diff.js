/* Copyright (C) 2023 NooBaa */
'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');

const dbg = require('../../util/debug_module')(__filename);

class BucketDiff {

    /**
     * @param {{
     *   first_bucket: string;
     *   second_bucket: string;
     *   s3_params: AWS.S3.ClientConfiguration
     * }} params
     */
    constructor({ first_bucket, second_bucket, s3_params }) {
        this.first_bucket = first_bucket;
        this.second_bucket = second_bucket;
        //We will assume at this stage that first_bucket and second_bucket are accessible via the same s3
        // If we want to do a diff on none s3 we should use noobaa namespace buckets.
        this.s3 = new AWS.S3(s3_params);
    }


    /**
     * @param {{ 
     *   first_bucket: string; 
     *   second_bucket: string; 
     *   prefix: string;
     *   max_keys: number;
     *   version: boolean;
     *   current_first_bucket_cont_token: string;
     *   current_second_bucket_cont_token: string;
     * }} params
     */
    async get_buckets_diff(params) {
        const {
            first_bucket,
            second_bucket,
            prefix,
            max_keys,
            version,
            current_first_bucket_cont_token,
            current_second_bucket_cont_token,
        } = params;

        //list the objects in the first bucket
        const first_bucket_response = await this._list_objects(first_bucket, prefix, max_keys, version, current_first_bucket_cont_token);

        const first_bucket_contents_left = this._object_grouped_by_key_and_omitted(first_bucket_response);

        const first_bucket_cont_token = version ?
            this._get_next_key_marker(first_bucket_response.IsTruncated, first_bucket_contents_left) :
            first_bucket_response.NextContinuationToken;

        const diff = {
            keys_diff_map: {
                first_bucket_only: [],
                second_bucket_only: [],
            },
            first_bucket_cont_token,
            second_bucket_cont_token: ''
        };

        // LMLM TODO only on a flag 
        // // return on an empty first bucket
        // if (!Object.keys(src_contents_left).length) return diff;
    }

    /**
     * @param {string} bucket_name
     * @param {string} prefix
     * @param {number} max_keys
     * @param {boolean} version
     * @param {string} continuation_token
     */
    async _list_objects(bucket_name, prefix, max_keys, version, continuation_token) {
        try {
            dbg.log1('BucketDiff _list_objects::', bucket_name, prefix, max_keys, continuation_token);
            const params = {
                Bucket: bucket_name,
                Prefix: prefix,
                MaxKeys: max_keys
            };
            if (version) {
                params.KeyMarker = continuation_token;
                return await this.s3.listObjectVersions(params).promise();
            } else {
                if (continuation_token) params.ContinuationToken = continuation_token;
                return await this.s3.listObjectsV2(params).promise();
            }
        } catch (err) {
            dbg.error('BucketDiff _list_objects: error:', err);
            throw err;
        }
    }

    /**
     * @param {import("aws-sdk/lib/request").PromiseResult<AWS.S3.ListObjectVersionsOutput, AWS.AWSError> | 
     *         import("aws-sdk/lib/request").PromiseResult<AWS.S3.ListObjectsV2Output, AWS.AWSError>} list
     * @param {string} [field]
     * 
     * _object_grouped_by_key_and_omitted will return the objects grouped by key.
     * When we have versioning enabled, if there is more than one key, it omits 
     * the last key from the object, in order to avoid processing incomplete list of object + version
     */
    _object_grouped_by_key_and_omitted(list, field) {
        let grouped_by_key = _.groupBy(list[field], "Key");
        // in case that the field passed is Contents, we should not omit as this is a list object and not list versions
        // and the use oof continuation token later on the road will lead us to skip the last key if omitted.
        if (list.IsTruncated && field !== "Contents") {
            const last_key_pos = list[field].length - 1;
            if (Object.keys(grouped_by_key).length > 1) {
                grouped_by_key = _.omit(grouped_by_key, list[field][last_key_pos].Key);
            }
        }
        return grouped_by_key;
    }

    /**
     * @param {boolean} is_truncated
     * @param {_.Dictionary<any[]>} list
     * if the list is truncated returns the the next key marker as the last key in the omitted objects list
     */
    _get_next_key_marker(is_truncated, list) {
        return is_truncated ? Object.keys(list)[Object.keys(list).length - 1] : '';
    }
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
// async get_keys_version_diff(src_keys, dst_keys, dst_next_cont_token, src_bucket_name, dst_bucket_name) {
// 
//         //LMLM probably should remove this or change as there are no maps here ? ? ? 
//         dbg.log0(`LMLM replication_server.get_keys_version_diff: src_keys: ${inspect(src_keys)} dst_keys: ${inspect(dst_keys)}`);
//         const to_replicate_map = {}; //LMLM should be key, version, size and not only key version.
//         // const dst_map = _.keyBy(dst_keys, 'Key');
//         const dest_key_array = Object.keys(dst_keys);
//         if (!dest_key_array.length) {
//             dbg.warn(`LMLM something went wrong, we should never reach get_keys_version_diff with dest_key_array.length === 0`);
//         }
// 
// May-29 11:16:29.038 [BGWorkers/32014]    [L0] core.server.bg_services.replication_scanner:: LMLM replication_server.get_keys_version_diff: src_keys: {
//     kubeconfig1: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'hDbHYWp7Yuin57lfn2Ax_JgBW0EICGv2', IsLatest: true, LastModified: 2023-05-28T07:18:58.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'Gd4HO4TcU6j8L4CEOpLiIYFA6zyDQh4L', IsLatest: false, LastModified: 2023-05-28T07:18:44.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'vyArVjYFjH4U3HXYiscHGSJRNpCNfRJY', IsLatest: false, LastModified: 2023-05-28T07:18:41.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig1', VersionId: 'G3UrvYDAgYK9hmbJXNtS5mNxDxh9eGqe', IsLatest: false, LastModified: 2023-05-28T07:18:40.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 4 ],
//     kubeconfig2: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: 'L_nUZafY7kX6ixwFbB_JOtZ3ajQ8aFwO', IsLatest: true, LastModified: 2023-05-28T07:45:42.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: '88VcjijkxiOsFZFOWVrHzwGBCHFBd9f5', IsLatest: false, LastModified: 2023-05-28T07:19:14.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig2', VersionId: '1dGK3cEPEukXxy3YimFi.UT0i6sq8qmW', IsLatest: false, LastModified: 2023-05-28T07:19:12.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 3 ],
//     kubeconfig3: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig3', VersionId: 'wAVByTy0CsldF3McIER79EHEI.Nrf3Nu', IsLatest: true, LastModified: 2023-05-28T07:19:22.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig3', VersionId: 'vfPb0GHPcUjp7sgpgt1RPJDNOcrRYun1', IsLatest: false, LastModified: 2023-05-28T07:19:20.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 2 ],
//     kubeconfig4: [ { ETag: '"133d21ca2c9604af034fab10ab6a6853"', ChecksumAlgorithm: [ [length]: 0 ], Size: 24599, StorageClass: 'STANDARD', Key: 'kubeconfig4', VersionId: 'YbikTcxfmghqdoIuxlkcT_oIdpaQnKsY', IsLatest: true, LastModified: 2023-05-28T07:19:32.000Z, Owner: { DisplayName: 'NooBaa', ID: '123' } }, [length]: 1 ]
//   } dst_keys: { kubeconfig1: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig2: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig3: '"133d21ca2c9604af034fab10ab6a6853"', kubeconfig4: '"133d21ca2c9604af034fab10ab6a6853"' }


// Checking lexicographic order <<--- LMLM not great explanation ¯\_(ツ)_/¯
//     for (const cur_src_key of Object.keys(src_keys)) {
//         //     const cur_src_key = src_content.Key;
//         //     dbg.log1('replication_server.get_keys_diff, src_key: ', i, cur_src_key);

//         // We will use src_contents_left to omit keys that we already passed 
//         // as in the next iteration we shouldn't iterate again on them.
//         // as _.omit creates a new object we don't need to deep clone src_keys.
//         let src_contents_left = src_keys;
//         dbg.log0(`LMLM replication_server.get_keys_version_diff cur_src_key ${cur_src_key}, src_contents_left ${inspect(src_contents_left)}`);
//         // case 1: 
//         if (cur_src_key > dest_key_array[dest_key_array.length - 1]) {
//             const ans = dst_next_cont_token ? { to_replicate_map, keep_listing_dst: true, src_contents_left } : {
//                 // to_replicate_map: src_contents_left.reduce((acc, cur_obj) => {
//                 //     acc[cur_obj.Key] = cur_obj.Size;
//                 //     return acc;
//                 // }, { ...to_replicate_map })
//                 ...to_replicate_map,
//                 ...src_contents_left
//             };
//             dbg.log1(`replication_server.get_keys_version_diff, case 1: ${dst_next_cont_token}  ans: ${ans}`);
//             dbg.log0(`LMLM replication_server.get_keys_version_diff, case 1: ${dst_next_cont_token}  ans: ${ans}`);
//             return ans;
//         }
//         // case 2
//         if (cur_src_key < dest_key_array[0]) {
//             dbg.log1(`replication_server.get_keys_version_diff, case 2: ${cur_src_key}`);
//             dbg.log0(`LMLM replication_server.get_keys_version_diff, case 2: ${cur_src_key}`);
//             to_replicate_map[cur_src_key] = src_keys[cur_src_key];
//             src_contents_left = _.omit(src_contents_left, cur_src_key);
//             continue;
//         }

//         // case 3: src_key is in range
//         dbg.log1(`replication_server.get_keys_version_diff, case 3: src_content ${cur_src_key} dst_content etag: ${dst_keys[cur_src_key]}`);
//         dbg.log0(`LMLM replication_server.get_keys_version_diff, case 3: src_content ${cur_src_key} dst_content etag: ${dst_keys[cur_src_key]}`);
//         if (dst_keys[cur_src_key]) {
//             dbg.log0(`LMLM replication_server.get_keys_version in Range ${dst_keys[cur_src_key]}`);
//             continue; //LMLM remove
//             // LMLM do the compare .... (need to compare the etags and get the key + the version needed.)
//             // const src_md_info = await replication_utils.get_object_md(this.noobaa_connection, src_bucket_name, cur_src_key);
//             // const dst_md_info = await replication_utils.get_object_md(this.noobaa_connection, dst_bucket_name, cur_src_key);

//             // const should_copy = replication_utils.check_data_or_md_changed(src_md_info, dst_md_info);
//             // if (should_copy) to_replicate_map[cur_src_key] = src_content.Size;
//         } else {
//             to_replicate_map[cur_src_key] = src_keys[cur_src_key];
//         }
//         src_contents_left = _.omit(src_contents_left, cur_src_key);
//     }
//     dbg.log1('replication_server.get_keys_version_diff result:', to_replicate_map);
//     dbg.log0(`LMLM replication_server.get_keys_version_diff result to_replicate_map: ${inspect(to_replicate_map)}`);
//     return { to_replicate_map };
// }


exports.BucketDiff = BucketDiff;
