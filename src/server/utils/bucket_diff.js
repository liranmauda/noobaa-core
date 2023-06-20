/* Copyright (C) 2023 NooBaa */
'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');

const dbg = require('../../util/debug_module')(__filename);

class BucketDiff {

    /**
     * @param {{
     *   first_bucket: string; //LMLM do we really want it in the constructor? 
     *   second_bucket: string; //LMLM do we really want it in the constructor? 
     *   ref_first_bucket_only: boolean;
     *   s3_params: AWS.S3.ClientConfiguration
     * }} params
     */
    constructor({ first_bucket, second_bucket, s3_params, ref_first_bucket_only }) {
        this.first_bucket = first_bucket;
        this.second_bucket = second_bucket;
        //using for replication, only first bucket to second bucket direction matter.
        this.ref_first_bucket_only = ref_first_bucket_only;
        //We will assume at this stage that first_bucket and second_bucket are accessible via the same s3
        // If we want to do a diff on none s3 we should use noobaa namespace buckets.
        this.s3 = new AWS.S3(s3_params);
    }


    /**
     * @param {{ 
     *   first_bucket: string; //LMLM remove and use this.first_bucket
     *   second_bucket: string; //LMLM remove and use this.second_bucket
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

        const diff = {
            keys_diff_map: {
                first_bucket_only: [],
                second_bucket_only: [],
            },
            first_bucket_cont_token: '',
            second_bucket_cont_token: ''
        };

        //list the objects in the first bucket
        //LMLM we should not always list, if we have contact left then we should use it.
        const {
            bucket_contents_left: first_bucket_contents_left,
            bucket_cont_token: first_bucket_cont_token
        } = await this.get_objects(first_bucket, prefix, max_keys, version, current_first_bucket_cont_token);

        diff.first_bucket_cont_token = first_bucket_cont_token;

        if (Object.keys(first_bucket_contents_left).length) {
            //LMLM we should not always list, if we have contact left then we should use it.
            const {
                bucket_contents_left: second_bucket_contents_left,
                bucket_cont_token: new_second_bucket_cont_token
            } = await this.get_objects(second_bucket, prefix, max_keys, version, current_second_bucket_cont_token);

            const test = await get_keys_diff(
                first_bucket_contents_left, second_bucket_contents_left, first_bucket_cont_token, new_second_bucket_cont_token);

            dbg.log('test:', test);
        } else if (!this.ref_first_bucket_only) { // LMLM probably can take all of this into the diff function... 
            let new_second_bucket_cont_token = current_second_bucket_cont_token;
            let keep_listing_dst = true;
            while (keep_listing_dst) {
                const second_bucket_response = await this._list_objects(
                    second_bucket, prefix, max_keys, version, new_second_bucket_cont_token);
                // dbg.log0('second_bucket_response', second_bucket_response);
                const second_bucket_contents_left = this._object_grouped_by_key_and_omitted(second_bucket_response, version);
                // dbg.log0('second_bucket_contents_left', second_bucket_contents_left);
                new_second_bucket_cont_token = this._get_next_key_marker(second_bucket_response, second_bucket_contents_left, version);
                // LMLM: TODO: we should decide how to cut it, I assume we can't keep adding in the memory... 
                keep_listing_dst = second_bucket_response.IsTruncated;
                // dbg.log0('new_second_bucket_cont_token', new_second_bucket_cont_token, 'keep_listing_dst', keep_listing_dst);
                diff.keys_diff_map.second_bucket_only = diff.keys_diff_map.second_bucket_only.concat(second_bucket_contents_left);
            }
            //LMLM for now, as we list all and not cutting, it will always be empty. when we cut it, we will have a value.
            diff.second_bucket_cont_token = new_second_bucket_cont_token;
        }

        dbg.log0('BucketDiff get_buckets_diff', diff);
        return diff;
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
     * @param {boolean} version
     * 
     * _object_grouped_by_key_and_omitted will return the objects grouped by key.
     * When we have versioning enabled, if there is more than one key, it omits 
     * the last key from the object, in order to avoid processing incomplete list of object + version
     */
    _object_grouped_by_key_and_omitted(list, version) {
        const field = version ? "Versions" : "Contents";
        let grouped_by_key = _.groupBy(list[field], "Key");
        // We should not omit if this is a list object and not list versions
        // and the use of continuation token later on the road will lead us to skip the last key if omitted.
        if (list.IsTruncated && version) {
            const last_key_pos = list[field].length - 1;
            if (Object.keys(grouped_by_key).length > 1) {
                grouped_by_key = _.omit(grouped_by_key, list[field][last_key_pos].Key);
            }
        }
        return grouped_by_key;
    }

    /**
     * @param {_.Dictionary<any[]>} list 
     * @param {boolean} version
     * 
     * @param {import("aws-sdk/lib/request").PromiseResult<AWS.S3.ListObjectVersionsOutput, AWS.AWSError> | 
     *         import("aws-sdk/lib/request").PromiseResult<AWS.S3.ListObjectsV2Output, AWS.AWSError>} bucket_response
     * if the list is truncated on a version list, returns the the next key marker as the last key in the omitted objects list
     * if it is a list without versions, return NextContinuationToken.
     */
    _get_next_key_marker(bucket_response, list, version) {
        if (version) return bucket_response.IsTruncated ? Object.keys(list)[Object.keys(list).length - 1] : '';
        return bucket_response.NextContinuationToken;
    }


    /**
     * @param {string} bucket
     * @param {string} prefix
     * @param {number} max_keys
     * @param {boolean} version
     * @param {string} curr_bucket_cont_token
     * 
     * get_objects will get a bucket and parameters and return the object we want to work on and the continuation token
     * 
     */
    async get_objects(bucket, prefix, max_keys, version, curr_bucket_cont_token) {
        const bucket_response = await this._list_objects(bucket, prefix, max_keys, version, curr_bucket_cont_token);
        dbg.log0('BucketDiff get_objects:: bucket_response', bucket_response);
        const bucket_contents_left = this._object_grouped_by_key_and_omitted(bucket_response, version);
        const bucket_cont_token = this._get_next_key_marker(bucket_response, bucket_contents_left, version);
        return { bucket_contents_left, bucket_cont_token };
    }
}

// get_keys_version_diff finds the object keys and versions that the source bucket contains but destination bucket doesn't
// LMLM ..... 
// iterate all first_bucket_keys and for each if:
// case 1: first_bucket_key is lexicographic bigger than last second_bucket_keys,
//         list second from next cont token if exist, else - replicate all remaining keys and their versions
// case 2: first_bucket_key is lexicographic smaller than first second_bucket_keys
//         replicate first_bucket_key + all it's versions and continue the loop
// case 3: first_bucket_key in dst list keys range - 
//         compare etags of the key in the dst list, and find the place to replicate from. <-- LMLM not great explanations... 
async function get_keys_diff(first_bucket_keys, second_bucket_keys, first_bucket_cont_token, second_bucket_cont_token) {

    const ans = {
        keys_diff_map: {
            first_bucket_only: {},
            second_bucket_only: {},
        },
        keys_contents_left: {
            first_bucket_contents_left: first_bucket_keys,
            second_bucket_contents_left: second_bucket_keys,
        },
        //LMLM we probably don't need it, as we will do a single run each time, and list only if the keys_contents_left is empty (per bucket)
        keep_listing_first_bucket: false,
        keep_listing_second_bucket: false,
    };

    const {
        ans: to_return,
        stop_compare,
    } = keys_out_of_range(ans, first_bucket_keys, second_bucket_keys, first_bucket_cont_token, second_bucket_cont_token);

    //LMLM TODO remove the print and make it a single line.
    if (stop_compare) {
        dbg.log0('LMLM stop_compare after keys_out_of_range');
        return to_return;
    }
    const { ans: new_to_return } = keys_in_range(
        ans, first_bucket_keys, second_bucket_keys, first_bucket_cont_token, second_bucket_cont_token);

    dbg.log0('LMLM', new_to_return);

    return ans;
}


function keys_out_of_range(ans, first_bucket_keys, second_bucket_keys, first_bucket_cont_token, second_bucket_cont_token) {
    const first_bucket_key_array = Object.keys(first_bucket_keys);
    const second_bucket_key_array = Object.keys(second_bucket_keys);
    // LMLM we will use stop compare for not keep iterating
    // If we met a condition where all of one bucket list is lexicographic larger then the other bucket, we should not keep comparing.
    let stop_compare = true;

    // second bucket list is empty 
    if (!Object.keys(second_bucket_key_array).length) {
        ans.keys_diff_map.first_bucket_only = first_bucket_keys;
        dbg.log('LMLM 1');
        return { ans, stop_compare };
    }

    // first bucket list is empty
    if (!Object.keys(first_bucket_key_array).length) {
        ans.keys_diff_map.second_bucket_only = second_bucket_keys;
        dbg.log('LMLM 1.1');
        return { ans, stop_compare };
    }

    // all keys in second bucket are lexicographic smaller
    if (first_bucket_key_array[0] > second_bucket_key_array[second_bucket_key_array.length - 1]) {
        ans.keys_diff_map.second_bucket_only = second_bucket_keys;
        if (second_bucket_cont_token) {
            ans.keep_listing_second_bucket = true;
            //in the next run we should not iterate over the already passed keys.
            ans.keys_contents_left.first_bucket_contents_left = first_bucket_keys;
        } else {
            ans.keys_diff_map.first_bucket_only = first_bucket_keys;
        }
        dbg.log1(`replication_server.get_keys_version_diff, case 1: ${second_bucket_cont_token}  ans: ${ans}`);
        dbg.log0(`LMLM replication_server.get_keys_version_diff, case 1: ${second_bucket_cont_token}  ans: ${ans}`);
        dbg.log('LMLM 2');
        return { ans, stop_compare };
    }

    //  all keys in first bucket are lexicographic smaller
    if (second_bucket_key_array[0] > first_bucket_key_array[first_bucket_key_array.length - 1]) {
        ans.keys_diff_map.first_bucket_only = first_bucket_keys;
        if (first_bucket_cont_token) {
            ans.keep_listing_first_bucket = true;
            //in the next run we should not iterate over the already passed keys.
            ans.keys_contents_left.second_bucket_contents_left = second_bucket_keys;
        } else {
            ans.keys_diff_map.second_bucket_only = second_bucket_keys;
        }
        dbg.log1(`replication_server.get_keys_version_diff, case 1: ${first_bucket_cont_token}  ans: ${ans}`);
        dbg.log0(`LMLM replication_server.get_keys_version_diff, case 1: ${first_bucket_cont_token}  ans: ${ans}`);
        dbg.log('LMLM 3');
        return { ans, stop_compare };
    }

    stop_compare = false;
    dbg.log('LMLM 4');
    return { ans, stop_compare };
}

function keys_in_range(ans, first_bucket_keys, second_bucket_keys, first_bucket_cont_token, second_bucket_cont_token) {
    const first_bucket_key_array = Object.keys(first_bucket_keys);
    const second_bucket_key_array = Object.keys(second_bucket_keys);

    // Checking lexicographic order << -- - LMLM not great explanation ¯\ _(ツ) _ / ¯
    for (const cur_first_bucket_key of first_bucket_key_array) {
        // dbg.log0('LMLM cur_first_bucket_key', cur_first_bucket_key);
        // dbg.log0('LMLM ans.keys_contents_left.first_bucket_contents_left', ans.keys_contents_left.first_bucket_contents_left);

        const first_bucket_curr_obj = ans.keys_contents_left.first_bucket_contents_left[cur_first_bucket_key];
        dbg.log0('LMLM first_bucket_curr_obj', first_bucket_curr_obj);
        // dbg.log0('LMLM first_bucket_curr_obj', first_bucket_curr_obj);
        // case 2: 
        if (cur_first_bucket_key < second_bucket_key_array[0]) {
            dbg.log1(`replication_server.get_keys_version_diff, case 2: ${cur_first_bucket_key}`);
            dbg.log0(`LMLM replication_server.get_keys_version_diff, case 2: ${cur_first_bucket_key}`);

            const contents_left = ans.keys_contents_left.first_bucket_contents_left[cur_first_bucket_key];
            const update_first_bucket_curr_obj = first_bucket_curr_obj ? first_bucket_curr_obj.concat(contents_left) : contents_left;

            ans.keys_diff_map.first_bucket_only[cur_first_bucket_key] = update_first_bucket_curr_obj;

            ans.keys_contents_left.first_bucket_contents_left = _.omit(
                ans.keys_contents_left.first_bucket_contents_left, cur_first_bucket_key);
            continue;
        }


        // case 3: first_bucket_key is is also in the second bucket 
        // dbg.log1(`replication_server.get_keys_version_diff, case 3: src_content ${cur_first_bucket_key} dst_content etag: ${second_bucket_keys[cur_first_bucket_key]}`);
        // dbg.log0(`LMLM replication_server.get_keys_version_diff, case 3: src_content ${cur_first_bucket_key} dst_content etag: ${second_bucket_keys[cur_first_bucket_key]}`);
        const second_bucket_curr_obj = ans.keys_contents_left.second_bucket_contents_left[cur_first_bucket_key];
        dbg.log0('LMLM second_bucket_curr_obj', second_bucket_curr_obj);

        if (second_bucket_curr_obj) {
            dbg.log0(`LMLM replication_server.get_keys_version in Range ${second_bucket_keys[cur_first_bucket_key]}`);

            // get the position of the etag in the second bucket for the same key name. 
            const etag_on_first_bucket = get_etag_pos(0, second_bucket_curr_obj, first_bucket_curr_obj); //LMLM maybe need to switch the order and check the latest Etag of the second one??

            // -1 ETag is not in the first bucket it is a diff
            // 0 Etag is on the latest, we just need to omit this key
            // n > 0  all n are diff.
            if (etag_on_first_bucket === -1) {
                dbg.log0('LMLM -1 ETag is not in the first bucket it is a diff', etag_on_first_bucket);
            } else if (etag_on_first_bucket !== 0) {
                // can happen only in version
                const first_bucket_diff = first_bucket_curr_obj.slice(0, etag_on_first_bucket);
                dbg.log0('LMLM first_bucket_diff!!!', first_bucket_diff);
            }
            // omit that key from both content lists.
            ans.keys_contents_left.first_bucket_contents_left = _.omit(
                ans.keys_contents_left.first_bucket_contents_left, cur_first_bucket_key);

            ans.keys_contents_left.second_bucket_contents_left = _.omit(
                ans.keys_contents_left.second_bucket_contents_left, cur_first_bucket_key);
            continue; //LMLM remove
            // LMLM do the compare .... (need to compare the etags and get the key + the version needed.)
            // const src_md_info = await replication_utils.get_object_md(this.noobaa_connection, src_bucket_name, cur_first_bucket_key);
            // const dst_md_info = await replication_utils.get_object_md(this.noobaa_connection, dst_bucket_name, cur_first_bucket_key);

            // const should_copy = replication_utils.check_data_or_md_changed(src_md_info, dst_md_info);
            // if (should_copy) to_replicate_map[cur_first_bucket_key] = src_content.Size;
        } else { //LMLM this is when  there is no such key name in this range.
            dbg.log0('LMLM TODO');
            // to_replicate_map[cur_first_bucket_key] = first_bucket_keys[cur_first_bucket_key];
        }
        // first_bucket_contents_left = _.omit(first_bucket_contents_left, cur_first_bucket_key);


        // if (cur_first_bucket_key < second_bucket_key_array[0]) {
        //     dbg.log1(`replication_server.get_keys_version_diff, case 2: ${cur_first_bucket_key}`);
        //     dbg.log0(`LMLM replication_server.get_keys_version_diff, case 2: ${cur_first_bucket_key}`);
        //     ans.keys_diff_map.first_bucket_only[cur_first_bucket_key] =
        //         ans.keys_diff_map.first_bucket_only[cur_first_bucket_key] ?
        //         ans.keys_diff_map.first_bucket_only[cur_first_bucket_key].concat(ans.first_bucket_contents_left[cur_first_bucket_key]) :
        //         ans.first_bucket_contents_left[cur_first_bucket_key];
        //     ans.first_bucket_contents_left = _.omit(ans.first_bucket_contents_left, cur_first_bucket_key);
        //     continue;
        // }
    }
    dbg.log0('LMLM ans', ans);
    return ans;
}

//LMLM TODO give an appropriate names... 
function get_etag_pos(pos, array_a, array_b) {
    // Getting the first etag of array_a
    const ETag = array_a[pos].ETag;
    const target_pos = array_b.findIndex(obj => obj.ETag === ETag);
    return target_pos;
}

exports.BucketDiff = BucketDiff;
