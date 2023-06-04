/* Copyright (C) 2023 NooBaa */
'use strict';

const _ = require('lodash');
const AWS = require('aws-sdk');

const dbg = require('../../util/debug_module')(__filename);

class BucketDiff {

    /**
     * @param {{
     *   bucket1: string;
     *   bucket2: string;
     *   s3_params: AWS.S3.ClientConfiguration
     * }} params
     */
    constructor({ bucket1, bucket2, s3_params }) {
        this.bucket1 = bucket1;
        this.bucket2 = bucket2;
        //We will assume at this stage that bucket1 and bucket2 are accessible via the same s3
        // If we want to do a diff on none s3 we should use noobaa namespace buckets.
        this.s3 = new AWS.S3(s3_params);
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

    // _object_grouped_by_key_and_omitted will return the objects grouped by key.
    // When we have versioning enabled, if there is more than one key, it omits 
    // the last key from the object, in order to avoid processing incomplete list of object + version
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
}


// _object_grouped_by_key_and_omitted will return the objects grouped by key.
// If there is more than one key, it omits the last key from the object,
// In order to avoid processing incomplete list of object + version
function _object_grouped_by_key_and_omitted(list) {
    let grouped_by_key = _.groupBy(list.Versions, "Key");
    if (list.IsTruncated) {
        const last_key_pos = list.Versions.length - 1;
        if (Object.keys(grouped_by_key).length > 1) {
            grouped_by_key = _.omit(grouped_by_key, list.Versions[last_key_pos].Key);
        }
    }
    return grouped_by_key;
}

// async list_buckets_and_compare(src_bucket, dst_bucket, prefix, cur_src_cont_token, cur_dst_cont_token) {

//     // list src_bucket
//     const src_list = await this.list_objects(src_bucket, prefix, cur_src_cont_token);
//     const ans = {
//         keys_sizes_map_to_copy: {},
//         src_cont_token: src_list.NextContinuationToken || '',
//         dst_cont_token: ''
//     };

//     // edge case 1: src list = [] , nothing to replicate
//     if (!src_list.Contents.length) return ans;

//     let src_contents_left = src_list.Contents;
//     let new_dst_cont_token = cur_dst_cont_token;
//     const last_src_key = src_list.Contents[src_list.Contents.length - 1].Key;

//     let keep_listing_dst = true;
//     while (keep_listing_dst) {
//         const dst_list = await this.list_objects(dst_bucket, prefix, new_dst_cont_token);

//         // edge case 2: dst list = [] , replicate all src_list
//         // edge case 3: all src_keys are lexicographic smaller than the first dst key, replicate all src_list
//         if (!dst_list.Contents.length || last_src_key < dst_list.Contents[0].Key) {
//             ans.keys_sizes_map_to_copy = src_contents_left.reduce(
//                 (acc, content1) => {
//                     acc[content1.Key] = content1.Size;
//                     return acc;
//                 }, { ...ans.keys_sizes_map_to_copy });
//             break;
//         }

//         // find keys to copy
//         const diff = await this.get_keys_diff(src_contents_left, dst_list.Contents, dst_list.NextContinuationToken,
//             src_bucket, dst_bucket);

//         keep_listing_dst = diff.keep_listing_dst;
//         src_contents_left = diff.src_contents_left;
//         ans.keys_sizes_map_to_copy = { ...ans.keys_sizes_map_to_copy, ...diff.to_replicate_map };

//         // advance dst token only when cur dst list could not contains next src list items
//         const last_dst_key = dst_list.Contents[dst_list.Contents.length - 1].Key;
//         if (last_src_key >= last_dst_key) {
//             new_dst_cont_token = dst_list.NextContinuationToken;
//         }
//     }
//     return {
//         ...ans,
//         // if src_list cont token is empty - dst_list cont token should be empty too
//         dst_cont_token: (src_list.NextContinuationToken && new_dst_cont_token) || ''
//     };
// }

exports.BucketDiff = BucketDiff;
