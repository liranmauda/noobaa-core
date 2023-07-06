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
     *   prefix: string;
     *   max_keys: number;
     *   version: boolean;
     *   current_first_bucket_cont_token: string;
     *   current_second_bucket_cont_token: string;
     * }} params
     */
    async get_buckets_diff(params) {
        const {
            prefix,
            max_keys,
            version,
            current_first_bucket_cont_token,
            current_second_bucket_cont_token,
        } = params;

        const diff = {
            keys_diff_map: {},
            first_bucket_cont_token: '',
            second_bucket_cont_token: '',
        };

        let first_bucket_contents_left;
        let first_bucket_cont_token;
        //list the objects in the first bucket
        ({
            bucket_contents_left: first_bucket_contents_left,
            bucket_cont_token: first_bucket_cont_token
        } = await this.get_objects(this.first_bucket, prefix, max_keys, version, current_first_bucket_cont_token));

        if (first_bucket_cont_token) diff.first_bucket_cont_token = first_bucket_cont_token;
        if (Object.keys(first_bucket_contents_left).length === 0) return diff;

        let second_bucket_contents_left;
        let second_bucket_cont_token;
        let new_second_bucket_cont_token = current_second_bucket_cont_token;
        let keep_listing_second_bucket = true;

        while (keep_listing_second_bucket) {
            ({
                bucket_contents_left: second_bucket_contents_left,
                bucket_cont_token: new_second_bucket_cont_token
            } = await this.get_objects(this.second_bucket, prefix, max_keys, version, new_second_bucket_cont_token));

            const keys_diff_response = await this.get_keys_diff(
                first_bucket_contents_left, second_bucket_contents_left, new_second_bucket_cont_token, version);

            first_bucket_contents_left = keys_diff_response.keys_contents_left;
            keep_listing_second_bucket = keys_diff_response.keep_listing_second_bucket;
            diff.keys_diff_map = { ...diff.keys_diff_map, ...keys_diff_response.keys_diff_map };

            const first_bucket_key_array = Object.keys(first_bucket_contents_left);
            const second_bucket_key_array = Object.keys(second_bucket_contents_left);

            if (first_bucket_key_array.length !== 0 && second_bucket_key_array.length !== 0) {
                const first_bucket_key_in_last_pos = first_bucket_key_array[first_bucket_key_array.length - 1];
                const second_bucket_key_in_last_pos = second_bucket_key_array[second_bucket_key_array.length - 1];
                if (first_bucket_key_in_last_pos >= second_bucket_key_in_last_pos) {
                    second_bucket_cont_token = new_second_bucket_cont_token;
                }
            }
        }

        dbg.log2('BucketDiff get_buckets_diff', diff);
        diff.second_bucket_cont_token = (first_bucket_cont_token && second_bucket_cont_token) ? new_second_bucket_cont_token : '';
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
     *         import("aws-sdk/lib/request").PromiseResult<AWS.S3.ListObjectsV2Output, AWS.AWSError>} list_objects_response
     * if the list is truncated on a version list, returns the the next key marker as the last key in the omitted objects list
     * if it is a list without versions, return NextContinuationToken.
     */
    _get_next_key_marker(list_objects_response, list, version) {
        if (version) return list_objects_response.IsTruncated ? Object.keys(list)[Object.keys(list).length - 1] : '';
        return list_objects_response.NextContinuationToken;
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
        dbg.log2('get_objects::', bucket, prefix, max_keys, version, curr_bucket_cont_token);
        const list_objects_response = await this._list_objects(bucket, prefix, max_keys, version, curr_bucket_cont_token);
        dbg.log2('BucketDiff get_objects:: bucket_response', list_objects_response);
        const bucket_contents_left = this._object_grouped_by_key_and_omitted(list_objects_response, version);
        const bucket_cont_token = this._get_next_key_marker(list_objects_response, bucket_contents_left, version);
        dbg.log2('BucketDiff get_objects:: bucket', bucket, 'bucket_contents_left', bucket_contents_left);
        return { bucket_contents_left, bucket_cont_token };
    }

    /**
     * @param {any} first_bucket_keys
     * @param {_.Dictionary<any[]>} second_bucket_keys
     * @param {string} second_bucket_cont_token
     * @param {boolean} version
     * 
     * get_keys_version_diff finds the object keys and versions that the source bucket contains but destination bucket doesn't
     */
    async get_keys_diff(first_bucket_keys, second_bucket_keys, second_bucket_cont_token, version) {
        const ans = {
            keys_diff_map: {},
            keys_contents_left: first_bucket_keys,
            keep_listing_second_bucket: false,
        };

        const stop_compare = this._keys_out_of_range(ans, second_bucket_keys);
        if (stop_compare) return ans;

        this._keys_in_range(ans, second_bucket_keys, second_bucket_cont_token, version);

        return ans;
    }


    /**
     * @param {{ keys_diff_map: any; keys_contents_left: any; keep_listing_second_bucket?: boolean; }} ans
     * @param {{}} second_bucket_keys
     */
    _keys_out_of_range(ans, second_bucket_keys) {
        const first_bucket_key_array = Object.keys(ans.keys_contents_left);
        const first_bucket_key_in_last_pos = first_bucket_key_array[first_bucket_key_array.length - 1];
        const second_bucket_key_array = Object.keys(second_bucket_keys);
        let stop_compare = true; //for readability

        // second bucket list is empty or all keys in first bucket are lexicographic smaller
        if (!Object.keys(second_bucket_key_array).length || first_bucket_key_in_last_pos < second_bucket_key_array[0]) {
            ans.keys_diff_map = { ...ans.keys_diff_map, ...ans.keys_contents_left };
            ans.keys_contents_left = {};
            return stop_compare;
        }

        stop_compare = false;
        return stop_compare;
    }

    /**
     * @param {{ keys_diff_map: any; keys_contents_left: any; keep_listing_second_bucket: boolean; }} ans
     * @param {{ [x: string]: any; }} second_bucket_keys
     * @param {string} second_bucket_cont_token
     * @param {boolean} version
     */
    _keys_in_range(ans, second_bucket_keys, second_bucket_cont_token, version) {
        const first_bucket_key_array = Object.keys(ans.keys_contents_left);
        const second_bucket_key_array = Object.keys(second_bucket_keys);
        const second_bucket_key_in_last_pos = second_bucket_key_array[first_bucket_key_array.length - 1];

        // Checking lexicographic order keys
        for (const cur_first_bucket_key of first_bucket_key_array) {

            // case 1:
            if (cur_first_bucket_key > second_bucket_key_in_last_pos) {
                this._keep_listing_or_return_ans(ans, second_bucket_cont_token);
                return ans;
            }

            const first_bucket_curr_obj = ans.keys_contents_left[cur_first_bucket_key];

            // case 2: 
            if (cur_first_bucket_key < second_bucket_key_array[0]) {
                this._populate_diff_map_and_omit_contents_left(ans, cur_first_bucket_key, first_bucket_curr_obj);
                continue;
            }

            // case 3: both key lists, from first bucket and from the second bucket are in the same range 
            const second_bucket_curr_obj = second_bucket_keys[cur_first_bucket_key];

            if (second_bucket_curr_obj) {
                // get the positions of the etag in the first bucket for the same key name.
                let should_continue;
                let etag_on_first_bucket = this._get_etag_pos(0, second_bucket_curr_obj, first_bucket_curr_obj);

                // no index, ETag is not in the first bucket:
                if (etag_on_first_bucket.length === 0) {
                    ({ should_continue, etag_on_first_bucket } = this._etag_not_in_first_bucket(
                        ans, etag_on_first_bucket, version, cur_first_bucket_key, first_bucket_curr_obj, second_bucket_curr_obj));
                    if (should_continue) continue;
                }

                // if there is one index then: 
                //     pos 0 Etag is on the latest, we just need to omit this key
                //     n > 0  all n are diff.    
                // if there is more then one index then:
                //     need to figure that out.... 
                if (etag_on_first_bucket.length === 1) {
                    if (etag_on_first_bucket[0] > 0) { // can happen only in version
                        const first_bucket_diff = first_bucket_curr_obj.slice(0, etag_on_first_bucket);
                        ans.keys_diff_map[cur_first_bucket_key] = first_bucket_diff;
                    } else {
                        dbg.log1('The same file with the same ETag found in both buckets on the latest versions', second_bucket_curr_obj);
                    }
                } else if (etag_on_first_bucket.length > 1) { // can happen only in version
                    // if all the etags are in a raw, treat them as one with the latest position
                    if (this._is_consecutive(etag_on_first_bucket)) {
                        if (etag_on_first_bucket[0] > 0) {
                            const first_bucket_diff = first_bucket_curr_obj.slice(0, etag_on_first_bucket[0]);
                            ans.keys_diff_map[cur_first_bucket_key] = first_bucket_diff;
                        } else {
                            dbg.log1('The same file with the same ETag found in both buckets on the latest versions', second_bucket_curr_obj);
                        }
                    } else {
                        //Gap: it will find only the first intersection and tread the rest as a diff, 
                        //     if there is multiple intersection it will not be true but it is a corner case. 
                        this._process_non_consecutive_etags(ans, etag_on_first_bucket, first_bucket_curr_obj, second_bucket_curr_obj);
                    }
                }
                ans.keys_contents_left = _.omit(ans.keys_contents_left, cur_first_bucket_key);
                continue;
            } else { //This is when there is no such key name on the second bucket in this range.
                this._populate_diff_map_and_omit_contents_left(ans, cur_first_bucket_key, first_bucket_curr_obj);
            }
        }
        return ans;
    }

    /**
     * @param {{ keys_diff_map: any; keys_contents_left: any; keep_listing_second_bucket: any; }} ans
     * @param {string} second_bucket_cont_token
     */
    _keep_listing_or_return_ans(ans, second_bucket_cont_token) {
        if (second_bucket_cont_token) {
            ans.keep_listing_second_bucket = true;
        } else {
            ans.keys_diff_map = { ...ans.keys_diff_map, ...ans.keys_contents_left };
            ans.keys_contents_left = {};
        }
        dbg.log1('_keep_listing_or_return_ans: ', second_bucket_cont_token, ans);
    }

    /**
     * @param {{ keys_diff_map: any; keys_contents_left: any; keep_listing_second_bucket?: boolean; }} ans
     * @param {string | any[]} etag_on_first_bucket
     * @param {boolean} version
     * @param {string} cur_first_bucket_key
     * @param {any[]} first_bucket_curr_obj
     * @param {any[]} second_bucket_curr_obj
     */
    _etag_not_in_first_bucket(ans, etag_on_first_bucket, version, cur_first_bucket_key, first_bucket_curr_obj, second_bucket_curr_obj) {
        let pos = 0;
        let should_continue = false;
        dbg.log1('ETag is not in the first bucket', etag_on_first_bucket);
        //     Version: someone wrote directly to the second bucket. need to drill down on the second bucket to see comparison. 
        //              or the first bucket have a list of max keys and all the keys versions there are newer (with different etag).
        if (version) {
            //We will need to drill down on the position of the etag in the second_bucket_curr_obj
            while (etag_on_first_bucket.length === 0 && pos < second_bucket_curr_obj.length) {
                pos += 1;
                etag_on_first_bucket = this._get_etag_pos(pos, second_bucket_curr_obj, first_bucket_curr_obj);
            }
            // If non of the ETags of this object exists in the first bucket all the versions are diff.
            if (etag_on_first_bucket.length === 0 && pos >= second_bucket_curr_obj.length) {
                this._populate_diff_map_and_omit_contents_left(ans, cur_first_bucket_key, first_bucket_curr_obj);
                should_continue = true;
            }
            return { should_continue, etag_on_first_bucket };
        } else { // in non version if the etag is not the same this is a diff.
            this._populate_diff_map_and_omit_contents_left(ans, cur_first_bucket_key, first_bucket_curr_obj);
            should_continue = true;
            return { should_continue, etag_on_first_bucket };
        }
    }

    _populate_diff_map_and_omit_contents_left(ans, cur_bucket_key, bucket_curr_obj) {
        ans.keys_diff_map[cur_bucket_key] = bucket_curr_obj;
        ans.keys_contents_left = _.omit(ans.keys_contents_left, cur_bucket_key);
    }

    /**
     * @param {number} pos
     * @param {any[]} first_obj
     * @param {any[]} second_obj
     */
    _get_etag_pos(pos, first_obj, second_obj) {
        // Getting the etag of first_obj is the position asked. 
        if (!first_obj[pos]) return [];
        const ETag = first_obj[pos].ETag;
        const target_pos = second_obj.reduce((indexes, obj, index) => {
            if (obj.ETag === ETag) {
                indexes.push(index);
            }
            return indexes;
        }, []);
        return target_pos;
    }

    /**
     * @param {number[]} array
     */
    _is_consecutive(array) {
        if (array.length <= 1) return true;
        // make sure that the array is ascending 
        array.sort(function(a, b) {
            return a - b;
        });
        for (let pos = 1; pos < array.length; pos++) {
            if (array[pos] !== array[pos - 1] + 1) {
                return false;
            }
        }
        return true;
    }

    _get_intersection_etags(first_array, second_array) {
        let intersection = [];
        for (let i = first_array.length - 1; i >= 0; i--) {
            const value = first_array[i].ETag;
            if (second_array.some(item => item.ETag === value)) {
                intersection.push(value);
            } else {
                break;
            }
        }
        intersection = intersection.reverse();
        return intersection;
    }

    /**
     * @param {{ keys_diff_map: any; keys_contents_left?: any; keep_listing_second_bucket?: boolean; }} ans
     * @param {any[]} etag_on_first_bucket
     * @param {any[]} first_bucket_curr_obj
     * @param {any[]} second_bucket_curr_obj
     */
    _process_non_consecutive_etags(ans, etag_on_first_bucket, first_bucket_curr_obj, second_bucket_curr_obj) {
        const first_bucket_etag_last_pos = etag_on_first_bucket.length - 1;
        // get the sliced list of the first bucket
        const first_bucket_sliced_obj = first_bucket_curr_obj.slice(0, etag_on_first_bucket[first_bucket_etag_last_pos] + 1);

        const etag_on_second_bucket = this._get_etag_pos(
            etag_on_first_bucket[0], first_bucket_curr_obj, second_bucket_curr_obj);
        // get the sliced list of the second bucket
        const second_bucket_etag_last_pos = etag_on_second_bucket.length - 1;
        const second_bucket_sliced_obj = second_bucket_curr_obj.slice(0, etag_on_second_bucket[second_bucket_etag_last_pos] + 1);
        const intersection = this._get_intersection_etags(first_bucket_sliced_obj, second_bucket_sliced_obj);
        first_bucket_sliced_obj.length -= intersection.length;
        if (first_bucket_sliced_obj.length) {
            const cur_first_bucket_key = first_bucket_sliced_obj[0].Key;
            ans.keys_diff_map[cur_first_bucket_key] = first_bucket_sliced_obj;
        }
    }

}
exports.BucketDiff = BucketDiff;
