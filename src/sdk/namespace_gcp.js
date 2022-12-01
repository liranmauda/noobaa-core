/* Copyright (C) 2016 NooBaa */
'use strict';

// const { TOTAL_TIMEOUT_DEFAULT } = require('@google-cloud/storage/build/src/storage');
const _ = require('lodash');
const util = require('util');

const P = require('../util/promise');
const dbg = require('../util/debug_module')(__filename);
// const stream_utils = require('../util/stream_utils');
// const s3_utils = require('../endpoint/s3/s3_utils');
// const cloud_utils = require('../util/cloud_utils');
// const blob_translator = require('./blob_translator');
// const stats_collector = require('./endpoint_stats_collector');
// const config = require('../../config');
const GoogleCloudStorage = require('../util/google_storage_wrap'); //LMLM why do we what to use the wrap and not directly @google-cloud/storage ? 

/**
 * @implements {nb.Namespace}
 */
class NamespaceGCP {


    constructor({ namespace_resource_id, rpc_client, project_id, target_bucket, client_email, private_key, access_mode }) {
        this.namespace_resource_id = namespace_resource_id;
        this.project_id = project_id;
        this.client_email = client_email;
        this.private_key = private_key;
        //gcs stands for google cloud storage
        this.gcs = new GoogleCloudStorage({
            projectId: this.project_id,
            credentials: {
                client_email: this.client_email,
                private_key: this.private_key,
            }
        });
        this.bucket = target_bucket;
        this.rpc_client = rpc_client;
        this.access_mode = access_mode;
    }

    get_write_resource() {
        return this;
    }

    is_server_side_copy(other, params) {
        //LMLM what is the case here, what determine server side copy? 
        return other instanceof NamespaceGCP &&
            this.private_key === other.private_key &&
            this.client_email === other.client_email;
    }

    get_bucket() {
        return this.bucket;
    }

    is_readonly_namespace() {
        if (this.access_mode && this.access_mode === 'READ_ONLY') {
            return true;
        }
        return false;
    }


    //     /////////////////
    //     // OBJECT LIST //
    //     /////////////////

    async list_objects(params, object_sdk) {
        // https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix
        // https://googleapis.dev/nodejs/storage/latest/Bucket.html#getFiles
        dbg.log0('LMLM: NamespaceGCP.list_objects:', this.bucket, inspect(params)); //remove the LMLM

        const options = {
            prefix: params.prefix,
            autoPaginate: false, //LMLM do we want to disable auto pagination and use maxResults as max keys? 
            maxResults: params.limit || 1000,
            pageToken: params.key_marker, //LMLM is this the correct way? 
        };

        if (params.delimiter) {
            options.delimiter = params.delimiter;
        }
        dbg.log0(`LMLM: NamespaceGCP.list_objects: options: ${inspect(options)}`); //LMLM remove this log
        const res = await this.gcs.bucket(this.bucket).getFiles(options);

        //         const res = await this.s3.listObjects({
        //             Bucket: this.bucket,
        //             Prefix: params.prefix,
        //             Delimiter: params.delimiter,
        //             Marker: params.key_marker,
        //             MaxKeys: params.limit,
        //         }).promise();

        dbg.log0('LMLM: NamespaceGCP.list_objects: bucket:', this.bucket, 'params:', inspect(params), 'list:', inspect(res)); //LMLM remove the LMLM

        //         return {
        //             objects: _.map(res.Contents, obj => this._get_s3_object_info(obj, params.bucket)),
        //             common_prefixes: _.map(res.CommonPrefixes, 'Prefix'),
        //             is_truncated: res.IsTruncated,
        //             next_marker: res.NextMarker,
        //         };
    }

    async list_uploads(params, object_sdk) {
        dbg.log0('NamespaceGCP.list_uploads:',
            this.bucket,
            inspect(params)
        );
        // TODO list uploads
        return {
            objects: [],
            common_prefixes: [],
            is_truncated: false,
        };
    }

    async list_object_versions(params, object_sdk) {
        dbg.log0('NamespaceGCP.list_object_versions:',
            this.bucket,
            inspect(params)
        );
        // TODO list object versions
        return {
            objects: [],
            common_prefixes: [],
            is_truncated: false,
        };
    }


    //     /////////////////
    //     // OBJECT READ //
    //     /////////////////

    //     _set_md_conditions(params, request) {
    //         if (params.md_conditions) {
    //             request.IfMatch = params.md_conditions.if_match_etag;
    //             request.IfNoneMatch = params.md_conditions.if_none_match_etag;
    //             if (params.md_conditions.if_modified_since) {
    //                 request.IfModifiedSince = new Date(params.md_conditions.if_modified_since);
    //             }
    //             if (params.md_conditions.if_unmodified_since) {
    //                 request.IfUnmodifiedSince = new Date(params.md_conditions.if_unmodified_since);
    //             }
    //         }
    //     }

    async read_object_md(params, object_sdk) {
        //         try {
        dbg.log0('NamespaceGCP.read_object_md:', this.bucket, inspect(params));
        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 PartNumber: params.part_number,
        //             };
        //             // If set, part_number is positive integer from 1 to 10000.
        //             // Usually part number is not provided and then we read a small "inline" range
        //             // to reduce the double latency for small objects.
        //             // can_use_get_inline - we shouldn't use inline get when part number exist or when heading a directory
        //             const can_use_get_inline = !params.part_number && !request.Key.endsWith('/');
        //             if (can_use_get_inline) {
        //                 request.Range = `bytes=0-${config.INLINE_MAX_SIZE - 1}`;
        //             }
        //             this._set_md_conditions(params, request);
        //             this._assign_encryption_to_request(params, request);
        //             let res;
        //             try {
        //                 res = can_use_get_inline ?
        //                     await this.s3.getObject(request).promise() :
        //                     await this.s3.headObject(request).promise();
        //             } catch (err) {
        //                 // catch invalid range error for objects of size 0 and trying head object instead
        //                 if (err.code !== 'InvalidRange') {
        //                     throw err;
        //                 }
        //                 res = await this.s3.headObject({ ...request, Range: undefined }).promise();
        //             }
        //             dbg.log0('NamespaceGCP.read_object_md:', this.bucket, inspect(params), 'res', inspect(res));
        //             return this._get_s3_object_info(res, params.bucket, params.part_number);
        //         } catch (err) {
        //             this._translate_error_code(params, err);
        //             dbg.warn('NamespaceGCP.read_object_md:', inspect(err));
        //             object_sdk.rpc_client.pool.update_issues_report({
        //                 namespace_resource_id: this.namespace_resource_id,
        //                 error_code: String(err.code),
        //                 time: Date.now(),
        //             });
        //             throw err;
        //         }
    }

    async read_object_stream(params, object_sdk) {
        dbg.log0('NamespaceGCP.read_object_stream:', this.bucket, inspect(_.omit(params, 'object_md.ns')));
        //         return new Promise((resolve, reject) => {
        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 Range: params.end ? `bytes=${params.start}-${params.end - 1}` : undefined,
        //                 PartNumber: params.part_number,
        //             };
        //             this._set_md_conditions(params, request);
        //             this._assign_encryption_to_request(params, request);
        //             const req = this.s3.getObject(request)
        //                 .on('error', err => {
        //                     this._translate_error_code(params, err);
        //                     dbg.warn('NamespaceGCP.read_object_stream:', inspect(err));
        //                     reject(err);
        //                 })
        //                 .on('httpHeaders', (statusCode, headers, res) => {
        //                     dbg.log0('NamespaceGCP.read_object_stream:',
        //                         this.bucket,
        //                         inspect(_.omit(params, 'object_md.ns')),
        //                         'statusCode', statusCode,
        //                         'headers', headers
        //                     );
        //                     if (statusCode >= 300) return; // will be handled by error event
        //                     req.removeListener('httpData', AWS.EventListeners.Core.HTTP_DATA);
        //                     req.removeListener('httpError', AWS.EventListeners.Core.HTTP_ERROR);
        //                     let count = 1;
        //                     const count_stream = stream_utils.get_tap_stream(data => {
        //                         stats_collector.instance(this.rpc_client).update_namespace_read_stats({
        //                             namespace_resource_id: this.namespace_resource_id,
        //                             bucket_name: params.bucket,
        //                             size: data.length,
        //                             count
        //                         });
        //                         // clear count for next updates
        //                         count = 0;
        //                     });
        //                     const read_stream = /** @type {import('stream').Readable} */
        //                         (res.httpResponse.createUnbufferedStream());
        //                     return resolve(read_stream.pipe(count_stream));
        //                 });
        //             req.send();
        //         });
    }


    //     ///////////////////
    //     // OBJECT UPLOAD //
    //     ///////////////////

    async upload_object(params, object_sdk) {
        dbg.log0('NamespaceGCP.upload_object:', this.bucket, inspect(params));
        //         let res;
        //         const Tagging = params.tagging && params.tagging.map(tag => tag.key + '=' + tag.value).join('&');
        //         if (params.copy_source) {
        //             const { copy_source, copy_source_range } = s3_utils.format_copy_source(params.copy_source);
        //             if (copy_source_range) {
        //                 // note that CopySourceRange is only supported by s3.uploadPartCopy()
        //                 throw new Error('NamespaceGCP.upload_object: CopySourceRange not supported by s3.copyObject()');
        //             }

        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 CopySource: copy_source,
        //                 ContentType: params.content_type,
        //                 Metadata: params.xattr,
        //                 MetadataDirective: params.xattr_copy ? 'COPY' : 'REPLACE',
        //                 Tagging,
        //                 TaggingDirective: params.tagging_copy ? 'COPY' : 'REPLACE',
        //             };

        //             this._assign_encryption_to_request(params, request);

        //             res = await this.s3.copyObject(request).promise();
        //         } else {
        //             let count = 1;
        //             const count_stream = stream_utils.get_tap_stream(data => {
        //                 stats_collector.instance(this.rpc_client).update_namespace_write_stats({
        //                     namespace_resource_id: this.namespace_resource_id,
        //                     bucket_name: params.bucket,
        //                     size: data.length,
        //                     count
        //                 });
        //                 // clear count for next updates
        //                 count = 0;
        //             });

        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 Body: params.source_stream.pipe(count_stream),
        //                 ContentLength: params.size,
        //                 ContentType: params.content_type,
        //                 ContentMD5: params.md5_b64,
        //                 Metadata: params.xattr,
        //                 Tagging,
        //             };

        //             this._assign_encryption_to_request(params, request);
        //             try {
        //                 res = await this.s3.putObject(request).promise();
        //             } catch (err) {
        //                 object_sdk.rpc_client.pool.update_issues_report({
        //                     namespace_resource_id: this.namespace_resource_id,
        //                     error_code: String(err.code),
        //                     time: Date.now(),
        //                 });
        //                 throw err;
        //             }
        //         }
        //         dbg.log0('NamespaceGCP.upload_object:', this.bucket, inspect(params), 'res', inspect(res));
        //         const etag = s3_utils.parse_etag(res.ETag);
        //         const last_modified_time = s3_utils.get_http_response_date(res);
        //         return { etag, version_id: res.VersionId, last_modified_time };
    }

    //     /////////////////////////////
    //     // OBJECT MULTIPART UPLOAD //
    //     /////////////////////////////

    async create_object_upload(params, object_sdk) {
        dbg.log0('NamespaceGCP.create_object_upload:', this.bucket, inspect(params));
        //         const Tagging = params.tagging && params.tagging.map(tag => tag.key + '=' + tag.value).join('&');
        //         const request = {
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             ContentType: params.content_type,
        //             Metadata: params.xattr,
        //             Tagging
        //         };
        //         this._assign_encryption_to_request(params, request);
        //         const res = await this.s3.createMultipartUpload(request).promise();

        //         dbg.log0('NamespaceGCP.create_object_upload:', this.bucket, inspect(params), 'res', inspect(res));
        //         return { obj_id: res.UploadId };
    }

    async upload_multipart(params, object_sdk) {
        dbg.log0('NamespaceGCP.upload_multipart:', this.bucket, inspect(params));
        //         let res;
        //         if (params.copy_source) {
        //             const { copy_source, copy_source_range } = s3_utils.format_copy_source(params.copy_source);
        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 UploadId: params.obj_id,
        //                 PartNumber: params.num,
        //                 CopySource: copy_source,
        //                 CopySourceRange: copy_source_range,
        //             };

        //             this._assign_encryption_to_request(params, request);

        //             res = await this.s3.uploadPartCopy(request).promise();
        //         } else {
        //             let count = 1;
        //             const count_stream = stream_utils.get_tap_stream(data => {
        //                 stats_collector.instance(this.rpc_client).update_namespace_write_stats({
        //                     namespace_resource_id: this.namespace_resource_id,
        //                     size: data.length,
        //                     count
        //                 });
        //                 // clear count for next updates
        //                 count = 0;
        //             });

        //             const request = {
        //                 Bucket: this.bucket,
        //                 Key: params.key,
        //                 UploadId: params.obj_id,
        //                 PartNumber: params.num,
        //                 Body: params.source_stream.pipe(count_stream),
        //                 ContentMD5: params.md5_b64,
        //                 ContentLength: params.size,
        //             };

        //             this._assign_encryption_to_request(params, request);
        //             try {
        //                 res = await this.s3.uploadPart(request).promise();
        //             } catch (err) {
        //                 object_sdk.rpc_client.pool.update_issues_report({
        //                     namespace_resource_id: this.namespace_resource_id,
        //                     error_code: String(err.code),
        //                     time: Date.now(),
        //                 });
        //                 throw err;
        //             }
        //         }
        //         dbg.log0('NamespaceGCP.upload_multipart:', this.bucket, inspect(params), 'res', inspect(res));
        //         const etag = s3_utils.parse_etag(res.ETag);
        //         return { etag };
    }

    async list_multiparts(params, object_sdk) {
        dbg.log0('NamespaceGCP.list_multiparts:', this.bucket, inspect(params));
        //         const res = await this.s3.listParts({
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             UploadId: params.obj_id,
        //             MaxParts: params.max,
        //             PartNumberMarker: params.num_marker,
        //         }).promise();

        dbg.log0('NamespaceGCP.list_multiparts:', this.bucket, inspect(params), 'res', inspect(res));
        //         return {
        //             is_truncated: res.IsTruncated,
        //             next_num_marker: res.NextPartNumberMarker,
        //             multiparts: _.map(res.Parts, p => ({
        //                 num: p.PartNumber,
        //                 size: p.Size,
        //                 etag: s3_utils.parse_etag(p.ETag),
        //                 last_modified: p.LastModified,
        //             }))
        //         };
    }

    async complete_object_upload(params, object_sdk) {
        dbg.log0('NamespaceGCP.complete_object_upload:', this.bucket, inspect(params));
        //         const res = await this.s3.completeMultipartUpload({
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             UploadId: params.obj_id,
        //             MultipartUpload: {
        //                 Parts: _.map(params.multiparts, p => ({
        //                     PartNumber: p.num,
        //                     ETag: `"${p.etag}"`,
        //                 }))
        //             }
        //         }).promise();

        dbg.log0('NamespaceGCP.complete_object_upload:', this.bucket, inspect(params), 'res', inspect(res));
        //         const etag = s3_utils.parse_etag(res.ETag);
        //         return { etag, version_id: res.VersionId };
    }

    async abort_object_upload(params, object_sdk) {
        dbg.log0('NamespaceGCP.abort_object_upload:', this.bucket, inspect(params));
        //         const res = await this.s3.abortMultipartUpload({
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             UploadId: params.obj_id,
        //         }).promise();

        dbg.log0('NamespaceGCP.abort_object_upload:', this.bucket, inspect(params), 'res', inspect(res));
    }

    //     //////////
    //     // ACLs //
    //     //////////

    async get_object_acl(params, object_sdk) {
        dbg.log0('NamespaceGCP.get_object_acl:', this.bucket, inspect(params));

        //         const res = await this.s3.getObjectAcl({
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             VersionId: params.version_id
        //         }).promise();

        //         dbg.log0('NamespaceGCP.get_object_acl:', this.bucket, inspect(params), 'res', inspect(res));

        //         return {
        //             owner: res.Owner,
        //             access_control_list: res.Grants
        //         };
    }

    async put_object_acl(params, object_sdk) {
        dbg.log0('NamespaceGCP.put_object_acl:', this.bucket, inspect(params));

        //         const res = await this.s3.putObjectAcl({
        //             Bucket: this.bucket,
        //             Key: params.key,
        //             VersionId: params.version_id,
        //             ACL: params.acl
        //         }).promise();

        //         dbg.log0('NamespaceGCP.put_object_acl:', this.bucket, inspect(params), 'res', inspect(res));
    }

    ///////////////////
    // OBJECT DELETE //
    ///////////////////

    async delete_object(params, object_sdk) {
        // https://googleapis.dev/nodejs/storage/latest/File.html#delete
        dbg.log0('NamespaceGCP.delete_object:', this.bucket, inspect(params));

        const res = await this.gcs.bucket(this.bucket).file(params.key).delete();

        dbg.log0('NamespaceGCP.delete_object:',
            this.bucket,
            inspect(params),
            'res', inspect(res)
        );

        return {};
    }

    async delete_multiple_objects(params, object_sdk) {
        // https://googleapis.dev/nodejs/storage/latest/File.html#delete
        dbg.log0('NamespaceGCP.delete_multiple_objects:', this.bucket, inspect(params));

        const res = await P.map_with_concurrency(10, params.objects, obj =>
            this.gcs.bucket(this.bucket).file(obj.key).delete()
            .then(() => ({}))
            .catch(err => ({ err_code: 'InternalError', err_message: err.message || 'InternalError' })));

        dbg.log0('NamespaceBlob.delete_multiple_objects:',
            this.bucket,
            inspect(params),
            'res', inspect(res)
        );

        return res;

    }


    ////////////////////
    // OBJECT TAGGING //
    ////////////////////

    async get_object_tagging(params, object_sdk) {
        throw new Error('TODO');
    }
    async delete_object_tagging(params, object_sdk) {
        throw new Error('TODO');
    }
    async put_object_tagging(params, object_sdk) {
        throw new Error('TODO');
    }

    ///////////////////
    //  OBJECT LOCK  //
    ///////////////////

    async get_object_legal_hold() {
        throw new Error('TODO');
    }
    async put_object_legal_hold() {
        throw new Error('TODO');
    }
    async get_object_retention() {
        throw new Error('TODO');
    }
    async put_object_retention() {
        throw new Error('TODO');
    }

    ///////////////////
    //      ULS      //
    ///////////////////

    async create_uls() {
        throw new Error('TODO');
    }
    async delete_uls() {
        throw new Error('TODO');
    }

    ///////////////
    // INTERNALS //
    ///////////////

    /**
     * 
     * @param {string} bucket 
     * @returns {nb.ObjectInfo}
     */
    _get_gcs_object_info(res, bucket) {
        // LMLM TODO...
        // const etag = s3_utils.parse_etag(res.ETag);
        // const xattr = _.extend(res.Metadata, {
        //     'noobaa-namespace-s3-bucket': this.bucket,
        // });
        // const ranges = res.ContentRange ? Number(res.ContentRange.split('/')[1]) : 0;
        // const size = ranges || res.ContentLength || res.Size || 0;
        // const last_modified_time = res.LastModified ? res.LastModified.getTime() : Date.now();
        // return {
        //     obj_id: res.UploadId || etag,
        //     bucket: bucket,
        //     key: res.Key,
        //     size,
        //     etag,
        //     create_time: last_modified_time,
        //     last_modified_time,
        //     version_id: res.VersionId,
        //     is_latest: res.IsLatest,
        //     delete_marker: res.DeleteMarker,
        //     content_type: res.ContentType,
        //     xattr,
        //     tag_count: res.TagCount,
        //     first_range_data: res.Body,
        //     num_multiparts: res.PartsCount,
        //     content_range: res.ContentRange,
        //     content_length: part_number ? res.ContentLength : size,
        //     encryption: undefined,
        //     lock_settings: undefined,
        //     md5_b64: undefined,
        //     num_parts: undefined,
        //     sha256_b64: undefined,
        //     stats: undefined,
        //     tagging: undefined,
        // };
    }

    //     _translate_error_code(params, err) {
    //         if (err.code === 'NoSuchKey') err.rpc_code = 'NO_SUCH_OBJECT';
    //         else if (err.code === 'NotFound') err.rpc_code = 'NO_SUCH_OBJECT';
    //         else if (err.code === 'InvalidRange') err.rpc_code = 'INVALID_RANGE';
    //         else if (params.md_conditions) {
    //             const md_conditions = params.md_conditions;
    //             if (err.code === 'PreconditionFailed') {
    //                 if (md_conditions.if_match_etag) err.rpc_code = 'IF_MATCH_ETAG';
    //                 else if (md_conditions.if_unmodified_since) err.rpc_code = 'IF_UNMODIFIED_SINCE';
    //             } else if (err.code === 'NotModified') {
    //                 if (md_conditions.if_modified_since) err.rpc_code = 'IF_MODIFIED_SINCE';
    //                 else if (md_conditions.if_none_match_etag) err.rpc_code = 'IF_NONE_MATCH_ETAG';
    //             }
    //         }
    //     }

    //     _assign_encryption_to_request(params, request) {
    //         if (params.copy_source && params.copy_source.encryption) {
    //             const { algorithm, key_b64 } = params.copy_source.encryption;
    //             request.CopySourceSSECustomerAlgorithm = algorithm;
    //             // TODO: There is a bug in the AWS S3 JS SDK that he encodes to base64 once again
    //             // This will generate an error of non correct key, this is why we decode and send as ascii string
    //             // Also the key_md5_b64 will be populated by the SDK.
    //             request.CopySourceSSECustomerKey = Buffer.from(key_b64, 'base64').toString('ascii');
    //         }

    //         if (params.encryption) {
    //             // TODO: How should we pass the context ('x-amz-server-side-encryption-context' var context_b64) if at all?
    //             const { algorithm, key_b64, kms_key_id } = params.encryption;
    //             if (key_b64) {
    //                 request.SSECustomerAlgorithm = algorithm;
    //                 // TODO: There is a bug in the AWS S3 JS SDK that he encodes to base64 once again
    //                 // This will generate an error of non correct key, this is why we decode and send as ascii string
    //                 // Also the key_md5_b64 will be populated by the SDK.
    //                 request.SSECustomerKey = Buffer.from(key_b64, 'base64').toString('ascii');
    //             } else {
    //                 request.ServerSideEncryption = algorithm;
    //                 request.SSEKMSKeyId = kms_key_id;
    //             }
    //         }
    //     }

}

function inspect(x) {
    return util.inspect(_.omit(x, 'source_stream'), true, 5, true);
}

module.exports = NamespaceGCP;
