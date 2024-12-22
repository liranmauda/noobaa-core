/* Copyright (C) 2016 NooBaa */
/*eslint max-lines-per-function: ["error", 550]*/
'use strict';

// setup coretest first to prepare the env
const coretest = require('./coretest');
coretest.setup({ pools_to_create: [coretest.POOL_LIST[1]] });

const system_store = require('../../server/system_services/system_store').get_instance();
const auth_server = require('../../server/common_services/auth_server');
const SensitiveString = require('../../util/sensitive_string');
const db_client = require('../../util/db_client').instance();
const P = require('../../util/promise');
const assert = require('assert');
const mocha = require('mocha');
const { S3 } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require("@smithy/node-http-handler");
const _ = require('lodash');
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const config = require('../../../config.js');
const { KeyRotator } = require('../../server/bg_services/key_rotator');

let s3;
let coretest_access_key;
let coretest_secret_key;
const BKT = `bucket.example`;
const key_rotator = new KeyRotator({ name: 'kr' });

config.MIN_CHUNK_AGE_FOR_DEDUP = 0;

/////////////// ROTATION & DISABLE & ENABLE TESTS /////////////////////////
mocha.describe('Rotation tests', function() {
    const { rpc_client, EMAIL, SYSTEM } = coretest;
    let accounts;
    let buckets;
    mocha.before(async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        const coretest_account = account_by_name(system_store.data.accounts, EMAIL);
        update_coretest_globals(coretest_account);
        s3 = configure_s3(coretest_access_key, coretest_secret_key);
        const pop = await populate_system(rpc_client);
        accounts = pop.accounts;
        buckets = pop.buckets;
    });
    mocha.after(async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await unpopulate_system(rpc_client, accounts, buckets);
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/active_root_key', 'key1');
        await key_rotator.run_batch();
        await system_store.load();
    });
    mocha.it('disable bucket master key test', async function() {
        const bucket = bucket_by_name(system_store.data.buckets, buckets[0].bucket_name);
        const db_chunks_before_dis = await db_client.collection('datachunks').find({ bucket: bucket._id, deleted: null });
        await rpc_client.system.disable_master_key({ entity: buckets[0].bucket_name, entity_type: 'BUCKET' });
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks_disabled(rpc_client, buckets[0].bucket_name, `key-${buckets[0].bucket_name}`, db_chunks_before_dis);
    });
    mocha.it('upload chunks to bucket - when bucket master key is disabled test', async function() {
        await put_object(buckets[0].bucket_name, 'key-decrypted-chunks-object', s3);
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks_disabled(rpc_client, buckets[0].bucket_name, 'key-decrypted-chunks-object');
    });
    mocha.it('enable bucket master key test', async function() {
        await rpc_client.system.enable_master_key({ entity: buckets[0].bucket_name, entity_type: 'BUCKET' });
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks(buckets[0].bucket_name, `key-${buckets[0].bucket_name}`, rpc_client);
    });
    mocha.it('upload chunks to bucket - when bucket master key is enabled test', async function() {
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks(buckets[0].bucket_name, 'key-decrypted-chunks-object', rpc_client);
        await delete_object(buckets[0].bucket_name, 'key-decrypted-chunks-object', s3);
    });
    mocha.it('disable account master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        const original_secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        await rpc_client.system.disable_master_key({ entity: EMAIL, entity_type: 'ACCOUNT' });
        await system_store.load();
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        await compare_secrets_disabled(secrets, system_store_account.master_key_id._id, original_secrets.system_store_secret);
        await P.all(_.map(system_store.data.pools, async pool => {
            if (!pool.cloud_pool_info) return;
            if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
            const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
            if (is_connection_is_account_s3_creds(
                    pool, 'pool', system_store_account._id, system_store_account.access_keys[0])) {
                // the pools secrets are still encrypted because they belong to different account
                compare_secrets(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
        await P.all(_.map(system_store.data.namespace_resources, async ns_resource => {
            if (!ns_resource.connection || ns_resource.connection.target_bucket.startsWith(BKT)) return;
            const ns_resources_secrets = await get_ns_resources_secrets_from_system_store_and_db(ns_resource);
            if (is_connection_is_account_s3_creds(
                    ns_resource, 'ns', system_store_account._id, system_store_account.access_keys[0])) {
                // the namespace resources secrets are still encrypted because they belong to different account
                compare_secrets(ns_resources_secrets.secrets, ns_resources_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('enable account master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        await rpc_client.system.enable_master_key({ entity: EMAIL, entity_type: 'ACCOUNT' });
        await system_store.load();
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);
        await P.all(_.map(system_store.data.pools, async pool => {
            if (!pool.cloud_pool_info) return;
            if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
            if (is_connection_is_account_s3_creds(
                    pool, 'pool', system_store_account._id, system_store_account.access_keys[0])) {
                const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
                // the pools secrets are still descrypted because they belong to different account
                compare_secrets(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
        await P.all(_.map(system_store.data.namespace_resources, async ns_resource => {
            if (!ns_resource.connection || ns_resource.connection.target_bucket.startsWith(BKT)) return;
            const ns_resources_secrets = await get_ns_resources_secrets_from_system_store_and_db(ns_resource);
            if (is_connection_is_account_s3_creds(
                    ns_resource, 'ns', system_store_account._id, system_store_account.access_keys[0])) {
                // the namespace resources secrets are still encrypted because they belong to different account
                compare_secrets(ns_resources_secrets.secrets, ns_resources_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('disable account master key test - account has pool', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        const original_secrets = await get_account_secrets_from_system_store_and_db(accounts[0].email, 's3_creds');
        await rpc_client.system.disable_master_key({
            entity: new SensitiveString(accounts[0].email),
            entity_type: 'ACCOUNT'
        });
        await system_store.load();
        const system_store_account = account_by_name(system_store.data.accounts, accounts[0].email);
        const secrets = await get_account_secrets_from_system_store_and_db(accounts[0].email, 's3_creds');
        await compare_secrets_disabled(secrets, system_store_account.master_key_id._id, original_secrets.system_store_secret);
        await P.all(_.map(system_store.data.pools, async pool => {
            if (!pool.cloud_pool_info || pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
            const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
            if (is_pool_of_account(pool, 'pool', system_store_account._id)) {
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            } else if (is_connection_is_account_s3_creds(
                    pool, 'pool', system_store_account._id, system_store_account.access_keys[0])) {
                compare_secrets(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
        await rpc_client.system.enable_master_key({
            entity: new SensitiveString(accounts[0].email),
            entity_type: 'ACCOUNT'
        });
    });
    mocha.it('disable account master key test - account has ns resource', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        const original_secrets = await get_account_secrets_from_system_store_and_db(accounts[2].email, 's3_creds');
        await rpc_client.system.disable_master_key({
            entity: new SensitiveString(accounts[2].email),
            entity_type: 'ACCOUNT'
        });
        await system_store.load();
        const system_store_account = account_by_name(system_store.data.accounts, accounts[2].email);
        const secrets = await get_account_secrets_from_system_store_and_db(accounts[2].email, 's3_creds');
        await compare_secrets_disabled(secrets, system_store_account.master_key_id._id, original_secrets.system_store_secret);
        await P.all(_.map(system_store.data.namespace_resources, async ns_resource => {
            if (!ns_resource.connection || ns_resource.connection.target_bucket.startsWith(BKT)) return;
            const ns_resources_secrets = await get_ns_resources_secrets_from_system_store_and_db(ns_resource);
            if (is_pool_of_account(ns_resource, 'ns', system_store_account._id)) {
                await compare_secrets_disabled(ns_resources_secrets.secrets, ns_resources_secrets.owner_account_master_key_id);
            } else if (is_connection_is_account_s3_creds(
                    ns_resource, 'ns', system_store_account._id, system_store_account.access_keys[0])) {
                compare_secrets(ns_resources_secrets.secrets, ns_resources_secrets.owner_account_master_key_id);
            }
        }));
        await rpc_client.system.enable_master_key({
            entity: new SensitiveString(accounts[2].email),
            entity_type: 'ACCOUNT'
        });
    });
    mocha.it('disable system master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        await rpc_client.system.disable_master_key({ entity: SYSTEM, entity_type: 'SYSTEM' });
        await system_store.load();
        const system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        await is_master_key_disabled(system_store_system.master_key_id._id, true);
        // check for each account master key is disabled and secrets are decrypted in db
        await P.all(_.map(system_store.data.accounts, async account => {
            if (!account.access_keys && account.email.unwrap() === "support@noobaa.com") {
                return;
            }
            const acc_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 's3_creds');
            await compare_secrets_disabled(acc_secrets, account.master_key_id._id);
            if (account.sync_credentials_cache && account.sync_credentials_cache.length > 0) {
                const cloud_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 'cloud_creds');
                await compare_secrets_disabled(cloud_secrets, account.master_key_id._id);
            }
        }));
        await P.all(_.map(system_store.data.pools, async pool => {
            if (pool.cloud_pool_info) {
                if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
                const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));

        await P.all(_.map(system_store.data.namespace_resources, async ns_resource => {
            if (!ns_resource.connection || ns_resource.connection.target_bucket.startsWith(BKT)) return;
            const ns_resources_secrets = await get_ns_resources_secrets_from_system_store_and_db(ns_resource);
            await compare_secrets_disabled(ns_resources_secrets.secrets, ns_resources_secrets.owner_account_master_key_id);
        }));
        await P.all(_.map(buckets, async bucket => {
            const sys_store_bucket = bucket_by_name(system_store.data.buckets, bucket.bucket_name);
            await is_master_key_disabled(sys_store_bucket.master_key_id._id, true);
            await build_chunks_of_bucket(rpc_client, bucket.bucket_name, SYSTEM);
            await compare_chunks_disabled(rpc_client, bucket.bucket_name, `key-${bucket.bucket_name}`);
        }));
    });
    mocha.it('create bucket after disable system master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        const bucket_name = 'bucket-after-disable-system';
        const key = 'object-after-disable-system';
        await s3.createBucket({ Bucket: bucket_name });
        const sys_store_bucket = bucket_by_name(system_store.data.buckets, bucket_name);
        await is_master_key_disabled(sys_store_bucket.master_key_id._id, true);
        await put_object(bucket_name, key, s3);
        await build_chunks_of_bucket(rpc_client, bucket_name, SYSTEM);
        await compare_chunks_disabled(rpc_client, bucket_name, key);
    });
    mocha.it('create account after disable system master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        const new_account_params = {
            has_login: false,
            s3_access: true,
            email: 'account-after-disable-ststem',
            name: 'account-after-disable-ststem'
        };
        const response_account = await rpc_client.account.create_account(new_account_params);
        const acc_secrets = await get_account_secrets_from_system_store_and_db('account-after-disable-ststem', 's3_creds');
        assert.strictEqual(response_account.access_keys[0].secret_key.unwrap(), acc_secrets.system_store_secret.unwrap());
        const account = account_by_name(system_store.data.accounts, 'account-after-disable-ststem');
        await compare_secrets_disabled(acc_secrets, account.master_key_id._id);
    });
    mocha.it('enable system master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        await rpc_client.system.enable_master_key({ entity: SYSTEM, entity_type: 'SYSTEM' });
        await system_store.load();
        const system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        await is_master_key_disabled(system_store_system.master_key_id._id, false);
        await P.all(_.map(system_store.data.accounts, async account => {
            if (!account.access_keys && account.email.unwrap() === "support@noobaa.com") {
                return;
            }
            const acc_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 's3_creds');
            await compare_secrets_disabled(acc_secrets, account.master_key_id._id);
            if (account.sync_credentials_cache && account.sync_credentials_cache[0]) { //account.email === 'coretest@noobaa.com'
                const cloud_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 'cloud_creds');
                await compare_secrets_disabled(cloud_secrets, account.master_key_id._id);
            }
            // when enabling the system master key, the accounts master keys are still disabled
            await is_master_key_disabled(account.master_key_id._id, true);
        }));
        await P.all(_.map(buckets, async bucket => {
            const sys_store_bucket = bucket_by_name(system_store.data.buckets, bucket.bucket_name);
            await is_master_key_disabled(sys_store_bucket.master_key_id._id, true);
            await build_chunks_of_bucket(rpc_client, bucket.bucket_name, SYSTEM);
            await compare_chunks_disabled(rpc_client, bucket.bucket_name, `key-${bucket.bucket_name}`);
        }));

        await P.all(_.map(system_store.data.pools, async pool => {
            if (pool.cloud_pool_info) {
                if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
                const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('enable bucket master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await rpc_client.system.enable_master_key({ entity: buckets[0].bucket_name, entity_type: 'BUCKET' });
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks(buckets[0].bucket_name, `key-${buckets[0].bucket_name}`, rpc_client);
    });

    mocha.it('rotate bucket master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        let system_store_bucket = bucket_by_name(system_store.data.buckets, buckets[0].bucket_name);
        const old_master_key_id = system_store_bucket.master_key_id;
        await rpc_client.system.rotate_master_key({ entity: buckets[0].bucket_name, entity_type: 'BUCKET' });
        await system_store.load();
        await build_chunks_of_bucket(rpc_client, buckets[0].bucket_name, SYSTEM);
        await compare_chunks(buckets[0].bucket_name, `key-${buckets[0].bucket_name}`, rpc_client);
        system_store_bucket = bucket_by_name(system_store.data.buckets, buckets[0].bucket_name);
        assert.notStrictEqual(old_master_key_id._id.toString(), system_store_bucket.master_key_id._id.toString());
    });

    mocha.it('enable account master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        await rpc_client.system.enable_master_key({ entity: EMAIL, entity_type: 'ACCOUNT' });
        await system_store.load();
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);

        await P.all(_.map(system_store.data.pools, async pool => {
            if (!pool.cloud_pool_info) return;
            if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
            const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
            if (is_connection_is_account_s3_creds(
                    pool, 'pool', system_store_account._id, system_store_account.access_keys[0])) {
                // the pools secrets are still decrypted because they belong to different account
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('rotate account master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        let system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const old_master_key_id = system_store_account.master_key_id._id;
        await system_store.load();
        await rpc_client.system.rotate_master_key({ entity: EMAIL, entity_type: 'ACCOUNT' });
        await system_store.load();
        system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        assert.notStrictEqual(old_master_key_id, system_store_account.master_key_id._id);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);

        await P.all(_.map(system_store.data.pools, async pool => {
            if (!pool.cloud_pool_info) return;
            if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
            if (is_connection_is_account_s3_creds(
                    pool, 'pool', system_store_account._id, system_store_account.access_keys[0])) {
                const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
                // the pools secrets are still decrypted because they belong to different account
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('rotate system master key test', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        // collect old data
        const old_accounts = [];
        const old_buckets = [];
        const old_pools = [];
        await P.all(_.map(system_store.data.accounts, async account => {
            if (!account.access_keys && account.email.unwrap() === "support@noobaa.com") {
                return;
            }
            const s3_creds = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 's3_creds');
            let cloud_creds;
            if (account.sync_credentials_cache && account.sync_credentials_cache[0]) {
                cloud_creds = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 'cloud_creds');
            }
            old_accounts.push({ email: account.email, s3_creds, cloud_creds, master_key: account.master_key_id });
        }));
        await P.all(_.map(buckets, async bucket => {
            const sys_store_bucket = bucket_by_name(system_store.data.buckets, bucket.bucket_name);
            old_buckets.push({ bucket_name: bucket.bucket_name, master_key: sys_store_bucket.master_key_id });
        }));
        await P.all(_.map(system_store.data.pools, async pool => {
            if (pool.cloud_pool_info) {
                if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
                const sys_account = account_by_id(system_store.data.accounts, pool.cloud_pool_info.access_keys.account_id._id);
                old_pools.push({ pool_name: pool.pool_name, master_key: sys_account.master_key_id });
            }
        }));
        let system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const old_system_master_key_id = system_store_system.master_key_id._id;
        await rpc_client.system.rotate_master_key({ entity: SYSTEM, entity_type: 'SYSTEM' });
        await system_store.load();
        system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        await is_master_key_disabled(system_store_system.master_key_id._id, false);
        assert.notStrictEqual(old_system_master_key_id.toString(), system_store_system.master_key_id._id.toString());
        await P.all(_.map(old_accounts, async account => {
            if (account.email.unwrap() === 'coretest@noobaa.com') {
                const acc_secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
                assert.strictEqual(account.s3_creds.system_store_secret.unwrap(), acc_secrets.system_store_secret.unwrap());
                assert.strictEqual(account.s3_creds.db_secret, acc_secrets.db_secret);
                assert.notStrictEqual(account.s3_creds.db_secret, account.s3_creds.system_store_secret.unwrap());
                await is_master_key_disabled(account.master_key._id, false);
                const sys_account = account_by_name(system_store.data.accounts, EMAIL);
                assert.notStrictEqual(account.master_key.cipher_key.toString('base64'),
                    sys_account.master_key_id.cipher_key.toString('base64'));
                compare_secrets(acc_secrets, sys_account.master_key_id._id);
                return;
            }
            const acc_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 's3_creds');
            assert.strictEqual(account.s3_creds.system_store_secret.unwrap(), acc_secrets.system_store_secret.unwrap());
            assert.strictEqual(account.s3_creds.db_secret, acc_secrets.db_secret);
            assert.strictEqual(account.s3_creds.db_secret, account.s3_creds.system_store_secret.unwrap());

            // the accounts master keys are still disabled
            await is_master_key_disabled(account.master_key._id, true);
            const sys_account = account_by_name(system_store.data.accounts, account.email.unwrap());
            assert.notStrictEqual(account.master_key.cipher_key.toString('base64'),
                sys_account.master_key_id.cipher_key.toString('base64'));
            await compare_secrets_disabled(acc_secrets, sys_account.master_key_id._id);

            if (account.sync_credentials_cache && account.sync_credentials_cache[0]) {
                const cloud_secrets = await get_account_secrets_from_system_store_and_db(account.email.unwrap(), 'cloud_creds');
                assert.strictEqual(account.cloud_secrets.system_store_secret.unwrap(), cloud_secrets.system_store_secret.unwrap());
                assert.strictEqual(account.cloud_secrets.db_secret, cloud_secrets.db_secret);
                assert.strictEqual(account.cloud_secrets.db_secret, account.cloud_secrets.system_store_secret.unwrap());
                await compare_secrets_disabled(cloud_secrets, account.master_key._id);
            }
        }));
        await P.all(_.map(old_buckets, async bucket => {
            const sys_store_bucket = bucket_by_name(system_store.data.buckets, bucket.bucket_name);
            const is_disabled = bucket.bucket_name !== buckets[0].bucket_name;
            await is_master_key_disabled(sys_store_bucket.master_key_id._id, is_disabled);
            assert.notStrictEqual(bucket.master_key.cipher_key.toString('base64'),
                sys_store_bucket.master_key_id.cipher_key.toString('base64'));
        }));
        await P.all(_.map(system_store.data.pools, async pool => {
            if (pool.cloud_pool_info) {
                if (pool.cloud_pool_info.target_bucket.startsWith(BKT)) return;
                const pools_secrets = await get_pools_secrets_from_system_store_and_db(pool);
                await compare_secrets_disabled(pools_secrets.secrets, pools_secrets.owner_account_master_key_id);
            }
        }));
    });

    mocha.it('test moving from ENV to files', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        // collect old data
        const old_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const master_key_id = old_system_store_system.master_key_id._id;
        process.env.NOOBAA_ROOT_SECRET = await fs.promises.readFile(config.ROOT_KEY_MOUNT + '/key1', 'utf8');
        await system_store.make_changes({
            update: {
                master_keys: [{
                    _id: master_key_id,
                    $set: {
                        master_key_id: '00000000aaaabbbbccccdddd',
                    },
                    $unset: { root_key_id: 1 }
                }]
            }
        });
        system_store.master_key_manager.is_initialized = false;
        system_store.master_key_manager.resolved_master_keys_by_id = {};
        await system_store.master_key_manager.load_root_key();
        await system_store.load();
        const old_cipher_key = old_system_store_system.master_key_id.cipher_key;
        const old_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        // restarting moving from env to files
        delete process.env.NOOBAA_ROOT_SECRET;
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/active_root_key', 'key1');
        system_store.master_key_manager.is_initialized = false;
        system_store.master_key_manager.resolved_master_keys_by_id = {};
        await system_store.master_key_manager.load_root_keys_from_mount();
        await system_store.load();
        const new_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const new_cipher_key = new_system_store_system.master_key_id.cipher_key;
        const new_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        await is_master_key_disabled(master_key_id, false);
        // encrypted keys be equal - as root key didn't change - just moved to file
        assert.strictEqual(old_cipher_key.toString(), new_cipher_key.toString());
        // decrypted keys be equal - if root secret was updated correctly
        assert.strictEqual(old_res_master_key.cipher_key.toString(), new_res_master_key.cipher_key.toString());
    });

    mocha.it('rotate root key test - validate key rotation', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        // collect old data
        const old_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const master_key_id = old_system_store_system.master_key_id._id;
        const old_cipher_key = old_system_store_system.master_key_id.cipher_key;
        const old_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        // rotating root key with new secret
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/key2', crypto.randomBytes(32).toString('base64'));
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/active_root_key', 'key2');
        await system_store.load();
        await key_rotator.run_batch();
        const new_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const new_cipher_key = new_system_store_system.master_key_id.cipher_key;
        const new_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        await is_master_key_disabled(master_key_id, false);
        // encrypted keys be unequal - if root secret was updated correctly
        assert.notStrictEqual(old_cipher_key.toString(), new_cipher_key.toString());
        // decrypted keys be equal - if root secret was updated correctly
        assert.strictEqual(old_res_master_key.cipher_key.toString(), new_res_master_key.cipher_key.toString());
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);
    });

    mocha.it('rotate root key test twice - validate no change', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        // collect old data
        const old_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const master_key_id = old_system_store_system.master_key_id._id;
        const old_cipher_key = old_system_store_system.master_key_id.cipher_key;
        const old_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        await system_store.load();
        await key_rotator.run_batch();
        const new_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const new_cipher_key = new_system_store_system.master_key_id.cipher_key;
        const new_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        await is_master_key_disabled(master_key_id, false);
        // encrypted keys be equal - as no change was made to active root-key
        assert.strictEqual(old_cipher_key.toString(), new_cipher_key.toString());
        // decrypted keys be equal - as always
        assert.strictEqual(old_res_master_key.cipher_key.toString(), new_res_master_key.cipher_key.toString());
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);
    });

    mocha.it('rotate root key test twice - validate key rotation as expected', async function() {
        this.timeout(600000); // eslint-disable-line no-invalid-this
        await system_store.load();
        // collect old data
        const old_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const master_key_id = old_system_store_system.master_key_id._id;
        const old_cipher_key = old_system_store_system.master_key_id.cipher_key;
        const old_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        // rotating root key with new secret
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/key3', crypto.randomBytes(32).toString('base64'));
        await fs.promises.writeFile(config.ROOT_KEY_MOUNT + '/active_root_key', 'key3');
        await system_store.load();
        await key_rotator.run_batch();
        const new_system_store_system = system_by_name(system_store.data.systems, SYSTEM);
        const new_cipher_key = new_system_store_system.master_key_id.cipher_key;
        const new_res_master_key = system_store.master_key_manager.resolved_master_keys_by_id[master_key_id];
        await is_master_key_disabled(master_key_id, false);
        // encrypted keys be unequal - if root secret was updated correctly
        assert.notStrictEqual(old_cipher_key.toString(), new_cipher_key.toString());
        // decrypted keys be equal - if root secret was updated correctly
        assert.strictEqual(old_res_master_key.cipher_key.toString(), new_res_master_key.cipher_key.toString());
        const system_store_account = account_by_name(system_store.data.accounts, EMAIL);
        const secrets = await get_account_secrets_from_system_store_and_db(EMAIL, 's3_creds');
        compare_secrets(secrets, system_store_account.master_key_id._id);
    });
});
// TODO:
// 1. add more tests for checking namespace resources
// 2. add tests for enable/disable account that has pool/namespace resource
////////////// HELPERS

async function put_object(bucket, key, s3_conf) {
    await s3_conf.putObject({
        Bucket: bucket,
        Key: key,
        Body: 'CHECKING CIPHER KEYS OF CHUNKS ON REGULAR UPLOADS',
        ContentType: 'text/plain'
    });
}

async function delete_object(bucket, key, s3_conf) {
    await s3_conf.deleteObject({
        Bucket: bucket,
        Key: key,
    });
}

function configure_s3(acc_key, sec_key) {
    return new S3({
        endpoint: coretest.get_http_address(),
        credentials: {
            accessKeyId: acc_key,
            secretAccessKey: sec_key,
        },
        forcePathStyle: true,
        region: config.DEFAULT_REGION,
        requestHandler: new NodeHttpHandler({
            httpAgent: new http.Agent({ keepAlive: false })
        }),
    });
}

function account_by_name(accounts, email) {
    return accounts.find(account => account.email.unwrap() === email);
}

function account_by_id(accounts, id) {
    return accounts.find(account => account._id.toString() === id.toString());
}

function system_by_name(systems, system) {
    return systems.find(sys => sys.name === system);
}

function pool_by_name(pools, pool_name) {
    return pools.find(pool => pool.name === pool_name);
}

function bucket_by_name(buckets, bucket_name) {
    return buckets.find(bucket => bucket.name.unwrap() === bucket_name);
}

async function check_master_key_in_db(master_key_id) {
    const db_account = await db_client.collection('master_keys').findOne({ _id: db_client.parse_object_id(master_key_id) });
    assert.ok(db_account);
}

function compare_secrets(secrets, master_key_id) {
    const { db_secret, system_store_secret, encrypt_and_compare_secret } = secrets;
    // 1. compare system store secret and original/response secret - should be equal
    if (encrypt_and_compare_secret) assert.strictEqual(system_store_secret.unwrap(), encrypt_and_compare_secret.unwrap());

    // 2. compare system store secret and db secret - should not be equal
    assert.notStrictEqual(system_store_secret.unwrap(), db_secret);

    // 3. encrypt the original/response secret
    const encrypted_secret_key = system_store.master_key_manager.encrypt_sensitive_string_with_master_key_id(
        system_store_secret, master_key_id);

    // 4. compare the encrypted original/response secret to db secret - should be equal
    assert.strictEqual(encrypted_secret_key.unwrap(), db_secret);
}

async function compare_chunks(bucket_name, key, rpc_client) {
    const db_bucket = await db_client.collection('buckets').findOne({ name: bucket_name });
    const db_chunks = await db_client.collection('datachunks').find({ bucket: db_bucket._id, deleted: null });
    const api_chunks = (await rpc_client.object.read_object_mapping_admin({ bucket: bucket_name, key })).chunks;
    await P.all(_.map(db_chunks, async db_chunk => {
        const chunk_api = api_chunks.find(api_chunk => api_chunk._id.toString() === db_chunk._id.toString());
        await check_master_key_in_db(db_chunk.master_key_id);
        assert.strictEqual(db_chunk.master_key_id.toString(), db_bucket.master_key_id.toString());
        assert.strictEqual(db_chunk.master_key_id.toString(), chunk_api.master_key_id.toString());
        const encrypted_secret_key = system_store.master_key_manager.encrypt_buffer_with_master_key_id(
            Buffer.from(chunk_api.cipher_key_b64, 'base64'), chunk_api.master_key_id);
        // check cipher key in db is encrypted by the father master key id
        assert.strictEqual(encrypted_secret_key.toString('base64'), db_chunk.cipher_key.toString('base64'));
    }));
}

function update_coretest_globals(coretest_account) {
    coretest_access_key = coretest_account.access_keys[0].access_key.unwrap();
    coretest_secret_key = coretest_account.access_keys[0].secret_key.unwrap();
}

async function populate_system(rpc_client) {
    const accounts = [];
    const buckets = [];

    const new_account_params = {
        has_login: false,
        s3_access: true,
    };
    const external_connection = {
        auth_method: 'AWS_V2',
        endpoint: coretest.get_http_address(),
        endpoint_type: 'S3_COMPATIBLE',
        name: 'conn1_rotate',
        identity: coretest_access_key || '123',
        secret: coretest_secret_key || 'abc',
    };

    // create buckets
    let i;
    for (i = 0; i < 5; i++) {
        await s3.createBucket({ Bucket: `rotate.bucket${i}` });
        buckets.push({ bucket_name: `rotate.bucket${i}` });
    }
    await s3.createBucket({ Bucket: `second.bucket` });
    await s3.createBucket({ Bucket: `third.bucket` });
    // create accounts
    for (i = 0; i < 10; i++) {
        const response_account = await rpc_client.account.create_account({
            ...new_account_params,
            email: `rotate_email${i}`,
            name: `rotate_name${i}`
        });
        accounts.push({ email: `rotate_email${i}`, create_account_result: response_account });
    }
    //create external connections
    await P.all(_.map(accounts, async cur_account => {
        await rpc_client.account.add_external_connection(external_connection, {
            auth_token: cur_account.create_account_result.token
        });
    }));
    // create pools
    await P.all(_.map(accounts.slice(0, 2), async cur_account => {
        const pool_name = `${cur_account.email}-cloud-pool`;
        await rpc_client.pool.create_cloud_pool({
            name: pool_name,
            connection: 'conn1_rotate',
            target_bucket: 'second.bucket',
        }, { auth_token: cur_account.create_account_result.token });
    }));
    // create namespace_resources
    await P.all(_.map(accounts.slice(2, 4), async cur_account => {
        const namespace_resource_name = `${cur_account.email}-namespace-resource`;
        await rpc_client.pool.create_namespace_resource({
            name: namespace_resource_name,
            connection: 'conn1_rotate',
            target_bucket: 'third.bucket'
        }, { auth_token: cur_account.create_account_result.token });
    }));
    // upload chunks to buckets
    await P.all(_.map(buckets, async cur_bucket => {
        const key = `key-${cur_bucket.bucket_name}`;
        await put_object(cur_bucket.bucket_name, key, s3);
    }));
    // upload to second.bucket too
    await put_object('second.bucket', 'key-second.bucket', s3);
    return { accounts, buckets };
}

async function compare_chunks_disabled(rpc_client, bucket_name, key, db_chunks_before_dis) {
    const db_bucket = await db_client.collection('buckets').findOne({ name: bucket_name });
    const db_master_key = await db_client.collection('master_keys').findOne({ _id: db_bucket.master_key_id });
    assert.ok(db_master_key.disabled === true);
    const db_chunks = await db_client.collection('datachunks').find({ bucket: db_bucket._id, deleted: null });
    const api_chunks = (await rpc_client.object.read_object_mapping_admin({ bucket: bucket_name, key })).chunks;
    await P.all(_.map(db_chunks, async db_chunk => {
        const chunk_api = api_chunks.find(api_chunk => api_chunk._id.toString() === db_chunk._id.toString());
        assert.strictEqual(db_chunk.master_key_id, undefined);
        assert.notStrictEqual(db_chunk.master_key_id, db_bucket.master_key_id.toString());
        assert.strictEqual(db_chunk.master_key_id, chunk_api.master_key_id);
        assert.strictEqual(chunk_api.cipher_key_b64, db_chunk.cipher_key.toString('base64'));
        if (db_chunks_before_dis) {
            assert.strictEqual(chunk_api._id.toString(), db_chunks_before_dis[0]._id.toString());
            assert.notStrictEqual(chunk_api.cipher_key_b64, db_chunks_before_dis[0].cipher_key.toString('base64'));
        }

    }));

}

async function compare_secrets_disabled(secrets, master_key_id, original_system_store_secret) {
    const db_master_key = await db_client.collection('master_keys').findOne({ _id: master_key_id });
    const { db_secret, system_store_secret } = secrets;
    assert.ok(db_master_key.disabled === true);
    assert.strictEqual(system_store_secret.unwrap(), db_secret);
    if (original_system_store_secret) assert.strictEqual(original_system_store_secret.unwrap(), db_secret);
    return true;
}

async function build_chunks_of_bucket(rpc_client, bucket_name, system_name) {

    const bucket = bucket_by_name(system_store.data.buckets, bucket_name);
    const db_chunks = await db_client.collection('datachunks').find({ bucket: bucket._id, deleted: null });
    const chunk_ids = _.map(db_chunks, chunk => chunk._id);
    const db_system = await db_client.collection('systems').findOne({ name: system_name });

    await rpc_client.scrubber.build_chunks({ chunk_ids }, {
        auth_token: auth_server.make_auth_token({
            system_id: db_system._id,
            role: 'admin'
        })
    });
}

async function get_account_secrets_from_system_store_and_db(email_address, type) {
    const db_account = await db_client.collection('accounts').findOne({ email: email_address });
    const system_store_account = account_by_name(system_store.data.accounts, email_address);
    let secrets;
    switch (type) {
        case 's3_creds':
            secrets = {
                db_secret: db_account.access_keys[0].secret_key,
                system_store_secret: system_store_account.access_keys[0].secret_key,
                //encrypt_and_compare_secret: system_store_account.access_keys[0].secret_key
            };
            break;
        case 'cloud_creds':
            secrets = {
                db_secret: db_account.sync_credentials_cache[0].secret_key,
                system_store_secret: system_store_account.sync_credentials_cache[0].secret_key,
            };
            break;
        default:
            throw Error('Invalid secrets type');
    }
    return secrets;
}

async function get_pools_secrets_from_system_store_and_db(system_store_pool) {
    const sys_account = account_by_id(system_store.data.accounts, system_store_pool.cloud_pool_info.access_keys.account_id._id);
    const db_pool = await db_client.collection('pools').findOne({ _id: system_store_pool._id });
    return {
        secrets: {
            db_secret: db_pool.cloud_pool_info.access_keys.secret_key,
            system_store_secret: system_store_pool.cloud_pool_info.access_keys.secret_key,
        },
        owner_account_master_key_id: sys_account.master_key_id._id
    };
}

async function get_ns_resources_secrets_from_system_store_and_db(system_store_ns_resources) {
    const sys_account = account_by_id(system_store.data.accounts, system_store_ns_resources.account._id);
    const db_ns_resource = await db_client.collection('namespace_resources').findOne({ _id: system_store_ns_resources._id });
    const ans = {
        secrets: {
            db_secret: db_ns_resource.connection.secret_key,
            system_store_secret: system_store_ns_resources.connection.secret_key,
        },
        owner_account_master_key_id: sys_account.master_key_id._id
    };
    return ans;

}

async function is_master_key_disabled(master_key_id, disabled_value) {
    const db_master_key = await db_client.collection('master_keys').findOne({ _id: master_key_id });
    assert.ok(db_master_key.disabled === disabled_value);
}

function is_connection_is_account_s3_creds(pool_or_ns_obj, pool_or_ns, account_id, account_s3_creds) {
    let ans;
    if (pool_or_ns === 'pool') {
        ans = pool_or_ns_obj.cloud_pool_info &&
            //pool_or_ns_obj.cloud_pool_info.access_keys.account_id._id === account_id &&
            pool_or_ns_obj.cloud_pool_info.access_keys.access_key.unwrap() === account_s3_creds.access_key.unwrap();
    }
    if (pool_or_ns === 'ns') {
        ans = pool_or_ns_obj.connection &&
            //pool_or_ns_obj.account._id === account_id &&
            pool_or_ns_obj.connection.access_key.unwrap() === account_s3_creds.access_key.unwrap();
    }
    return ans;
}

function is_pool_of_account(pool_or_ns_obj, pool_or_ns, account_id, account_s3_creds) {
    let ans;
    if (pool_or_ns === 'pool') {
        ans = pool_or_ns_obj.cloud_pool_info &&
            pool_or_ns_obj.cloud_pool_info.access_keys.account_id._id === account_id;
    }
    if (pool_or_ns === 'ns') {
        ans = pool_or_ns_obj.connection &&
            pool_or_ns_obj.account._id === account_id;
    }
    return ans;
}

async function unpopulate_system(rpc_client, accounts, buckets) {
    // delete objects from all buckets
    await P.all(_.map(buckets, async cur_bucket => {
        const key = `key-${cur_bucket.bucket_name}`;
        await delete_object(cur_bucket.bucket_name, key, s3);
    }));
    // delete from second.bucket too
    await delete_object('second.bucket', 'key-second.bucket', s3);

    // delete buckets
    let i;
    for (i = 0; i < 5; i++) {
        await s3.deleteBucket({ Bucket: `rotate.bucket${i}` });
    }
    await s3.deleteBucket({ Bucket: 'second.bucket' });
    await s3.deleteBucket({ Bucket: 'third.bucket' });
    // delete namespace_resources
    await P.all(_.map(accounts.slice(2, 4), async cur_account => {
        const namespace_resource_name = `${cur_account.email}-namespace-resource`;
        await rpc_client.pool.delete_namespace_resource({
            name: namespace_resource_name
        }, { auth_token: cur_account.create_account_result.token });
    }));

    // Prevent accounts from preventing pool deletions (by using a pool as default resource)
    // by disabling s3 access for all accounts.
    await P.all(_.map(accounts, async account => {
        await rpc_client.account.update_account_s3_access({
            email: account.email,
            s3_access: false
        });
    }));

    // delete pools
    await P.all(_.map(accounts.slice(0, 2), async cur_account => {
        const pool_name = `${cur_account.email}-cloud-pool`;
        await delete_pool_from_db(rpc_client, pool_name);
    }));

    //delete accounts
    await P.all(_.map(accounts, async account => {
        await rpc_client.account.delete_account({ email: account.email });
    }));
}

async function delete_pool_from_db(rpc_client, pool_name) {
    await rpc_client.pool.delete_pool({ name: pool_name });
    const pool = pool_by_name(system_store.data.pools, pool_name);
    await system_store.make_changes({
        remove: {
            pools: [pool._id]
        }
    });
}
