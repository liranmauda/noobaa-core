/* Copyright (C) 2024 NooBaa */
'use strict';

const { ObjectId } = require('../util/objectId_utils');

/**
 * generate_id will generate an id that we use to identify entities (such as account, bucket, etc.). 
 */
// TODO: 
// - reuse this function in NC NSFS
// - this function implantation should be db_client.new_object_id(), 
//   but to align with manage nsfs we won't change it now
function generate_id() {
    return new ObjectId();
}

/**
 * check_root_account_owns_user checks if an account is owned by root account
 * @param {object} root_account
 * @param {object} account
 */
function check_root_account_owns_user(root_account, account) {
    if (account.owner === undefined) return false;
    return root_account._id === account.owner;
}

/**
 * @returns {boolean} true if the current environment is a NooBaa non containerized environment
 */
function is_nc_environment() {
    return process.env.NC_NSFS_NO_DB_ENV && process.env.NC_NSFS_NO_DB_ENV === 'true';
}

// EXPORTS
exports.generate_id = generate_id;
exports.check_root_account_owns_user = check_root_account_owns_user;
exports.is_nc_environment = is_nc_environment;
