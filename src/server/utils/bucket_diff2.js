/* Copyright (C) 2023 NooBaa */
'use strict';

const minimist = require('minimist');

const dbg = require('../../util/debug_module')(__filename);
if (!dbg.get_process_name()) dbg.set_process_name('bucket_diff');
dbg.original_console();

const HELP = `
Help:

    LMLM TODO
`;

const USAGE = `
Usage:

    LMLM TODO
`;

const ARGUMENTS = `
Arguments:

    LMLM TODO
`;

const OPTIONS = `
Options:

    LMLM TODO
`;

const WARNINGS = `
WARNING:

    LMLM TODO
`;

function print_usage() {
    console.warn(HELP);
    console.warn(USAGE.trimStart());
    console.warn(ARGUMENTS.trimStart());
    console.warn(OPTIONS.trimStart());
    console.warn(WARNINGS.trimStart());
    process.exit(1);
}

async function main(argv = minimist(process.argv.slice(2))) {
    try {
        if (argv.help || argv.h) return print_usage();
        if (argv.debug) {
            const debug_level = Number(argv.debug) || 0;
            dbg.set_module_level(debug_level, 'core');
        }

        dbg.warn(WARNINGS);
        dbg.log('bucket_diff: setting up ...', argv);


    } catch (err) {
        dbg.error('bucket_diff: exit on error', err.stack || err);
        process.exit(2);
    }
}


//LMLM should we even check:  _is_bucket_exist(){} ? 

exports.main = main;

if (require.main === module) main();
