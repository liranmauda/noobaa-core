/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const P = require('../../util/promise');
const promise_utils = require('../../util/promise_utils');
var mocha = require('mocha');
// var assert = require('assert');
var Prefetch = require('../../util/prefetch');

function log(...args) {
    if (process.env.SUPPRESS_LOGS) return;
    console.log(...args);
}


mocha.describe('prefetch', function() {

    mocha.it('should work', function() {
        var pr = {};
        return P.fcall(function() {
                var id = 0;
                pr = new Prefetch({
                    low_length: 30,
                    high_length: 32,
                    load: function(count) {
                        var n = count;
                        log('... LOAD', n, '(' + count + ')', 'length', pr.length);
                        return promise_utils.delay(5).then(function() {
                            log('>>> LOAD', n, '(' + count + ')', 'length', pr.length);
                            return _.times(n, function() {
                                id += 1;
                                return id;
                            });
                        });
                    }
                });
            })
            .then(() => promise_utils.delay(10))
            .then(function() {
                log('A - length', pr.length);
                var promise = Promise.resolve();
                _.times(10, function() {
                    promise = promise.then(() => promise_utils.delay(0)).then(function() {
                        return pr.fetch(2).then(function(res) {
                            log('A - fetch', res, 'length', pr.length);
                        });
                    });
                });
                return promise;
            })
            .then(() => promise_utils.delay(10))
            .then(function() {
                log('B - length', pr.length);
                return P.all(_.times(10, function() {
                    return pr.fetch(2).then(function(res) {
                        log('B - fetch', res, 'length', pr.length);
                    });
                }));
            })
            .then(() => promise_utils.delay(10))
            .then(function() {
                log('length', pr.length);
            });
    });

});
