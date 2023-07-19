/* Copyright (C) 2016 NooBaa */
/* eslint-disable no-undef */
'use strict';

const https = require('https');

const ssl_utils = require('../../../util/ssl_utils');
const nb_native = require('../../../util/nb_native');

describe('ssl_utils', function() {

    const x509 = nb_native().x509;
    const x509_verify = nb_native().x509_verify;

    describe('nb_native().x509()', function() {

        it('should generate cert', function() {
            const x = x509();
            check_cert(x);

            const res = x509_verify(x);
            expect(typeof res).toBe('object');
            expect(res.owner).toEqual(res.issuer);
            expect(typeof res.owner).toBe('object');
            expect(res.owner.CN).toBe('selfsigned.noobaa.io');
        });

        it('should generate self-signed https cert', async function() {
            await test_https_with_cert(x509({ dns: 'localhost' }));
            await test_https_with_cert(x509({ dns: 'localhost', days: 1 }));
        });

        it('should generate unauthorized https cert due to expired date', async function() {
            try {
                await test_https_with_cert(x509({ dns: 'localhost', days: -1 }));
                fail('did not detect expired certificate');
            } catch (err) {
                expect(err.message).toBe('certificate has expired');
            }

            // but should work as unauthorized cert
            await test_https_with_cert(x509({ dns: 'localhost', days: -1 }), { rejectUnauthorized: false });
        });

        it('should generate unauthorized https cert due to DNS mismatch', async function() {
            try {
                await test_https_with_cert(x509());
                fail('did not detect DNS mismatch');
            } catch (err) {
                expect(err.message).toBe(
                    'Hostname/IP does not match certificate\'s altnames: Host: localhost. ' +
                    'is not in the cert\'s altnames: DNS:selfsigned.noobaa.io'
                );
            }

            // but should work as unauthorized cert
            await test_https_with_cert(x509(), { rejectUnauthorized: false });
        });

    });

    describe('generate_ssl_certificate', function() {

        it('should generate a valid cert', function() {
            const x = ssl_utils.generate_ssl_certificate();
            check_cert(x);

            const res = x509_verify(x);
            expect(typeof res).toBe('object');
            expect(res.owner).toEqual(res.issuer);
            expect(typeof res.owner).toBe('object');
            expect(res.owner.CN).toBe('selfsigned.noobaa.io');
        });

        it('should detect invalid key', function() {
            const x = ssl_utils.generate_ssl_certificate();
            check_cert(x);

            // update the key to be invalid
            x.key = x.key.slice(0, 500) + '!' + x.key.slice(501);

            expect(() => ssl_utils.verify_ssl_certificate(x)).toThrow();
            expect(() => x509_verify(x)).toThrow();
        });

        it('should detect invalid cert', function() {
            const x = ssl_utils.generate_ssl_certificate();
            check_cert(x);

            // update the cert to be invalid
            x.cert = x.cert.slice(0, 500) + '!' + x.cert.slice(501);

            expect(() => ssl_utils.verify_ssl_certificate(x)).toThrow();
            expect(() => x509_verify(x)).toThrow();
        });

        it('should detect mismatch key cert', function() {
            const x = ssl_utils.generate_ssl_certificate();
            check_cert(x);

            const other = ssl_utils.generate_ssl_certificate();
            check_cert(other);

            // replace the key with another valid key
            x.key = other.key;

            expect(() => ssl_utils.verify_ssl_certificate(x)).toThrow();
            expect(() => x509_verify(x)).toThrow();
        });

    });

    function check_cert(x) {
        expect(typeof x.key).toBe('string');
        expect(typeof x.cert).toBe('string');
        const key_lines = x.key.trim().split('\n');
        const cert_lines = x.cert.trim().split('\n');
        expect(key_lines[0]).toBe('-----BEGIN PRIVATE KEY-----');
        expect(key_lines[key_lines.length - 1]).toBe('-----END PRIVATE KEY-----');
        expect(cert_lines[0]).toBe('-----BEGIN CERTIFICATE-----');
        expect(cert_lines[cert_lines.length - 1]).toBe('-----END CERTIFICATE-----');
        ssl_utils.verify_ssl_certificate(x);
    }

    async function test_https_with_cert(ssl_cert, { rejectUnauthorized = true } = {}) {
        const server = https.createServer({ ...ssl_cert, honorCipherOrder: true });
        try {
            await new Promise((resolve, reject) => {
                server.on('error', reject);
                server.on('request', (req, res) => {
                    req.on('data', d => d);
                    res.end(JSON.stringify(req.headers, null, 4));
                });
                server.listen(resolve);
            });
            const { port } = server.address();
            await new Promise((resolve, reject) => {
                const req = https.request({
                    method: 'GET',
                    port,
                    ca: ssl_cert.cert,
                    rejectUnauthorized,
                    timeout: 1000,
                });
                req.on('error', reject);
                req.on('response', res => {
                    res.on('data', d => d);
                    res.on('error', reject);
                    res.on('end', resolve);
                });
                req.end();
            });
        } finally {
            server.close();
        }
    }

});
