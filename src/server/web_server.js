/* Copyright (C) 2016 NooBaa */
'use strict';

/** @typedef {import('http').IncomingMessage} HttpIncomingMessage */
/** @typedef {import('http').ServerResponse} HttpServerResponse */
/** @typedef {import('fs').Stats} FsStats */
/** @typedef {(req: HttpIncomingMessage, res: HttpServerResponse) => void} HttpDefaultHandler */

// load .env file before any other modules so that it will contain
// all the arguments even when the modules are loading.
require('../util/dotenv').load();
require('../util/panic');
require('../util/fips');

const dbg = require('../util/debug_module')(__filename);
if (!dbg.get_process_name()) dbg.set_process_name('WebServer');
const debug_config = require('../util/debug_config');

const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const mime = require('mime-types');
const http_request_logger = require('../util/http_request_logger');
const P = require('../util/promise');
const ssl_utils = require('../util/ssl_utils');
const pkg = require('../../package.json');
const config = require('../../config.js');
const license_info = require('./license_info');
const db_client = require('../util/db_client');
const system_store = require('./system_services/system_store').get_instance();
const prom_reporting = require('./analytic_services/prometheus_reporting');
const account_server = require('./system_services/account_server');
const stats_aggregator = require('./system_services/stats_aggregator');
const addr_utils = require('../util/addr_utils');
const kube_utils = require('../util/kube_utils');
const http_utils = require('../util/http_utils');
const server_rpc = require('./server_rpc');
const node_server = require('./node_services/node_server');

const rootdir = path.join(__dirname, '..', '..');
const dev_mode = (process.env.DEV_MODE === 'true');
const http_port = process.env.PORT || '5001';
const https_port = process.env.SSL_PORT || '5443';

async function main() {
    try {

        if (process.env.NOOBAA_LOG_LEVEL) {
            const dbg_conf = debug_config.get_debug_config(process.env.NOOBAA_LOG_LEVEL);
            dbg_conf.core.map(module => dbg.set_module_level(dbg_conf.level, module));
        }


        //Set KeepAlive to all http/https agents in webserver
        http_utils.update_http_agents({ keepAlive: true });
        http_utils.update_https_agents({ keepAlive: true });

        server_rpc.register_system_services();
        server_rpc.register_node_services();
        server_rpc.register_object_services();
        server_rpc.register_common_services();
        server_rpc.rpc.router.default = 'fcall://fcall';

        // Default HTTP handler for non-/rpc/ routes (/version, /public, metrics, oauth).
        const web_handler = create_web_server_handler();

        process.env.PORT = http_port;
        process.env.SSL_PORT = https_port;

        system_store.once('load', async () => {
            await account_server.ensure_support_account();
            if (process.env.CREATE_SYS_NAME && process.env.CREATE_SYS_EMAIL &&
                system_store.data.systems.length === 0) {
                dbg.log0(`creating system for kubernetes: ${process.env.CREATE_SYS_NAME}. email: ${process.env.CREATE_SYS_EMAIL}`);
                await server_rpc.client.system.create_system({
                    name: process.env.CREATE_SYS_NAME,
                    email: process.env.CREATE_SYS_EMAIL,
                    password: process.env.CREATE_SYS_PASSWD || 'DeMo1',
                    must_change_password: true
                });
            }
        });

        await db_client.instance().connect();

        // we register the rpc before listening on the port
        // in order for the rpc services to be ready immediately
        // with the http services like /version
        const http_server = http.createServer();
        server_rpc.rpc.register_http_transport(http_server, web_handler);
        server_rpc.rpc.register_ws_transport(http_server);
        await P.ninvoke(http_server, 'listen', http_port);

        const ssl_cert_info = await ssl_utils.get_ssl_cert_info('MGMT');
        const ssl_options = { ...ssl_cert_info.cert, honorCipherOrder: true };
        ssl_utils.apply_tls_config(ssl_options, 'MGMT');
        const https_server = https.createServer(ssl_options);
        server_rpc.rpc.register_http_transport(https_server, web_handler);
        ssl_cert_info.on('update', updated_cert_info => {
            dbg.log0("Setting updated MGMT ssl certs for web server.");
            const updated_ssl_options = { ...updated_cert_info.cert, honorCipherOrder: true };
            ssl_utils.apply_tls_config(updated_ssl_options, 'MGMT');
            https_server.setSecureContext(updated_ssl_options);
        });
        server_rpc.rpc.register_ws_transport(https_server);
        await P.ninvoke(https_server, 'listen', https_port);

        // Try to start the metrics server.
        await prom_reporting.start_server(config.WS_METRICS_SERVER_PORT);

        dbg.log0('WebServer waiting for SystemStore load...');
        await system_store.wait_for_load();
        dbg.log0('WebServer SystemStore loaded, starting node monitor');
        await node_server.start_monitor();

    } catch (err) {
        dbg.error('Web Server FAILED TO START', err.stack || err);
        process.exit(1);
    }
}

/**
 * Creates the default HTTP handler for non-RPC routes.
 * @returns {HttpDefaultHandler}
 */
function create_web_server_handler() {
    const middlewares = [
        http_request_logger(dev_mode ? 'dev' : 'combined'),
        https_redirect_handler,
        route_dispatcher,
        error_404,
    ];
    return (req, res) => run_middleware_chain(req, res, middlewares);
}

/**
 * Runs a connect-style middleware chain.
 * @param {HttpIncomingMessage} req
 * @param {HttpServerResponse} res
 * @param {Array<Function>} middlewares
 * @param {number} [index]
 */
function run_middleware_chain(req, res, middlewares, index = 0) {
    if (res.writableEnded || res.headersSent) return;
    const middleware = middlewares[index];
    if (!middleware) return;

    const next = err => {
        if (err) return error_handler(err, req, res);
        run_middleware_chain(req, res, middlewares, index + 1);
    };

    try {
        middleware(req, res, next);
    } catch (err) {
        error_handler(err, req, res);
    }
}

/**
 * Dispatches non-RPC HTTP routes.
 * @param {HttpIncomingMessage} req
 * @param {HttpServerResponse} res
 * @param {Function} next
 */
function route_dispatcher(req, res, next) {
    const { pathname, method } = parse_request_url(req);

    if (method === 'GET' && pathname === '/version') {
        return wrap_async_handler(get_version_handler)(req, res, next);
    }
    if (method === 'GET' && pathname === '/oauth/authorize') {
        return wrap_async_handler(oauth_authorize_handler)(req, res, next);
    }
    if (method === 'GET' && pathname === '/metrics/nsfs_stats') {
        metrics_nsfs_stats_handler(req, res);
        return;
    }
    if (method === 'GET' && pathname === '/') {
        redirect(res, '/version');
        return;
    }

    if (config.PROMETHEUS_ENABLED) {
        if (pathname.startsWith('/metrics/web_server')) {
            return proxy_metrics(req, res, config.WS_METRICS_SERVER_PORT, '/metrics/web_server');
        }
        if (pathname.startsWith('/metrics/bg_workers')) {
            return proxy_metrics(req, res, config.BG_METRICS_SERVER_PORT, '/metrics/bg_workers');
        }
        if (pathname.startsWith('/metrics/hosted_agents')) {
            return proxy_metrics(req, res, config.HA_METRICS_SERVER_PORT, '/metrics/hosted_agents');
        }
    }

    if (pathname.startsWith('/public/license-info')) {
        return license_info.serve_http(req, res);
    }
    if (pathname === '/public/eula' || pathname.startsWith('/public/eula/')) {
        return serve_file(req, res, path.join(rootdir, 'EULA.pdf'));
    }
    if (pathname === '/public/audit.csv') {
        return serve_file(req, res, path.join('/log', 'audit.csv'));
    }
    if (pathname.startsWith('/public/')) {
        return run_middleware_chain(req, res, [
            cache_control(dev_mode ? 0 : 10 * 60),
            serve_static_dir(path.join(rootdir, 'build', 'public'), '/public/'),
        ], 0);
    }

    return next();
}

/**
 * Wraps an async route handler for connect-style middleware.
 * @param {Function} handler
 * @returns {Function}
 */
function wrap_async_handler(handler) {
    return (req, res, next) => {
        P.resolve()
            .then(() => handler(req, res))
            .catch(next);
    };
}

/**
 * Parses the pathname and method from a request URL.
 * @param {HttpIncomingMessage} req
 * @returns {{ pathname: string, method: string }}
 */
function parse_request_url(req) {
    const parsed = new URL(req.url || '/', 'http://localhost');
    return {
        pathname: parsed.pathname,
        method: req.method || 'GET',
    };
}

/**
 * Sends an HTTP redirect response.
 * @param {HttpServerResponse} res
 * @param {string} location
 * @param {number} [status_code]
 */
function redirect(res, location, status_code = 302) {
    res.statusCode = status_code;
    res.setHeader('Location', location);
    res.end();
}

/**
 * Forwards a request to a local metrics server.
 * @param {HttpIncomingMessage} req
 * @param {HttpServerResponse} res
 * @param {number} target_port
 * @param {string} mount_prefix
 */
function proxy_metrics(req, res, target_port, mount_prefix) {
    let proxy_path = req.url.slice(mount_prefix.length);
    if (!proxy_path || proxy_path === '') proxy_path = '/';
    if (!proxy_path.startsWith('/')) proxy_path = '/' + proxy_path;

    const proxy_req = http.request({
        hostname: 'localhost',
        port: target_port,
        method: req.method,
        path: proxy_path,
        headers: req.headers,
    }, proxy_res => {
        res.writeHead(proxy_res.statusCode, proxy_res.headers);
        proxy_res.pipe(res);
    });

    const on_proxy_error = err => {
        dbg.warn('metrics proxy error', mount_prefix, err.message || err);
        if (!res.headersSent) {
            res.statusCode = 502;
            res.end('Bad Gateway');
        }
    };

    proxy_req.on('error', on_proxy_error);
    req.on('error', on_proxy_error);
    req.pipe(proxy_req);
}

/**
 * Serves a single file from disk.
 * @param {HttpIncomingMessage} req
 * @param {HttpServerResponse} res
 * @param {string} file_path
 */
function serve_file(req, res, file_path) {
    fs.stat(file_path, (err, stat) => {
        if (err || !stat.isFile()) {
            res.statusCode = 404;
            res.end();
            return;
        }
        stream_file(req, res, file_path, stat);
    });
}

/**
 * Serves static files from a directory under a URL prefix.
 * @param {string} root_dir
 * @param {string} url_prefix
 * @returns {Function}
 */
function serve_static_dir(root_dir, url_prefix) {
    const resolved_root = path.resolve(root_dir);
    return (req, res, next) => {
        const { pathname } = parse_request_url(req);
        if (!pathname.startsWith(url_prefix)) return next();

        let rel_path = pathname.slice(url_prefix.length);
        if (!rel_path || rel_path === '/') rel_path = 'index.html';

        const file_path = path.resolve(resolved_root, rel_path);
        if (!file_path.startsWith(resolved_root + path.sep) && file_path !== resolved_root) {
            res.statusCode = 403;
            res.end();
            return;
        }

        fs.stat(file_path, (err, stat) => {
            if (err) return next();
            if (stat.isDirectory()) {
                const index_path = path.join(file_path, 'index.html');
                return fs.stat(index_path, (index_err, index_stat) => {
                    if (index_err) return next();
                    stream_file(req, res, index_path, index_stat);
                });
            }
            stream_file(req, res, file_path, stat);
        });
    };
}

/**
 * Streams a file to the HTTP response.
 * @param {HttpIncomingMessage} req
 * @param {HttpServerResponse} res
 * @param {string} file_path
 * @param {FsStats} stat
 */
function stream_file(req, res, file_path, stat) {
    if (req.method === 'HEAD') {
        res.statusCode = 200;
        res.setHeader('Content-Type', mime.lookup(file_path) || 'application/octet-stream');
        res.setHeader('Content-Length', stat.size);
        res.end();
        return;
    }
    res.statusCode = 200;
    res.setHeader('Content-Type', mime.lookup(file_path) || 'application/octet-stream');
    res.setHeader('Content-Length', stat.size);
    fs.createReadStream(file_path).pipe(res);
}

function https_redirect_handler(req, res, next) {
    // HTTPS redirect:
    // since we want to provide secure and certified connections
    // for the entire application, so once a request for http arrives,
    // we redirect it to https.
    // it was suggested to use the req.secure flag to check that.
    // however our nodejs server is always http so the flag is false,
    // and on heroku only the router does ssl,
    // so we need to pull the heroku router headers to check.
    const fwd_proto = req.headers['x-forwarded-proto'];
    if (fwd_proto === 'http') {
        const host = req.headers.host;
        return redirect(res, 'https://' + host + req.url);
    }
    return next();
}

async function get_version_handler(req, res) {
    // Authorize bearer token version endpoint
    if (config.NOOBAA_VERSION_AUTH_ENABLED && !http_utils.authorize_bearer(req, res)) return;
    const { status, version } = await getVersion(req.url);
    res.statusCode = status;
    if (version) {
        res.setHeader('Content-Type', 'text/plain');
        res.end(version);
    } else {
        res.end();
    }
}

async function getVersion(route) {
    const registered = server_rpc.is_service_registered('system_api.read_system');
    if (registered && system_store.is_finished_initial_load) {
        return {
            status: 200,
            version: pkg.version
        };
    } else {
        dbg.log0(`${route} returning 404, service_registered(${registered}), system_store loaded(${system_store.is_finished_initial_load})`);
        return { status: 404 };
    }
}

// An oauth authorize endpoint that forwards to the OAuth authorization server.
async function oauth_authorize_handler(req, res) {
    const {
        KUBERNETES_SERVICE_HOST,
        KUBERNETES_SERVICE_PORT,
        NOOBAA_SERVICE_ACCOUNT,
        OAUTH_AUTHORIZATION_ENDPOINT
    } = process.env;

    if (!KUBERNETES_SERVICE_HOST || !KUBERNETES_SERVICE_PORT) {
        dbg.warn('/oauth/authorize: oauth is supported only on OpenShift deployments');
        res.statusCode = 500;
        res.end();
        return;
    }

    if (!OAUTH_AUTHORIZATION_ENDPOINT) {
        dbg.warn('/oauth/authorize: oauth support was not configured for this system');
        res.statusCode = 500;
        res.end();
        return;
    }

    if (!NOOBAA_SERVICE_ACCOUNT) {
        dbg.warn('/oauth/authorize: noobaa k8s service account name is not available');
        res.statusCode = 500;
        res.end();
        return;
    }

    let redirect_host;
    if (dev_mode) {
        redirect_host = `https://localhost:${https_port}`;

    } else {
        const { system_address } = system_store.data.systems[0];
        redirect_host = addr_utils.get_base_address(system_address, {
            hint: 'EXTERNAL',
            protocol: 'https'
        }).toString();
    }

    const k8s_namespace = await kube_utils.read_namespace();
    const client_id = `system:serviceaccount:${k8s_namespace}:${NOOBAA_SERVICE_ACCOUNT}`;
    const redirect_uri = new URL(config.OAUTH_REDIRECT_ENDPOINT, redirect_host);
    const return_url = new URL(req.url, 'http://dummy').searchParams.get('return-url');
    const authorization_endpoint = new URL(OAUTH_AUTHORIZATION_ENDPOINT);
    authorization_endpoint.searchParams.set('client_id', client_id);
    authorization_endpoint.searchParams.set('response_type', 'code');
    authorization_endpoint.searchParams.set('scope', config.OAUTH_REQUIRED_SCOPE);
    authorization_endpoint.searchParams.set('redirect_uri', redirect_uri.toString());
    authorization_endpoint.searchParams.set('state', decodeURIComponent(return_url));

    redirect(res, authorization_endpoint.toString());
}

function metrics_nsfs_stats_handler(req, res) {
    let nsfs_report = '';

    const nsfs_counters = stats_aggregator.get_nsfs_io_stats();
    // Building the report per io and value
    for (const [key, value] of Object.entries(nsfs_counters)) {
        const metric = `noobaa_nsfs_io_${key}`.toLowerCase();
        nsfs_report += `${metric}: ${value}<br>`;
    }

    const op_stats = stats_aggregator.get_op_stats();
    // Building the report per op name key and value
    for (const [op_name, obj] of Object.entries(op_stats)) {
        nsfs_report += `<br>`;
        for (const [key, value] of Object.entries(obj)) {
            const metric = `noobaa_nsfs_op_${op_name}_${key}`.toLowerCase();
            nsfs_report += `${metric}: ${value}<br>`;
        }
    }

    const fs_workers_stats = stats_aggregator.get_fs_workers_stats();
    // Building the report per fs worker name key and value
    for (const [fs_worker_name, obj] of Object.entries(fs_workers_stats)) {
        nsfs_report += `<br>`;
        for (const [key, value] of Object.entries(obj)) {
            const metric = `noobaa_nsfs_fs_${fs_worker_name}_${key}`.toLowerCase();
            nsfs_report += `${metric}: ${value}<br>`;
        }
    }

    dbg.log1(`_create_nsfs_report: nsfs_report ${nsfs_report}`);

    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/html');
    res.end(nsfs_report);
}

// using router before static files to optimize -
// since we usually have less routes then files, and the routes are in memory.
function cache_control(seconds) {
    const millis = 1000 * seconds;
    return (req, res, next) => {
        res.setHeader("Cache-Control", "public, max-age=" + seconds);
        res.setHeader("Expires", new Date(Date.now() + millis).toUTCString());
        return next();
    };
}

// roughly based on express.errorHandler from connect's errorHandler.js
function error_handler(err, req, res) {
    console.error('ERROR:', err);
    let e;
    if (dev_mode) {
        // show internal info only on development
        e = err;
    } else {
        e = _.pick(err, 'statusCode', 'message', 'reload');
    }
    e.statusCode = err.status || res.statusCode;
    if (e.statusCode < 400) {
        e.statusCode = 500;
    }
    res.statusCode = e.statusCode;

    if (can_accept_html(req)) {
        const ctx = { //common_api.common_server_data(req);
            data: {}
        };
        if (dev_mode) {
            e.data = _.extend(ctx.data, e.data);
        } else {
            e.data = ctx.data;
        }
        res.setHeader('Content-Type', 'text/html');
        return res.end(`<html>
<head>
    <style>
        body {
            color: #242E35;
        }
    </style>
</head>
<body>
    <h1>NooBaa</h1>
    <h2>${e.message}</h2>
    <h3>(Error Code ${e.statusCode})</h3>
    <p><a href="/">Take me back ...</a></p>
</body>
</html>`);
    } else if (request_accepts(req, 'json')) {
        res.setHeader('Content-Type', 'application/json');
        return res.end(JSON.stringify(e));
    } else {
        res.setHeader('Content-Type', 'text/plain');
        return res.end(e.message || e.toString());
    }
}

function error_404(req, res, next) {
    return next({
        status: 404, // not found
        message: 'We dug the earth, but couldn\'t find your requested URL'
    });
}

// decide if the client can accept html reply.
// the xhr flag in the request (X-Requested-By header) is not commonly sent
// see https://github.com/angular/angular.js/commit/3a75b1124d062f64093a90b26630938558909e8d
// the accept headers from angular http contain */* so will match anything.
// so finally we fallback to check the url.

function can_accept_html(req) {
    const { pathname } = parse_request_url(req);
    return !is_xhr(req) && request_accepts(req, 'html') && pathname.indexOf('/api/') !== 0;
}

/**
 * Returns true when the client sent an XMLHttpRequest.
 * @param {HttpIncomingMessage} req
 * @returns {boolean}
 */
function is_xhr(req) {
    return req.headers['x-requested-with'] === 'XMLHttpRequest';
}

/**
 * Returns true when the Accept header includes the given type.
 * @param {HttpIncomingMessage} req
 * @param {string} type
 * @returns {boolean}
 */
function request_accepts(req, type) {
    const accept = req.headers.accept || '';
    if (type === 'html') return (/\btext\/html\b/).test(accept) || accept.includes('*/*');
    if (type === 'json') return (/\bapplication\/json\b/).test(accept) || accept.includes('*/*');
    return accept.includes(type);
}

exports.main = main;

if (require.main === module) main();
