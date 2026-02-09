/* Copyright (C) 2016 NooBaa */
'use strict';

const _ = require('lodash');
const tls = require('tls');
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const url = require('url');
const WS = require('ws');

const P = require('../util/promise');
const dbg = require('../util/debug_module')(__filename);
const config = require('../../config');
const url_utils = require('../util/url_utils');
const ssl_utils = require('../util/ssl_utils');
const EventEmitter = require('events').EventEmitter;
const RpcN2NConnection = require('./rpc_n2n');

const N2N_STAR = 'n2n://*';
const N2N_CONFIG_PORT_PICK = ['min', 'max', 'port'];
const N2N_CONFIG_FIELDS_PICK = [
    'offer_ipv4',
    'offer_ipv6',
    'accept_ipv4',
    'accept_ipv6',
    'offer_internal',
    'tcp_active',
    'tcp_permanent_passive',
    'tcp_transient_passive',
    'tcp_simultaneous_open',
    'tcp_tls',
    'udp_port',
    'udp_dtls',
    'stun_servers',
    'public_ips'
];

let global_tcp_permanent_passive;

/**
 *
 * RpcN2NAgent
 *
 * Represents an endpoint for N2N connections using WebSocket.
 * Exchanges ws_url via the signaller; initiator connects to acceptor's WS server.
 *
 */
class RpcN2NAgent extends EventEmitter {

    constructor(options) {
        super();
        options = options || {};

        this.setMaxListeners(100);

        const send_signal = options.send_signal;

        this.n2n_config = {
            offer_ipv4: true,
            offer_ipv6: true,
            accept_ipv4: true,
            accept_ipv6: true,
            offer_internal: config.N2N_OFFER_INTERNAL,
            tcp_active: true,
            tcp_permanent_passive: {
                min: 60101,
                max: 60600
            },
            tcp_transient_passive: false,
            tcp_simultaneous_open: false,
            tcp_tls: true,
            udp_port: true,
            udp_dtls: false,
            stun_servers: [],
            public_ips: [],
            ssl_options: {
                rejectUnauthorized: false,
                secureContext: tls.createSecureContext({ honorCipherOrder: true, ...ssl_utils.generate_ssl_certificate() }),
            },
            signaller: (target, info) => send_signal({
                source: this.rpc_address,
                target: target,
                info: info
            })
        };

        /** @type {Object.<string, RpcN2NConnection>} pending accept token -> conn */
        this._pending_accepts = {};
        this._n2n_ws_http_server = null;
        this._n2n_ws_server = null;
    }

    set_rpc_address(rpc_address) {
        dbg.log('set_rpc_address:', rpc_address, 'was', this.rpc_address);
        this.rpc_address = rpc_address;
    }

    reset_rpc_address() {
        this.set_rpc_address('');
    }

    set_any_rpc_address() {
        this.set_rpc_address(N2N_STAR);
    }

    set_ssl_context(secure_context_params) {
        this.n2n_config.ssl_options.secureContext =
            tls.createSecureContext({ ...secure_context_params, honorCipherOrder: true });
    }

    update_n2n_config(n2n_config) {
        dbg.log0('UPDATE N2N CONFIG', n2n_config);
        _.each(n2n_config, (val, key) => {
            if (key === 'tcp_permanent_passive') {
                const prev = this.n2n_config.tcp_permanent_passive;
                const conf = prev ? _.pick(prev, N2N_CONFIG_PORT_PICK) : prev;
                dbg.log0('update_n2n_config: update tcp_permanent_passive old', conf, 'new', val);
                if (!val || !_.isEqual(conf, val)) {
                    this.disconnect();
                    if (val) {
                        if (!global_tcp_permanent_passive) {
                            global_tcp_permanent_passive = _.clone(val);
                        }
                        this.n2n_config.tcp_permanent_passive = global_tcp_permanent_passive;
                    } else {
                        this.n2n_config.tcp_permanent_passive = val;
                    }
                }
            } else {
                this.n2n_config[key] = val;
            }
        });

        this.emit('reset_n2n');
        const remaining_listeners = this.listenerCount('reset_n2n');
        if (remaining_listeners) {
            dbg.warn('update_n2n_config: remaining listeners on reset_n2n event',
                remaining_listeners, '(probably a connection that forgot to call close)');
        }
    }

    disconnect() {
        const conf = this.n2n_config.tcp_permanent_passive;
        if (this._n2n_ws_http_server) {
            dbg.log0('close N2N WS server');
            this._n2n_ws_http_server.close();
            this._n2n_ws_http_server = null;
            this._n2n_ws_server = null;
            global_tcp_permanent_passive = null;
        }
        if (conf && conf.server) {
            conf.server.close();
            conf.server = null;
            global_tcp_permanent_passive = null;
        }
    }

    get_plain_n2n_config() {
        const n2n_config =
            _.pick(this.n2n_config, N2N_CONFIG_FIELDS_PICK);
        n2n_config.tcp_permanent_passive =
            n2n_config.tcp_permanent_passive ?
            _.pick(n2n_config.tcp_permanent_passive, N2N_CONFIG_PORT_PICK) :
            n2n_config.tcp_permanent_passive;
        return n2n_config;
    }

    /**
     * Ensure the N2N WebSocket server is listening (for accept path).
     * Uses tcp_permanent_passive port range and tcp_tls for TLS.
     */
    _ensure_n2n_ws_server() {
        if (this._n2n_ws_http_server) {
            return P.resolve(this._n2n_ws_http_server);
        }
        const conf = this.n2n_config.tcp_permanent_passive;
        if (!conf) {
            return P.reject(new Error('N2N WS: tcp_permanent_passive not configured'));
        }

        let port_range;
        if (typeof conf === 'object' && conf.port !== undefined) {
            port_range = conf.port;
        } else if (typeof conf === 'object' && conf.min !== undefined) {
            port_range = conf;
        } else {
            port_range = { min: 60101, max: 60600 };
        }
        const port = typeof port_range === 'number' ? port_range :
            (port_range.min + Math.floor(Math.random() * (port_range.max - port_range.min + 1)));

        const use_tls = this.n2n_config.tcp_tls;
        const create_server = use_tls ?
            () => https.createServer(this.n2n_config.ssl_options, _n2n_http_handler) :
            () => http.createServer(_n2n_http_handler);

        function _n2n_http_handler(req, res) {
            res.statusCode = 426;
            res.setHeader('Upgrade', 'websocket');
            res.end('Upgrade Required');
        }

        const http_server = create_server();
        const ws_server = new WS.Server({ noServer: true });

        ws_server.on('headers', (headers, req) => {
            // allow any origin for N2N
        });

        const self = this;
        http_server.on('upgrade', (request, socket, head) => {
            const parsed = url.parse(request.url || '', true);
            const token = parsed.query && parsed.query.n2n;
            if (!token) {
                dbg.warn('N2N WS upgrade without n2n token');
                socket.destroy();
                return;
            }
            ws_server.handleUpgrade(request, socket, head, ws => {
                const conn = self._pending_accepts[token];
                delete self._pending_accepts[token];
                if (!conn) {
                    dbg.warn('N2N WS unknown or expired token');
                    ws.close();
                    return;
                }
                conn._fulfill(ws);
            });
        });

        return P.fromCallback(cb => {
            http_server.listen(port, '0.0.0.0', () => {
                this._n2n_ws_http_server = http_server;
                this._n2n_ws_server = ws_server;
                if (!global_tcp_permanent_passive) {
                    global_tcp_permanent_passive = _.clone(conf);
                }
                global_tcp_permanent_passive.ws_http_server = http_server;
                cb();
            });
        }).catch(err => {
            dbg.warn('N2N WS server listen failed', port, err);
            throw err;
        }).then(() => this._n2n_ws_http_server);
    }

    _get_n2n_ws_url(token) {
        const server = this._n2n_ws_http_server;
        if (!server) {
            throw new Error('N2N WS server not listening');
        }
        const port = server.address().port;
        const protocol = this.n2n_config.tcp_tls ? 'wss' : 'ws';
        const host = (this.n2n_config.public_ips && this.n2n_config.public_ips[0]) || 'localhost';
        const path = token ? `/?n2n=${token}` : '/';
        return `${protocol}://${host}:${port}${path}`;
    }

    /**
     * Called by RpcN2NConnection.accept(). Register pending conn and return ws_url with token.
     */
    accept_ws_connection(conn) {
        const token = crypto.randomBytes(16).toString('hex');
        this._pending_accepts[token] = conn;
        return this._ensure_n2n_ws_server()
            .then(() => ({ ws_url: this._get_n2n_ws_url(token) }));
    }

    accept_signal(params) {
        dbg.log1('N2N AGENT accept_signal:', params, 'my rpc_address', this.rpc_address);

        const source = url_utils.quick_parse(params.source);
        const target = url_utils.quick_parse(params.target);
        if (!this.rpc_address || !target ||
            (this.rpc_address !== N2N_STAR && this.rpc_address !== target.href)) {
            throw new Error('N2N MISMATCHING PEER ID ' + params.target +
                ' my rpc_address ' + this.rpc_address);
        }
        const conn = new RpcN2NConnection(source, this);
        conn.once('connect', () => this.emit('connection', conn));
        return conn.accept(params.info);
    }

}

module.exports = RpcN2NAgent;
