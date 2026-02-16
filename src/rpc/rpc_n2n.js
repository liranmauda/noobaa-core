/* Copyright (C) 2016 NooBaa */
'use strict';

const P = require('../util/promise');
const dbg = require('../util/debug_module')(__filename);
const RpcBaseConnection = require('./rpc_base_conn');
const RpcWsConnection = require('./rpc_ws');
const url_utils = require('../util/url_utils');

/**
 *
 * RpcN2NConnection
 *
 * n2n - node-to-node or noobaa-to-noobaa, essentially p2p, but noobaa branded.
 * Uses WebSocket over the N2N signaller (replaces legacy ICE/NAT traversal with direct WS to peer's ws_url).
 *
 */
class RpcN2NConnection extends RpcBaseConnection {

    constructor(addr_url, n2n_agent) {
        if (!n2n_agent) throw new Error('N2N AGENT NOT REGISTERED');
        super(addr_url);
        this.n2n_agent = n2n_agent;
        this._ws_conn = null; // set when connected (initiator) or when _fulfill(ws) (acceptor)
        this._n2n_keepalive_interval = null;

        this.reset_n2n_listener = () => {
            const reset_err = new Error('N2N RESET');
            reset_err.stack = '';
            this.emit('error', reset_err);
        };
        n2n_agent.on('reset_n2n', this.reset_n2n_listener);
    }

    _connect() {
        const self = this;
        return P.try(() => self.n2n_agent.n2n_config.signaller(self.url.href, {}))
            .then(info => {
                const ws_url = info && info.ws_url;
                if (!ws_url) {
                    throw new Error('N2N WS: peer did not return ws_url');
                }
                dbg.log1('N2N WS CONNECT to', ws_url);
                const ws_conn = new RpcWsConnection(url_utils.quick_parse(ws_url));
                self._ws_conn = ws_conn;
                ws_conn.on('error', err => self.emit('error', err));
                ws_conn.on('close', (code, reason) => {
                    const closed_err = new Error(`N2N WS CLOSED${code === undefined ? '' : ` (code=${code})`}`);
                    closed_err.stack = '';
                    self.emit('error', closed_err);
                });
                ws_conn.once('connect', () => {
                    self._send = msg => ws_conn._send(msg);
                    ws_conn.on('message', msg => self.emit('message', msg));
                    self._start_n2n_keepalive(ws_conn.ws);
                    dbg.log1('N2N WS CONNECTED', self.connid);
                    self.emit('connect');
                });
                return ws_conn.connect();
            })
            .catch(err => {
                self.emit('error', err);
            });
    }

    /**
     * Accept: return our ws_url (with token) so the initiator can connect.
     * The connection will be fulfilled when the initiator's WS arrives (agent calls _fulfill).
     */
    accept(remote_info) {
        return this.n2n_agent.accept_ws_connection(this);
    }

    /**
     * Called by RpcN2NAgent when an incoming WS connection is received for this pending accept.
     * @param {object} ws - raw WebSocket from ws library
     */
    _fulfill(ws) {
        if (this._ws_conn) {
            dbg.warn('N2N _fulfill: already fulfilled', this.connid);
            return;
        }
        this._ws_conn = { ws };
        ws.binaryType = 'fragments';
        ws.on('error', err => this.emit('error', err));
        ws.on('close', (code, reason) => {
            const closed_err = new Error(`N2N WS CLOSED${code === undefined ? '' : ` (code=${code})`}`);
            closed_err.stack = '';
            this.emit('error', closed_err);
        });
        ws.on('message', (fragments, flags) => this.emit('message', fragments));
        this._start_n2n_keepalive(ws);
        this._send = async msg => {
            const opts = { fin: false, binary: true, compress: false };
            for (let i = 0; i < msg.length; ++i) {
                opts.fin = (i + 1 === msg.length);
                ws.send(msg[i], opts);
            }
        };
        dbg.log1('N2N WS ACCEPTED', this.connid);
        this.emit('connect');
    }

    _start_n2n_keepalive(ws) {
        const WS = require('ws');
        const KEEPALIVE_MS = 10000;
        this._clear_n2n_keepalive();
        this._n2n_keepalive_interval = setInterval(() => {
            if (ws && ws.readyState === WS.OPEN) ws.ping();
        }, KEEPALIVE_MS);
        if (this._n2n_keepalive_interval.unref) this._n2n_keepalive_interval.unref();
    }

    _clear_n2n_keepalive() {
        if (this._n2n_keepalive_interval) {
            clearInterval(this._n2n_keepalive_interval);
            this._n2n_keepalive_interval = null;
        }
    }

    _close() {
        dbg.log0('_close', this.connid);
        this._clear_n2n_keepalive();
        this.n2n_agent.removeListener('reset_n2n', this.reset_n2n_listener);
        if (this._ws_conn) {
            if (typeof this._ws_conn._close === 'function') {
                this._ws_conn._close();
            } else if (this._ws_conn.ws) {
                const WS = require('ws');
                if (this._ws_conn.ws.readyState !== WS.CLOSED && this._ws_conn.ws.readyState !== WS.CLOSING) {
                    this._ws_conn.ws.close();
                }
            }
            this._ws_conn = null;
        }
    }

    async _send(msg) {
        // this default error impl will be overridden once connected
        throw new Error('N2N NOT CONNECTED');
    }

}


module.exports = RpcN2NConnection;
