/*
 * peer-mqtt-shim.js
 * Substitut compatible amb la mínima API de PeerJS que fa servir el joc
 * (new Peer(id), peer.connect(id), eventos open/connection/data/close/error),
 * però enviant els missatges a través de brokers MQTT públics (WebSocket segur).
 * Com que tot passa per un servidor intermedi, funciona entre qualsevol xarxa
 * (dades mòbils, wifi, escola...) sense problemes de NAT/WebRTC.
 *
 * Cal carregar abans mqtt.js:
 *   <script src="https://cdn.jsdelivr.net/npm/mqtt@5.10.1/dist/mqtt.min.js"></script>
 *   <script src="peer-mqtt-shim.js"></script>
 */
(function () {
    'use strict';
    if (!window.mqtt) { console.warn('[shim] mqtt.js no s\'ha carregat; el mode online no estarà disponible.'); return; }

    // El primer que respongui es fa servir; els altres són de reserva.
    const BROKERS = [
        'wss://broker.emqx.io:8084/mqtt',
        'wss://broker.hivemq.com:8884/mqtt',
        'wss://test.mosquitto.org:8081'
    ];
    const T = 'culopr/v1/';
    const rnd = () => Math.random().toString(36).slice(2, 10);
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    class Emitter {
        constructor() { this._h = {}; }
        on(e, f) { (this._h[e] = this._h[e] || []).push(f); return this; }
        emit(e, ...a) { (this._h[e] || []).slice().forEach((f) => { try { f(...a); } catch (x) { console.error(x); } }); }
    }

    class Conn extends Emitter {
        constructor(peer, remote, cid, client) {
            super();
            this.peer = peer; this.peerId = remote; this.cid = cid; this.client = client;
            this.open = false; this.closed = false;
        }
        send(m) {
            if (!this.open || this.closed) return;
            this.peer._pub(this.client, this.peerId, { k: 'data', cid: this.cid, d: m });
        }
        close() {
            if (this.closed) return;
            if (this.client) this.peer._pub(this.client, this.peerId, { k: 'close', cid: this.cid });
            this._end();
        }
        _opened() { if (this.open || this.closed) return; this.open = true; this.emit('open'); }
        _end() {
            if (this.closed) return;
            this.closed = true; this.open = false;
            delete this.peer._conns[this.cid];
            this.emit('close');
        }
    }

    class Peer extends Emitter {
        constructor(id) {
            super();
            this.requested = typeof id === 'string' && !!id;
            this.id = this.requested ? id : 'g' + rnd();
            this.open = false; this.destroyed = false;
            this._conns = {}; this._clients = {}; this._primary = -1;
            this._announced = !this.requested; // els convidats anuncien la presència sempre
            this._init();
        }

        _inbox(id) { return T + 'i/' + id; }
        _pres(id) { return T + 'p/' + id; }
        _pub(client, toId, obj) { try { client.publish(this._inbox(toId), JSON.stringify(obj)); } catch (e) { /* ignora */ } }
        _announce(c) { try { c.publish(this._pres(this.id), 'online', { retain: true }); } catch (e) { /* ignora */ } }

        _client(j) {
            if (this._clients[j]) return this._clients[j];
            const p = new Promise((resolve, reject) => {
                let settled = false;
                const c = window.mqtt.connect(BROKERS[j], {
                    clientId: 'culopr_' + rnd() + rnd(),
                    clean: true, keepalive: 20, connectTimeout: 6000, reconnectPeriod: 2000,
                    will: { topic: this._pres(this.id), payload: 'offline', retain: true, qos: 0 }
                });
                c._pl = {};
                const to = setTimeout(() => {
                    if (!settled) { settled = true; try { c.end(true); } catch (e) { /* ignora */ } reject(new Error('timeout ' + BROKERS[j])); }
                }, 7000);
                c.on('connect', () => {
                    c.subscribe(this._inbox(this.id));
                    if (this._announced) this._announce(c);
                    if (!settled) { settled = true; clearTimeout(to); resolve(c); }
                });
                c.on('error', () => { /* mqtt.js reintenta sol */ });
                c.on('message', (topic, payload) => this._onMsg(c, topic, payload.toString()));
            });
            p.catch(() => { delete this._clients[j]; });
            this._clients[j] = p;
            return p;
        }

        _probe(c, id, ms) {
            return new Promise((resolve) => {
                const t = this._pres(id);
                let done = false;
                const finish = (v) => {
                    if (done) return;
                    done = true; clearTimeout(to);
                    c._pl[t] = (c._pl[t] || []).filter((x) => x !== f);
                    resolve(v);
                };
                const f = (s) => finish(s === 'online');
                const to = setTimeout(() => finish(false), ms);
                (c._pl[t] = c._pl[t] || []).push(f);
                c.subscribe(t);
            });
        }

        _watch(c, conn) {
            const t = this._pres(conn.peerId);
            (c._pl[t] = c._pl[t] || []).push((s) => { if (s === 'offline') conn._end(); });
            c.subscribe(t);
        }

        _onMsg(c, topic, s) {
            if (this.destroyed) return;
            if (topic.indexOf(T + 'p/') === 0) { (c._pl[topic] || []).slice().forEach((f) => f(s)); return; }
            let m; try { m = JSON.parse(s); } catch (e) { return; }
            if (!m) return;
            if (m.k === 'conn') {
                if (!this.open || this._conns[m.cid]) return;
                const conn = new Conn(this, m.from, m.cid, c);
                this._conns[m.cid] = conn;
                this._watch(c, conn);
                this._pub(c, m.from, { k: 'acc', cid: m.cid });
                this.emit('connection', conn);
                setTimeout(() => conn._opened(), 0);
                return;
            }
            const conn = this._conns[m.cid];
            if (!conn) return;
            if (m.k === 'acc') { if (conn._acc) conn._acc(true); conn._opened(); }
            else if (m.k === 'data') conn.emit('data', m.d);
            else if (m.k === 'close') conn._end();
        }

        async _init() {
            let c = null, j0 = -1;
            for (let j = 0; j < BROKERS.length; j++) {
                try { c = await this._client(j); j0 = j; break; } catch (e) { /* prova el següent */ }
            }
            if (this.destroyed) return;
            if (!c) { this.emit('error', { type: 'network', message: 'No es pot connectar a cap servidor MQTT' }); return; }
            this._primary = j0;
            if (this.requested && await this._probe(c, this.id, 1200)) {
                this.emit('error', { type: 'unavailable-id', message: 'ID ocupat' });
                return;
            }
            this._announced = true;
            this._announce(c);
            this.open = true;
            this.emit('open', this.id);
        }

        connect(targetId) {
            const conn = new Conn(this, targetId, rnd() + rnd(), null);
            this._conns[conn.cid] = conn;
            this._dial(conn);
            return conn;
        }

        async _dial(conn) {
            for (let n = 0; n < 100 && !this.open && !this.destroyed; n++) await sleep(50);
            const order = [];
            if (this._primary >= 0) order.push(this._primary);
            for (let j = 0; j < BROKERS.length; j++) if (order.indexOf(j) < 0) order.push(j);
            for (const j of order) {
                let c; try { c = await this._client(j); } catch (e) { continue; }
                if (this.destroyed || conn.closed) return;
                if (!(await this._probe(c, conn.peerId, 2500))) continue;
                conn.client = c;
                this._watch(c, conn);
                const ok = await new Promise((res) => {
                    conn._acc = res;
                    this._pub(c, conn.peerId, { k: 'conn', cid: conn.cid, from: this.id });
                    setTimeout(() => res(false), 6000);
                });
                if (ok) return;
            }
            if (!conn.closed && !this.destroyed) {
                conn.emit('error', { type: 'peer-unavailable' });
                this.emit('error', { type: 'peer-unavailable', message: 'Sala no trobada' });
            }
        }

        reconnect() { /* mqtt.js es reconnecta automàticament */ }

        destroy() {
            if (this.destroyed) return;
            this.destroyed = true; this.open = false;
            Object.keys(this._conns).forEach((k) => this._conns[k].close());
            Object.keys(this._clients).forEach((j) => {
                this._clients[j].then((c) => {
                    try { c.publish(this._pres(this.id), 'offline', { retain: true }); } catch (e) { /* ignora */ }
                    setTimeout(() => { try { c.end(); } catch (e) { /* ignora */ } }, 200);
                }).catch(() => { });
            });
            this.emit('close');
        }
    }

    window.Peer = Peer;
})();
