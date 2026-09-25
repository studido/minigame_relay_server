'use strict';
// Dogfight 313 relay server. Knows nothing about gameplay: it manages rooms
// (join by code, host = creator, host picks settings/seed) and fans messages
// out to the other players in the same room. Clients own their own jets.
const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocketServer } = require('ws');

const MAX_PLAYERS = 4;
const CODE_ALPHABET = '23456789ABCDEFGHJKMNPQRSTUVWXYZ'; // no 0/1/I/L/O
const CODE_LEN = 4;
const RESUME_GRACE_MS = 60_000;     // a dropped player can rejoin within a minute
const MAX_MSG_BYTES = 16 * 1024;
const RATE_CAPACITY = 120;          // token bucket: burst
const RATE_REFILL = 50;             // tokens per second
const RATE_MAX_STRIKES = 10;        // consecutive dropped messages before we cut the socket
const MAX_ROOMS = 500;
const PING_INTERVAL_MS = 25_000;
const STALE_AFTER_MS = 90_000;

function makeCode() {
  let s = '';
  for (let i = 0; i < CODE_LEN; i++) s += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
  return s;
}

function cleanName(v) {
  if (typeof v !== 'string') return null;
  const s = v.trim().slice(0, 14);
  return s || null;
}

function createRelay() {
  const rooms = new Map(); // code -> { code, hostId, players: Map(id -> player), settings, seed, started }
  const conns = new Set(); // live sockets, for clean shutdown
  let nextPlayerId = 1;

  function send(p, obj) {
    if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify(obj));
  }
  function sendConn(conn, obj) {
    if (conn.ws.readyState === 1) conn.ws.send(JSON.stringify(obj));
  }
  function err(conn, code, msg) { sendConn(conn, { t: 'error', code, msg }); }
  function broadcast(room, obj, exceptId) {
    const s = JSON.stringify(obj);
    for (const p of room.players.values()) {
      if (p.id !== exceptId && p.ws && p.ws.readyState === 1) p.ws.send(s);
    }
  }
  function rosterOf(room) {
    return [...room.players.values()].map((p) => ({
      id: p.id, name: p.name, host: p.id === room.hostId, ready: p.ready, connected: !!p.ws,
    }));
  }
  function broadcastRoster(room) { broadcast(room, { t: 'roster', players: rosterOf(room) }); }

  function endRoom(room, reason) {
    rooms.delete(room.code);
    broadcast(room, { t: 'ended', reason });
    for (const p of room.players.values()) {
      clearTimeout(p.purgeT);
      if (p.ws) { try { p.ws.close(1000, reason); } catch { /* already closing */ } }
      if (p.conn) p.conn.player = null;
      p.room = null;
    }
  }

  function leaveRoom(p) {
    const room = p.room;
    if (!room) return;
    if (p.id === room.hostId) { endRoom(room, 'hostLeft'); return; }
    room.players.delete(p.id);
    p.room = null;
    broadcastRoster(room);
    if ([...room.players.values()].every((o) => !o.ws)) endRoom(room, 'empty');
  }

  function joinedPayload(room, p) {
    return {
      t: 'joined', code: room.code, id: p.id, token: p.token, hostId: room.hostId,
      settings: room.settings, seed: room.seed, started: room.started, players: rosterOf(room),
    };
  }

  function attachPlayer(conn, player) {
    player.ws = conn.ws;
    player.conn = conn;
    player.disconnectedAt = null;
    clearTimeout(player.purgeT);
    conn.player = player;
  }

  function rateOk(conn) {
    const now = Date.now();
    conn.tokens = Math.min(RATE_CAPACITY, conn.tokens + (now - conn.lastRateCheck) / 1000 * RATE_REFILL);
    conn.lastRateCheck = now;
    if (conn.tokens >= 1) { conn.tokens -= 1; conn.strikes = 0; return true; }
    if (++conn.strikes > RATE_MAX_STRIKES) conn.ws.close(4000, 'rate limit');
    return false;
  }

  function onMessage(conn, m) {
    if (m.t === 'create' || m.t === 'join' || m.t === 'resume') {
      if (conn.player) return err(conn, 'alreadyInRoom', 'leave your current room first');
      if (m.t === 'create') {
        const p = {
          id: nextPlayerId++, token: crypto.randomBytes(8).toString('hex'),
          name: cleanName(m.name) || 'Pilot', ws: null, conn: null, ready: false, room: null, purgeT: null,
        };
        if (rooms.size >= MAX_ROOMS) return err(conn, 'serverFull', 'too many rooms, try again later');
        let code; do { code = makeCode(); } while (rooms.has(code));
        const room = { code, hostId: p.id, players: new Map([[p.id, p]]), settings: null, seed: null, started: false };
        rooms.set(code, room);
        p.room = room;
        attachPlayer(conn, p);
        return sendConn(conn, joinedPayload(room, p));
      }
      const code = typeof m.code === 'string' ? m.code.toUpperCase().trim() : '';
      const room = rooms.get(code);
      if (!room) return err(conn, 'badCode', 'no room with that code');
      if (m.t === 'join') {
        if (room.players.size >= MAX_PLAYERS) return err(conn, 'roomFull', 'room is full');
        const name = cleanName(m.name);
        if (!name) return err(conn, 'badName', 'callsign required');
        const p = {
          id: nextPlayerId++, token: crypto.randomBytes(8).toString('hex'),
          name, ws: null, conn: null, ready: false, room, purgeT: null,
        };
        room.players.set(p.id, p);
        attachPlayer(conn, p);
        sendConn(conn, joinedPayload(room, p));
        return broadcastRoster(room);
      }
      // resume after a dropped connection
      const p = room.players.get(m.id);
      if (!p || p.token !== m.token || p.ws) return err(conn, 'badResume', 'cannot resume that session');
      attachPlayer(conn, p);
      sendConn(conn, joinedPayload(room, p));
      return broadcastRoster(room);
    }

    const p = conn.player;
    if (!p || !p.room) return err(conn, 'noRoom', 'create or join a room first');
    const room = p.room;

    switch (m.t) {
      case 'settings': {
        if (p.id !== room.hostId) return err(conn, 'notHost', 'only the host changes settings');
        room.settings = m.d ?? null;
        return broadcast(room, { t: 'settings', d: room.settings });
      }
      case 'start': {
        if (p.id !== room.hostId) return err(conn, 'notHost', 'only the host starts the match');
        room.seed = Number.isFinite(m.seed) ? m.seed : crypto.randomInt(2 ** 31);
        room.started = true;
        return broadcast(room, { t: 'start', seed: room.seed });
      }
      case 'ready': {
        p.ready = !!m.v;
        return broadcastRoster(room);
      }
      case 'leave':
        conn.player = null;
        return leaveRoom(p);
      case 'state':
      case 'event':
        return broadcast(room, { t: 'from', from: p.id, k: m.t, d: m.d }, p.id);
      default:
    }
  }

  function onClose(conn) {
    conns.delete(conn);
    const p = conn.player;
    conn.player = null;
    if (!p || !p.room || p.conn !== conn) return;
    p.ws = null;
    p.conn = null;
    const room = p.room;
    if (p.id === room.hostId) { endRoom(room, 'hostLeft'); return; }
    broadcastRoster(room);
    p.disconnectedAt = Date.now();
    p.purgeT = setTimeout(() => {
      if (p.ws || !p.room) return;
      room.players.delete(p.id);
      p.room = null;
      broadcastRoster(room);
      if ([...room.players.values()].every((o) => !o.ws)) endRoom(room, 'empty');
    }, RESUME_GRACE_MS + 60_000); // spare slots linger a bit beyond the resume window
    p.purgeT.unref?.();
  }

  const server = http.createServer((req, res) => {
    if (req.url === '/health') {
      let players = 0;
      for (const r of rooms.values()) players += r.players.size;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, rooms: rooms.size, players, uptime: process.uptime() | 0 }));
      return;
    }
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('dogfight relay\n');
  });

  const wss = new WebSocketServer({ server, maxPayload: MAX_MSG_BYTES });
  wss.on('connection', (ws) => {
    const conn = { ws, player: null, tokens: RATE_CAPACITY, lastRateCheck: Date.now(), strikes: 0, lastAlive: Date.now() };
    conns.add(conn);
    ws.on('pong', () => { conn.lastAlive = Date.now(); });
    ws.on('message', (data) => {
      conn.lastAlive = Date.now();
      if (!rateOk(conn)) return;
      let m;
      try { m = JSON.parse(data.toString()); } catch { return; }
      if (!m || typeof m.t !== 'string') return;
      try { onMessage(conn, m); } catch (e) { console.error('message handler error:', e); }
    });
    ws.on('close', () => onClose(conn));
    ws.on('error', () => { /* close follows */ });
  });

  const heartbeat = setInterval(() => {
    const now = Date.now();
    for (const conn of conns) {
      if (now - conn.lastAlive > STALE_AFTER_MS) conn.ws.terminate();
      else conn.ws.ping();
    }
  }, PING_INTERVAL_MS);
  heartbeat.unref?.();

  return {
    rooms,
    server,
    listen(port = 8080) {
      return new Promise((resolve) => server.listen(port, '127.0.0.1', () => resolve(server.address().port)));
    },
    listenAny(port = 8080) {
      return new Promise((resolve) => server.listen(port, () => resolve(server.address().port)));
    },
    close() {
      clearInterval(heartbeat);
      for (const conn of conns) { try { conn.ws.terminate(); } catch { /* ignore */ } }
      for (const room of [...rooms.values()]) endRoom(room, 'shutdown');
      return new Promise((resolve) => wss.close(() => server.close(() => resolve())));
    },
  };
}

if (require.main === module) {
  const relay = createRelay();
  const port = +(process.env.PORT || 8080);
  relay.listenAny(port).then((p) => console.log(`dogfight relay listening on :${p}`));
  const shutdown = () => relay.close().then(() => process.exit(0));
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createRelay, MAX_PLAYERS };
