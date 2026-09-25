'use strict';
// Integration test for the relay: spins the real server on a random local port
// and drives it with real WebSocket clients. Run: npm test
const assert = require('node:assert');
const { WebSocket } = require('ws');
const { createRelay, MAX_PLAYERS } = require('../server');

function client(port) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  const inbox = [];
  const waiters = [];
  ws.on('message', (d) => {
    const m = JSON.parse(d.toString());
    const i = waiters.findIndex((w) => w.pred(m));
    if (i >= 0) { const w = waiters[i]; waiters.splice(i, 1); w.resolve(m); }
    else inbox.push(m);
  });
  return {
    ws,
    open: new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej); }),
    send: (m) => ws.send(JSON.stringify(m)),
    close: () => ws.close(),
    terminate: () => ws.terminate(),
    wait(pred, ms = 2000) {
      const hit = inbox.findIndex(pred);
      if (hit >= 0) { const m = inbox[hit]; inbox.splice(hit, 1); return Promise.resolve(m); }
      return new Promise((resolve, reject) => {
        const to = setTimeout(() => reject(new Error(`timeout waiting for message; inbox: ${JSON.stringify(inbox)}`)), ms);
        waiters.push({ pred, resolve: (m) => { clearTimeout(to); resolve(m); } });
      });
    },
    drain: () => inbox.splice(0),
  };
}

async function expectError(c, code, sendMsg) {
  if (sendMsg) c.send(sendMsg);
  const m = await c.wait((x) => x.t === 'error');
  assert.strictEqual(m.code, code, `expected error ${code}, got ${m.code} (${m.msg})`);
}

async function main() {
  const relay = createRelay();
  const port = await relay.listen(0);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let passed = 0;
  const test = async (name, fn) => {
    await fn();
    passed++;
    console.log(`ok - ${name}`);
  };

  await test('create returns a 4-char code and host roster', async () => {
    const a = client(port); await a.open;
    a.send({ t: 'create', name: 'Hosty' });
    const j = await a.wait((m) => m.t === 'joined');
    assert.match(j.code, /^[2-9A-HJKMNP-Z]{4}$/);
    assert.strictEqual(j.hostId, j.id);
    assert.strictEqual(j.players.length, 1);
    assert.ok(j.players[0].host);
    assert.ok(j.token);
    a.drain(); a.terminate(); relay.rooms.clear();
  });

  await test('join with a bad code is rejected', async () => {
    const a = client(port); await a.open;
    await expectError(a, 'badCode', { t: 'join', code: 'ZZZZ', name: 'X' });
    a.terminate();
  });

  await test('roster fills to MAX_PLAYERS then rejects the next joiner', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const { code } = await host.wait((m) => m.t === 'joined');
    const others = [];
    for (let i = 0; i < MAX_PLAYERS - 1; i++) {
      const c = client(port); await c.open;
      c.send({ t: 'join', code, name: 'P' + i });
      await c.wait((m) => m.t === 'joined');
      others.push(c);
    }
    const extra = client(port); await extra.open;
    await expectError(extra, 'roomFull', { t: 'join', code, name: 'Loser' });
    const roster = await host.wait((m) => m.t === 'roster' && m.players.length === MAX_PLAYERS);
    assert.ok(roster);
    extra.terminate();
    for (const c of [host, ...others]) c.terminate();
    relay.rooms.clear();
  });

  await test('state/event messages reach everyone except the sender', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    const bj = await b.wait((m) => m.t === 'joined');
    await b.wait((m) => m.t === 'roster');
    host.drain();
    host.send({ t: 'state', d: { pos: [1, 2, 3] } });
    const got = await b.wait((m) => m.t === 'from');
    assert.strictEqual(got.from, hj.id);
    assert.strictEqual(got.k, 'state');
    assert.deepStrictEqual(got.d, { pos: [1, 2, 3] });
    host.send({ t: 'event', d: { kind: 'flares' } });
    const ev = await b.wait((m) => m.t === 'from' && m.k === 'event');
    assert.strictEqual(ev.from, hj.id);
    host.drain(); b.drain(); host.terminate(); b.terminate(); relay.rooms.clear();
  });

  await test('only the host can change settings and start', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    await b.wait((m) => m.t === 'joined');
    await host.wait((m) => m.t === 'roster');
    await expectError(b, 'notHost', { t: 'settings', d: { ai: { count: 3 } } });
    await expectError(b, 'notHost', { t: 'start' });
    host.send({ t: 'settings', d: { ai: { count: 3 } } });
    const s = await b.wait((m) => m.t === 'settings');
    assert.deepStrictEqual(s.d, { ai: { count: 3 } });
    host.send({ t: 'start', seed: 313 });
    const st = await b.wait((m) => m.t === 'start');
    assert.strictEqual(st.seed, 313);
    host.terminate(); b.terminate(); relay.rooms.clear();
  });

  await test('a dropped non-host player can resume with code+id+token', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    const bj = await b.wait((m) => m.t === 'joined');
    b.terminate();
    const gone = await host.wait((m) => m.t === 'roster' && m.players.some((p) => p.name === 'B' && !p.connected));
    assert.ok(gone);
    const b2 = client(port); await b2.open;
    b2.send({ t: 'resume', code: hj.code, id: bj.id, token: bj.token });
    const back = await b2.wait((m) => m.t === 'joined');
    assert.strictEqual(back.id, bj.id);
    const roster = await host.wait((m) => m.t === 'roster' && m.players.every((p) => p.connected));
    assert.ok(roster);
    await expectError(b2, 'alreadyInRoom', { t: 'create', name: 'Sneaky' });
    host.terminate(); b2.terminate(); relay.rooms.clear();
  });

  await test('resume with a wrong token is rejected', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    const bj = await b.wait((m) => m.t === 'joined');
    b.terminate();
    await host.wait((m) => m.t === 'roster' && m.players.some((p) => !p.connected));
    const evil = client(port); await evil.open;
    await expectError(evil, 'badResume', { t: 'resume', code: hj.code, id: bj.id, token: 'wrongtoken' });
    host.terminate(); evil.terminate(); relay.rooms.clear();
  });

  await test('resume take-over works even while the old socket is still open (zombie)', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    const bj = await b.wait((m) => m.t === 'joined');
    // Old socket stays OPEN (zombie) — server still sees the player as connected.
    const b2 = client(port); await b2.open;
    b2.send({ t: 'resume', code: hj.code, id: bj.id, token: bj.token });
    const back = await b2.wait((m) => m.t === 'joined');
    assert.strictEqual(back.id, bj.id, 'take-over joined as the same player id');
    // The stale socket must get kicked.
    await new Promise((res, rej) => { b.ws.on('close', (code2) => { assert.strictEqual(code2, 4001); res(); }); setTimeout(() => rej(new Error('stale socket was not closed')), 2000); });
    // State from the new socket relays; the old one must not receive it.
    let oldSockGotRelay = false;
    b.ws.on('message', () => { oldSockGotRelay = true; });
    host.send({ t: 'state', d: { x: 1 } });
    const relayed = await b2.wait((m) => m.t === 'from' && m.k === 'state');
    assert.strictEqual(relayed.d.x, 1);
    await sleep(100);
    assert.strictEqual(oldSockGotRelay, false, 'stale socket must not receive room traffic');
    host.terminate(); b.terminate(); b2.terminate(); relay.rooms.clear();
  });

  await test('join after start works (join-in-progress): joined carries started+seed, traffic flows', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    host.send({ t: 'start', seed: 313 });
    await host.wait((m) => m.t === 'start');
    const late = client(port); await late.open;
    late.send({ t: 'join', code: hj.code, name: 'Late' });
    const lj = await late.wait((m) => m.t === 'joined');
    assert.strictEqual(lj.started, true, 'joined reports the match is running');
    assert.strictEqual(lj.seed, 313, 'joined carries the shared world seed');
    const roster = await host.wait((m) => m.t === 'roster' && m.players.length === 2);
    assert.ok(roster);
    // the late joiner receives live traffic immediately
    host.send({ t: 'state', d: { jets: [] } });
    const got = await late.wait((m) => m.t === 'from' && m.from === hj.id);
    assert.ok(got);
    host.terminate(); late.terminate(); relay.rooms.clear();
  });

  await test('host leaving ends the match for everyone', async () => {
    const host = client(port); await host.open;
    host.send({ t: 'create', name: 'H' });
    const hj = await host.wait((m) => m.t === 'joined');
    const b = client(port); await b.open;
    b.send({ t: 'join', code: hj.code, name: 'B' });
    await b.wait((m) => m.t === 'joined');
    host.terminate();
    const ended = await b.wait((m) => m.t === 'ended');
    assert.strictEqual(ended.reason, 'hostLeft');
    await sleep(100);
    assert.strictEqual(relay.rooms.size, 0);
    b.terminate();
  });

  await test('oversized messages are rejected', async () => {
    const a = client(port); await a.open;
    a.send({ t: 'create', name: 'H' });
    await a.wait((m) => m.t === 'joined');
    a.ws.send(JSON.stringify({ t: 'state', d: 'x'.repeat(64 * 1024) }));
    await new Promise((res, rej) => { a.ws.on('close', res); setTimeout(() => rej(new Error('expected close')), 2000); });
  });

  await test('/health responds ok', async () => {
    const http = require('node:http');
    const body = await new Promise((resolve, reject) => {
      http.get(`http://127.0.0.1:${port}/health`, (res) => {
        let s = '';
        res.on('data', (c) => { s += c; });
        res.on('end', () => resolve(s));
      }).on('error', reject);
    });
    const j = JSON.parse(body);
    assert.strictEqual(j.ok, true);
    assert.ok('rooms' in j && 'players' in j);
  });

  await relay.close();
  console.log(`\n${passed} tests passed`);
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
