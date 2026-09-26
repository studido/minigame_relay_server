// Probe the LIVE relay: create room, send {t:'team'}, check roster echo.
const WebSocket = require('ws');

const URL = 'wss://bf3-dogfight-relay.fly.dev';
const name = 'probe-' + Math.floor(Math.random() * 10000);

const ws = new WebSocket(URL);
let done = false;

function bail(msg, code) {
  if (done) return;
  done = true;
  console.log(msg);
  ws.close();
  process.exit(code);
}

setTimeout(() => bail('TIMEOUT: no roster echo with team (or connect failed)', 2), 12000);

ws.on('open', () => { console.log('ws open'); ws.send(JSON.stringify({ t: 'create', name })); });
ws.on('message', (raw) => {
  let m; try { m = JSON.parse(raw); } catch { return; }
  console.log('recv:', JSON.stringify(m));
  if (m.t === 'joined') {
    console.log('joined ok, sending team=1');
    ws.send(JSON.stringify({ t: 'team', v: 1 }));
  } else if (m.t === 'roster') {
    const me = m.players.find((p) => p.name === name);
    console.log('roster received:', JSON.stringify(m.players));
    if (me && me.team === 1) bail('PASS: live relay echoes team=1 in roster', 0);
    bail(`FAIL: roster does not reflect team pick (me=${JSON.stringify(me)})`, 1);
  } else if (m.t === 'error') {
    bail(`SERVER ERROR: ${m.code} ${m.msg || ''}`, 1);
  }
});
ws.on('error', (e) => bail('CONNECT ERROR: ' + e.message, 3));
