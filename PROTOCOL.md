# Relay protocol

All messages are JSON over WebSocket. The server knows nothing about gameplay;
it manages rooms and relays. Unknown `t` values from room members are dropped.

## Client -> server

| Message | When | Meaning |
|---|---|---|
| `{t:'create', name}` | lobby | Create a room, you become host |
| `{t:'join', code, name}` | lobby | Join a room by code (max 4 players) |
| `{t:'resume', code, id, token}` | after a dropped socket | Reclaim your slot (within ~1 min) |
| `{t:'settings', d}` | host only, lobby | Arbitrary settings object, stored and broadcast |
| `{t:'ready', v}` | lobby | Set ready flag (roster broadcast) |
| `{t:'start', seed}` | host only | Start the match; seed defaults to server random |
| `{t:'state', d}` | in match, ~30 Hz | Own jets' snapshots; fanned out to the room |
| `{t:'event', d}` | in match | Discrete events (fire, hit, flares, kill...) |
| `{t:'leave'}` | any time | Leave the room |

## Server -> client

| Message | Meaning |
|---|---|
| `{t:'joined', code, id, token, hostId, settings, seed, started, players}` | Room joined (keep `id`/`token` for resume) |
| `{t:'roster', players:[{id,name,host,ready,connected}]}` | Room membership changed |
| `{t:'settings', d}` | Host changed settings |
| `{t:'start', seed}` | Match started, use this terrain/world seed |
| `{t:'from', from, k:'state'\|'event', d}` | Relayed message from player `from` |
| `{t:'ended', reason}` | Room closed (`hostLeft`, `empty`, `shutdown`); match is over |
| `{t:'error', code, msg}` | Rejected request |

Error codes: `badCode`, `roomFull`, `badName`, `badResume`, `notHost`, `noRoom`,
`alreadyInRoom`, `serverFull`.

## Notes

- If the host's socket drops, the room ends immediately (`ended`, `hostLeft`).
  Host migration may come later; for now the group re-joins a new room.
- A dropped non-host slot is kept for ~1 minute; `resume` with the same
  `code`, `id` and `token` reclaims it. A wrong token gets `badResume`.
- Resume is a take-over: the token authenticates the player, so a `resume` that
  arrives while the old socket still looks alive (zombie/close-limbo after a
  network drop) succeeds and severs the stale socket with close code 4001.
  Clients must therefore only auto-resume a session they own — explicit create/
  join actions should start fresh.
- Rate limit: 50 msgs/s sustained, 120 burst per connection; sustained abuse
  closes the socket. Payload cap 16 KB per message. Keepalives: server pings
  every 25 s; a connection silent for 90 s is terminated.
- `GET /health` returns `{ok, rooms, players, uptime}` for uptime checks.
