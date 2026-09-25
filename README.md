# minigame_relay_server

WebSocket room relay for the Dogfight 313 minigame. It does not simulate the
game — it creates rooms joinable by 4-letter codes (up to 4 players), lets the
host set shared settings and the match seed, and fans every player's
state/event messages out to the rest of the room. See `PROTOCOL.md`.

## Run locally

```
npm install
npm start          # listens on :8080 (override with PORT=x)
node test/relay.test.js   # integration test: rooms, relay, resume, host-leave
```

Health check: `GET /health` -> `{ok, rooms, players, uptime}`.

## Deploy to Fly.io

One-time setup (~5 min):

1. Install flyctl: `winget install Fly-io.flyctl` (Windows) or see fly.io/docs.
2. `fly auth signup` (or `fly auth login`).
3. From this folder: `fly deploy`. It reads `fly.toml` + `Dockerfile`, builds,
   and starts the app. If the app name is taken, rename `app` in `fly.toml`.
4. Your address: `wss://bf3-dogfight-relay.fly.dev` (TLS handled by Fly).

Config notes:

- `primary_region = "fra"` (Frankfurt) is the NA/EU compromise; change to
  `iad`, `lhr`, `syd`, ... and re-deploy if your group's geography differs.
- `min_machines_running = 1` keeps it always on (~$1.94/mo for 256 MB).
  Bandwidth is billed per GB (~$0.02 NA/EU); a busy 10-player month is a
  couple of dollars at most.
- Check it after deploy: `fly status`, and browse `https://<app>.fly.dev/health`.
