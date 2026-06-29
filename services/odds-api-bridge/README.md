# odds-api-bridge (v2)

Persistent WebSocket bridge from **odds-api.io** -> **Supabase `live_odds`**.

Why a separate service: odds-api.io allows two WS connections per key, and Cloudflare Workers cap requests at ~30s — so the bridge runs on Railway (always-on Node) and the Lovable app subscribes via Supabase Realtime.

## Deploy on Railway

1. New Project -> Deploy from GitHub -> select this repo.
2. Settings -> **Root Directory** = `services/odds-api-bridge`.
3. Variables — add the three required:
   - `ODDS_API_IO_KEY`
   - `SUPABASE_URL` (`https://<project-ref>.supabase.co` בלבד — בלי `/rest/v1` ובלי `/` בסוף)
   - `SUPABASE_SERVICE_ROLE_KEY`
   - Remove any legacy injected-code variables if they exist: `BRIDGE_DEPLOY_LABEL`, `BRIDGE_INDEX_B64`, `BRIDGE_NORMALIZE_B64`, `BRIDGE_PACKAGE_B64`, `BOOT_PATCH`, `BRIDGE_PATCH`, `ODDS_API_BRIDGE`, `ODDS_API_BRIDGE_CODE`, `INDEX_JS`, `PATCH`, `BRIDGE_VERSION`.
4. In **Settings → Deploy**, verify the Start Command is exactly `npm start`.
   It must not contain `BOOT_PATCH`, `node -e`, `echo $... > index.js`, or any
   command that writes `/app/index.js` from an environment variable.
5. Use **Redeploy without cache / Clear build cache**. Logs should show:
   ```
   [preflight] OK: no legacy bridge patch variables detected
   [boot] capacity: 1 key(s) × 2 WS × 10 sports = 20 sport-slots
   [ws#0] connecting ... markets=0 url=wss://api.odds-api.io/v3/ws?apiKey=***&channels=odds%2Cscores%2Cstatus&sport=...
   [ws#1] connecting ... markets=0 url=wss://api.odds-api.io/v3/ws?apiKey=***&channels=odds%2Cscores%2Cstatus&sport=...
   [metrics] msgs=... upserts=... queue=0
   ```

## Health

`GET /health` returns JSON with `status`, `wsConnected`, `lastMessageAgeMs`, `stats`.

## What it does

- Holds two persistent WebSocket connections per API key to `wss://api.odds-api.io/v3/ws`.
- Omits the `markets` parameter so odds-api.io streams all available markets.
- Upserts each odds change into `public.live_odds` (and parent row in `odds_api_events`).
- Reconnects with exponential backoff. Watchdog forces reconnect after 90s of silence.

## What it never does

- No REST `/events` or `/odds` polling.
- No touching of `bets` / `transactions` / `profiles`.
- No `markets` parameter in the WS URL. If Railway logs show `&markets=`, it is running an old injected patch or wrong service/root directory.
- No `[boot-patch]` line. If that appears, Railway is still running a legacy
  environment/start-command patch before this package is loaded; delete it from
  both service variables and shared/project variables, then redeploy without cache.
