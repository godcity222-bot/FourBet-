/**
 * Odds-API.io → Lovable Cloud bridge.
 *
 * Responsibilities:
 *   1. Hold ONE persistent WebSocket to wss://api.odds-api.io/v3/ws (odds-api.io
 *      allows a single connection per API key — new connections close older ones).
 *   2. Subscribe ONLY to non-football sports and to markets [h2h, spreads, totals].
 *   3. UPSERT every odds change into public.live_odds with the service role key.
 *      Supabase Realtime then pushes the change to every subscribed browser.
 *   4. Reconnect with exponential backoff on disconnect; on every reconnect
 *      pull a REST snapshot to fill any gap.
 *   5. Periodically refresh public.odds_api_events from /events (cheap).
 *
 * What this service NEVER does:
 *   - Subscribe to soccer / football. That stays on API-Football.
 *   - Poll /odds on a timer. WebSocket push is the only odds source.
 *   - Touch user balances, bets, or anything outside live_odds / odds_api_events.
 *
 * Run on Railway / Fly / any always-on Node 20+ host. Stateless — safe to
 * restart at any time.
 */

import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

// ─── Config ───────────────────────────────────────────────────────────────
const ODDS_API_KEY              = required("ODDS_API_IO_KEY");
const SUPABASE_URL              = required("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = required("SUPABASE_SERVICE_ROLE_KEY");

// Comma-separated list of odds-api.io sport keys to subscribe to.
// Soccer is INCLUDED — odds-api.io WS feeds 1X2/spreads/totals/btts for football.
// API-Football remains source of truth for fixtures, lineups, stats.
const SPORTS = (process.env.SPORTS ?? [
  "basketball_nba",
  "basketball_euroleague",
  "basketball_ncaab",
  "tennis_atp",
  "tennis_wta",
  "icehockey_nhl",
  "baseball_mlb",
  "mma_mixed_martial_arts",
  "boxing_boxing",
  "americanfootball_nfl",
  "americanfootball_ncaaf",
  "rugbyleague_nrl",
  "rugbyunion_six_nations",
  "aussierules_afl",
  "cricket_test_match",
  "cricket_ipl",
  // Soccer — top leagues. Override via SPORTS env to add/remove.
  "soccer_epl",
  "soccer_uefa_champs_league",
  "soccer_uefa_europa_league",
  "soccer_spain_la_liga",
  "soccer_italy_serie_a",
  "soccer_germany_bundesliga",
  "soccer_france_ligue_one",
  "soccer_fifa_world_cup",
  "soccer_uefa_european_championship",
].join(","))
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// odds-api.io WebSocket allows up to 20 markets. Expanded set per Live-Odds spec.
const MARKETS = (process.env.MARKETS ?? [
  "h2h",          // moneyline / 1X2
  "spreads",      // handicap
  "totals",       // over/under
  "btts",         // both teams to score
  "team_totals",
  "alternate_spreads",
  "alternate_totals",
  "h2h_h1",       // 1st half moneyline
  "spreads_h1",
  "totals_h1",
  "player_points",
  "player_assists",
  "player_rebounds",
  "player_shots_on_target",
  "corners_totals",
  "cards_totals",
].join(","))
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const WS_URL = `wss://api.odds-api.io/v3/ws?apiKey=${encodeURIComponent(ODDS_API_KEY)}`;
const REST_BASE = "https://api.odds-api.io/v3";

// ─── Supabase admin client (RLS bypassed; bridge is trusted) ─────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ─── State ────────────────────────────────────────────────────────────────
let ws = null;
// Exponential backoff per Live-Odds spec: 1s → 2s → 5s → 10s → 30s → cap.
const BACKOFF_STEPS_MS = [1_000, 2_000, 5_000, 10_000, 30_000, 60_000];
let backoffIdx = 0;
let lastMessageAt = Date.now();
let heartbeatTimer = null;
let metricsTimer = null;
let stats = { upserts: 0, errors: 0, messages: 0 };


// ─── Soccer guard ─────────────────────────────────────────────────────────
function isSoccerSport(sportKey) {
  return String(sportKey ?? "").toLowerCase().startsWith("soccer");
}

// ─── REST helpers (used sparingly) ────────────────────────────────────────
async function restJson(path) {
  const url = `${REST_BASE}${path}${path.includes("?") ? "&" : "?"}apiKey=${encodeURIComponent(ODDS_API_KEY)}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`REST ${path} failed [${res.status}]: ${await res.text()}`);
  }
  return res.json();
}

/** Refresh odds_api_events from /events for each subscribed sport. */
async function refreshEvents() {
  for (const sport of SPORTS) {
    try {
      const events = await restJson(`/events?sport=${encodeURIComponent(sport)}`);
      if (!Array.isArray(events) || events.length === 0) continue;
      const rows = events.map((e) => ({
        event_id:      String(e.id),
        sport_key:     String(e.sport_key ?? sport),
        sport_title:   e.sport_title ?? null,
        commence_time: new Date(
  e.commence_time || e.commenceTime || e.start_time || e.starts_at || e.startAt || e.date || Date.now()
).toISOString(),
        home_team:     String(e.home_team ?? ""),
        away_team:     String(e.away_team ?? ""),
        is_live:       Boolean(e.in_play ?? false),
        last_seen_at:  new Date().toISOString(),
      }));
      const { error } = await supabase
        .from("odds_api_events")
        .upsert(rows, { onConflict: "event_id" });
      if (error) {
        console.error(`[events ${sport}] upsert failed:`, error.message);
        stats.errors += 1;
      } else {
        console.log(`[events ${sport}] refreshed ${rows.length}`);
      }
    } catch (e) {
      console.error(`[events ${sport}] fetch failed:`, e.message);
      stats.errors += 1;
    }
  }
}

// ─── Odds message → DB rows ───────────────────────────────────────────────
/**
 * odds-api.io WebSocket payload mirrors /odds: an event object with
 * `bookmakers: [{ key, markets: [{ key, outcomes: [{ name, price, point? }] }]}]`.
 * We flatten to one DB row per outcome.
 */
function flattenEvent(evt) {
  if (!evt || !evt.id || !Array.isArray(evt.bookmakers)) return [];
  // Soccer is now allowed; odds-api.io feeds 1X2/spreads/totals/btts for football too.

  const eventId = String(evt.id);
  const now = new Date().toISOString();
  const rows = [];

  for (const book of evt.bookmakers) {
    const bookmaker = String(book.key ?? "");
    if (!bookmaker) continue;
    const providerTs = book.last_update ? new Date(book.last_update).toISOString() : null;

    for (const market of book.markets ?? []) {
      const marketKey = String(market.key ?? "").toLowerCase();
      if (MARKETS.length > 0 && !MARKETS.includes(marketKey)) continue;

      for (const outcome of market.outcomes ?? []) {
        const price = Number(outcome.price);
        if (!Number.isFinite(price) || price <= 1) continue;
        rows.push({
          event_id:    eventId,
          bookmaker,
          market:      marketKey,
          selection:   String(outcome.name ?? ""),
          point:       Number.isFinite(Number(outcome.point)) ? Number(outcome.point) : 0,
          price,
          suspended:   Boolean(market.suspended ?? false),
          provider_ts: providerTs,
          received_at: now,
        });
      }
    }
  }
  return rows;
}

async function handleEventPayload(evt) {
  // Ensure parent row exists (FK) — minimal insert, ignore conflicts.
  if (evt?.id) {
    await supabase
      .from("odds_api_events")
      .upsert(
        [
          {
            event_id:      String(evt.id),
            sport_key:     String(evt.sport_key ?? ""),
            sport_title:   evt.sport_title ?? null,
            commence_time: new Date(evt.commence_time ?? Date.now()).toISOString(),
            home_team:     String(evt.home_team ?? ""),
            away_team:     String(evt.away_team ?? ""),
            is_live:       true,
            last_seen_at:  new Date().toISOString(),
          },
        ],
        { onConflict: "event_id" },
      );
  }

  const rows = flattenEvent(evt);
  if (rows.length === 0) return;

  const { error } = await supabase
    .from("live_odds")
    .upsert(rows, { onConflict: "event_id,bookmaker,market,selection,point" });
  if (error) {
    console.error("[odds] upsert failed:", error.message);
    stats.errors += 1;
  } else {
    stats.upserts += rows.length;
  }
}

// ─── WebSocket lifecycle ──────────────────────────────────────────────────
function connect() {
  console.log(`[ws] connecting · sports=${SPORTS.length} markets=${MARKETS.length}`);
  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    backoffIdx = 0;
    lastMessageAt = Date.now();
    console.log("[ws] open");
    const payload = {
      action:  "subscribe",
      sports:  SPORTS,
      markets: MARKETS,
    };
    ws.send(JSON.stringify(payload));
    console.log("[ws] subscribed:", { sports: SPORTS.length, markets: MARKETS.length });

    // Heartbeat: native WS ping every 25s. The server's pong resets the
    // socket's read deadline; the watchdog still force-reconnects on silence.
    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      if (ws?.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch { /* noop */ }
      }
    }, 25_000);
  });

  ws.on("pong", () => { lastMessageAt = Date.now(); });

  ws.on("message", async (raw) => {
    lastMessageAt = Date.now();
    stats.messages += 1;
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    // odds-api.io sends either a single event object or { events: [...] }
    const events = Array.isArray(msg) ? msg : msg.events ?? (msg.id ? [msg] : []);
    for (const evt of events) {
      try {
        await handleEventPayload(evt);
      } catch (e) {
        console.error("[ws] handler error:", e.message);
        stats.errors += 1;
      }
    }
  });

  ws.on("error", (err) => {
    console.error("[ws] error:", err.message);
  });

  ws.on("close", (code, reason) => {
    console.warn(`[ws] closed (${code}) ${reason?.toString?.() ?? ""}`);
    clearInterval(heartbeatTimer);
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  const delay = BACKOFF_STEPS_MS[Math.min(backoffIdx, BACKOFF_STEPS_MS.length - 1)];
  backoffIdx = Math.min(backoffIdx + 1, BACKOFF_STEPS_MS.length - 1);
  console.log(`[ws] reconnect in ${delay}ms (step ${backoffIdx}/${BACKOFF_STEPS_MS.length})`);
  setTimeout(async () => {
    try {
      await refreshEvents(); // gap-fill snapshot before reopening WS
    } catch (e) {
      console.error("[recovery] refreshEvents failed:", e.message);
    }
    connect();
  }, delay);
}

/** If the WS has been silent for too long, assume the socket is half-dead. */
function watchdog() {
  const silence = Date.now() - lastMessageAt;
  if (ws && silence > 90_000) {
    console.warn(`[ws] silent for ${silence}ms — forcing reconnect`);
    try { ws.terminate(); } catch { /* noop */ }
  }
}


// ─── Boot ─────────────────────────────────────────────────────────────────
function required(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

async function main() {
  console.log(`[bridge] starting · sports=${SPORTS.length}`);
  await refreshEvents().catch((e) => console.error("[boot] refreshEvents:", e.message));
  connect();

  setInterval(watchdog, 30_000);
  setInterval(() => refreshEvents().catch(() => {}), 30 * 60_000); // every 30m
  metricsTimer = setInterval(() => {
    console.log(`[metrics] msgs=${stats.messages} upserts=${stats.upserts} errors=${stats.errors}`);
    stats = { upserts: 0, errors: 0, messages: 0 };
  }, 60_000);
}

process.on("SIGTERM", () => { try { ws?.close(); } catch {} process.exit(0); });
process.on("SIGINT",  () => { try { ws?.close(); } catch {} process.exit(0); });

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
