/**
 * odds-api-bridge v2
 *
 * Single persistent WebSocket from odds-api.io -> Supabase upserts.
 * Stateless, restart-safe, designed for Railway / Fly / any Node 20+ host.
 */

import http from "node:http";
import crypto from "node:crypto";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

// Polyfill native WebSocket for Node < 22 (Supabase realtime requires it)
if (!globalThis.WebSocket) {
  globalThis.WebSocket = WebSocket;
}

// ─── Config ───────────────────────────────────────────────────────────────
const required = (name) => {
  const v = process.env[name];
  if (!v) {
    console.error(`[boot] missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
};

const ODDS_API_KEY              = required("ODDS_API_IO_KEY");
const SUPABASE_URL              = required("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = required("SUPABASE_SERVICE_ROLE_KEY");
const PORT                      = Number(process.env.PORT ?? 3000);
const VERSION                   = "2.0.1";

const KEY_FINGERPRINT = crypto.createHash("sha256")
  .update(ODDS_API_KEY).digest("hex").slice(0, 8);

const SPORTS = (process.env.SPORTS ?? [
  "football", "basketball", "tennis", "baseball", "american-football",
  "ice-hockey", "mixed-martial-arts", "boxing", "rugby", "cricket",
].join(",")).split(",").map(s => s.trim()).filter(Boolean);

const MARKETS = (process.env.MARKETS ??
  "ML,Spread,Totals,BTTS,DC,DNB,ALT_Spread,ALT_Totals,Team_Totals," +
  "H2H_H1,Totals_H1,Spread_H1,BTTS_H1,DC_H1,Corners_Totals")
  .split(",").map(s => s.trim()).filter(Boolean);

const wsParams = new URLSearchParams({ apiKey: ODDS_API_KEY, markets: MARKETS.join(",") });
if (SPORTS.length && SPORTS.length <= 10) wsParams.set("sport", SPORTS.join(","));
const WS_URL = `wss://api.odds-api.io/v3/ws?${wsParams.toString()}`;

const EVENT_ID_PREFIX = process.env.EVENT_ID_PREFIX ?? "oddsapi:";
const BACKOFF_MS = [1_000, 2_000, 5_000, 10_000, 30_000, 60_000];
const WATCHDOG_MS = 90_000;
const METRICS_INTERVAL_MS = 30_000;

// ─── Supabase ─────────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ─── State ────────────────────────────────────────────────────────────────
let ws = null;
let backoffIdx = 0;
let lastMessageAt = Date.now();
let shuttingDown = false;
let reconnectTimer = null;

const stats = {
  startedAt: new Date().toISOString(),
  messages: 0,
  upserts: 0,
  parentUpserts: 0,
  errors: 0,
  reconnects: 0,
  lastError: null,
  lastUpsertAt: null,
};

// ─── Helpers ──────────────────────────────────────────────────────────────
const safeIso = (v, fb = new Date()) => {
  const d = new Date(v ?? fb);
  return Number.isNaN(d.getTime()) ? fb.toISOString() : d.toISOString();
};

const toDbMarket = (n) => {
  const k = String(n ?? "").trim().toLowerCase();
  if (k === "ml") return "h2h";
  if (k === "spread") return "spreads";
  if (k === "alt_spread" || k === "alternate_spread") return "alternate_spreads";
  if (k === "totals") return "totals";
  if (k === "alt_totals" || k === "alternate_totals") return "alternate_totals";
  if (k === "team_totals" || k === "tt") return "team_totals";
  if (k === "btts" || k === "both_teams_to_score") return "btts";
  if (k === "dc" || k === "double_chance") return "double_chance";
  if (k === "dnb" || k === "draw_no_bet") return "draw_no_bet";
  if (k === "h2h_h1" || k === "ml_h1") return "h2h_h1";
  if (k === "totals_h1") return "totals_h1";
  if (k === "spread_h1" || k === "spreads_h1") return "spreads_h1";
  if (k === "btts_h1") return "btts_h1";
  if (k === "dc_h1" || k === "double_chance_h1") return "double_chance_h1";
  if (k === "corners_totals") return "corners_totals";
  return k;
};

const SUPPORTED_MARKETS = new Set([
  "h2h", "spreads", "totals", "btts", "double_chance", "draw_no_bet",
  "alternate_spreads", "alternate_totals", "team_totals",
  "h2h_h1", "totals_h1", "spreads_h1", "btts_h1", "double_chance_h1",
  "corners_totals",
]);

const MARKET_BASE = {
  spreads: "spreads",
  alternate_spreads: "spreads",
  totals: "totals",
  alternate_totals: "totals",
  corners_totals: "totals",
  btts: "btts",
  btts_h1: "btts",
  double_chance: "double_chance",
  double_chance_h1: "double_chance",
  h2h: "h2h",
  h2h_h1: "h2h",
  spreads_h1: "spreads",
  totals_h1: "totals",
  draw_no_bet: "draw_no_bet",
  team_totals: "team_totals",
};

const isLive = (v) => {
  const s = String(v ?? "").toLowerCase();
  return s === "live" || s === "in_play" || s === "inplay";
};

function rememberError(stage, err) {
  const msg = err instanceof Error ? err.message : String(err ?? "unknown");
  stats.errors += 1;
  stats.lastError = { at: new Date().toISOString(), stage, message: msg };
  console.error(`[err:${stage}] ${msg}`);
}

// ─── Parent row (odds_api_events) ─────────────────────────────────────────
async function upsertEventRow(evt) {
  if (!evt?.id) return;
  const row = {
    event_id: `${EVENT_ID_PREFIX}${String(evt.id)}`,
    sport_key: String(evt.sport?.slug ?? evt.sport_key ?? ""),
    sport_title: evt.sport?.name ?? evt.sport_title ?? evt.league?.name ?? null,
    commence_time: safeIso(evt.date ?? evt.commence_time),
    home_team: String(evt.home ?? evt.home_team ?? ""),
    away_team: String(evt.away ?? evt.away_team ?? ""),
    is_live: isLive(evt.status ?? evt.in_play),
    last_seen_at: new Date().toISOString(),
  };
  if (!row.sport_key && !row.home_team && !row.away_team) return;

  const { error } = await supabase
    .from("odds_api_events")
    .upsert([row], { onConflict: "event_id" });
  if (error) {
    rememberError("parent_upsert", error);
    throw error;
  }
  stats.parentUpserts += 1;
}

// ─── Flatten WS event to live_odds rows ───────────────────────────────────
function flattenEvent(evt) {
  if (!evt?.id || !Array.isArray(evt.markets)) return [];
  const bookmaker = String(evt.bookie ?? evt.bookmaker ?? "");
  if (!bookmaker) return [];

  const eventId = `${EVENT_ID_PREFIX}${String(evt.id)}`;
  const now = new Date().toISOString();
  const homeName = evt.home ?? evt.home_team ?? "Home";
  const awayName = evt.away ?? evt.away_team ?? "Away";
  const rows = [];

  for (const market of evt.markets) {
    const dbMarket = toDbMarket(market.name);
    if (!SUPPORTED_MARKETS.has(dbMarket)) continue;
    const baseType = MARKET_BASE[dbMarket] ?? dbMarket;
    const providerTs = market.updatedAt ? safeIso(market.updatedAt) : now;

    for (const odd of market.odds ?? []) {
      if (baseType === "h2h") {
        const trio = [
          { selection: homeName, price: odd.home },
          { selection: "Draw",   price: odd.draw },
          { selection: awayName, price: odd.away },
        ];
        for (const t of trio) {
          const p = Number(t.price);
          if (!Number.isFinite(p) || p <= 1) continue;
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: t.selection, price: p, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "spreads") {
        const home = Number(odd.home), away = Number(odd.away);
        const point = Number(odd.hcp ?? odd.handicap ?? odd.point);
        if (Number.isFinite(home) && home > 1 && Number.isFinite(point)) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: homeName, price: home, point,
            received_at: now, provider_updated_at: providerTs,
          });
        }
        if (Number.isFinite(away) && away > 1 && Number.isFinite(point)) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: awayName, price: away, point: -point,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "totals") {
        const over = Number(odd.over), under = Number(odd.under);
        const point = Number(odd.total ?? odd.point);
        if (!Number.isFinite(point)) continue;
        if (Number.isFinite(over) && over > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: "Over", price: over, point,
            received_at: now, provider_updated_at: providerTs,
          });
        }
        if (Number.isFinite(under) && under > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: "Under", price: under, point,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "btts") {
        const yes = Number(odd.yes ?? odd.Yes);
        const no  = Number(odd.no  ?? odd.No);
        if (Number.isFinite(yes) && yes > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: "Yes", price: yes, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
        if (Number.isFinite(no) && no > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: "No", price: no, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "double_chance") {
        const pairs = [
          { sel: "1X", price: odd.homeDraw ?? odd["1X"] ?? odd.home_draw },
          { sel: "12", price: odd.homeAway ?? odd["12"] ?? odd.home_away },
          { sel: "X2", price: odd.drawAway ?? odd["X2"] ?? odd.draw_away },
        ];
        for (const t of pairs) {
          const p = Number(t.price);
          if (!Number.isFinite(p) || p <= 1) continue;
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: t.sel, price: p, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "draw_no_bet") {
        const home = Number(odd.home);
        const away = Number(odd.away);
        if (Number.isFinite(home) && home > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: homeName, price: home, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
        if (Number.isFinite(away) && away > 1) {
          rows.push({
            event_id: eventId, bookmaker, market: dbMarket,
            selection: awayName, price: away, point: null,
            received_at: now, provider_updated_at: providerTs,
          });
        }
      } else if (baseType === "team_totals") {
        const point = Number(odd.total ?? odd.point);
        if (!Number.isFinite(point)) continue;
        const side = String(odd.team ?? odd.side ?? "").toLowerCase();
        const teamName = side === "home" ? homeName : side === "away" ? awayName : null;
        if (teamName) {
          const over = Number(odd.over);
          const under = Number(odd.under);
          if (Number.isFinite(over) && over > 1) {
            rows.push({
              event_id: eventId, bookmaker, market: dbMarket,
              selection: `${teamName} Over`, price: over, point,
              received_at: now, provider_updated_at: providerTs,
            });
          }
          if (Number.isFinite(under) && under > 1) {
            rows.push({
              event_id: eventId, bookmaker, market: dbMarket,
              selection: `${teamName} Under`, price: under, point,
              received_at: now, provider_updated_at: providerTs,
            });
          }
        } else {
          const ho = Number(odd.home_over), hu = Number(odd.home_under);
          const ao = Number(odd.away_over), au = Number(odd.away_under);
          if (Number.isFinite(ho) && ho > 1) rows.push({ event_id: eventId, bookmaker, market: dbMarket, selection: `${homeName} Over`, price: ho, point, received_at: now, provider_updated_at: providerTs });
          if (Number.isFinite(hu) && hu > 1) rows.push({ event_id: eventId, bookmaker, market: dbMarket, selection: `${homeName} Under`, price: hu, point, received_at: now, provider_updated_at: providerTs });
          if (Number.isFinite(ao) && ao > 1) rows.push({ event_id: eventId, bookmaker, market: dbMarket, selection: `${awayName} Over`, price: ao, point, received_at: now, provider_updated_at: providerTs });
          if (Number.isFinite(au) && au > 1) rows.push({ event_id: eventId, bookmaker, market: dbMarket, selection: `${awayName} Under`, price: au, point, received_at: now, provider_updated_at: providerTs });
        }
      }
    }
  }
  return rows;
}

async function handleEvent(evt) {
  try {
    await upsertEventRow(evt);
  } catch {
    return;
  }
  const rows = flattenEvent(evt);
  if (!rows.length) return;

  const { error } = await supabase
    .from("live_odds")
    .upsert(rows, { onConflict: "event_id,bookmaker,market,selection,point" });
  if (error) {
    rememberError("live_odds_upsert", error);
    return;
  }
  stats.upserts += rows.length;
  stats.lastUpsertAt = new Date().toISOString();
}

// ─── WebSocket lifecycle ──────────────────────────────────────────────────
function scheduleReconnect() {
  if (shuttingDown) return;
  const delay = BACKOFF_MS[Math.min(backoffIdx, BACKOFF_MS.length - 1)];
  backoffIdx += 1;
  stats.reconnects += 1;
  console.warn(`[ws] reconnecting in ${delay}ms (attempt ${backoffIdx})`);
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connect, delay);
}

function connect() {
  if (shuttingDown) return;
  console.log(`[ws] connecting key=${KEY_FINGERPRINT} sports=${SPORTS.length} markets=${MARKETS.join(",")}`);

  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log("[ws] open");
    backoffIdx = 0;
    lastMessageAt = Date.now();
  });

  ws.on("message", async (data) => {
    lastMessageAt = Date.now();
    stats.messages += 1;
    let payload;
    try { payload = JSON.parse(data.toString()); }
    catch (e) { rememberError("parse", e); return; }

    const events = Array.isArray(payload?.events)
      ? payload.events
      : (payload?.id ? [payload] : []);

    for (const evt of events) {
      await handleEvent(evt);
    }
  });

  ws.on("close", (code, reason) => {
    console.warn(`[ws] close code=${code} reason=${reason?.toString() || "(none)"}`);
    scheduleReconnect();
  });

  ws.on("error", (err) => {
    rememberError("ws", err);
    try { ws.close(); } catch {}
  });
}

// ─── Watchdog ─────────────────────────────────────────────────────────────
setInterval(() => {
  const age = Date.now() - lastMessageAt;
  if (age > WATCHDOG_MS && ws?.readyState === WebSocket.OPEN) {
    console.warn(`[watchdog] no messages for ${age}ms — forcing reconnect`);
    try { ws.terminate(); } catch {}
  }
}, 15_000);

// ─── Metrics log ──────────────────────────────────────────────────────────
setInterval(() => {
  console.log(
    `[metrics] msgs=${stats.messages} upserts=${stats.upserts} `
    + `parent=${stats.parentUpserts} errors=${stats.errors} `
    + `reconnects=${stats.reconnects} wsOpen=${ws?.readyState === WebSocket.OPEN}`
  );
}, METRICS_INTERVAL_MS);

// ─── HTTP /health ─────────────────────────────────────────────────────────
http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    const body = {
      status: ws?.readyState === WebSocket.OPEN ? "ok" : "degraded",
      version: VERSION,
      wsConnected: ws?.readyState === WebSocket.OPEN,
      lastMessageAgeMs: Date.now() - lastMessageAt,
      keyFingerprint: KEY_FINGERPRINT,
      sports: SPORTS,
      markets: MARKETS,
      stats,
    };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(body, null, 2));
    return;
  }
  res.writeHead(404); res.end();
}).listen(PORT, () => console.log(`[http] /health on :${PORT}`));

// ─── Graceful shutdown ────────────────────────────────────────────────────
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log(`[shutdown] ${sig}`);
    shuttingDown = true;
    try { ws?.close(); } catch {}
    setTimeout(() => process.exit(0), 500);
  });
}

connect();
