// Fourbet Realtime Odds Worker
// Connects to odds-api.io WebSocket and forwards normalized odds rows
// to the app's public ingest endpoint.
//
// Required env vars:
//   ODDS_API_IO_KEY        — odds-api.io API key
//   INGEST_URL             — https://www.fourbet.cc/api/public/hooks/sync-odds-realtime
//   REALTIME_INGEST_KEY    — must match the secret stored in Lovable Cloud
//
// Optional:
//   SPORTS                 — comma-separated; defaults to all supported sports
//   MARKETS                — comma-separated odds-api.io market codes;
//                            default "ML,Spread,Totals,BTTS,DC,DNB"
//   FLUSH_MS               — batch flush interval; default 1500

import WebSocket from "ws";

const API_KEY = process.env.ODDS_API_IO_KEY;
const INGEST_URL = process.env.INGEST_URL;
const INGEST_KEY = process.env.REALTIME_INGEST_KEY;
const FLUSH_MS = Number(process.env.FLUSH_MS || 1500);

const SPORTS = (
  process.env.SPORTS ||
  "football,basketball,baseball,hockey,tennis,mma,americanfootball,rugby"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const MARKETS = (process.env.MARKETS || "ML,Spread,Totals,BTTS,DC,DNB")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (!API_KEY || !INGEST_URL || !INGEST_KEY) {
  console.error("Missing env: ODDS_API_IO_KEY / INGEST_URL / REALTIME_INGEST_KEY");
  process.exit(1);
}

const MARKET_MAP = {
  ML: "h2h",
  Spread: "spreads",
  Totals: "totals",
  BTTS: "btts",
  DC: "double_chance",
  DNB: "draw_no_bet",
};

// --- Backoff config -------------------------------------------------------
// Provider sends 1013 ("Try Again Later") when we reconnect too fast.
// Respect it with exponential backoff per sport. Reset only after a stable
// connection (>= STABLE_MS without a close).
const BASE_BACKOFF_MS = 5_000;        // start delay
const MAX_BACKOFF_MS  = 5 * 60_000;   // cap 5 min
const COOLDOWN_BACKOFF_MS = 30_000;   // min wait when server says 1013
const STABLE_MS = 60_000;             // mark "healthy" after 60s open
const JITTER_PCT = 0.25;

const backoff = new Map();  // sport -> current backoff ms
const timers  = new Map();  // sport -> pending reconnect timer

let buffer = [];
let lastSeq = null;

function pushRows(rows) {
  if (!rows.length) return;
  buffer.push(...rows);
}

async function flush() {
  if (buffer.length === 0) return;
  const batch = buffer.splice(0, 500);
  try {
    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${INGEST_KEY}`,
      },
      body: JSON.stringify({ rows: batch }),
    });
    if (!res.ok) {
      console.error("[ingest] non-2xx", res.status, await res.text());
      if (buffer.length < 5000) buffer.unshift(...batch);
    } else {
      const j = await res.json().catch(() => ({}));
      console.log(
        `[ingest] ok received=${j.received} upserted=${j.upserted} skipped=${j.skipped_unknown_event}`,
      );
    }
  } catch (e) {
    console.error("[ingest] error", e?.message || e);
    if (buffer.length < 5000) buffer.unshift(...batch);
  }
}

setInterval(flush, FLUSH_MS);

function normalize(msg) {
  if (msg.type !== "updated" || !msg.id || !Array.isArray(msg.markets)) return [];
  const eventId = String(msg.id);
  const bookmaker = String(msg.bookie || "unknown");
  const providerTs = msg.timestamp
    ? new Date(Number(msg.timestamp) * 1000).toISOString()
    : null;

  const out = [];
  for (const m of msg.markets) {
    const dbMarket = MARKET_MAP[m.name];
    if (!dbMarket) continue;
    const oddsList = Array.isArray(m.odds) ? m.odds : [];
    for (const o of oddsList) {
      if (dbMarket === "h2h") {
        if (o.home != null) out.push(row(eventId, bookmaker, "h2h", "home", 0, o.home, providerTs));
        if (o.draw != null) out.push(row(eventId, bookmaker, "h2h", "draw", 0, o.draw, providerTs));
        if (o.away != null) out.push(row(eventId, bookmaker, "h2h", "away", 0, o.away, providerTs));
      } else if (dbMarket === "totals") {
        const point = num(o.hdp);
        if (point == null) continue;
        if (o.over != null)  out.push(row(eventId, bookmaker, "totals", "over",  point, o.over,  providerTs));
        if (o.under != null) out.push(row(eventId, bookmaker, "totals", "under", point, o.under, providerTs));
      } else if (dbMarket === "spreads") {
        const point = num(o.hdp);
        if (point == null) continue;
        if (o.home != null) out.push(row(eventId, bookmaker, "spreads", "home",  point, o.home, providerTs));
        if (o.away != null) out.push(row(eventId, bookmaker, "spreads", "away", -point, o.away, providerTs));
      } else if (dbMarket === "btts") {
        const yes = o.yes ?? o.Yes ?? o.YES;
        const no  = o.no  ?? o.No  ?? o.NO;
        if (yes != null) out.push(row(eventId, bookmaker, "btts", "yes", 0, yes, providerTs));
        if (no  != null) out.push(row(eventId, bookmaker, "btts", "no",  0, no,  providerTs));
      } else if (dbMarket === "double_chance") {
        const homeDraw = o["1x"] ?? o["1X"] ?? o.home_draw;
        const homeAway = o["12"] ?? o.home_away;
        const awayDraw = o["x2"] ?? o["X2"] ?? o.away_draw;
        if (homeDraw != null) out.push(row(eventId, bookmaker, "double_chance", "1x", 0, homeDraw, providerTs));
        if (homeAway != null) out.push(row(eventId, bookmaker, "double_chance", "12", 0, homeAway, providerTs));
        if (awayDraw != null) out.push(row(eventId, bookmaker, "double_chance", "x2", 0, awayDraw, providerTs));
      } else if (dbMarket === "draw_no_bet") {
        if (o.home != null) out.push(row(eventId, bookmaker, "draw_no_bet", "home", 0, o.home, providerTs));
        if (o.away != null) out.push(row(eventId, bookmaker, "draw_no_bet", "away", 0, o.away, providerTs));
      }
    }
  }
  return out;
}

function row(event_id, bookmaker, market, selection, point, price, provider_ts) {
  return {
    event_id,
    bookmaker,
    market,
    selection,
    point: Number(point) || 0,
    price: Number(price),
    suspended: false,
    ...(provider_ts ? { provider_ts } : {}),
  };
}

function num(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function jitter(ms) {
  const delta = ms * JITTER_PCT;
  return Math.floor(ms - delta + Math.random() * 2 * delta);
}

function scheduleReconnect(sport, closeCode) {
  // Don't stack timers
  if (timers.has(sport)) return;

  const prev = backoff.get(sport) ?? BASE_BACKOFF_MS;
  // 1013 = Try Again Later → force a strong cooldown floor
  const floor = closeCode === 1013 ? COOLDOWN_BACKOFF_MS : BASE_BACKOFF_MS;
  const next  = Math.min(MAX_BACKOFF_MS, Math.max(floor, prev * 2));
  backoff.set(sport, next);

  const wait = jitter(next);
  console.log(`[ws:${sport}] reconnect in ${Math.round(wait / 1000)}s (code=${closeCode})`);
  const t = setTimeout(() => {
    timers.delete(sport);
    connect(sport);
  }, wait);
  timers.set(sport, t);
}

function connect(sport) {
  const url = new URL("wss://api.odds-api.io/v3/ws");
  url.searchParams.set("apiKey", API_KEY);
  url.searchParams.set("sport", sport);
  url.searchParams.set("markets", MARKETS.join(","));
  if (lastSeq != null) url.searchParams.set("lastSeq", String(lastSeq));

  console.log(`[ws:${sport}] connecting`);
  const ws = new WebSocket(url.toString());

  let alive = true;
  let stableTimer = null;
  let closed = false;

  const ping = setInterval(() => {
    if (!alive) {
      try { ws.terminate(); } catch {}
      return;
    }
    alive = false;
    try { ws.ping(); } catch {}
  }, 30_000);

  ws.on("pong", () => (alive = true));

  ws.on("open", () => {
    console.log(`[ws:${sport}] open`);
    // If we stay connected for STABLE_MS, reset backoff for this sport
    stableTimer = setTimeout(() => {
      backoff.set(sport, BASE_BACKOFF_MS);
    }, STABLE_MS);
  });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.seq != null) lastSeq = Math.max(lastSeq ?? 0, Number(msg.seq) || 0);
    const rows = normalize(msg);
    if (rows.length) pushRows(rows);
  });

  ws.on("error", (e) => {
    // Avoid noisy stack — close handler will schedule reconnect
    console.error(`[ws:${sport}] error ${e?.message || e}`);
  });

  ws.on("close", (code, reasonBuf) => {
    if (closed) return;
    closed = true;
    clearInterval(ping);
    if (stableTimer) clearTimeout(stableTimer);
    const reason = reasonBuf?.toString?.() || "";
    console.warn(`[ws:${sport}] closed code=${code} reason=${reason}`);
    scheduleReconnect(sport, code);
  });
}

// Stagger initial connections so we don't open 8 sockets in the same tick
SPORTS.forEach((sport, i) => {
  setTimeout(() => connect(sport), i * 750);
});

console.log(
  `[worker] started for sports: ${SPORTS.join(", ")} markets: ${MARKETS.join(",")}`,
);

// Graceful shutdown so Railway redeploys are clean
for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, () => {
    console.log(`[worker] ${sig} received, flushing…`);
    flush().finally(() => process.exit(0));
  });
}
