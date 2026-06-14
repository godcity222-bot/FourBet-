/**
 * odds-api-bridge v2.0.2
 *
 * Single persistent WebSocket from odds-api.io -> Supabase upserts.
 * Stateless, restart-safe, designed for Railway / Fly / any Node 20+ host.
 */

import http from "node:http";
import crypto from "node:crypto";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

// WebSocket polyfill for supabase-js realtime
if (!globalThis.WebSocket) {
  globalThis.WebSocket = WebSocket;
}

// ─── Config ───────────────────────────────────────────────────────────────
const required = (name) => {
  const v = process.env[name];
  if (!v) {
    console.error(`[fatal] missing env ${name}`);
    process.exit(1);
  }
  return v;
};

const VERSION = "2.0.2";
const PORT = Number(process.env.PORT || 8080);
const ODDS_API_KEY = required("ODDS_API_IO_KEY");
const SUPABASE_URL = required("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = required("SUPABASE_SERVICE_ROLE_KEY");

const SPORTS = (process.env.SPORTS ||
  "soccer,basketball,tennis,baseball,americanfootball,icehockey,mma,boxing,rugby,cricket"
).split(",").map(s => s.trim()).filter(Boolean);

const MARKETS = (process.env.MARKETS ||
  "ML,Spread,Totals,BTTS,DC,DNB,ALT_Spread,ALT_Totals,Team_Totals,H2H_H1,Totals_H1,Spread_H1,BTTS_H1,DC_H1,Corners_Totals"
).split(",").map(s => s.trim()).filter(Boolean);

const WS_URL = `wss://api.odds-api.io/v3/stream?apiKey=${ODDS_API_KEY}&sports=${SPORTS.join(",")}&markets=${MARKETS.join(",")}`;

// ─── Supabase ─────────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ─── State ────────────────────────────────────────────────────────────────
let ws = null;
let shuttingDown = false;
let reconnectAttempts = 0;
let lastMessageAt = null;
let messagesReceived = 0;
let rowsUpserted = 0;
let parseErrors = 0;
const pending = new Map(); // key -> row, flushed in batches

// ─── Helpers ──────────────────────────────────────────────────────────────
function maskKey(k) { return k ? `${k.slice(0, 4)}...${k.slice(-4)}` : ""; }

function handleMessage(msg) {
  if (!msg || typeof msg !== "object") return;
  // Expected shape from odds-api.io:
  // { eventId, bookmaker, market, selection, price, point?, updatedAt }
  const eventId = msg.eventId || msg.event_id;
  const bookmaker = msg.bookmaker;
  const market = msg.market;
  const selection = msg.selection;
  const price = msg.price ?? msg.odds;
  if (!eventId || !bookmaker || !market || !selection || price == null) return;

  const key = `${eventId}|${bookmaker}|${market}|${selection}`;
  pending.set(key, {
    event_id: String(eventId).startsWith("oddsapi:") ? eventId : `oddsapi:${eventId}`,
    bookmaker,
    market,
    selection,
    price: Number(price),
    point: msg.point != null ? Number(msg.point) : null,
    provider_updated_at: msg.updatedAt ? new Date(msg.updatedAt).toISOString() : new Date().toISOString(),
    received_at: new Date().toISOString(),
  });
}

async function flush() {
  if (pending.size === 0) return;
  const rows = Array.from(pending.values());
  pending.clear();
  try {
    const { error } = await supabase
      .from("live_odds")
      .upsert(rows, { onConflict: "event_id,bookmaker,market,selection" });
    if (error) {
      console.error("[err:upsert]", error.message);
    } else {
      rowsUpserted += rows.length;
    }
  } catch (e) {
    console.error("[err:flush]", e.message);
  }
}
setInterval(flush, 1000);

// ─── WebSocket ────────────────────────────────────────────────────────────
function connect() {
  if (shuttingDown) return;
  console.log(`[ws] connecting key=${maskKey(ODDS_API_KEY)} sports=${SPORTS.length} markets=${MARKETS.join(",")}`);
  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log("[ws] open");
    reconnectAttempts = 0;
  });

  ws.on("message", (raw) => {
    messagesReceived++;
    lastMessageAt = new Date().toISOString();
    const text = raw.toString().trim();
    if (!text) return;

    // odds-api.io sometimes packs multiple JSON objects per frame,
    // separated by \n (NDJSON) or concatenated directly.
    const lines = text.split(/\r?\n/).filter(Boolean);

    for (const line of lines) {
      try {
        handleMessage(JSON.parse(line));
      } catch {
        // Fallback: scan for balanced {...} chunks
        let depth = 0, start = 0, inStr = false, esc = false, found = 0;
        for (let i = 0; i < line.length; i++) {
          const c = line[i];
          if (inStr) {
            if (esc) esc = false;
            else if (c === "\\") esc = true;
            else if (c === '"') inStr = false;
          } else {
            if (c === '"') inStr = true;
            else if (c === "{") { if (depth === 0) start = i; depth++; }
            else if (c === "}") {
              depth--;
              if (depth === 0) {
                try { handleMessage(JSON.parse(line.slice(start, i + 1))); found++; }
                catch { /* skip bad chunk */ }
              }
            }
          }
        }
        if (found === 0) {
          parseErrors++;
          if (parseErrors % 100 === 1) {
            console.error(`[err:parse] total=${parseErrors} sample-len=${line.length}`);
          }
        }
      }
    }
  });

  ws.on("close", (code, reason) => {
    console.log(`[ws] close code=${code} reason=${reason?.toString() || ""}`);
    if (shuttingDown) return;
    reconnectAttempts++;
    const delay = Math.min(30000, 1000 * Math.pow(2, Math.min(reconnectAttempts, 5)));
    console.log(`[ws] reconnect in ${delay}ms (attempt ${reconnectAttempts})`);
    setTimeout(connect, delay);
  });

  ws.on("error", (err) => {
    console.error("[ws] error", err.message);
  });
}

// ─── HTTP /health ─────────────────────────────────────────────────────────
http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    const body = {
      version: VERSION,
      ok: ws?.readyState === WebSocket.OPEN,
      wsState: ws?.readyState,
      lastMessageAt,
      messagesReceived,
      rowsUpserted,
      parseErrors,
      pendingBuffer: pending.size,
      sports: SPORTS,
      markets: MARKETS,
      uptime: process.uptime(),
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
