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
import pkg from "../package.json" with { type: "json" };

// ─── Config ───────────────────────────────────────────────────────────────
const FORBIDDEN_PATCH_ENV_VARS = [
  "BRIDGE_DEPLOY_LABEL",
  "BRIDGE_INDEX_B64",
  "BRIDGE_NORMALIZE_B64",
  "BRIDGE_PACKAGE_B64",
  "BOOT_PATCH",
  "BRIDGE_PATCH",
  "ODDS_API_BRIDGE",
  "ODDS_API_BRIDGE_CODE",
  "INDEX_JS",
  "PATCH",
  "BRIDGE_VERSION",
];

const activePatchEnvVars = FORBIDDEN_PATCH_ENV_VARS.filter((name) => Boolean(process.env[name]));
if (activePatchEnvVars.length) {
  console.error(`[boot] refused to start odds-api-bridge v${pkg.version}: remove legacy Railway variable(s): ${activePatchEnvVars.join(", ")}`);
  console.error("[boot] these variables can inject the old v2.0.24 bridge with markets=20/ws_chunks=1 and must not exist on this service");
  process.exit(1);
}

const required = (name) => {
  const v = process.env[name];
  if (!v) {
    console.error(`[boot] missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
};

const ODDS_API_KEY              = required("ODDS_API_IO_KEY");
// Optional additional keys. Each key allows 2 WS connections × 10 sports.
const ODDS_API_KEY_2            = (process.env.ODDS_API_IO_KEY_2 ?? "").trim();
const ODDS_API_KEY_3            = (process.env.ODDS_API_IO_KEY_3 ?? "").trim();
const ODDS_API_KEYS             = [ODDS_API_KEY, ODDS_API_KEY_2, ODDS_API_KEY_3].filter(Boolean);
const SUPABASE_URL              = required("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = required("SUPABASE_SERVICE_ROLE_KEY");
const PORT                      = Number(process.env.PORT ?? 3000);
const VERSION                   = pkg.version;

const normalizeSupabaseUrl = (raw) => {
  let value = String(raw ?? "").trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1).trim();
  }
  try {
    return new URL(value).origin;
  } catch {
    return value.replace(/\/+$/, "").replace(/\/(rest|auth|storage|realtime)\/v1$/i, "");
  }
};

const SUPABASE_BASE_URL = normalizeSupabaseUrl(SUPABASE_URL);

const KEY_FINGERPRINT = crypto.createHash("sha256")
  .update(ODDS_API_KEY).digest("hex").slice(0, 8);

// Full default sport set. WS endpoint caps at 10 sports per connection, so we
// chunk into multiple parallel WS connections (see WS_SPORT_CHUNKS below).
// Without chunking, ANY sport beyond #10 — handball, futsal, badminton, golf,
// cycling, squash, waterpolo, etc. — gets fixtures from the REST seed loop
// but NEVER gets market frames, so live_odds is permanently empty for them.
const SPORTS = (process.env.SPORTS ?? [
  "football", "basketball", "tennis", "baseball", "american-football",
  "ice-hockey", "mixed-martial-arts", "boxing", "rugby", "cricket",
  "table-tennis", "esports", "volleyball", "handball", "futsal",
  "badminton", "snooker", "darts", "aussie-rules", "cycling",
  "golf", "squash", "waterpolo",
].join(",")).split(",").map(s => s.trim()).filter(Boolean);

// Provider update (confirmed by odds-api.io support, Marcus):
//   1. Omitting the `markets` parameter streams ALL markets the bookmaker offers.
//   2. One API key allows TWO parallel WS connections.
// So instead of filtering to 20 markets per key, we open 2 connections per key,
// each subscribing to half of SPORTS, with NO `markets` filter → full coverage.
const MARKET_GROUPS = [[]]; // kept for log/health compatibility, unused for filtering
const MARKETS = [];         // empty → "all markets" (provider default)

// Provider caps each WS subscription at 10 sports. Each API key allows 2 WS
// connections. So a single key can cover up to 2 × 10 = 20 sports.
// We hard-cap chunk size at 10 and split SPORTS as evenly as possible.
const PROVIDER_SPORTS_PER_WS_CAP = 10;
const WS_CONNECTIONS_PER_KEY = 2;
const MAX_SPORTS_PER_KEY = PROVIDER_SPORTS_PER_WS_CAP * WS_CONNECTIONS_PER_KEY;
const TOTAL_CAPACITY = MAX_SPORTS_PER_KEY * ODDS_API_KEYS.length;

let EFFECTIVE_SPORTS = SPORTS;
let DROPPED_SPORTS = [];
if (SPORTS.length > TOTAL_CAPACITY) {
  EFFECTIVE_SPORTS = SPORTS.slice(0, TOTAL_CAPACITY);
  DROPPED_SPORTS = SPORTS.slice(TOTAL_CAPACITY);
}

// Balanced chunk size, never exceeding the provider's 10-sport cap.
const WS_SPORT_CHUNK_SIZE = Math.min(
  PROVIDER_SPORTS_PER_WS_CAP,
  Math.max(Math.ceil(EFFECTIVE_SPORTS.length / WS_CONNECTIONS_PER_KEY), 1),
);
const WS_SPORT_CHUNKS = [];
for (let i = 0; i < EFFECTIVE_SPORTS.length; i += WS_SPORT_CHUNK_SIZE) {
  WS_SPORT_CHUNKS.push(EFFECTIVE_SPORTS.slice(i, i + WS_SPORT_CHUNK_SIZE));
}
if (WS_SPORT_CHUNKS.length === 0) WS_SPORT_CHUNKS.push([]);

// Build the (key, sportsChunk) pairs that drive `connections`. Each key gets
// up to WS_CONNECTIONS_PER_KEY chunks. Extra chunks roll over to the next key.
const WS_PLAN = [];
WS_SPORT_CHUNKS.forEach((sportsChunk, chunkIdx) => {
  const keyIdx = Math.floor(chunkIdx / WS_CONNECTIONS_PER_KEY);
  const apiKey = ODDS_API_KEYS[keyIdx];
  if (!apiKey) return; // not enough keys for this chunk
  const slotIdx = chunkIdx % WS_CONNECTIONS_PER_KEY;
  WS_PLAN.push({
    apiKey,
    keyIdx,
    sportsChunk,
    markets: [],
    groupLabel: `key${keyIdx + 1}-conn${slotIdx + 1}`,
  });
});

function buildWsUrl(sportsChunk, apiKey, _markets) {
  const params = new URLSearchParams({
    apiKey,
    channels: "odds,scores,status",
  });
  // NOTE: intentionally NOT setting `markets` — provider streams ALL markets
  // when the parameter is omitted (per odds-api.io support, Marcus).
  // Sport cap is 10 per WS; we already enforce this via WS_SPORT_CHUNK_SIZE.
  if (sportsChunk.length) params.set("sport", sportsChunk.join(","));
  return `wss://api.odds-api.io/v3/ws?${params.toString()}`;
}




const EVENT_ID_PREFIX = process.env.EVENT_ID_PREFIX ?? "oddsapi:";
const BACKOFF_MS = [1_000, 2_000, 5_000, 10_000, 30_000, 60_000];
const WATCHDOG_MS = 90_000;
const METRICS_INTERVAL_MS = 30_000;
const LIVE_PROVIDER_MAX_AGE_MS = 8 * 60_000;

// ─── Request Budget / Rate Limiter ────────────────────────────────────────
// Provider quota is 10,000 req/hour. We target a SOFT cap well below that so
// bursts can't drain the bucket. Two windows: per-minute (smooths bursts) and
// per-hour (enforces the daily-ish budget). When the soft cap is hit, low-
// priority calls are dropped; high-priority calls wait briefly.
const REQ_BUDGET_PER_HOUR   = Number(process.env.REQ_BUDGET_PER_HOUR ?? 6_000);
const REQ_BUDGET_PER_MINUTE = Number(process.env.REQ_BUDGET_PER_MINUTE ?? 90);
const REQ_HARD_CAP_PER_HOUR = Number(process.env.REQ_HARD_CAP_PER_HOUR ?? 9_500);

// Priority: lower = more important. Live/critical=0, prematch=1, secondary=2.
const PRIORITY = { critical: 0, normal: 1, low: 2 };

const reqWindow = { minute: [], hour: [] }; // arrays of timestamps (ms)
const reqStats = {
  total: 0, denied: 0, deniedByPriority: { 0: 0, 1: 0, 2: 0 },
  byEndpoint: {}, bySport: {},
};

function pruneWindows(now) {
  const m = now - 60_000;
  const h = now - 3_600_000;
  while (reqWindow.minute.length && reqWindow.minute[0] < m) reqWindow.minute.shift();
  while (reqWindow.hour.length && reqWindow.hour[0] < h) reqWindow.hour.shift();
}

function budgetCheck(priority) {
  const now = Date.now();
  pruneWindows(now);
  const hourUsed   = reqWindow.hour.length;
  const minuteUsed = reqWindow.minute.length;
  // Hard cap — never exceed, regardless of priority.
  if (hourUsed >= REQ_HARD_CAP_PER_HOUR) return { ok: false, reason: "hard_cap_hour" };
  // Soft cap — only critical gets through past the soft hourly budget.
  if (hourUsed >= REQ_BUDGET_PER_HOUR && priority > PRIORITY.critical) {
    return { ok: false, reason: "soft_cap_hour" };
  }
  // Per-minute throttle — critical waits a tick, others drop.
  if (minuteUsed >= REQ_BUDGET_PER_MINUTE) {
    if (priority === PRIORITY.critical) return { ok: true, waitMs: 1_000 };
    return { ok: false, reason: "minute_cap" };
  }
  return { ok: true, waitMs: 0 };
}

async function budgetedFetch(url, { priority = PRIORITY.normal, endpoint = "unknown", sport = "n/a", init } = {}) {
  const decision = budgetCheck(priority);
  reqStats.byEndpoint[endpoint] = (reqStats.byEndpoint[endpoint] ?? 0) + 1;
  reqStats.bySport[sport]       = (reqStats.bySport[sport] ?? 0) + 1;
  if (!decision.ok) {
    reqStats.denied += 1;
    reqStats.deniedByPriority[priority] = (reqStats.deniedByPriority[priority] ?? 0) + 1;
    console.warn(`[budget] DROP endpoint=${endpoint} sport=${sport} priority=${priority} reason=${decision.reason} hour=${reqWindow.hour.length}/${REQ_BUDGET_PER_HOUR} min=${reqWindow.minute.length}/${REQ_BUDGET_PER_MINUTE}`);
    const err = new Error(`request denied by budget: ${decision.reason}`);
    err.code = "BUDGET_DENIED";
    throw err;
  }
  if (decision.waitMs) await new Promise((r) => setTimeout(r, decision.waitMs));
  const now = Date.now();
  reqWindow.minute.push(now);
  reqWindow.hour.push(now);
  reqStats.total += 1;
  return fetch(url, init);
}

// ─── Negative market cache ────────────────────────────────────────────────
// (event_id::market) -> expireAt. Skip markets that returned empty so we
// don't repeatedly query for the same blank data.
const EMPTY_MARKET_TTL_MS = Number(process.env.EMPTY_MARKET_TTL_MS ?? 30 * 60_000);
const emptyMarketCache = new Map();
function isMarketEmpty(eventId, market) {
  const k = `${eventId}::${market}`;
  const exp = emptyMarketCache.get(k);
  if (!exp) return false;
  if (exp < Date.now()) { emptyMarketCache.delete(k); return false; }
  return true;
}
function markMarketEmpty(eventId, market) {
  emptyMarketCache.set(`${eventId}::${market}`, Date.now() + EMPTY_MARKET_TTL_MS);
}

// ─── HTTP seed loop config ────────────────────────────────────────────────
// Backoff schedule used ONLY when the provider returns HTTP 429 (rate limit)
// or 5xx. Per-sport: wait 60s, then 120s, then 240s, then cap at 480s.
// We never burst-retry on 429 — that just deepens the throttle.
const SEED_RETRY_BACKOFF_MS = [60_000, 120_000, 240_000, 480_000];
const SEED_RETRY_MAX_ATTEMPTS = 6;
// Time between sports inside a single seed pass. Sequential (NOT parallel)
// so we never have 10 simultaneous HTTP requests racing for the quota.
const SEED_SPORT_GAP_MS = Number(process.env.SEED_SPORT_GAP_MS ?? 5_000);
// Time between full seed passes (all sports). Default 10 min.
const SEED_PASS_INTERVAL_MS = Number(process.env.SEED_PASS_INTERVAL_MS ?? 600_000);
// Master switch — opt-in. If false, no HTTP seeding happens (WS only).
const SEED_ENABLED = String(process.env.SEED_ENABLED ?? "false").toLowerCase() === "true";
// Where to seed from. Defaults to the odds-api.io REST events endpoint.
const SEED_BASE_URL = process.env.SEED_BASE_URL ?? "https://api.odds-api.io/v3/events";


// ─── Supabase ─────────────────────────────────────────────────────────────
if (typeof globalThis.WebSocket === "undefined") {
  globalThis.WebSocket = WebSocket;
}

const supabase = createClient(SUPABASE_BASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { enabled: false, transport: WebSocket },
});

// ─── State ────────────────────────────────────────────────────────────────
// ─── State ────────────────────────────────────────────────────────────────
// One entry per WS connection (one per chunk of <=10 sports). Watchdog,
// reconnect, and metrics all iterate over this array.
const connections = WS_PLAN.map((plan, idx) => ({
  idx,
  sports: plan.sportsChunk,
  apiKey: plan.apiKey,
  keyIdx: plan.keyIdx,
  markets: plan.markets,
  groupLabel: plan.groupLabel,
  url: buildWsUrl(plan.sportsChunk, plan.apiKey, plan.markets),
  ws: null,
  backoffIdx: 0,
  lastMessageAt: Date.now(),
  lastSecondaryMarketAt: Date.now(),
  connectedAt: Date.now(),
  reconnectTimer: null,
}));
let shuttingDown = false;

// If h2h keeps flowing but other markets go silent for this long, force a
// reconnect — the upstream provider has stopped pushing them on this socket.
const SECONDARY_MARKET_STALE_MS = Number(process.env.SECONDARY_MARKET_STALE_MS ?? 10 * 60_000);
// Hard-cycle the socket every N ms to keep the subscription fresh even when
// nothing looks wrong. Default: 30 min.
const FORCE_RECONNECT_MS = Number(process.env.FORCE_RECONNECT_MS ?? 30 * 60_000);

const stats = {
  startedAt: new Date().toISOString(),
  messages: 0,
  upserts: 0,
  parentUpserts: 0,
  errors: 0,
  parseErrors: 0,
  reconnects: 0,
  lastError: null,
  lastUpsertAt: null,
  lastSecondaryMarketAt: null,
  // rate-limit telemetry
  ws1013Count: 0,
  rateLimited429Count: 0,
  lastMessageAt: 0,
  // rolling per-minute counters
  _windowStartMs: Date.now(),
  _windowMessages: 0,
  _windowTennisFrames: 0,
  _windowScoreFrames: 0,
  _windowStatusFrames: 0,
  _windowScoreFramesWithMeta: 0,
  _windowScoreFramesMissingMeta: 0,
  _windowStatusFramesWithMeta: 0,
  _windowStatusFramesMissingMeta: 0,
  messagesLastMinute: 0,
  tennisFramesLastMinute: 0,
  scoreFramesLastMinute: 0,
  statusFramesLastMinute: 0,
  scoreFramesWithMetaLastMinute: 0,
  scoreFramesMissingMetaLastMinute: 0,
  statusFramesWithMetaLastMinute: 0,
  statusFramesMissingMetaLastMinute: 0,
};

// ─── Helpers ──────────────────────────────────────────────────────────────
const safeIso = (v, fb = new Date()) => {
  const d = new Date(v ?? fb);
  return Number.isNaN(d.getTime()) ? fb.toISOString() : d.toISOString();
};

// Map provider market name (case/space/punct insensitive) to our canonical
// db key. Mirror the UI's expected keys in BettingMarkets.tsx + the names
// listed in SUPPORTED_PROVIDER_MARKETS (src/lib/oddsMarketBuilder.ts).
const PROVIDER_MARKET_MAP = {
  // Core
  ml: "h2h",
  match_winner: "h2h",
  "1x2": "h2h",
  spread: "spreads",
  asian_handicap: "spreads",
  european_handicap: "spreads",
  alt_spread: "alternate_spreads",
  alternate_spread: "alternate_spreads",
  alternative_asian_handicap: "alternate_spreads",
  totals: "totals",
  "goals_over/under": "totals",
  alt_totals: "alternate_totals",
  alternate_totals: "alternate_totals",
  alternative_total_goals: "alternate_totals",
  alternative_goal_line: "alternate_totals",
  team_totals: "team_totals",
  tt: "team_totals",
  team_total_goals_home: "team_totals",
  team_total_goals_away: "team_totals",
  btts: "btts",
  both_teams_to_score: "btts",
  dc: "double_chance",
  double_chance: "double_chance",
  dnb: "draw_no_bet",
  draw_no_bet: "draw_no_bet",
  // Halves
  half_time_result: "h2h_h1",
  h2h_h1: "h2h_h1",
  ml_h1: "h2h_h1",
  totals_h1: "totals_h1",
  totals_ht: "totals_h1",
  "1st_half_totals": "totals_h1",
  spread_h1: "spreads_h1",
  spreads_h1: "spreads_h1",
  spread_ht: "spreads_h1",
  "1st_half_handicap": "spreads_h1",
  btts_h1: "btts_h1",
  both_teams_to_score_ht: "btts_h1",
  dc_h1: "double_chance_h1",
  double_chance_h1: "double_chance_h1",
  // Quarters
  spread_1q: "spreads_q1",
  totals_1q: "totals_q1",
  ml_q2: "h2h_q2", ml_q3: "h2h_q3", ml_q4: "h2h_q4",
  totals_q2: "totals_q2", totals_q3: "totals_q3", totals_q4: "totals_q4",
  spread_q2: "spreads_q2", spread_q3: "spreads_q3", spread_q4: "spreads_q4",
  // Hockey periods
  ml_p1: "h2h_p1", ml_p2: "h2h_p2", ml_p3: "h2h_p3",
  totals_p1: "totals_p1", totals_p2: "totals_p2", totals_p3: "totals_p3",
  spread_p1: "spreads_p1", spread_p2: "spreads_p2", spread_p3: "spreads_p3",
  empty_net_goal: "empty_net_goal",
  will_go_to_shootout: "will_go_to_shootout",
  // Corners & cards
  corners_totals: "corners_totals",
  total_corners: "corners_totals",
  corners: "corners_totals",
  alternative_corners: "alternate_corners_totals",
  corners_spread: "corners_hcp",
  corners_spreads: "corners_hcp",
  corner_handicap: "corners_hcp",
  corners_totals_ht: "corners_totals_h1",
  corners_spread_ht: "corners_hcp_h1",
  bookings_totals: "cards_totals",
  bookings_spread: "cards_hcp",
  bookings_spreads: "cards_hcp",
  // Correct score & specials
  correct_score: "correct_score",
  "odd/even": "odd_even",
  odd_even: "odd_even",
  "odd/even_ht": "odd_even_h1",
  odd_even_ht: "odd_even_h1",
  "ht/ft": "ht_ft",
  ht_ft: "ht_ft",
  "result/both_teams_to_score": "result_btts",
  "result/total_goals": "result_total",
  clean_sheet_home: "clean_sheet_home",
  clean_sheet_away: "clean_sheet_away",
  win_to_nil_home: "win_to_nil_home",
  win_to_nil_away: "win_to_nil_away",
  multi_goals: "multi_goals",
  exact_goals: "exact_goals",
  race_to_2_goals: "race_to_2_goals",
  race_to_3_goals: "race_to_3_goals",
  race_to_10_points: "race_to_10_points",
  race_to_15_points: "race_to_15_points",
  race_to_20_points: "race_to_20_points",
  highest_scoring_half: "highest_scoring_half",
  highest_scoring_quarter: "highest_scoring_quarter",
  score_both_halves_home: "score_both_halves_home",
  score_both_halves_away: "score_both_halves_away",
  win_both_halves: "win_both_halves",
  penalty_in_match: "penalty_in_match",
  red_card_in_match: "red_card_in_match",
  // Goalscorers
  anytime_goalscorer: "anytime_scorer",
  first_goalscorer: "first_scorer",
  last_goalscorer: "last_scorer",
  player_to_score_2_or_more: "player_score_2plus",
  "player_to_score_hat-trick": "player_hattrick",
  player_to_score_hat_trick: "player_hattrick",
  // Tennis
  set_winner: "set_winner",
  "1st_set_winner": "set_winner_s1",
  "2nd_set_winner": "set_winner_s2",
  "3rd_set_winner": "set_winner_s3",
  total_sets: "total_sets",
  total_games: "total_games",
  correct_set_score: "correct_set_score",
  player_to_win_a_set: "player_win_set",
  // Basketball player props
  player_points: "player_points",
  player_rebounds: "player_rebounds",
  player_assists: "player_assists",
  "player_3-pointers_made": "player_threes",
  player_3_pointers_made: "player_threes",
  player_steals: "player_steals",
  player_blocks: "player_blocks",
  // Baseball
  "1st_inning_ml": "h2h_inn1",
  "5_innings_ml": "h2h_f5",
  "5_innings_totals": "totals_f5",
  "5_innings_spread": "spreads_f5",
  // MMA / Boxing
  method_of_victory: "method_of_victory",
  round_betting: "round_betting",
  fight_goes_the_distance: "fight_distance",
  total_rounds: "total_rounds",
  // American football
  anytime_touchdown_scorer: "anytime_td",
  first_touchdown_scorer: "first_td",
  total_touchdowns: "total_tds",
  // Outrights
  outright_winner: "outright_winner",
  top_4_finish: "outright_top_4",
  top_6_finish: "outright_top_6",
  top_10_finish: "outright_top_10",
  relegation: "outright_relegation",
};

const toDbMarket = (n) => {
  const k = String(n ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  return PROVIDER_MARKET_MAP[k] ?? k;
};

// Markets whose payload is shaped identically to one of the core handlers.
// Anything not listed here goes through the generic listed-selection handler
// (handles `odds[]` arrays of `{name|selection|label, price|odds, point?}`).
const MARKET_BASE = {
  // h2h shape (home/draw/away or home/away)
  h2h: "h2h", h2h_h1: "h2h",
  h2h_q2: "h2h", h2h_q3: "h2h", h2h_q4: "h2h",
  h2h_p1: "h2h", h2h_p2: "h2h", h2h_p3: "h2h",
  h2h_inn1: "h2h", h2h_f5: "h2h",
  draw_no_bet: "draw_no_bet",
  double_chance: "double_chance", double_chance_h1: "double_chance",
  // spreads shape
  spreads: "spreads", spreads_h1: "spreads", spreads_q1: "spreads",
  spreads_q2: "spreads", spreads_q3: "spreads", spreads_q4: "spreads",
  spreads_p1: "spreads", spreads_p2: "spreads", spreads_p3: "spreads",
  spreads_f5: "spreads",
  alternate_spreads: "spreads",
  corners_hcp: "spreads", corners_hcp_h1: "spreads",
  cards_hcp: "spreads",
  // totals shape
  totals: "totals", totals_h1: "totals", totals_q1: "totals",
  totals_q2: "totals", totals_q3: "totals", totals_q4: "totals",
  totals_p1: "totals", totals_p2: "totals", totals_p3: "totals",
  totals_f5: "totals",
  alternate_totals: "totals",
  corners_totals: "totals", corners_totals_h1: "totals",
  alternate_corners_totals: "totals",
  cards_totals: "totals", total_rounds: "totals", total_sets: "totals",
  total_games: "totals", total_tds: "totals",
  // btts/yes-no shape
  btts: "btts", btts_h1: "btts",
  odd_even: "btts", odd_even_h1: "btts",
  clean_sheet_home: "btts", clean_sheet_away: "btts",
  win_to_nil_home: "btts", win_to_nil_away: "btts",
  score_both_halves_home: "btts", score_both_halves_away: "btts",
  win_both_halves: "btts",
  penalty_in_match: "btts", red_card_in_match: "btts",
  empty_net_goal: "btts", will_go_to_shootout: "btts",
  fight_distance: "btts",
  // team_totals
  team_totals: "team_totals",
};

// Anything else goes through the listed-selection generic handler.
const LISTED_GENERIC_MARKETS = new Set([
  "correct_score","ht_ft","result_btts","result_total",
  "multi_goals","exact_goals","race_to_2_goals","race_to_3_goals",
  "race_to_10_points","race_to_15_points","race_to_20_points",
  "highest_scoring_half","highest_scoring_quarter",
  "anytime_scorer","first_scorer","last_scorer",
  "player_score_2plus","player_hattrick",
  "set_winner","set_winner_s1","set_winner_s2","set_winner_s3",
  "correct_set_score","player_win_set",
  "player_points","player_rebounds","player_assists",
  "player_threes","player_steals","player_blocks",
  "method_of_victory","round_betting",
  "anytime_td","first_td",
  "outright_winner","outright_top_4","outright_top_6","outright_top_10",
  "outright_relegation",
]);

const SUPPORTED_MARKETS = new Set([
  ...Object.keys(MARKET_BASE),
  ...LISTED_GENERIC_MARKETS,
]);

const isLive = (v) => {
  if (v === true) return true;
  const s = String(v ?? "").toLowerCase();
  return ["live", "in_play", "inplay", "in-play", "playing", "running", "active", "true"].includes(s);
};

const isStaleLiveProviderTs = (providerTs) => {
  const ms = new Date(providerTs).getTime();
  return Number.isFinite(ms) && Date.now() - ms > LIVE_PROVIDER_MAX_AGE_MS;
};

function formatError(err) {
  if (err instanceof Error) return err.message;
  if (err && typeof err === "object") {
    const shaped = {
      message: err.message ?? null,
      details: err.details ?? null,
      hint: err.hint ?? null,
      code: err.code ?? null,
    };
    try {
      return JSON.stringify(shaped);
    } catch {
      return String(err);
    }
  }
  return String(err ?? "unknown");
}

function rememberError(stage, err, { quiet = false } = {}) {
  const msg = formatError(err);
  stats.errors += 1;
  stats.lastError = { at: new Date().toISOString(), stage, message: msg };
  if (!quiet) console.error(`[err:${stage}] ${msg}`);
}

// ─── Parent row (odds_api_events) ─────────────────────────────────────────
import {
  buildBridgeEventRow,
  normalizeClock,
  BUCKET_MARKETS,
  normalizeBucketSelection,
  makeBucketDiagnostics,
  SIDE_HANDICAP_MARKETS,
  canonicalizeMatchSide,
  isQuarterLine,
} from "./normalize.js";

// TRACK A — EXACT/MULTI diagnostics (per-bucket/band ingestion counts).
const bucketDiag = makeBucketDiagnostics();
let lastBucketDiagLog = 0;
const BUCKET_DIAG_INTERVAL_MS = 60_000;
function maybeLogBucketDiag() {
  const now = Date.now();
  if (now - lastBucketDiagLog < BUCKET_DIAG_INTERVAL_MS) return;
  lastBucketDiagLog = now;
  console.log(`[bucket-diag] ${JSON.stringify(bucketDiag.snapshot())}`);
}

async function upsertEventRow(evt) {
  const row = buildBridgeEventRow(evt, {
    nowIso: new Date().toISOString(),
    eventIdPrefix: EVENT_ID_PREFIX,
    isLive,
    safeIso,
  });
  if (!row) return false;
  const { error } = await supabase
    .from("odds_api_events")
    .upsert([row], { onConflict: "event_id" });
  if (error) {
    rememberError("parent_upsert", error);
    throw error;
  }
  stats.parentUpserts += 1;
  return true;
}


// ─── Flatten WS event to live_odds rows ───────────────────────────────────
const seenMarketNames = new Set();

function flattenEvent(evt) {
  if (!evt?.id || !Array.isArray(evt.markets)) return [];
  const bookmaker = String(evt.bookie ?? evt.bookmaker ?? "");
  if (!bookmaker) return [];

  const eventId = `${EVENT_ID_PREFIX}${String(evt.id)}`;
  const now = new Date().toISOString();
  const defaultProviderTs = safeIso(evt.updatedAt ?? evt.updated_at ?? now);
  const homeName = evt.home ?? evt.home_team ?? "Home";
  const awayName = evt.away ?? evt.away_team ?? "Away";
  const rows = [];

  const eventIsLive = isLive(evt.status ?? evt.in_play);
  const pushRow = ({ market, selection, price, point = 0, providerTs = defaultProviderTs }) => {
    const p = Number(price);
    const pt = Number(point ?? 0);
    if (!Number.isFinite(p) || p <= 1) return;
    if (eventIsLive && isStaleLiveProviderTs(providerTs)) return;
    rows.push({
      event_id: eventId,
      bookmaker,
      market,
      selection: String(selection),
      price: p,
      point: Number.isFinite(pt) ? pt : 0,
      suspended: false,
      received_at: now,
      provider_ts: safeIso(providerTs),
    });
  };

  for (const market of evt.markets) {
    const dbMarket = toDbMarket(market.name);
    // Debug-sample every unique raw market name once so we can see the
    // payload shape for corner/card spreads (or any market we don't yet map).
    if (!seenMarketNames.has(market.name)) {
      seenMarketNames.add(market.name);
      const sampleOdd = (market.odds ?? [])[0];
      console.log(`[market-sample] name="${market.name}" dbMarket="${dbMarket}" supported=${SUPPORTED_MARKETS.has(dbMarket)} sampleOdd=${JSON.stringify(sampleOdd)}`);
    }
    // No whitelist gate — accept ALL markets the provider streams.
    // Unknown markets fall through to shape-sniffing in the generic branch below.

    const baseType = MARKET_BASE[dbMarket] ?? dbMarket;
    const providerTs = market.updatedAt ? safeIso(market.updatedAt) : defaultProviderTs;

    for (const odd of market.odds ?? []) {
      if (baseType === "h2h") {
        pushRow({ market: dbMarket, selection: homeName, price: odd.home, providerTs });
        pushRow({ market: dbMarket, selection: "Draw", price: odd.draw, providerTs });
        pushRow({ market: dbMarket, selection: awayName, price: odd.away, providerTs });
      } else if (baseType === "spreads") {
        const point = Number(odd.hdp ?? odd.hcp ?? odd.handicap ?? odd.point ?? odd.line ?? odd.spread);
        if (!Number.isFinite(point)) continue;
        // TRACK B — for corners_hcp / cards_hcp, drop quarter-lines at
        // ingestion (policy: integer + .5 only). Belt-and-suspenders with
        // the resolver-level reject.
        if (SIDE_HANDICAP_MARKETS.has(dbMarket) && isQuarterLine(point)) continue;
        const homePrice = odd.home ?? odd.homeOdds ?? odd.home_price ?? odd.h;
        const awayPrice = odd.away ?? odd.awayOdds ?? odd.away_price ?? odd.a;
        pushRow({ market: dbMarket, selection: homeName, price: homePrice, point, providerTs });
        pushRow({ market: dbMarket, selection: awayName, price: awayPrice, point: -point, providerTs });
      } else if (baseType === "totals") {
        const point = Number(odd.hdp ?? odd.total ?? odd.point ?? odd.line);
        if (!Number.isFinite(point)) continue;
        const overPrice = odd.over ?? odd.Over ?? odd.o;
        const underPrice = odd.under ?? odd.Under ?? odd.u;
        pushRow({ market: dbMarket, selection: "Over", price: overPrice, point, providerTs });
        pushRow({ market: dbMarket, selection: "Under", price: underPrice, point, providerTs });
      } else if (baseType === "btts") {
        pushRow({ market: dbMarket, selection: "Yes", price: odd.yes ?? odd.Yes, providerTs });
        pushRow({ market: dbMarket, selection: "No", price: odd.no ?? odd.No, providerTs });
      } else if (baseType === "double_chance") {
        pushRow({ market: dbMarket, selection: "1X", price: odd.homeDraw ?? odd["1X"] ?? odd.home_draw, providerTs });
        pushRow({ market: dbMarket, selection: "12", price: odd.homeAway ?? odd["12"] ?? odd.home_away, providerTs });
        pushRow({ market: dbMarket, selection: "X2", price: odd.drawAway ?? odd["X2"] ?? odd.draw_away, providerTs });
      } else if (baseType === "draw_no_bet") {
        pushRow({ market: dbMarket, selection: homeName, price: odd.home, providerTs });
        pushRow({ market: dbMarket, selection: awayName, price: odd.away, providerTs });
      } else if (baseType === "team_totals") {
        const point = Number(odd.total ?? odd.point);
        if (!Number.isFinite(point)) continue;
        const side = String(odd.team ?? odd.side ?? "").toLowerCase();
        const teamName = side === "home" ? homeName : side === "away" ? awayName : null;
        if (teamName) {
          pushRow({ market: dbMarket, selection: `${teamName} Over`, price: odd.over, point, providerTs });
          pushRow({ market: dbMarket, selection: `${teamName} Under`, price: odd.under, point, providerTs });
        } else {
          pushRow({ market: dbMarket, selection: `${homeName} Over`, price: odd.home_over, point, providerTs });
          pushRow({ market: dbMarket, selection: `${homeName} Under`, price: odd.home_under, point, providerTs });
          pushRow({ market: dbMarket, selection: `${awayName} Over`, price: odd.away_over, point, providerTs });
          pushRow({ market: dbMarket, selection: `${awayName} Under`, price: odd.away_under, point, providerTs });
        }
      } else if (BUCKET_MARKETS.has(dbMarket)) {
        // TRACK A — EXACT_GOALS / MULTI_GOALS bucket handler.
        // These markets are discrete buckets/bands, NOT Over/Under lines.
        // Only accept named selections that map to a canonical bucket; drop
        // any Over/Under/h2h/yes-no shape (which historically produced
        // degenerate `point=0` rows that could never settle).
        const rawSel =
          odd.name ?? odd.selection ?? odd.label ?? odd.outcome ??
          odd.score ?? odd.result ?? null;
        const priceVal =
          odd.price ?? odd.odds ?? odd.odd ?? odd.value ?? odd.decimal ??
          odd.dec ?? odd.coefficient;
        const canonical = normalizeBucketSelection(dbMarket, rawSel);
        bucketDiag.record(dbMarket, canonical);
        if (canonical && priceVal != null) {
          pushRow({
            market: dbMarket,
            selection: canonical,
            price: priceVal,
            point: 0,
            providerTs,
          });
        }
        // else: drop silently — either not a bucket shape (Over/Under, etc.)
        // or a bucket we don't recognize. Counted in bucketDiag.dropped.
      } else {
        // Generic listed-selection handler + shape sniffing for ANY market
        // the provider streams that we haven't explicitly mapped above.
        // 1) Named selection shape: {name|selection|label, price|odds, point?}
        // 2) h2h shape:    {home, draw?, away}
        // 3) totals shape: {over, under, total|point|line}
        // 4) spreads shape:{home, away, hdp|hcp|handicap|point}
        // 5) yes/no shape: {yes, no}
        const sel =
          odd.name ?? odd.selection ?? odd.label ?? odd.outcome ??
          odd.player ?? odd.player_name ?? odd.runner ?? odd.team ??
          odd.score ?? odd.result ?? odd.round ?? odd.method ?? null;
        const priceVal =
          odd.price ?? odd.odds ?? odd.odd ?? odd.value ?? odd.decimal ??
          odd.dec ?? odd.coefficient;
        const point = Number(
          odd.point ?? odd.line ?? odd.total ?? odd.hdp ?? odd.handicap ?? 0,
        );
        if (sel != null && priceVal != null) {
          // TRACK B — for corners_hcp / cards_hcp side handicap markets, the
          // provider sometimes streams outright/progression outcomes (e.g.
          // "2D", "3E/3F/3G", "W73") through the generic branch. These are
          // NOT match handicap sides and must be dropped, along with
          // quarter lines. Only accept rows we can canonicalize to
          // home/away, and rewrite the selection to homeName/awayName so
          // downstream resolvers stay uniform.
          if (SIDE_HANDICAP_MARKETS.has(dbMarket)) {
            if (isQuarterLine(point)) continue;
            const side = canonicalizeMatchSide(sel, homeName, awayName);
            if (side !== "home" && side !== "away") continue;
            pushRow({
              market: dbMarket,
              selection: side === "home" ? homeName : awayName,
              price: priceVal,
              point: Number.isFinite(point) ? point : 0,
              providerTs,
            });
          } else {
            pushRow({
              market: dbMarket,
              selection: String(sel),
              price: priceVal,
              point: Number.isFinite(point) ? point : 0,
              providerTs,
            });
          }
        } else if (odd.over != null || odd.under != null) {
          const pt = Number(odd.total ?? odd.point ?? odd.line ?? odd.hdp ?? 0);
          if (odd.over  != null) pushRow({ market: dbMarket, selection: "Over",  price: odd.over,  point: pt, providerTs });
          if (odd.under != null) pushRow({ market: dbMarket, selection: "Under", price: odd.under, point: pt, providerTs });
        } else if (odd.yes != null || odd.no != null) {
          if (odd.yes != null) pushRow({ market: dbMarket, selection: "Yes", price: odd.yes, providerTs });
          if (odd.no  != null) pushRow({ market: dbMarket, selection: "No",  price: odd.no,  providerTs });
        } else if (odd.home != null || odd.away != null || odd.draw != null) {
          const hcp = Number(odd.hdp ?? odd.hcp ?? odd.handicap ?? odd.point ?? odd.line ?? odd.spread);
          if (Number.isFinite(hcp)) {
            // TRACK B — quarter-line policy also enforced on the generic spreads-shape fallback.
            if (SIDE_HANDICAP_MARKETS.has(dbMarket) && isQuarterLine(hcp)) continue;
            if (odd.home != null) pushRow({ market: dbMarket, selection: homeName, price: odd.home, point:  hcp, providerTs });
            if (odd.away != null) pushRow({ market: dbMarket, selection: awayName, price: odd.away, point: -hcp, providerTs });
          } else {
            if (odd.home != null) pushRow({ market: dbMarket, selection: homeName, price: odd.home, providerTs });
            if (odd.draw != null) pushRow({ market: dbMarket, selection: "Draw",   price: odd.draw, providerTs });
            if (odd.away != null) pushRow({ market: dbMarket, selection: awayName, price: odd.away, providerTs });
          }
        }

      }
    }
  }
  return rows;
}

// ─── Tennis raw-payload debugger ─────────────────────────────────────────
// Diagnostic only: dumps the FIRST N tennis frames we receive from
// odds-api.io so we can verify whether the provider actually exposes
// games-in-current-set / current-game-points / current-server. Enable on
// Railway with TENNIS_DEBUG=1. Capped to avoid log spam in production.
const TENNIS_DEBUG = process.env.TENNIS_DEBUG === "1";
const TENNIS_DEBUG_MAX = Number(process.env.TENNIS_DEBUG_MAX ?? 25);
const TENNIS_DEBUG_SAMPLE_ANY = String(process.env.TENNIS_DEBUG_SAMPLE_ANY ?? "true").toLowerCase() !== "false";
const tennisDebugSeen = new Set();
let tennisDebugCount = 0;
const TENNIS_FIELD_PATTERNS = [
  "point", "points", "game", "games", "serve", "server", "current_server",
  "currentserver", "currentscore", "current_score", "scoreboard",
  "live_score", "match_state", "event_state", "set_score", "sets_score",
  "pointbypoint", "current_set", "currentset", "in_game", "ingame",
];

function tennisFieldHits(payload) {
  if (!payload || typeof payload !== "object") return [];
  try {
    const json = JSON.stringify(payload).toLowerCase();
    return TENNIS_FIELD_PATTERNS.filter((k) => json.includes(k));
  } catch {
    return [];
  }
}

function tennisFieldPaths(payload, prefix = "", out = []) {
  if (!payload || typeof payload !== "object" || out.length >= 40) return out;
  for (const [key, value] of Object.entries(payload)) {
    const path = prefix ? `${prefix}.${key}` : key;
    const lower = key.toLowerCase();
    if (TENNIS_FIELD_PATTERNS.some((needle) => lower.includes(needle))) out.push(path);
    if (value && typeof value === "object" && out.length < 40) tennisFieldPaths(value, path, out);
  }
  return out;
}

// First-look diagnostic: dump the FULL shape of the first few score/status
// payloads we ever receive. Truncated to 2000 chars to stay log-friendly.
// Once we know the schema we can wire score/status into proper storage and
// drop these samples.
const SAMPLE_LIMIT_PER_TYPE = Number(process.env.FRAME_SAMPLE_LIMIT ?? 5);
const sampleCounts = new Map();
function sampleFrameShape(frameType, payload) {
  if (!frameType || frameType === "odds") return;
  const seen = sampleCounts.get(frameType) ?? 0;
  if (seen >= SAMPLE_LIMIT_PER_TYPE) return;
  sampleCounts.set(frameType, seen + 1);
  let body;
  try {
    body = JSON.stringify(payload).slice(0, 2000);
  } catch {
    body = "(unserializable)";
  }
  console.log(`[frame-sample:${frameType}] #${seen + 1} keys=${Object.keys(payload ?? {}).join(",")} body=${body}`);
}

const sportsSeenLastMinute = new Map();
function trackSportSeen(item) {
  if (!item || typeof item !== "object") return;
  const sport =
    item.sport ||
    item.sport_key ||
    item.sportKey ||
    item.sport_slug ||
    item.sportSlug ||
    item.sport_name ||
    item.category ||
    item.league?.sport ||
    item.league?.name ||
    item.tournament?.name ||
    item.event?.sport ||
    item.metadata?.sport ||
    "unknown";
  const key = String(sport || "unknown").toLowerCase();
  sportsSeenLastMinute.set(key, (sportsSeenLastMinute.get(key) || 0) + 1);
}

function isTennisEvent(evt) {
  const slug = String(
    evt?.sport?.slug ?? evt?.sport_key ?? evt?.sport ?? evt?.sportSlug ?? evt?.sport_slug ?? "",
  ).toLowerCase();
  const sportName = String(evt?.sport?.name ?? evt?.sport_name ?? "").toLowerCase();
  return slug === "tennis" || slug.startsWith("tennis") || sportName === "tennis";
}

function debugTennisPayload(evt, rawPayload) {
  if (!TENNIS_DEBUG) return;
  if (tennisDebugCount >= TENNIS_DEBUG_MAX) return;
  if (!isTennisEvent(evt) && !isTennisEvent(rawPayload)) return;
  const live = isLive(evt?.status ?? evt?.in_play ?? evt?.is_live ?? evt?.live ?? evt?.state);
  if (!live && !TENNIS_DEBUG_SAMPLE_ANY) return;
  const id = String(evt?.id ?? "");
  const hits = tennisFieldHits(evt);
  const paths = tennisFieldPaths(evt);
  const debugKey = `${id || "no-id"}:${hits.join("|")}:${paths.slice(0, 8).join("|")}`;
  if (tennisDebugSeen.has(debugKey)) return;
  tennisDebugSeen.add(debugKey);
  tennisDebugCount += 1;
  console.log(
    `[tennis-debug] #${tennisDebugCount}/${TENNIS_DEBUG_MAX} id=${id} `
      + `home=${evt?.home ?? evt?.home_team} away=${evt?.away ?? evt?.away_team} `
      + `topKeys=${Object.keys(evt || {}).join(",")} hits=${hits.join(",") || "(none)"} `
      + `paths=${paths.slice(0, 25).join(",") || "(none)"} `
      + `payload=${JSON.stringify(evt).slice(0, 4000)}`,
  );
}


async function handleEvent(evt) {
  // Skip events that ended long ago — provider sometimes replays old
  // fixtures on (re)subscribe, and we don't want to keep upserting them.
  const commenceMs = new Date(evt?.date ?? evt?.commence_time ?? 0).getTime();
  if (Number.isFinite(commenceMs) && commenceMs > 0) {
    const ageHours = (Date.now() - commenceMs) / 3_600_000;
    const live = isLive(evt?.status ?? evt?.in_play);
    if (!live && ageHours > 4) return;
  }

  // WS price frames (updated/created from a single bookmaker) carry ONLY
  // {bookie, date, id, markets} — no sport, no teams. buildBridgeEventRow
  // returns null for those, so we can't upsert the parent row from the frame.
  // Instead, look the parent up in cache (or DB) and enrich the event so
  // flattenEvent can produce correct selection names.
  const rawId = String(evt?.id ?? "");
  let parentOk = false;
  if (rawId) {
    try {
      parentOk = await upsertEventRow(evt);
    } catch {
      return; // parent upsert failed → don't try children (FK)
    }
  }
  if (!parentOk && rawId) {
    const meta = getEventMeta(rawId) ?? await fetchAndRememberEventMeta(rawId);
    if (!meta) {
      stats.orphanOddsFrames = (stats.orphanOddsFrames ?? 0) + 1;
      return; // no parent anywhere → would FK-fail on live_odds
    }
    // Hydrate event with cached team / sport so flattenEvent picks correct
    // selection names instead of "Home"/"Away".
    if (!evt.home && !evt.home_team && meta.homeTeam) evt.home = meta.homeTeam;
    if (!evt.away && !evt.away_team && meta.awayTeam) evt.away = meta.awayTeam;
    if (!evt.sport_key && meta.sportKey) evt.sport_key = meta.sportKey;
    stats.parentReused = (stats.parentReused ?? 0) + 1;
    // Touch parent last_seen_at so the row doesn't look stale.
    const eventId = `${EVENT_ID_PREFIX}${rawId}`;
    supabase
      .from("odds_api_events")
      .update({ last_seen_at: new Date().toISOString() })
      .eq("event_id", eventId)
      .then(({ error }) => { if (error) rememberError("parent_touch", error, { quiet: true }); });
  }
  // Remember sport/league for score/status enrichment — those frames carry
  // only {id, scores, status, timestamp, type} with NO sport field, so the
  // only way to attribute them to a sport is by joining on this cache.
  rememberEventMeta(rawId, evt);
  const rawRows = flattenEvent(evt);
  maybeLogBucketDiag(); // TRACK A — periodic bucket diagnostics.
  if (!rawRows.length) return;

  // Dedupe by conflict key — Postgres rejects upsert when the same
  // (event_id, bookmaker, market, selection, point) appears twice in one batch
  // with "ON CONFLICT DO UPDATE command cannot affect row a second time".
  // Keep the last occurrence (latest price wins within the frame).
  const dedup = new Map();
  for (const r of rawRows) {
    const k = `${r.event_id}|${r.bookmaker}|${r.market}|${r.selection}|${r.point ?? ""}`;
    dedup.set(k, r);
  }
  let rows = Array.from(dedup.values());

  // Source-aware merge guard: when this frame is from the REST extended
  // poller, drop rows whose key was already written by WS with a fresher
  // provider_ts. WS frames bypass this guard — they always win.
  if (evt?.__source === "rest") {
    rows = rows.filter((r) => {
      const k = `${r.event_id}|${r.bookmaker}|${r.market}|${r.selection}|${r.point ?? ""}`;
      const wsTs = wsLastTsByKey.get(k);
      if (wsTs == null) return true;
      const restTs = new Date(r.provider_ts).getTime();
      return Number.isFinite(restTs) && restTs >= wsTs;
    });
    if (rows.length === 0) return;
  }

  // Sort rows by the exact conflict key so concurrent upserters (WS + REST +
  // multiple bookmakers landing on the same event tick) acquire row locks in
  // a consistent order. This is the standard remedy for Postgres 40P01
  // "deadlock detected" on high-contention upserts. Sort is O(n log n) on
  // small batches (<200 rows typical), negligible vs the network round-trip.
  rows.sort((a, b) => {
    if (a.event_id !== b.event_id) return a.event_id < b.event_id ? -1 : 1;
    if (a.bookmaker !== b.bookmaker) return a.bookmaker < b.bookmaker ? -1 : 1;
    if (a.market !== b.market) return a.market < b.market ? -1 : 1;
    if (a.selection !== b.selection) return a.selection < b.selection ? -1 : 1;
    return (a.point ?? 0) - (b.point ?? 0);
  });

  let { error } = await supabase
    .from("live_odds")
    .upsert(rows, { onConflict: "event_id,bookmaker,market,selection,point" });
  // 40P01 = Postgres deadlock_detected. Loser is chosen arbitrarily; a single
  // retry after a tiny jittered backoff almost always succeeds because the
  // sorted-key ordering means concurrent writers won't deadlock again on the
  // same tuples. Only retry once — persistent 40P01 signals a real problem.
  if (error && (error.code === "40P01" || /deadlock/i.test(error.message ?? ""))) {
    stats.upsertDeadlockRetries = (stats.upsertDeadlockRetries ?? 0) + 1;
    await new Promise((r) => setTimeout(r, 25 + Math.floor(Math.random() * 50)));
    ({ error } = await supabase
      .from("live_odds")
      .upsert(rows, { onConflict: "event_id,bookmaker,market,selection,point" }));
  }
  if (error) {
    rememberError("live_odds_upsert", error);
    return;
  }
  stats.upserts += rows.length;
  stats.lastUpsertAt = new Date().toISOString();
  if (evt?.__source !== "rest") {
    // Remember WS provider_ts so REST never overwrites with a stale price.
    for (const r of rows) {
      const k = `${r.event_id}|${r.bookmaker}|${r.market}|${r.selection}|${r.point ?? ""}`;
      const tsMs = new Date(r.provider_ts).getTime();
      if (Number.isFinite(tsMs)) wsLastTsByKey.set(k, tsMs);
    }
  } else {
    stats.restExtendedRowsMerged = (stats.restExtendedRowsMerged ?? 0) + rows.length;
  }
  // Track when we last received ANY non-h2h market so the watchdog can detect
  // an upstream that quietly stopped pushing secondaries while h2h keeps flowing.
  if (rows.some((r) => r.market !== "h2h")) {
    const ts = Date.now();
    for (const c of connections) c.lastSecondaryMarketAt = ts;
    stats.lastSecondaryMarketAt = new Date().toISOString();
  }
}

// ─── REST extended-markets merger ─────────────────────────────────────────
// WS streams the markets the provider promotes per sport; REST /odds/multi
// returns the full bookmaker catalog including extended/long-tail markets
// (player props, anytime scorer, method-of-victory, race-to-N, etc.). This
// loop polls REST for live events and feeds each per-bookmaker block back
// through handleEvent(), so the SAME flatten + upsert pipeline applies and
// the SAME (event_id,bookmaker,market,selection,point) conflict key
// naturally dedupes against the WS stream. wsLastTsByKey (above) additionally
// guards against a stale REST tick overwriting a fresher WS price.
const wsLastTsByKey = new Map(); // upsert-key -> provider_ts (ms)
const WS_TS_KEY_MAX = 200_000;

const REST_EXTENDED_ENABLED   = String(process.env.REST_EXTENDED_ENABLED ?? "true").toLowerCase() !== "false";
const REST_EXTENDED_POLL_MS   = Number(process.env.REST_EXTENDED_POLL_MS ?? 60_000);
const REST_EXTENDED_CHUNK     = Number(process.env.REST_EXTENDED_CHUNK ?? 10);
const REST_EXTENDED_MAX_EVENTS= Number(process.env.REST_EXTENDED_MAX_EVENTS ?? 80);
const REST_EXTENDED_BASE_URL  = process.env.REST_EXTENDED_BASE_URL ?? "https://api.odds-api.io/v3/odds/multi";

function chunkArr(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

// Convert REST `{id, home, away, bookmakers: {bookie: [markets]}}` into one
// synthesized WS-shaped event per bookmaker, then route through handleEvent
// with __source="rest" so the source-aware guard above can apply.
async function mergeRestEventThroughWsPath(ev) {
  if (!ev || ev.id == null || !ev.bookmakers || typeof ev.bookmakers !== "object") return;
  const baseEvt = {
    id: ev.id,
    home: ev.home, away: ev.away,
    home_team: ev.home, away_team: ev.away,
    date: ev.date,
    status: ev.status,
    sport: ev.sport,
    sport_key: ev.sport?.slug ?? ev.sport_key,
    league: ev.league,
    updatedAt: ev.updatedAt ?? new Date().toISOString(),
    __source: "rest",
  };
  for (const [bookie, markets] of Object.entries(ev.bookmakers)) {
    if (!Array.isArray(markets) || markets.length === 0) continue;
    try {
      await handleEvent({ ...baseEvt, bookie, bookmaker: bookie, markets });
    } catch (err) {
      rememberError("rest_merge", err, { quiet: true });
    }
  }
}

async function fetchLiveEventIdsForRest(limit) {
  const { data, error } = await supabase
    .from("odds_api_events")
    .select("event_id, last_seen_at")
    .eq("is_live", true)
    .order("last_seen_at", { ascending: false })
    .limit(limit);
  if (error) {
    rememberError("rest_extended:list", error, { quiet: true });
    return [];
  }
  const ids = [];
  for (const row of data ?? []) {
    const eid = String(row.event_id ?? "");
    const raw = eid.startsWith(EVENT_ID_PREFIX) ? eid.slice(EVENT_ID_PREFIX.length) : eid;
    if (raw) ids.push(raw);
  }
  return ids;
}

let restExtendedPassNum = 0;
async function runRestExtendedPass() {
  if (shuttingDown) return;
  restExtendedPassNum += 1;
  const ids = await fetchLiveEventIdsForRest(REST_EXTENDED_MAX_EVENTS);
  if (ids.length === 0) {
    stats.restExtendedLastSkippedReason = "no_live_events";
    return;
  }
  let mergedEvents = 0;
  let httpCalls = 0;
  for (const chunk of chunkArr(ids, REST_EXTENDED_CHUNK)) {
    if (shuttingDown) break;
    const url = `${REST_EXTENDED_BASE_URL}?eventIds=${chunk.join(",")}&apiKey=${ODDS_API_KEY}`;
    let res;
    try {
      res = await budgetedFetch(url, {
        priority: PRIORITY.normal,
        endpoint: "v3/odds/multi",
        sport: "mixed",
        init: { headers: { accept: "application/json" } },
      });
    } catch (err) {
      if (err?.code !== "BUDGET_DENIED") rememberError("rest_extended:fetch", err, { quiet: true });
      continue;
    }
    httpCalls += 1;
    if (!res.ok) {
      if (res.status === 429) stats.rateLimited429Count += 1;
      rememberError("rest_extended:http", new Error(`HTTP ${res.status}`), { quiet: true });
      continue;
    }
    let payload;
    try { payload = await res.json(); }
    catch (err) { rememberError("rest_extended:parse", err, { quiet: true }); continue; }
    const events = Array.isArray(payload) ? payload : (Array.isArray(payload?.events) ? payload.events : []);
    for (const ev of events) {
      await mergeRestEventThroughWsPath(ev);
      mergedEvents += 1;
    }
  }
  stats.restExtendedPasses = (stats.restExtendedPasses ?? 0) + 1;
  stats.restExtendedLastRunAt = new Date().toISOString();
  stats.restExtendedLastEventsMerged = mergedEvents;
  stats.restExtendedLastHttpCalls = httpCalls;
  if (wsLastTsByKey.size > WS_TS_KEY_MAX) {
    const drop = Math.ceil(WS_TS_KEY_MAX * 0.1);
    let i = 0;
    for (const k of wsLastTsByKey.keys()) {
      wsLastTsByKey.delete(k);
      if (++i >= drop) break;
    }
  }
  console.log(`[rest-extended#${restExtendedPassNum}] events=${ids.length} merged=${mergedEvents} httpCalls=${httpCalls}`);
}

if (REST_EXTENDED_ENABLED) {
  console.log(`[rest-extended] enabled — poll every ${REST_EXTENDED_POLL_MS}ms, chunk=${REST_EXTENDED_CHUNK}, maxEvents=${REST_EXTENDED_MAX_EVENTS}`);
  setTimeout(async function loop() {
    if (shuttingDown) return;
    try { await runRestExtendedPass(); } catch (err) { rememberError("rest_extended:loop", err, { quiet: true }); }
    if (shuttingDown) return;
    setTimeout(loop, REST_EXTENDED_POLL_MS);
  }, 20_000);
}

// ─── Football + Basketball: fetchAllMarkets dedicated poller ──────────────
// The generic /odds/multi loop above runs sport-agnostic at 60s. Football and
// basketball have the richest long-tail catalogs (player props, race-to-N,
// alt lines, asian handicaps, method-of-victory), so we additionally poll
// those two sports on a tighter cadence with `markets=all` to pull every
// market the provider exposes per event. Results flow through the same
// mergeRestEventThroughWsPath path so dedupe + WS-freshness guard apply.
const ALL_MARKETS_ENABLED   = String(process.env.ALL_MARKETS_ENABLED ?? "true").toLowerCase() !== "false";
const ALL_MARKETS_POLL_MS   = Number(process.env.ALL_MARKETS_POLL_MS ?? 45_000);
const ALL_MARKETS_CHUNK     = Number(process.env.ALL_MARKETS_CHUNK ?? 8);
const ALL_MARKETS_MAX_EVENTS= Number(process.env.ALL_MARKETS_MAX_EVENTS ?? 60);
const ALL_MARKETS_SPORTS    = (process.env.ALL_MARKETS_SPORTS ?? "soccer,football,basketball")
  .split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
const ALL_MARKETS_BASE_URL  = process.env.ALL_MARKETS_BASE_URL ?? "https://api.odds-api.io/v3/odds/multi";

async function fetchLiveEventIdsBySport(sports, limit) {
  const { data, error } = await supabase
    .from("odds_api_events")
    .select("event_id, sport_key, last_seen_at")
    .eq("is_live", true)
    .order("last_seen_at", { ascending: false })
    .limit(limit * 3);
  if (error) {
    rememberError("all_markets:list", error, { quiet: true });
    return [];
  }
  const out = [];
  for (const row of data ?? []) {
    const sk = String(row.sport_key ?? "").toLowerCase();
    if (!sports.some((s) => sk.includes(s))) continue;
    const eid = String(row.event_id ?? "");
    const raw = eid.startsWith(EVENT_ID_PREFIX) ? eid.slice(EVENT_ID_PREFIX.length) : eid;
    if (raw) out.push(raw);
    if (out.length >= limit) break;
  }
  return out;
}

// Pulls the full per-event market catalog from the provider. `markets=all`
// tells odds-api.io to return every market it has for the event(s), bypassing
// the default per-sport curated subset that the WS stream is limited to.
async function fetchAllMarkets(eventIds) {
  if (!Array.isArray(eventIds) || eventIds.length === 0) return [];
  const url = `${ALL_MARKETS_BASE_URL}?eventIds=${eventIds.join(",")}&markets=all&apiKey=${ODDS_API_KEY}`;
  let res;
  try {
    res = await budgetedFetch(url, {
      priority: PRIORITY.normal,
      endpoint: "v3/odds/multi?markets=all",
      sport: "football_basketball",
      init: { headers: { accept: "application/json" } },
    });
  } catch (err) {
    if (err?.code !== "BUDGET_DENIED") rememberError("all_markets:fetch", err, { quiet: true });
    return [];
  }
  if (!res.ok) {
    if (res.status === 429) stats.rateLimited429Count += 1;
    rememberError("all_markets:http", new Error(`HTTP ${res.status}`), { quiet: true });
    return [];
  }
  let payload;
  try { payload = await res.json(); }
  catch (err) { rememberError("all_markets:parse", err, { quiet: true }); return []; }
  return Array.isArray(payload) ? payload : (Array.isArray(payload?.events) ? payload.events : []);
}

let allMarketsPassNum = 0;
async function runAllMarketsPass() {
  if (shuttingDown) return;
  allMarketsPassNum += 1;
  const ids = await fetchLiveEventIdsBySport(ALL_MARKETS_SPORTS, ALL_MARKETS_MAX_EVENTS);
  if (ids.length === 0) {
    stats.allMarketsLastSkippedReason = "no_live_events";
    return;
  }
  let mergedEvents = 0;
  let httpCalls = 0;
  for (const chunk of chunkArr(ids, ALL_MARKETS_CHUNK)) {
    if (shuttingDown) break;
    const events = await fetchAllMarkets(chunk);
    httpCalls += 1;
    for (const ev of events) {
      await mergeRestEventThroughWsPath(ev);
      mergedEvents += 1;
    }
  }
  stats.allMarketsPasses = (stats.allMarketsPasses ?? 0) + 1;
  stats.allMarketsLastRunAt = new Date().toISOString();
  stats.allMarketsLastEventsMerged = mergedEvents;
  stats.allMarketsLastHttpCalls = httpCalls;
  console.log(`[all-markets#${allMarketsPassNum}] sports=${ALL_MARKETS_SPORTS.join("+")} events=${ids.length} merged=${mergedEvents} httpCalls=${httpCalls}`);
}

if (ALL_MARKETS_ENABLED) {
  console.log(`[all-markets] enabled — sports=${ALL_MARKETS_SPORTS.join("+")} poll every ${ALL_MARKETS_POLL_MS}ms, chunk=${ALL_MARKETS_CHUNK}, maxEvents=${ALL_MARKETS_MAX_EVENTS}`);
  setTimeout(async function loop() {
    if (shuttingDown) return;
    try { await runAllMarketsPass(); } catch (err) { rememberError("all_markets:loop", err, { quiet: true }); }
    if (shuttingDown) return;
    setTimeout(loop, ALL_MARKETS_POLL_MS);
  }, 30_000);
}


// ─── Event meta cache (id → sport) for score/status enrichment ───────────
// score/status frames look like {id, scores, status, timestamp, type} — no
// sport, no league, no teams. To know which sport a frame belongs to we
// remember meta from odds frames (and a boot-time prime from Supabase).
const EVENT_META_TTL_MS = Number(process.env.EVENT_META_TTL_MS ?? 24 * 60 * 60_000);
const EVENT_META_MAX    = Number(process.env.EVENT_META_MAX ?? 50_000);
const eventMetaById = new Map(); // id(string) → { sportKey, sportTitle, updatedAt }

function rememberEventMeta(rawId, evt) {
  const id = String(rawId ?? evt?.id ?? "");
  if (!id) return;
  const sportKey = String(evt?.sport?.slug ?? evt?.sport_key ?? "").toLowerCase();
  const sportTitle = evt?.league?.name ?? evt?.sport_title ?? evt?.sport?.name ?? null;
  const homeTeam = evt?.home ?? evt?.home_team ?? null;
  const awayTeam = evt?.away ?? evt?.away_team ?? null;
  const prev = eventMetaById.get(id);
  eventMetaById.set(id, {
    sportKey: sportKey || prev?.sportKey || "",
    sportTitle: sportTitle || prev?.sportTitle || null,
    homeTeam: homeTeam || prev?.homeTeam || null,
    awayTeam: awayTeam || prev?.awayTeam || null,
    updatedAt: Date.now(),
  });
  if (eventMetaById.size > EVENT_META_MAX) {
    const drop = Math.ceil(EVENT_META_MAX * 0.1);
    let i = 0;
    for (const k of eventMetaById.keys()) {
      eventMetaById.delete(k);
      if (++i >= drop) break;
    }
  }
}

function getEventMeta(rawId) {
  const id = String(rawId ?? "");
  if (!id) return null;
  const m = eventMetaById.get(id);
  if (!m) return null;
  if (Date.now() - m.updatedAt > EVENT_META_TTL_MS) {
    eventMetaById.delete(id);
    return null;
  }
  return m;
}

async function fetchAndRememberEventMeta(rawId) {
  const id = String(rawId ?? "");
  if (!id) return null;
  const prefixedId = id.startsWith(EVENT_ID_PREFIX) ? id : `${EVENT_ID_PREFIX}${id}`;
  const { data, error } = await supabase
    .from("odds_api_events")
    .select("event_id, sport_key, sport_title, home_team, away_team")
    .eq("event_id", prefixedId)
    .maybeSingle();
  if (error || !data) return null;
  const meta = {
    sportKey: String(data.sport_key ?? "").toLowerCase(),
    sportTitle: data.sport_title ?? null,
    homeTeam: data.home_team ?? null,
    awayTeam: data.away_team ?? null,
    updatedAt: Date.now(),
  };
  eventMetaById.set(id, meta);
  return meta;
}

async function primeEventMetaCache() {
  try {
    const since = new Date(Date.now() - 24 * 3_600_000).toISOString();
    const { data, error } = await supabase
      .from("odds_api_events")
      .select("event_id, sport_key, sport_title, home_team, away_team")
      .gte("last_seen_at", since)
      .limit(EVENT_META_MAX);
    if (error) {
      console.warn(`[cache-prime] failed: ${error.message}`);
      return;
    }
    let n = 0;
    for (const r of data ?? []) {
      const eid = String(r.event_id ?? "");
      const id = eid.startsWith(EVENT_ID_PREFIX) ? eid.slice(EVENT_ID_PREFIX.length) : eid;
      if (!id) continue;
      eventMetaById.set(id, {
        sportKey: String(r.sport_key ?? "").toLowerCase(),
        sportTitle: r.sport_title ?? null,
        homeTeam: r.home_team ?? null,
        awayTeam: r.away_team ?? null,
        updatedAt: Date.now(),
      });
      n += 1;
    }
    console.log(`[cache-prime] loaded ${n} event meta entries from odds_api_events`);
  } catch (e) {
    console.warn(`[cache-prime] error: ${e?.message ?? e}`);
  }
}

// Handle score/status frame. Looks up sport via eventMetaById, counts
// with/missing-meta, and patches the parent row (scores/is_live/last_seen_at)
// — never an upsert, so we don't create orphan rows without sport.
async function handleScoreOrStatusFrame(payload) {
  const rawId = payload?.id;
  if (rawId == null) return;
  const isStatus = String(payload.type).toLowerCase() === "status";
  const meta = getEventMeta(rawId) ?? await fetchAndRememberEventMeta(rawId);

  if (meta) {
    if (isStatus) stats._windowStatusFramesWithMeta += 1;
    else stats._windowScoreFramesWithMeta += 1;
    const sport = meta.sportKey || "unknown";
    sportsSeenLastMinute.set(sport, (sportsSeenLastMinute.get(sport) || 0) + 1);
    if (sport === "tennis") stats._windowTennisFrames += 1;
  } else {
    if (isStatus) stats._windowStatusFramesMissingMeta += 1;
    else stats._windowScoreFramesMissingMeta += 1;
  }

  const eventId = `${EVENT_ID_PREFIX}${String(rawId)}`;
  const patch = { last_seen_at: new Date().toISOString() };
  if (payload.scores && typeof payload.scores === "object") patch.scores = payload.scores;
  const clock = normalizeClock(payload.clock ?? payload.timer ?? payload.gameClock ?? payload);
  if (clock) {
    if (clock.running == null && payload.status) clock.running = String(payload.status).toLowerCase() === "live";
    patch.clock = clock;
    patch.clock_synced_at = patch.last_seen_at;
  }
  if (isStatus && payload.status) {
    patch.is_live = String(payload.status).toLowerCase() === "live";
  }
  const { error } = await supabase
    .from("odds_api_events")
    .update(patch)
    .eq("event_id", eventId);
  if (error) rememberError("score_status_update", error, { quiet: true });
}

// ─── WebSocket lifecycle ──────────────────────────────────────────────────
function scheduleReconnect(conn, { rateLimited = false } = {}) {
  if (shuttingDown) return;
  // On WS 1013 (provider cooldown) use exponential backoff capped at 60s
  // instead of the aggressive 1s/2s schedule — hammering deepens the throttle.
  let delay;
  if (rateLimited) {
    delay = Math.min(60_000, 5_000 * Math.pow(2, Math.max(0, conn.backoffIdx)));
  } else {
    delay = BACKOFF_MS[Math.min(conn.backoffIdx, BACKOFF_MS.length - 1)];
  }
  conn.backoffIdx += 1;
  stats.reconnects += 1;
  console.warn(`[ws#${conn.idx}] reconnecting in ${delay}ms (attempt ${conn.backoffIdx}${rateLimited ? ", 1013-backoff" : ""})`);
  clearTimeout(conn.reconnectTimer);
  conn.reconnectTimer = setTimeout(() => connect(conn), delay);
}

function connect(conn) {
  if (shuttingDown) return;
  const redactedUrl = conn.url.replace(/apiKey=[^&]+/i, "apiKey=***");
  const keyFp = crypto.createHash("sha256").update(conn.apiKey).digest("hex").slice(0, 8);
  console.log(`[ws#${conn.idx}] connecting group=${conn.groupLabel} keyIdx=${conn.keyIdx} keyFp=${keyFp} sports=${conn.sports.length}(${conn.sports.join(",") || "(all)"}) markets=${conn.markets.length} url=${redactedUrl}`);
  console.log("[odds-ws-subscriptions]", JSON.stringify({
    connectionId: conn.idx,
    group: conn.groupLabel,
    keyIdx: conn.keyIdx,
    sports: conn.sports,
    markets: conn.markets,
    isTennisIncluded: conn.sports.some((s) => String(s).toLowerCase() === "tennis"),
    isTableTennisIncluded: conn.sports.some((s) => String(s).toLowerCase() === "table-tennis"),
  }));

  conn.ws = new WebSocket(conn.url);

  conn.ws.on("open", () => {
    console.log(`[ws#${conn.idx}] open`);
    conn.backoffIdx = 0;
    conn.lastMessageAt = Date.now();
    conn.lastSecondaryMarketAt = Date.now();
    conn.connectedAt = Date.now();
  });

  conn.ws.on("message", async (data) => {
    conn.lastMessageAt = Date.now();
    stats.messages += 1;
    stats._windowMessages += 1;
    stats.lastMessageAt = Date.now();

    const text = data.toString().trim();
    if (!text) return;

    const payloads = [];
    const tryParse = (chunk) => {
      try {
        payloads.push(JSON.parse(chunk));
        return true;
      } catch {
        return false;
      }
    };

    if (!tryParse(text)) {
      for (const line of text.split(/\r?\n/).map((part) => part.trim()).filter(Boolean)) {
        if (tryParse(line)) continue;

        let depth = 0;
        let start = -1;
        let inString = false;
        let escaped = false;

        for (let i = 0; i < line.length; i += 1) {
          const ch = line[i];
          if (start === -1) {
            if (/\s/.test(ch)) continue;
            start = i;
          }

          if (inString) {
            if (escaped) {
              escaped = false;
            } else if (ch === "\\") {
              escaped = true;
            } else if (ch === '"') {
              inString = false;
            }
            continue;
          }

          if (ch === '"') {
            inString = true;
            continue;
          }
          if (ch === "{" || ch === "[") depth += 1;
          if (ch === "}" || ch === "]") {
            depth -= 1;
            if (depth === 0 && start !== -1) {
              tryParse(line.slice(start, i + 1));
              start = -1;
            }
          }
        }
      }
    }

    if (!payloads.length) {
      stats.parseErrors += 1;
      rememberError(
        "parse",
        new Error(`Could not parse websocket frame (${text.length} chars)`),
        { quiet: stats.parseErrors > 5 && stats.parseErrors % 100 !== 0 },
      );
      return;
    }

    for (const payload of payloads) {
      const frameType = String(payload?.type ?? "").toLowerCase();

      // Sample raw shape of the first N score/status frames per process —
      // historic diagnostic; kept until we're confident in the schema.
      sampleFrameShape(frameType, payload);

      // score/status frames carry ONLY {id, scores, status, timestamp, type}
      // — no sport/league/teams. They cannot enter the events pipeline; we
      // enrich them via eventMetaById and patch the parent row directly.
      if (frameType === "score" || frameType === "status") {
        if (frameType === "score") stats._windowScoreFrames += 1;
        else stats._windowStatusFrames += 1;
        await handleScoreOrStatusFrame(payload);
        continue;
      }

      const events = Array.isArray(payload?.events)
        ? payload.events
        : Array.isArray(payload?.data)
          ? payload.data
          : Array.isArray(payload)
            ? payload
            : (payload?.id ? [payload] : []);

      // odds/updated frames — sport lives on the event itself.
      trackSportSeen(payload);
      for (const evt of events) {
        if (isTennisEvent(evt) || isTennisEvent(payload)) stats._windowTennisFrames += 1;
        trackSportSeen(evt);
        debugTennisPayload(evt, payload);
        await handleEvent(evt);
      }
    }

  });

  conn.ws.on("close", (code, reason) => {
    const reasonStr = reason?.toString() || "(none)";
    console.warn(`[ws#${conn.idx}] close code=${code} reason=${reasonStr}`);
    const rateLimited = code === 1013 || /cooldown|rate|limit/i.test(reasonStr);
    if (rateLimited) stats.ws1013Count += 1;
    scheduleReconnect(conn, { rateLimited });
  });

  conn.ws.on("error", (err) => {
    rememberError(`ws#${conn.idx}`, err);
    try { conn.ws.close(); } catch {}
  });
}

// ─── Watchdog ─────────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const conn of connections) {
    if (!conn.ws || conn.ws.readyState !== WebSocket.OPEN) continue;
    const age = now - conn.lastMessageAt;
    if (age > WATCHDOG_MS) {
      console.warn(`[watchdog#${conn.idx}] no messages for ${age}ms — forcing reconnect`);
      try { conn.ws.terminate(); } catch {}
      continue;
    }
    const secAge = now - conn.lastSecondaryMarketAt;
    const sinceConnect = now - conn.connectedAt;
    if (sinceConnect > SECONDARY_MARKET_STALE_MS && secAge > SECONDARY_MARKET_STALE_MS) {
      console.warn(`[watchdog#${conn.idx}] no secondary markets for ${secAge}ms — forcing reconnect`);
      try { conn.ws.terminate(); } catch {}
      continue;
    }
    if (sinceConnect > FORCE_RECONNECT_MS) {
      console.log(`[watchdog#${conn.idx}] periodic reconnect after ${sinceConnect}ms`);
      try { conn.ws.terminate(); } catch {}
    }
  }
}, 15_000);

// ─── Metrics log ──────────────────────────────────────────────────────────
setInterval(() => {
  const openCount = connections.filter((c) => c.ws?.readyState === WebSocket.OPEN).length;
  console.log(
    `[metrics] msgs=${stats.messages} upserts=${stats.upserts} `
    + `parent=${stats.parentUpserts} reused=${stats.parentReused ?? 0} `
    + `orphan=${stats.orphanOddsFrames ?? 0} errors=${stats.errors} `
    + `reconnects=${stats.reconnects} wsOpen=${openCount}/${connections.length}`
  );
}, METRICS_INTERVAL_MS);

// ─── Per-minute health log (rate-limit visibility) ───────────────────────
setInterval(() => {
  stats.messagesLastMinute = stats._windowMessages;
  stats.tennisFramesLastMinute = stats._windowTennisFrames;
  stats.scoreFramesLastMinute = stats._windowScoreFrames;
  stats.statusFramesLastMinute = stats._windowStatusFrames;
  stats.scoreFramesWithMetaLastMinute    = stats._windowScoreFramesWithMeta;
  stats.scoreFramesMissingMetaLastMinute = stats._windowScoreFramesMissingMeta;
  stats.statusFramesWithMetaLastMinute   = stats._windowStatusFramesWithMeta;
  stats.statusFramesMissingMetaLastMinute = stats._windowStatusFramesMissingMeta;
  stats._windowMessages = 0;
  stats._windowTennisFrames = 0;
  stats._windowScoreFrames = 0;
  stats._windowStatusFrames = 0;
  stats._windowScoreFramesWithMeta = 0;
  stats._windowScoreFramesMissingMeta = 0;
  stats._windowStatusFramesWithMeta = 0;
  stats._windowStatusFramesMissingMeta = 0;
  stats._windowStartMs = Date.now();
  const openConnections = connections.filter((c) => c.ws?.readyState === WebSocket.OPEN).length;
  const reconnectingConnections = connections.length - openConnections;
  console.log("[odds-ws-health]", JSON.stringify({
    openConnections,
    expectedOpenConnections: connections.length,
    reconnectingConnections,
    lastMessageAt: stats.lastMessageAt ? new Date(stats.lastMessageAt).toISOString() : null,
    messagesLastMinute: stats.messagesLastMinute,
    tennisFramesLastMinute: stats.tennisFramesLastMinute,
    scoreFramesLastMinute: stats.scoreFramesLastMinute,
    statusFramesLastMinute: stats.statusFramesLastMinute,
    scoreFramesWithMetaLastMinute: stats.scoreFramesWithMetaLastMinute,
    scoreFramesMissingMetaLastMinute: stats.scoreFramesMissingMetaLastMinute,
    statusFramesWithMetaLastMinute: stats.statusFramesWithMetaLastMinute,
    statusFramesMissingMetaLastMinute: stats.statusFramesMissingMetaLastMinute,
    eventMetaCacheSize: eventMetaById.size,
    rateLimited429Count: stats.rateLimited429Count,
    ws1013Count: stats.ws1013Count,
  }));
  console.log("[odds-ws-sports-seen]", JSON.stringify({
    sportsSeenLastMinute: Object.fromEntries(sportsSeenLastMinute),
  }));
  sportsSeenLastMinute.clear();
}, 60_000);


// ─── HTTP seed loop with exponential backoff on 429 ───────────────────────
// Some sports never produce WS frames between matches. The seed loop polls
// the REST snapshot for each sport so we don't miss new fixtures. Crucially:
//
//   • Sports are seeded SEQUENTIALLY (never 10-in-parallel).
//   • On HTTP 429 (or 5xx) we back off 60s → 120s → 240s → 480s (capped),
//     up to SEED_RETRY_MAX_ATTEMPTS. We do not retry hard on 429.
//   • On 2xx we reset that sport's attempt counter to 0.
//   • The whole pass sleeps SEED_PASS_INTERVAL_MS before starting again.
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Per-sport cool-off. When the provider returns 429 we stop retrying that
// sport for SPORT_COOLOFF_MS — burning more requests just deepens the throttle.
const SPORT_COOLOFF_MS = Number(process.env.SPORT_COOLOFF_MS ?? 30 * 60_000);
const sportCooloffUntil = new Map();
function isSportCooled(sport) {
  const until = sportCooloffUntil.get(sport);
  if (!until) return false;
  if (until < Date.now()) { sportCooloffUntil.delete(sport); return false; }
  return true;
}
function coolSport(sport, ms = SPORT_COOLOFF_MS) {
  sportCooloffUntil.set(sport, Date.now() + ms);
}

async function seedFetchWithBackoff(sport) {
  if (isSportCooled(sport)) {
    const remaining = Math.round((sportCooloffUntil.get(sport) - Date.now()) / 1000);
    console.log(`[seed:${sport}] skipped — in cool-off (${remaining}s left)`);
    return null;
  }
  const url = `${SEED_BASE_URL}?apiKey=${ODDS_API_KEY}&sport=${encodeURIComponent(sport)}`;
  for (let attempt = 0; attempt < SEED_RETRY_MAX_ATTEMPTS; attempt += 1) {
    if (shuttingDown) return null;
    let res;
    try {
      res = await budgetedFetch(url, {
        priority: PRIORITY.low,        // seed is background — never starves Live
        endpoint: "v3/events",
        sport,
        init: { headers: { accept: "application/json" } },
      });
    } catch (err) {
      if (err?.code === "BUDGET_DENIED") return null;
      rememberError(`seed:${sport}:network`, err);
      const wait = SEED_RETRY_BACKOFF_MS[Math.min(attempt, SEED_RETRY_BACKOFF_MS.length - 1)];
      console.warn(`[seed:${sport}] network error — waiting ${wait}ms before retry (attempt ${attempt + 1})`);
      await sleep(wait);
      continue;
    }

    if (res.ok) return res;

    // 429 → STOP. Cool the sport for 30 min; don't burn more requests.
    if (res.status === 429) {
      stats.rateLimited429Count += 1;
      rememberError(`seed:${sport}:http`, new Error("HTTP 429"));
      coolSport(sport);
      console.warn(`[seed:${sport}] HTTP 429 — cooling sport for ${SPORT_COOLOFF_MS / 60_000}min`);
      return null;
    }

    // 5xx → short backoff, but only inside this pass.
    if (res.status >= 500) {
      const wait = SEED_RETRY_BACKOFF_MS[Math.min(attempt, SEED_RETRY_BACKOFF_MS.length - 1)];
      rememberError(`seed:${sport}:http`, new Error(`HTTP ${res.status}`));
      console.warn(`[seed:${sport}] HTTP ${res.status} — backing off ${wait}ms (attempt ${attempt + 1}/${SEED_RETRY_MAX_ATTEMPTS})`);
      await sleep(wait);
      continue;
    }

    // Any other status (4xx besides 429) is permanent — log and stop retrying.
    rememberError(`seed:${sport}:http`, new Error(`HTTP ${res.status}`));
    console.error(`[seed:${sport}] permanent HTTP ${res.status} — giving up this pass`);
    coolSport(sport, 10 * 60_000); // also cool to avoid hammering
    return null;
  }
  console.error(`[seed:${sport}] giving up after ${SEED_RETRY_MAX_ATTEMPTS} attempts`);
  return null;
}

async function seedOneSport(sport) {
  const res = await seedFetchWithBackoff(sport);
  if (!res) return 0;
  let payload;
  try {
    payload = await res.json();
  } catch (err) {
    rememberError(`seed:${sport}:parse`, err);
    return 0;
  }
  const events = Array.isArray(payload?.events)
    ? payload.events
    : Array.isArray(payload) ? payload : [];
  let upserts = 0;
  for (const evt of events) {
    if (shuttingDown) break;
    try {
      await upsertEventRow(evt);
      upserts += 1;
    } catch {
      /* counted in rememberError */
    }
  }
  return upserts;
}

let seedPassNum = 0;
async function runSeedPass() {
  if (shuttingDown) return;
  seedPassNum += 1;
  console.log(`[seed#${seedPassNum}] start for ${SPORTS.length} sports`);
  let total = 0;
  for (const sport of SPORTS) {
    if (shuttingDown) break;
    total += await seedOneSport(sport);
    if (shuttingDown) break;
    await sleep(SEED_SPORT_GAP_MS);
  }
  console.log(`[seed#${seedPassNum}] done — ${total} total upserts`);
}

if (SEED_ENABLED) {
  console.log(
    `[seed] enabled — pass every ${SEED_PASS_INTERVAL_MS}ms, ` +
    `${SEED_SPORT_GAP_MS}ms between sports, ` +
    `backoff on 429: ${SEED_RETRY_BACKOFF_MS.join("/")}ms`
  );
  // Kick off after a small delay so the WS opens first, then loop forever.
  setTimeout(async function loop() {
    if (shuttingDown) return;
    try { await runSeedPass(); } catch (err) { rememberError("seed:pass", err); }
    if (shuttingDown) return;
    setTimeout(loop, SEED_PASS_INTERVAL_MS);
  }, 15_000);
}

// ─── HTTP /health ─────────────────────────────────────────────────────────

const INTERNAL_HEALTH_TOKEN = process.env.INTERNAL_HEALTH_TOKEN ?? "";

function checkInternalToken(req) {
  if (!INTERNAL_HEALTH_TOKEN) return true; // unset = allow (dev only)
  const auth = req.headers["authorization"] ?? "";
  const m = /^Bearer\s+(.+)$/i.exec(auth);
  return !!m && m[1] === INTERNAL_HEALTH_TOKEN;
}

function tennisLiveSnapshot() {
  const openConnections = connections.filter((c) => c.ws?.readyState === WebSocket.OPEN).length;
  const reconnectingConnections = connections.length - openConnections;
  return {
    ws: {
      openConnections,
      expectedOpenConnections: connections.length,
      reconnectingConnections,
      ws1013Count: stats.ws1013Count ?? 0,
      lastMessageAt: stats.lastMessageAt ? new Date(stats.lastMessageAt).toISOString() : null,
      messagesLastMinute: stats.messagesLastMinute ?? 0,
    },
    scores: {
      scoreFramesLastMinute: stats.scoreFramesLastMinute ?? 0,
      statusFramesLastMinute: stats.statusFramesLastMinute ?? 0,
      scoreFramesWithMetaLastMinute: stats.scoreFramesWithMetaLastMinute ?? 0,
      scoreFramesMissingMetaLastMinute: stats.scoreFramesMissingMetaLastMinute ?? 0,
    },
    tennis: {
      tennisFramesLastMinute: stats.tennisFramesLastMinute ?? 0,
    },
  };
}

http.createServer((req, res) => {
  if (req.url === "/health/tennis-live") {
    if (!checkInternalToken(req)) {
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(tennisLiveSnapshot()));
    return;
  }
  if (req.url === "/health" || req.url === "/") {
    const openCount = connections.filter((c) => c.ws?.readyState === WebSocket.OPEN).length;
    const oldestMsgAge = Math.max(...connections.map((c) => Date.now() - c.lastMessageAt));
    const body = {
      status: openCount === connections.length ? "ok" : (openCount > 0 ? "degraded" : "down"),
      version: VERSION,
      wsConnected: openCount,
      wsTotal: connections.length,
      lastMessageAgeMs: oldestMsgAge,
      keyFingerprint: KEY_FINGERPRINT,
      sports: SPORTS,
      sportChunks: connections.map((c) => ({ idx: c.idx, group: c.groupLabel, keyIdx: c.keyIdx, sports: c.sports, marketCount: c.markets.length, open: c.ws?.readyState === WebSocket.OPEN })),
      markets: MARKETS,
      marketGroups: MARKET_GROUPS,
      stats,
      budget: {
        perHour:   { used: reqWindow.hour.length,   soft: REQ_BUDGET_PER_HOUR,   hard: REQ_HARD_CAP_PER_HOUR },
        perMinute: { used: reqWindow.minute.length, soft: REQ_BUDGET_PER_MINUTE },
        totals:    reqStats,
        emptyMarketsCached: emptyMarketCache.size,
        cooledSports: Array.from(sportCooloffUntil.entries())
          .filter(([, t]) => t > Date.now())
          .map(([s, t]) => ({ sport: s, secLeft: Math.round((t - Date.now()) / 1000) })),
      },
    };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(body, null, 2));
    return;
  }
  res.writeHead(404); res.end();
}).listen(PORT, () => console.log(`[http] /health on :${PORT}`));

// ─── Per-minute tennis-live summary log ───────────────────────────────────
setInterval(() => {
  console.log("[tennis-live-health]", JSON.stringify(tennisLiveSnapshot()));
}, 60_000);

// ─── Shutdown ─────────────────────────────────────────────────────────────
function shutdown(sig) {
  console.log(`[shutdown] ${sig}`);
  shuttingDown = true;
  for (const c of connections) {
    clearTimeout(c.reconnectTimer);
    try { c.ws?.close(); } catch {}
  }
  setTimeout(() => process.exit(0), 1000);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("uncaughtException", (e) => { rememberError("uncaught", e); });
process.on("unhandledRejection", (e) => { rememberError("unhandled", e); });

// ─── Boot ─────────────────────────────────────────────────────────────────
// Loud, unmistakable fingerprint so Railway ops can confirm exactly which
// revision is live and whether the Track A / Track B code paths are loaded.
// If any of these show `MISSING`, the container is running a stale image.
const FEATURE_FLAGS = {
  bucket_markets_loaded: typeof BUCKET_MARKETS !== "undefined" && BUCKET_MARKETS.has("exact_goals") && BUCKET_MARKETS.has("multi_goals"),
  side_handicap_markets_loaded: typeof SIDE_HANDICAP_MARKETS !== "undefined" && SIDE_HANDICAP_MARKETS.has("corners_hcp") && SIDE_HANDICAP_MARKETS.has("cards_hcp"),
  canonicalize_match_side: typeof canonicalizeMatchSide === "function",
  is_quarter_line: typeof isQuarterLine === "function",
  normalize_bucket_selection: typeof normalizeBucketSelection === "function",
  bucket_diag_logger: typeof maybeLogBucketDiag === "function",
};
console.log(`[boot] === odds-api-bridge fingerprint === bridge="odds-api-bridge" version=v${VERSION} node=${process.version} git_sha=${process.env.RAILWAY_GIT_COMMIT_SHA ?? process.env.GIT_SHA ?? "unset"} built_at=${process.env.BUILD_TIMESTAMP ?? "unset"} feature_flags=${JSON.stringify(FEATURE_FLAGS)}`);
console.log(`[boot] starting odds-api-bridge v${VERSION} file=${import.meta.url} cwd=${process.cwd()} keys=${ODDS_API_KEYS.length} primaryKeyFp=${KEY_FINGERPRINT} ws_connections=${connections.length} sports=${SPORTS.length} marketGroups=${MARKET_GROUPS.length} totalMarkets=${MARKETS.length}`);
// Note: provider streams ALL markets when the `markets` param is omitted,
// and each API key permits 2 parallel WS connections (cap: 10 sports per WS).
console.log(`[boot] capacity: ${ODDS_API_KEYS.length} key(s) × ${WS_CONNECTIONS_PER_KEY} WS × ${PROVIDER_SPORTS_PER_WS_CAP} sports = ${TOTAL_CAPACITY} sport-slots; using ${EFFECTIVE_SPORTS.length}/${SPORTS.length} sports across ${WS_PLAN.length} connection(s)`);
if (DROPPED_SPORTS.length) {
  console.warn(`[boot] ⚠ ${DROPPED_SPORTS.length} sport(s) dropped (over capacity): ${DROPPED_SPORTS.join(",")} — add ODDS_API_IO_KEY_2 to cover them`);
}


if (SUPABASE_BASE_URL !== SUPABASE_URL) {
  console.warn(`[boot] normalized SUPABASE_URL from ${SUPABASE_URL} to ${SUPABASE_BASE_URL}`);
}
// Prime the meta cache BEFORE opening the WS so the first wave of score/
// status frames can already be attributed to a sport. Non-blocking failure.
primeEventMetaCache().finally(() => {
  for (const conn of connections) connect(conn);
});

