/**
 * Pure helpers for the WS bridge — no side effects, no env access.
 * Extracted so they can be unit-tested independently (see
 * src/lib/__tests__/odds-events-clock-scores.test.ts).
 */

export function normalizeClock(raw) {
  if (!raw || typeof raw !== "object") return null;
  const minute = Number(raw.minute);
  const playedSeconds = Number(raw.playedSeconds ?? raw.played_seconds);
  const remainingSeconds = Number(raw.remainingSeconds ?? raw.remaining_seconds);
  const period = Number(raw.period);
  const hasAny =
    Number.isFinite(minute) ||
    Number.isFinite(playedSeconds) ||
    Number.isFinite(remainingSeconds) ||
    Number.isFinite(period) ||
    typeof raw.running === "boolean" ||
    typeof raw.statusDetail === "string" ||
    typeof raw.status_detail === "string";
  if (!hasAny) return null;
  const out = {};
  if (Number.isFinite(minute)) out.minute = minute;
  if (Number.isFinite(playedSeconds)) out.playedSeconds = playedSeconds;
  if (Number.isFinite(remainingSeconds)) out.remainingSeconds = remainingSeconds;
  if (Number.isFinite(period)) out.period = period;
  if (typeof raw.running === "boolean") out.running = raw.running;
  const status = raw.statusDetail ?? raw.status_detail;
  if (typeof status === "string" && status) out.statusDetail = status;
  const injury = Number(raw.injuryTime ?? raw.injury_time);
  if (Number.isFinite(injury)) out.injuryTime = injury;
  if (raw.serve === "home" || raw.serve === "away") out.serve = raw.serve;
  return out;
}

export function normalizeScores(raw) {
  if (!raw) return null;
  if (Array.isArray(raw) && raw.length >= 2) {
    const home = Number(raw[0]);
    const away = Number(raw[1]);
    if (Number.isFinite(home) && Number.isFinite(away)) return { home, away };
  }
  if (typeof raw === "object" && !Array.isArray(raw)) {
    const home = Number(raw.home ?? raw.h ?? raw.homeScore ?? raw.home_score);
    const away = Number(raw.away ?? raw.a ?? raw.awayScore ?? raw.away_score);
    if (Number.isFinite(home) && Number.isFinite(away)) {
      const out = { home, away };
      if (raw.periods && typeof raw.periods === "object") {
        out.periods = raw.periods;
      }
      return out;
    }
  }
  return null;
}

/**
 * Build the parent `odds_api_events` row from a WS frame.
 *
 * CRITICAL: WS price frames usually omit clock/scores. This builder MUST
 * leave those columns OFF the row (rather than setting them to null) so the
 * upsert preserves the snapshot stored by the REST `sync-odds-events` job.
 */
export function buildBridgeEventRow(evt, { nowIso, eventIdPrefix, isLive, safeIso }) {
  if (!evt?.id) return null;
  const clock = normalizeClock(evt.clock ?? evt.timer ?? evt.gameClock);
  const scores = normalizeScores(evt.scores ?? evt.score);
  const row = {
    event_id: `${eventIdPrefix}${String(evt.id)}`,
    sport_key: String(evt.sport?.slug ?? evt.sport_key ?? ""),
    // Prefer the provider's per-event league/tournament name. WS frames often
    // include both `sport.name` (generic: "Football") and `league.name`
    // (specific: "Brazil - Serie A"); using sport first was overwriting the
    // full league label that the betting cards need above each card.
    sport_title: evt.league?.name ?? evt.sport_title ?? evt.sport?.name ?? null,
    commence_time: safeIso(evt.date ?? evt.commence_time),
    home_team: String(evt.home ?? evt.home_team ?? ""),
    away_team: String(evt.away ?? evt.away_team ?? ""),
    is_live: isLive(evt.status ?? evt.in_play),
    last_seen_at: nowIso,
  };
  if (clock) {
    row.clock = clock;
    row.clock_synced_at = nowIso;
  }
  if (scores) row.scores = scores;
  if (!row.sport_key && !row.home_team && !row.away_team) return null;
  // commence_time is NOT NULL in odds_api_events — drop rows whose date/
  // commence_time didn't parse, so upsert won't fail with a NOT NULL violation.
  if (!row.commence_time) return null;
  return row;
}

/**
 * TRACK A — EXACT_GOALS / MULTI_GOALS bucket normalization.
 *
 * These markets are NOT Over/Under lines — they are discrete buckets
 * (EXACT_GOALS: 0..6 and 7+) or bands (MULTI_GOALS: 0-1, 2-3, 4-6, 7+).
 * The odds-api provider emits them with named selections; historically our
 * generic branch also accepted degenerate `{over, under, total=0}` shapes,
 * which produced meaningless "Over point=0" / "Under point=0" rows that
 * could never settle. The bridge now feeds candidate selections through
 * `normalizeBucketSelection` and drops anything that is not a canonical
 * bucket/band label.
 *
 * Returned canonicals:
 *   EXACT_GOALS → "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7+"
 *   MULTI_GOALS → "0-1" | "2-3" | "4-6" | "7+"
 *
 * Returns `null` for anything that can't be mapped (Over/Under/etc.).
 */
export const EXACT_GOAL_BUCKETS = ["0", "1", "2", "3", "4", "5", "6", "7+"];
export const MULTI_GOAL_BANDS = ["0-1", "2-3", "4-6", "7+"];
export const BUCKET_MARKETS = new Set(["exact_goals", "multi_goals"]);

function extractIntsSorted(raw) {
  const s = String(raw ?? "").replace(/[+]/g, " plus ");
  const matches = s.match(/\d+/g);
  if (!matches) return [];
  const ints = matches.map((x) => parseInt(x, 10)).filter(Number.isFinite);
  ints.sort((a, b) => a - b);
  return ints;
}

export function normalizeBucketSelection(market, rawSelection) {
  const raw = String(rawSelection ?? "").trim();
  if (!raw) return null;
  const lower = raw.toLowerCase();

  // Reject shape-sniffed shapes that make no sense here.
  if (lower === "over" || lower === "under" || lower === "yes" || lower === "no") {
    return null;
  }
  if (lower === "home" || lower === "away" || lower === "draw") return null;

  const openEnded =
    /\bor\s+more\b/.test(lower) ||
    /\+/.test(raw) ||
    /\bplus\b/.test(lower) ||
    /\bmore\b/.test(lower);
  const ints = extractIntsSorted(raw);

  if (market === "exact_goals") {
    // Named forms: "0 goals", "1 goal", "no goals", "2+", "7 or more".
    if (/\bno\s+goals?\b/.test(lower) && ints.length === 0) return "0";
    if (ints.length === 1) {
      const n = ints[0];
      if (openEnded) return n >= 7 ? "7+" : String(n);
      if (n >= 0 && n <= 6) return String(n);
      if (n >= 7) return "7+";
    }
    return null;
  }

  if (market === "multi_goals") {
    if (ints.length === 2) {
      const [a, b] = ints;
      if (a === 0 && b === 1) return "0-1";
      if (a === 2 && b === 3) return "2-3";
      if (a === 4 && b === 6) return "4-6";
      return null;
    }
    if (ints.length === 1) {
      const n = ints[0];
      if (openEnded && n >= 7) return "7+";
      if (n >= 7) return "7+";
      // "5 or 6", "0 or 1" edge shapes handled above via 2-int extraction.
    }
    if (/\b7\+\b/.test(raw) || /\b7\s*(or\s+more|plus)\b/.test(lower)) return "7+";
    return null;
  }

  return null;
}

/**
 * TRACK A diagnostic helper — collect per-bucket counts so we can inspect
 * ingestion via `getBucketDiagnostics()`.
 */
export function makeBucketDiagnostics() {
  const state = {
    exact_goals: Object.fromEntries(EXACT_GOAL_BUCKETS.map((k) => [k, 0])),
    multi_goals: Object.fromEntries(MULTI_GOAL_BANDS.map((k) => [k, 0])),
    dropped: { exact_goals: 0, multi_goals: 0 },
  };
  return {
    record(market, canonical) {
      if (!BUCKET_MARKETS.has(market)) return;
      if (canonical == null) {
        state.dropped[market] = (state.dropped[market] ?? 0) + 1;
        return;
      }
      state[market][canonical] = (state[market][canonical] ?? 0) + 1;
    },
    snapshot() {
      return JSON.parse(JSON.stringify(state));
    },
  };
}

/**
 * TRACK B — Canonical side normalization for corners_hcp / cards_hcp.
 *
 * The provider streams team-side handicap rows under a variety of outcome
 * names: literal `Home`/`Away`, the actual team name, and (in tournament
 * feeds) opaque bracket placeholders like `2D`, `2G`, `3E/3F/3G/3I/3J`,
 * `W73`, `1A`, etc. The latter are outright / progression outcomes that
 * MUST NOT be ingested as match handicap rows.
 *
 * Returns "home" | "away" | null. Null → drop the row.
 *
 * Accepts:
 *   - literal "home" / "away" (any case, punctuation stripped)
 *   - short aliases: h / a, 1 / 2, host / guest, local / visitor
 *   - team-name match against homeName / awayName (accent-insensitive, no
 *     punctuation, case-insensitive)
 *
 * Rejects:
 *   - bracket placeholders: /^\d[A-Z]$/, /^[A-Z]\d+$/ (W73), progressions
 *     with "/" separators, "3rd", "runner", numeric-only tokens
 *   - anything else that isn't provably home or away
 */
export const SIDE_HANDICAP_MARKETS = new Set([
  "corners_hcp",
  "cards_hcp",
  "corners_hcp_h1",
]);

function normSideKey(v) {
  return String(v ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, "")
    .toLowerCase();
}

const HOME_ALIASES = new Set(["home", "h", "1", "host", "local"]);
const AWAY_ALIASES = new Set(["away", "a", "2", "guest", "visitor", "visitors"]);

// Bracket / outright placeholder shapes that must never be treated as sides.
const BRACKET_PATTERNS = [
  /^\d+[a-z]$/i,          // 2D, 3F, 12B
  /^[a-z]\d+$/i,          // W73, L4
  /^\d+[a-z]\d*$/i,       // 3E1
  /\//,                    // 3E/3F/3G/3I/3J
  /^(runner|winner|advance|to\s*advance|group)/i,
];

export function canonicalizeMatchSide(rawSelection, homeName, awayName) {
  const key = normSideKey(rawSelection);
  if (!key) return null;

  // Reject bracket/outright placeholders early (raw string check).
  const raw = String(rawSelection ?? "").trim();
  for (const rx of BRACKET_PATTERNS) {
    if (rx.test(raw)) return null;
  }

  if (HOME_ALIASES.has(key)) return "home";
  if (AWAY_ALIASES.has(key)) return "away";

  const homeKey = normSideKey(homeName);
  const awayKey = normSideKey(awayName);
  if (homeKey && key === homeKey) return "home";
  if (awayKey && key === awayKey) return "away";
  return null;
}

/**
 * Quarter-line detector for CORNERS_AH / CARDS_AH policy.
 * Returns true for .25 / .75 fractional parts. Integer and .5 lines return false.
 */
export function isQuarterLine(point) {
  const n = Number(point);
  if (!Number.isFinite(n)) return true; // reject unknown as quarter
  const frac = Math.abs(n - Math.trunc(n));
  return Math.abs(frac - 0.25) < 1e-9 || Math.abs(frac - 0.75) < 1e-9;
}
