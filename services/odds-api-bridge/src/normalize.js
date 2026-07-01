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

/* ─────────────────────────────────────────────────────────────────────
 * PLAYER-PROP TAXONOMY (bridge preserves player identity)
 *
 * The `live_odds` table's composite PK is
 *   (event_id, bookmaker, market, selection, point)
 * with no dedicated player column. To avoid a schema change and PK
 * churn, we encode the player identity INTO `selection` for every
 * player-prop market:
 *
 *   Over/Under shape → `${player}|Over` / `${player}|Under`
 *   Yes/No     shape → `${player}|Yes`  / `${player}|No`
 *   Anytime    shape → `${player}` (single-side, no delimiter)
 *
 * Rows where a player cannot be extracted are DROPPED at the bridge —
 * they were previously ingested as bare "Over"/"Under" rows with no
 * player identity, which is the display bug we're closing here.
 * ────────────────────────────────────────────────────────────────── */

// Canonical dbMarket names (post-`toDbMarket`) that carry per-player
// selections. `anytime_scorer` / `first_scorer` / `last_scorer` are the
// classic single-side "player named as selection" shape — routing them
// through buildPlayerPropRows guarantees the extractor drops O/U/Y/N
// junk rows and encodes identity uniformly.
export const PLAYER_PROP_MARKETS = new Set([
  // Soccer — scorer markets (anytime/first/last)
  "anytime_scorer",
  "first_scorer",
  "last_scorer",
  // Soccer — stat props
  "player_shots",
  "player_shots_on_target",
  "player_shots_on_target_outside_box",
  "player_headed_shots_on_target",
  "player_passes",
  "player_tackles",
  "player_fouls",
  "player_fouls_committed",
  "player_to_be_fouled",
  "player_cards",
  "player_to_be_booked",
  "player_to_assist",
  "player_to_score_or_assist",
  "player_goals_milestones",
  "player_assists_milestones",
  "goalkeeper_saves",
  "goalkeeper_saves_home",
  "goalkeeper_saves_away",
  // Basketball
  "player_points",
  "player_rebounds",
  "player_assists",
  "player_threes",
  "player_steals",
  "player_blocks",
  "player_points_milestones",
  "player_rebounds_milestones",
  "player_assists_milestones",
  "player_threes_milestones",
  "player_first_basket",
  // AFL / catch-all
  "total_player_disposals",
  "player_props",
]);


// Canonical bucket per market — the display layer uses this to group.
export const PLAYER_PROP_TAXONOMY = {
  anytime_scorer: "PLAYER_ANYTIME_SCORER",
  first_scorer: "PLAYER_FIRST_SCORER",
  last_scorer: "PLAYER_LAST_SCORER",
  player_shots: "PLAYER_SHOTS_OU",
  player_shots_on_target: "PLAYER_SHOTS_ON_TARGET_OU",
  player_shots_on_target_outside_box: "PLAYER_SHOTS_ON_TARGET_OU",
  player_headed_shots_on_target: "PLAYER_SHOTS_ON_TARGET_OU",
  player_passes: "PLAYER_PASSES_OU",
  player_tackles: "PLAYER_TACKLES_OU",
  player_fouls: "PLAYER_FOULS_COMMITTED_OU",
  player_fouls_committed: "PLAYER_FOULS_COMMITTED_OU",
  player_to_be_fouled: "PLAYER_TO_BE_FOULED_OU",
  player_cards: "PLAYER_CARDS",
  player_to_be_booked: "PLAYER_TO_BE_BOOKED",
  player_to_assist: "PLAYER_TO_ASSIST",
  player_to_score_or_assist: "PLAYER_TO_SCORE_OR_ASSIST",
  player_goals_milestones: "PLAYER_GOALS_MILESTONES",
  player_assists_milestones: "PLAYER_ASSISTS_MILESTONES",
  goalkeeper_saves: "GOALKEEPER_SAVES_OU",
  goalkeeper_saves_home: "GOALKEEPER_SAVES_OU",
  goalkeeper_saves_away: "GOALKEEPER_SAVES_OU",
  player_points: "PLAYER_POINTS_OU",
  player_rebounds: "PLAYER_REBOUNDS_OU",
  player_assists: "PLAYER_ASSISTS_OU",
  player_threes: "PLAYER_THREES_OU",
  player_steals: "PLAYER_STEALS_OU",
  player_blocks: "PLAYER_BLOCKS_OU",
  player_points_milestones: "PLAYER_POINTS_MILESTONES",
  player_rebounds_milestones: "PLAYER_REBOUNDS_MILESTONES",
  player_threes_milestones: "PLAYER_THREES_MILESTONES",
  player_first_basket: "PLAYER_FIRST_BASKET",
  total_player_disposals: "PLAYER_DISPOSALS_OU",
  player_props: "PLAYER_PROPS_GENERIC",
};

const OU_YN_RE = /^\s*(over|under|yes|no|o|u|y|n)\s*$/i;

/**
 * Extract a human player name from a provider odd payload. Returns the
 * trimmed string or null. Explicitly ignores Over/Under/Yes/No labels so
 * they never sneak in as fake "players".
 */
export function extractPlayerName(odd) {
  if (!odd || typeof odd !== "object") return null;
  const candidates = [
    odd.player,
    odd.player_name,
    odd.playerName,
    odd.participant,
    odd.participant_name,
    odd.runner,
    odd.runner_name,
    odd.competitor,
    odd.description,
    odd.entity,
    odd.athlete,
    // Only fall back to name/label/outcome when they aren't O/U/Y/N —
    // some feeds put the player in `name` and the side in `selection`,
    // others do the opposite.
    !OU_YN_RE.test(String(odd.name ?? "")) ? odd.name : null,
    !OU_YN_RE.test(String(odd.label ?? "")) ? odd.label : null,
    !OU_YN_RE.test(String(odd.selection ?? "")) ? odd.selection : null,
    !OU_YN_RE.test(String(odd.outcome ?? "")) ? odd.outcome : null,
  ];
  for (const c of candidates) {
    if (c == null) continue;
    const s = String(c).trim();
    if (!s) continue;
    if (OU_YN_RE.test(s)) continue;
    // Ignore obvious non-player tokens.
    if (/^\d+(\.\d+)?$/.test(s)) continue;
    return s;
  }
  return null;
}

/**
 * Extract the Over/Under/Yes/No side (when present) from a provider
 * payload. Returns "Over"|"Under"|"Yes"|"No" or null.
 */
export function extractOverUnderSide(odd) {
  if (!odd || typeof odd !== "object") return null;
  const raw =
    odd.side ?? odd.selection ?? odd.name ?? odd.label ?? odd.outcome ?? null;
  const s = String(raw ?? "").trim().toLowerCase();
  if (s === "over" || s === "o") return "Over";
  if (s === "under" || s === "u") return "Under";
  if (s === "yes" || s === "y") return "Yes";
  if (s === "no" || s === "n") return "No";
  return null;
}

/**
 * Build rows for a player-prop market. Emits `${player}|Side` selections
 * and preserves the point/line. Handles three payload shapes:
 *   1. Over/Under with paired keys: { player, over, under, hdp|point|line }
 *   2. Yes/No paired keys:          { player, yes, no }
 *   3. Single-side listed selection:{ player|name|label, price|odds, point? }
 *      → uses extractOverUnderSide to decide Over/Under/Yes/No, or falls
 *        back to `${player}` for anytime shape (no side, no line).
 * Returns an array of { selection, price, point } tuples ready for pushRow.
 */
export function buildPlayerPropRows(odd) {
  const rows = [];
  if (!odd || typeof odd !== "object") return rows;
  const player = extractPlayerName(odd);
  if (!player) return rows; // drop — no identity, unusable
  const point = Number(
    odd.hdp ?? odd.point ?? odd.line ?? odd.total ?? odd.handicap ?? 0,
  );
  const pt = Number.isFinite(point) ? point : 0;

  // Paired Over/Under
  const overPrice = odd.over ?? odd.Over ?? odd.o;
  const underPrice = odd.under ?? odd.Under ?? odd.u;
  if (overPrice != null || underPrice != null) {
    if (overPrice != null)
      rows.push({ selection: `${player}|Over`, price: overPrice, point: pt });
    if (underPrice != null)
      rows.push({ selection: `${player}|Under`, price: underPrice, point: pt });
    return rows;
  }
  // Paired Yes/No
  const yesPrice = odd.yes ?? odd.Yes;
  const noPrice = odd.no ?? odd.No;
  if (yesPrice != null || noPrice != null) {
    if (yesPrice != null)
      rows.push({ selection: `${player}|Yes`, price: yesPrice, point: 0 });
    if (noPrice != null)
      rows.push({ selection: `${player}|No`, price: noPrice, point: 0 });
    return rows;
  }
  // Single-side listed selection
  const price =
    odd.price ?? odd.odds ?? odd.odd ?? odd.value ?? odd.decimal ?? odd.dec ??
    odd.coefficient;
  if (price == null) return rows;
  const side = extractOverUnderSide(odd);
  if (side) {
    rows.push({ selection: `${player}|${side}`, price, point: pt });
  } else {
    // Anytime shape (goalscorer-like, but for stats): single-side price
    // attached to a player with no O/U marker. Encode as bare player name.
    rows.push({ selection: player, price, point: pt });
  }
  return rows;
}

