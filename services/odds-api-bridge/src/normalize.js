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
