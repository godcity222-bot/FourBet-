/**
 * liveOddsMerge — overlay live_odds rows (odds-api.io WebSocket bridge) on
 * top of the REST-snapshot `realMarkets` object consumed by BettingMarkets.
 *
 * For each market we average across bookmakers (matching the existing
 * useEffectiveMatchOdds policy) and only override when at least one row
 * exists. Markets without live rows pass through unchanged.
 */

import type { LiveOddRow } from "@/hooks/useLiveOdds";
import type { RealMarkets } from "@/components/match/BettingMarkets";

// Pre-match: aligned with `matches_with_live_odds()` (2 hours). If the DB
// surfaces the match as having live odds, the detail page must render them.
const LIVE_ABSOLUTE_MAX_AGE_MS = 2 * 60 * 60 * 1000;
// In-play quotes are streamed continuously, but some provider markets only
// push on price changes. Keep a short live window without blanking stable lines.
const INPLAY_ABSOLUTE_MAX_AGE_MS = 8 * 60 * 1000;

let absoluteMaxAgeMs = LIVE_ABSOLUTE_MAX_AGE_MS;
// In prematch mode the provider often marks every tradable price `suspended`
// (no live trading yet) even though the opening line is the price we want to
// surface. When the match has not kicked off, treat suspended rows as usable
// so prematch markets (set winners, totals/handicap games & sets, etc.) still
// render. The live cutover flips this back to strict mode automatically.
let allowSuspendedRows = false;

function rowTimestampMs(row: Pick<LiveOddRow, "provider_ts" | "received_at">): number {
  // Treat a row as "fresh" if EITHER signal is recent. Some markets (e.g.
  // correct_score) carry a stale provider_ts because the price has not moved
  // in hours, while our sync keeps re-confirming the row and refreshes
  // received_at every cycle. Picking the max keeps those stable markets
  // visible while still honoring a newer provider quote when one arrives.
  const providerMs = row.provider_ts ? new Date(row.provider_ts).getTime() : NaN;
  const receivedMs = row.received_at ? new Date(row.received_at).getTime() : NaN;
  const pOk = Number.isFinite(providerMs);
  const rOk = Number.isFinite(receivedMs);
  if (pOk && rOk) return Math.max(providerMs, receivedMs);
  if (pOk) return providerMs;
  if (rOk) return receivedMs;
  return NaN;
}


function isRecent(row: Pick<LiveOddRow, "provider_ts" | "received_at">): boolean {
  const ms = rowTimestampMs(row);
  if (!Number.isFinite(ms)) return false;
  return Date.now() - ms <= absoluteMaxAgeMs;
}

function freshRows(rows: LiveOddRow[]): LiveOddRow[] {
  const absolute = rows.filter((row) => isRecent(row));
  // Do not compare unrelated markets against the single newest row. Provider
  // side markets often update less frequently than totals/h2h, but remain
  // valid within the 2-hour pre-match freshness window.
  return absolute;
}

function avg(rows: LiveOddRow[]): number | null {
  const usable = freshRows(
    rows.filter(
      (r) => (allowSuspendedRows || !r.suspended) && Number.isFinite(Number(r.price)),
    ),
  );
  if (usable.length === 0) return null;
  const sum = usable.reduce((a, r) => a + Number(r.price), 0);
  return Number((sum / usable.length).toFixed(3));
}

function pick(
  rows: LiveOddRow[],
  market: string,
  selection: string | RegExp,
  point = 0,
): number | null {
  const matches = rows.filter((r) => {
    if (r.market !== market) return null as never;
    if (Number(r.point ?? 0) !== point) return null as never;
    if (typeof selection === "string") return r.selection?.toLowerCase() === selection.toLowerCase();
    return selection.test(r.selection ?? "");
  });
  return avg(matches);
}

function uniquePoints(rows: LiveOddRow[], market: string): number[] {
  const set = new Set<number>();
  for (const r of rows) if (r.market === market) set.add(Number(r.point ?? 0));
  return [...set].sort((a, b) => a - b);
}

function normalizeScoreSelection(value: string): string {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/^(?:correct\s+score\s*)/i, "")
    .replace(/\s*[-–]\s*/g, ":");
}

// Normalize team names for fuzzy comparison: lowercase, strip diacritics,
// drop common club suffixes, and remove non-alphanumerics.
function normTeam(s: string): string {
  return String(s ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(fc|cf|sc|ac|afc|sk|if|bk|cd|ud|rc|fk|sv)\b/g, "")
    .replace(/[^a-z0-9]+/g, "");
}

function isDrawSel(sel: string): boolean {
  const s = String(sel ?? "").trim().toLowerCase();
  return s === "draw" || s === "tie" || s === "x" || s === "the draw";
}

export function mergeLiveOdds(
  base: RealMarkets,
  liveRows: LiveOddRow[],
  homeTeam: string,
  awayTeam: string,
  opts: { isLive?: boolean; sportKey?: string | null } = {},
): RealMarkets {
  absoluteMaxAgeMs = opts.isLive ? INPLAY_ABSOLUTE_MAX_AGE_MS : LIVE_ABSOLUTE_MAX_AGE_MS;
  allowSuspendedRows = !opts.isLive;
  // NOTE: previously this early-returned `base` when liveRows was empty.
  // That meant an in-play event with a dead feed kept showing prematch DC /
  // DNB / 1X2 as if they were live — exactly the sure-win risk we have to
  // block. For prematch events the short-circuit is still safe; for live we
  // continue so the result-market freshness gate at the bottom can strip
  // those markets even when no rows arrived.
  if (!opts.isLive && (!liveRows || liveRows.length === 0)) return base;
  if (!liveRows) liveRows = [];
  const out: NonNullable<RealMarkets> = { ...(base ?? {}) };

  const home = homeTeam.trim();
  const away = awayTeam.trim();
  const homeN = normTeam(home);
  const awayN = normTeam(away);
  const isTennis = String(opts.sportKey ?? "").toLowerCase().startsWith("tennis");

  // Tennis: tokenize player names ("Last, First" / "First Last" / doubles
  // "Surname A / Surname B") into a lowercase token set, ignoring single-char
  // initials and punctuation so selection text matches the team name robustly.
  const tennisTokens = (s: string): string[] =>
    String(s ?? "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length >= 2);
  const homeTennisTokens = isTennis ? new Set(tennisTokens(home)) : null;
  const awayTennisTokens = isTennis ? new Set(tennisTokens(away)) : null;
  const tennisMatch = (sel: string, target: Set<string> | null, other: Set<string> | null) => {
    if (!target || target.size === 0) return false;
    const sTokens = tennisTokens(sel);
    if (sTokens.length === 0) return false;
    let hit = 0, otherHit = 0;
    for (const t of sTokens) {
      if (target.has(t)) hit++;
      if (other && other.has(t)) otherHit++;
    }
    if (hit === 0) return false;
    // Reject if selection looks more like the opposite player (doubles guard).
    if (otherHit > hit) return false;
    return true;
  };

  // Identify a selection as the home/away side. Accepts:
  //   - literal "home"/"away" or "1"/"2"
  //   - exact team-name match
  //   - fuzzy match after normalization (strips "FC", punctuation, accents)
  //   - tennis: token-set match against player names
  const isHomeSel = (sel: string): boolean => {
    const raw = String(sel ?? "").trim();
    if (!raw) return false;
    const low = raw.toLowerCase();
    if (low === "home" || low === "1") return true;
    if (low === home.toLowerCase()) return true;
    const n = normTeam(raw);
    if (n && homeN && (n === homeN || n.includes(homeN) || homeN.includes(n))) return true;
    if (isTennis && tennisMatch(raw, homeTennisTokens, awayTennisTokens)) return true;
    return false;
  };
  const isAwaySel = (sel: string): boolean => {
    const raw = String(sel ?? "").trim();
    if (!raw) return false;
    const low = raw.toLowerCase();
    if (low === "away" || low === "2") return true;
    if (low === away.toLowerCase()) return true;
    const n = normTeam(raw);
    if (n && awayN && (n === awayN || n.includes(awayN) || awayN.includes(n))) return true;
    if (isTennis && tennisMatch(raw, awayTennisTokens, homeTennisTokens)) return true;
    return false;
  };

  const homeRe = { test: (s: string) => isHomeSel(s) } as unknown as RegExp;
  const awayRe = { test: (s: string) => isAwaySel(s) } as unknown as RegExp;

  // ── h2h (moneyline / 1X2) ───────────────────────────────────────────────
  {
    const h2hRows = liveRows.filter(
      (r) => r.market === "h2h" && Number(r.point ?? 0) === 0 && (allowSuspendedRows || !r.suspended),
    );
    const drawRows = h2hRows.filter((r) => isDrawSel(r.selection));
    let homeRows = h2hRows.filter((r) => isHomeSel(r.selection));
    let awayRows = h2hRows.filter((r) => isAwaySel(r.selection));

    // Fallback: if name-based matching failed, infer 1/2 by position within
    // each bookmaker's non-draw rows. odds-api always orders [home, ?draw, away].
    if (homeRows.length === 0 || awayRows.length === 0) {
      const nonDraw = h2hRows.filter((r) => !isDrawSel(r.selection));
      const byBook = new Map<string, typeof nonDraw>();
      for (const r of nonDraw) {
        const k = r.bookmaker ?? "";
        if (!byBook.has(k)) byBook.set(k, []);
        byBook.get(k)!.push(r);
      }
      const inferredHome: typeof nonDraw = [];
      const inferredAway: typeof nonDraw = [];
      for (const [, rows] of byBook) {
        if (rows.length === 2) {
          inferredHome.push(rows[0]);
          inferredAway.push(rows[1]);
        }
      }
      if (homeRows.length === 0) homeRows = inferredHome;
      if (awayRows.length === 0) awayRows = inferredAway;
    }

    const h = avg(homeRows);
    const a = avg(awayRows);
    const d = avg(drawRows);
    if (h != null && a != null) {
      out.h2h = { home: h, draw: d ?? out.h2h?.draw ?? null, away: a };
    }
  }

  // ── totals (over/under) — main + alternates merged ──────────────────────
  // For tennis the `totals` market refers to total SETS — routed into a
  // dedicated bucket below so the games ladder remains separate.
  if (!isTennis) {
    const TOTALS_KEYS = new Set(["totals", "alternate_totals", "alternative_totals"]);
    const tRows = liveRows.filter((r) => TOTALS_KEYS.has(r.market));
    if (tRows.length > 0) {
      const points = [...new Set(tRows.map((r) => Number(r.point ?? 0)))];
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.totals ?? {}) };
      for (const p of points) {
        const over = avg(tRows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(tRows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.totals = merged;
    }
  }



  // ── spreads (handicap) — main + alternates ──────────────────────────────
  // For tennis the `spreads` market refers to SET handicap — routed below.
  if (!isTennis) {
    const SPREAD_KEYS = new Set(["spreads", "alternate_spreads", "alternative_spread"]);
    const sRows = liveRows.filter((r) => SPREAD_KEYS.has(r.market));
    if (sRows.length > 0) {
      const points = [...new Set(sRows.map((r) => Number(r.point ?? 0)))];
      const merged: Record<string, { home: number | null; away: number | null }> = { ...(out.spreads ?? {}) };
      for (const p of points) {
        const h = avg(sRows.filter((r) => Number(r.point) === p && homeRe.test(r.selection)));
        const a =
          avg(sRows.filter((r) => Number(r.point) === -p && awayRe.test(r.selection))) ??
          avg(sRows.filter((r) => Number(r.point) === p && awayRe.test(r.selection)));
        if (h != null && a != null) merged[String(p)] = { home: h, away: a };
      }
      if (Object.keys(merged).length > 0) out.spreads = merged;
    }
  }

  // ── Tennis-specific markets ─────────────────────────────────────────────
  if (isTennis) {
    // total games (full ladder)
    {
      const rows = liveRows.filter((r) => r.market === "totals_(games)");
      if (rows.length > 0) {
        const merged: Record<string, { over: number | null; under: number | null }> = {};
        for (const p of uniquePoints(rows, "totals_(games)")) {
          const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
          const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
          if (over != null && under != null) merged[String(p)] = { over, under };
        }
        if (Object.keys(merged).length > 0) out.tennis_totals_games = merged;
      }
    }
    // game handicap (full ladder, by player name)
    // Batch L5 P3 — GAMES_HANDICAP. Only integer or .5 lines are wagerable
    // (quarter-lines .25/.75 are rejected by the live guard, so drop them
    // from the UI up-front to avoid rendering unwagerable rows).
    {
      const rows = liveRows.filter((r) => r.market === "spread_(games)");
      if (rows.length > 0) {
        const rawPoints = [...new Set(rows.map((r) => Number(r.point ?? 0)))];
        const points = rawPoints.filter((p) => {
          if (!Number.isFinite(p)) return false;
          const frac = Math.abs(p - Math.trunc(p));
          return frac === 0 || Math.abs(frac - 0.5) < 1e-9;
        });
        const merged: Record<string, { home: number | null; away: number | null }> = {};
        for (const p of points) {
          const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
          const a =
            avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
            avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
          if (h != null && a != null) merged[String(p)] = { home: h, away: a };
        }
        if (Object.keys(merged).length > 0) out.tennis_spreads_games = merged;
      }
    }
    // total sets (provider key: totals)
    {
      const rows = liveRows.filter((r) => r.market === "totals");
      if (rows.length > 0) {
        const merged: Record<string, { over: number | null; under: number | null }> = {};
        for (const p of uniquePoints(rows, "totals")) {
          const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
          const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
          if (over != null && under != null) merged[String(p)] = { over, under };
        }
        if (Object.keys(merged).length > 0) out.tennis_totals_sets = merged;
      }
    }
    // set handicap (provider key: spreads)
    {
      const rows = liveRows.filter((r) => r.market === "spreads");
      if (rows.length > 0) {
        const points = [...new Set(rows.map((r) => Number(r.point ?? 0)))];
        const merged: Record<string, { home: number | null; away: number | null }> = {};
        for (const p of points) {
          const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
          const a =
            avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
            avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
          if (h != null && a != null) merged[String(p)] = { home: h, away: a };
        }
        if (Object.keys(merged).length > 0) out.tennis_spreads_sets = merged;
      }
    }
    // set 1 winner (ml_1st_set, alias h2h_h1, alias ml_ht, alias set_winner_s1)
    {
      const rows = liveRows.filter(
        (r) =>
          (r.market === "ml_1st_set" ||
            r.market === "h2h_h1" ||
            r.market === "ml_ht" ||
            r.market === "set_winner_s1") &&
          (allowSuspendedRows || !r.suspended),
      );
      if (rows.length > 0) {
        const h = avg(rows.filter((r) => isHomeSel(r.selection)));
        const a = avg(rows.filter((r) => isAwaySel(r.selection)));
        if (h != null && a != null) out.tennis_set1_h2h = { home: h, away: a };
      }
    }
    // set 2 winner (ml_2nd_set, alias set_winner_s2)
    {
      const rows = liveRows.filter(
        (r) =>
          (r.market === "ml_2nd_set" || r.market === "set_winner_s2") &&
          (allowSuspendedRows || !r.suspended),
      );
      if (rows.length > 0) {
        const h = avg(rows.filter((r) => isHomeSel(r.selection)));
        const a = avg(rows.filter((r) => isAwaySel(r.selection)));
        if (h != null && a != null) out.tennis_set2_h2h = { home: h, away: a };
      }
    }
  }


  // ── Correct Score ───────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "correct_score" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const byScore = new Map<string, LiveOddRow[]>();
      for (const r of rows) {
        const score = normalizeScoreSelection(r.selection);
        if (!score) continue;
        if (!byScore.has(score)) byScore.set(score, []);
        byScore.get(score)!.push(r);
      }
      const scores = [...byScore.entries()]
        .map(([score, scoreRows]) => ({ score, price: avg(scoreRows) }))
        .filter((x): x is { score: string; price: number } => x.price != null)
        .sort((a, b) => a.price - b.price);
      if (scores.length > 0) out.correct_score = scores;
    }
  }

  // ── BTTS ─────────────────────────────────────────────────────────────────
  {
    const yes = pick(liveRows, "btts", /^yes$/i);
    const no = pick(liveRows, "btts", /^no$/i);
    if (yes != null && no != null) out.btts = { yes, no };
  }

  // ── Double Chance (1X / 12 / X2) ────────────────────────────────────────
  {
    const dcRows = liveRows.filter((r) => r.market === "double_chance" && (allowSuspendedRows || !r.suspended));
    if (dcRows.length > 0) {
      const homeDraw = avg(dcRows.filter((r) => /^1x$/i.test(r.selection)));
      const homeAway = avg(dcRows.filter((r) => /^12$/i.test(r.selection)));
      const awayDraw = avg(dcRows.filter((r) => /^x2$/i.test(r.selection)));
      if (homeDraw != null && homeAway != null && awayDraw != null) {
        out.double_chance = { home_draw: homeDraw, home_away: homeAway, away_draw: awayDraw };
      }
    }
  }

  // ── Draw No Bet (Home / Away, push on draw) ─────────────────────────────
  {
    const dnbRows = liveRows.filter((r) => r.market === "draw_no_bet" && (allowSuspendedRows || !r.suspended));
    if (dnbRows.length > 0) {
      const h = avg(dnbRows.filter((r) => isHomeSel(r.selection)));
      const a = avg(dnbRows.filter((r) => isAwaySel(r.selection)));
      if (h != null && a != null) out.draw_no_bet = { home: h, away: a };
    }
  }

  // ── Team totals (per-side over/under) ───────────────────────────────────
  // Combines three provider shapes into one bucket:
  //  1. `team_totals` rows where selection encodes "<team> over/under" (soccer).
  //  2. `team_total_home` / `team_total_away` with selection just over/under.
  //  3. `team_total_(points)_home` / `team_total_(points)_away` (basketball).
  {
    const TT_COMBINED = new Set([
      "team_totals",
      "team_total_home",
      "team_total_away",
      "team_total_(points)_home",
      "team_total_(points)_away",
    ]);
    const ttRows = liveRows.filter((r) => TT_COMBINED.has(r.market));
    if (ttRows.length > 0) {
      const tt: NonNullable<NonNullable<RealMarkets>["team_totals"]> = {
        home: { ...(out.team_totals?.home ?? {}) },
        away: { ...(out.team_totals?.away ?? {}) },
      };
      const isHomeMarket = (m: string) => m === "team_total_home" || m === "team_total_(points)_home";
      const isAwayMarket = (m: string) => m === "team_total_away" || m === "team_total_(points)_away";
      // Bucket rows by side using either market name (aliases) or selection text.
      const homeRows = ttRows.filter((r) => isHomeMarket(r.market) || (r.market === "team_totals" && homeRe.test(r.selection)));
      const awayRows = ttRows.filter((r) => isAwayMarket(r.market) || (r.market === "team_totals" && awayRe.test(r.selection)));
      const pointsFor = (rows: LiveOddRow[]) => [...new Set(rows.map((r) => Number(r.point ?? 0)))];
      for (const p of pointsFor(homeRows)) {
        const o = avg(homeRows.filter((r) => Number(r.point) === p && /\bover\b/i.test(r.selection)));
        const u = avg(homeRows.filter((r) => Number(r.point) === p && /\bunder\b/i.test(r.selection)));
        if (o != null && u != null) tt.home![String(p)] = { over: o, under: u };
      }
      for (const p of pointsFor(awayRows)) {
        const o = avg(awayRows.filter((r) => Number(r.point) === p && /\bover\b/i.test(r.selection)));
        const u = avg(awayRows.filter((r) => Number(r.point) === p && /\bunder\b/i.test(r.selection)));
        if (o != null && u != null) tt.away![String(p)] = { over: o, under: u };
      }
      out.team_totals = tt;
    }
  }

  // ── h2h 1st half ────────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "h2h_h1");
    if (rows.length > 0) {
      const h = avg(rows.filter((r) => isHomeSel(r.selection)));
      const a = avg(rows.filter((r) => isAwaySel(r.selection)));
      const d = avg(rows.filter((r) => isDrawSel(r.selection)));
      if (h != null && a != null) {
        out.h2h_1h = { home: h, draw: d ?? out.h2h_1h?.draw ?? null, away: a };
      }
    }
  }

  // ── totals 1st half ─────────────────────────────────────────────────────
  // Provider variants folded together: `totals_h1` and `1st_half_goal_line`
  // (bookmaker-specific alt name for the same market).
  {
    // v2.0.32 — fold three bookmaker aliases for the same 1H total goals market
    // so the totals_1h card populates regardless of which provider name arrives.
    const H1_TOTAL_KEYS = new Set([
      "totals_h1",
      "1st_half_goal_line",
      "1st_half_total_goals",
    ]);
    const rows = liveRows.filter((r) => H1_TOTAL_KEYS.has(r.market));
    if (rows.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.totals_1h ?? {}) };
      const points = [...new Set(rows.map((r) => Number(r.point ?? 0)))];
      for (const p of points) {
        const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.totals_1h = merged;
    }
  }

  // ── spreads 1st half ────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "spreads_h1");
    if (rows.length > 0) {
      const merged: Record<string, { home: number | null; away: number | null }> = { ...(out.spreads_1h ?? {}) };
      for (const p of uniquePoints(rows, "spreads_h1")) {
        const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
        const a =
          avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
          avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
        if (h != null && a != null) merged[String(p)] = { home: h, away: a };
      }
      if (Object.keys(merged).length > 0) out.spreads_1h = merged;
    }
  }

  // ── BTTS 1st half ───────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "btts_h1" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const yes = avg(rows.filter((r) => /^yes$/i.test(r.selection)));
      const no = avg(rows.filter((r) => /^no$/i.test(r.selection)));
      if (yes != null && no != null) out.btts_1h = { yes, no };
    }
  }

  // ── Double Chance 1st half ──────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "double_chance_h1" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const homeDraw = avg(rows.filter((r) => /^1x$/i.test(r.selection)));
      const homeAway = avg(rows.filter((r) => /^12$/i.test(r.selection)));
      const awayDraw = avg(rows.filter((r) => /^x2$/i.test(r.selection)));
      if (homeDraw != null && homeAway != null && awayDraw != null) {
        out.double_chance_1h = { home_draw: homeDraw, home_away: homeAway, away_draw: awayDraw };
      }
    }
  }

  // ── corners totals 1st half ─────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "corners_totals_h1");
    if (rows.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.corners_totals_1h ?? {}) };
      for (const p of uniquePoints(rows, "corners_totals_h1")) {
        const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.corners_totals_1h = merged;
    }
  }

  // ── Basketball Q1: h2h / totals / spreads ───────────────────────────────
  // `h2h_q1` and `ml_q1` are aliases for the Quarter-1 moneyline.
  {
    const h2hQ1 = liveRows.filter((r) => (r.market === "h2h_q1" || r.market === "ml_q1") && (allowSuspendedRows || !r.suspended));
    if (h2hQ1.length > 0) {
      const h = avg(h2hQ1.filter((r) => isHomeSel(r.selection)));
      const a = avg(h2hQ1.filter((r) => isAwaySel(r.selection)));
      if (h != null && a != null) out.h2h_q1 = { home: h, away: a };
    }
    const tQ1 = liveRows.filter((r) => r.market === "totals_q1");
    if (tQ1.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.totals_q1 ?? {}) };
      for (const p of uniquePoints(tQ1, "totals_q1")) {
        const over = avg(tQ1.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(tQ1.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.totals_q1 = merged;
    }
    // Q1 spreads — accept both `spreads_q1` (plural) and `spread_q1` (singular
    // alias emitted by some odds-api books).
    const sQ1 = liveRows.filter((r) => r.market === "spreads_q1" || r.market === "spread_q1");
    if (sQ1.length > 0) {
      const merged: Record<string, { home: number | null; away: number | null }> = { ...(out.spreads_q1 ?? {}) };
      const points = [...new Set(sQ1.map((r) => Number(r.point ?? 0)))];
      for (const p of points) {
        const h = avg(sQ1.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
        const a =
          avg(sQ1.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
          avg(sQ1.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
        if (h != null && a != null) merged[String(p)] = { home: h, away: a };
      }
      if (Object.keys(merged).length > 0) out.spreads_q1 = merged;
    }
    // Halftime moneyline (basketball). Provider key: `ml_ht`.
    // For tennis, ml_ht is an alias of ml_1st_set and is consumed above into
    // tennis_set1_h2h — do not surface it as a standalone halftime market.
    if (!isTennis) {
      const mlHT = liveRows.filter((r) => r.market === "ml_ht" && (allowSuspendedRows || !r.suspended));
      if (mlHT.length > 0) {
        const h = avg(mlHT.filter((r) => isHomeSel(r.selection)));
        const a = avg(mlHT.filter((r) => isAwaySel(r.selection)));
        if (h != null && a != null) out.h2h_ht = { home: h, away: a };
      }
    }
  }

  // ── corners totals ──────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "corners_totals");
    if (rows.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.corners_totals ?? {}) };
      for (const p of uniquePoints(rows, "corners_totals")) {
        const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.corners_totals = merged;
    }
  }

  // ── cards totals ────────────────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "cards_totals");
    if (rows.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.cards_totals ?? {}) };
      for (const p of uniquePoints(rows, "cards_totals")) {
        const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.cards_totals = merged;
    }
  }

  // ── corners handicap / spread ───────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "corners_hcp");
    if (rows.length > 0) {
      const merged: Record<string, { home: number | null; away: number | null }> = { ...(out.corners_spreads ?? {}) };
      const points = new Set<number>();
      for (const r of rows) points.add(Number(r.point ?? 0));
      for (const p of points) {
        const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
        const a =
          avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
          avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
        if (h != null || a != null) merged[String(p)] = { home: h, away: a };
      }
      if (Object.keys(merged).length > 0) out.corners_spreads = merged;
    }
  }

  // ── cards handicap / spread ─────────────────────────────────────────────
  {
    const rows = liveRows.filter((r) => r.market === "cards_hcp");
    if (rows.length > 0) {
      const merged: Record<string, { home: number | null; away: number | null }> = { ...(out.cards_handicap ?? {}) };
      const points = new Set<number>();
      for (const r of rows) points.add(Number(r.point ?? 0));
      for (const p of points) {
        const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
        const a =
          avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
          avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
        if (h != null || a != null) merged[String(p)] = { home: h, away: a };
      }
      if (Object.keys(merged).length > 0) out.cards_handicap = merged;
    }
  }

  // ── 2nd-half ML / 1X2 (provider key: ml_2h, selections = team names + "Draw")
  {
    const rows = liveRows.filter((r) => r.market === "ml_2h" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const h = avg(rows.filter((r) => isHomeSel(r.selection)));
      const a = avg(rows.filter((r) => isAwaySel(r.selection)));
      const d = avg(rows.filter((r) => isDrawSel(r.selection)));
      if (h != null && a != null) {
        out.h2h_2h = { home: h, draw: d ?? out.h2h_2h?.draw ?? null, away: a };
      }
    }
  }

  // ── 2nd-half totals (provider key: totals_2h)
  {
    const rows = liveRows.filter((r) => r.market === "totals_2h");
    if (rows.length > 0) {
      const merged: Record<string, { over: number | null; under: number | null }> = { ...(out.totals_2h ?? {}) };
      for (const p of uniquePoints(rows, "totals_2h")) {
        const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
        const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
        if (over != null && under != null) merged[String(p)] = { over, under };
      }
      if (Object.keys(merged).length > 0) out.totals_2h = merged;
    }
  }

  // ── 2nd-half BTTS (provider key: both_teams_to_score_2h)
  {
    const rows = liveRows.filter((r) => r.market === "both_teams_to_score_2h" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const yes = avg(rows.filter((r) => /^yes$/i.test(r.selection)));
      const no = avg(rows.filter((r) => /^no$/i.test(r.selection)));
      if (yes != null && no != null) out.btts_2h = { yes, no };
    }
  }

  // ── Per-side Over/Under buckets (team_corners / team_shots / team_shots_on_target)
  const sideBucket = (
    marketKey: string,
  ): Record<string, { over: number | null; under: number | null }> | null => {
    const rows = liveRows.filter((r) => r.market === marketKey);
    if (rows.length === 0) return null;
    const merged: Record<string, { over: number | null; under: number | null }> = {};
    for (const p of uniquePoints(rows, marketKey)) {
      const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
      const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
      if (over != null && under != null) merged[String(p)] = { over, under };
    }
    return Object.keys(merged).length > 0 ? merged : null;
  };
  {
    const ch = sideBucket("team_corners_home"); if (ch) out.team_corners_home = ch;
    const ca = sideBucket("team_corners_away"); if (ca) out.team_corners_away = ca;
    const sh = sideBucket("team_shots_home"); if (sh) out.team_shots_home = sh;
    const sa = sideBucket("team_shots_away"); if (sa) out.team_shots_away = sa;
    const oh = sideBucket("team_shots_on_target_home"); if (oh) out.team_shots_on_target_home = oh;
    const oa = sideBucket("team_shots_on_target_away"); if (oa) out.team_shots_on_target_away = oa;
  }

  // ── Match-wide advanced totals (allow partial Over-only rows from provider).
  const totalOuBucket = (
    marketKey: string,
  ): Record<string, { over: number | null; under: number | null }> | null => {
    const rows = liveRows.filter((r) => r.market === marketKey && (allowSuspendedRows || !r.suspended));
    if (rows.length === 0) return null;
    const merged: Record<string, { over: number | null; under: number | null }> = {};
    for (const p of uniquePoints(rows, marketKey)) {
      const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
      const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
      if (over != null || under != null) merged[String(p)] = { over, under };
    }
    return Object.keys(merged).length > 0 ? merged : null;
  };
  {
    const ts = totalOuBucket("total_shots"); if (ts) out.total_shots_ou = ts;
    const tsot = totalOuBucket("total_shots_on_target"); if (tsot) out.total_shots_on_target_ou = tsot;
    const tf = totalOuBucket("total_fouls"); if (tf) out.total_fouls_ou = tf;
    const toff = totalOuBucket("total_offsides"); if (toff) out.total_offsides_ou = toff;
    const gks = totalOuBucket("goalkeeper_saves"); if (gks) out.goalkeeper_saves_ou = gks;
  }

  // ── Group 3 (advanced) — Over/Under buckets requiring both sides ────────
  const ouBucket = (
    marketKey: string,
  ): Record<string, { over: number | null; under: number | null }> | null => {
    const rows = liveRows.filter((r) => r.market === marketKey && (allowSuspendedRows || !r.suspended));
    if (rows.length === 0) return null;
    const merged: Record<string, { over: number | null; under: number | null }> = {};
    for (const p of uniquePoints(rows, marketKey)) {
      const over = avg(rows.filter((r) => Number(r.point) === p && /^over$/i.test(r.selection)));
      const under = avg(rows.filter((r) => Number(r.point) === p && /^under$/i.test(r.selection)));
      if (over != null && under != null) merged[String(p)] = { over, under };
    }
    return Object.keys(merged).length > 0 ? merged : null;
  };
  {
    const altC = ouBucket("alternate_corners_totals"); if (altC) out.alt_corners_totals = altC;
    const c2 = ouBucket("corners_2-way") ?? ouBucket("corners_2_way"); if (c2) out.corners_2way_totals = c2;
    const tch = ouBucket("team_cards_home"); if (tch) out.team_cards_home_ou = tch;
    const tca = ouBucket("team_cards_away"); if (tca) out.team_cards_away_ou = tca;
    const toh = ouBucket("team_offsides_home"); if (toh) out.team_offsides_home_ou = toh;
    const toa = ouBucket("team_offsides_away"); if (toa) out.team_offsides_away_ou = toa;
  }

  // ── Group 3 — team-name handicap buckets (home/away keyed by point) ─────
  const teamHcpBucket = (
    marketKey: string,
  ): Record<string, { home: number | null; away: number | null }> | null => {
    const rows = liveRows.filter((r) => r.market === marketKey && (allowSuspendedRows || !r.suspended));
    if (rows.length === 0) return null;
    const points = new Set<number>();
    for (const r of rows) points.add(Number(r.point ?? 0));
    const merged: Record<string, { home: number | null; away: number | null }> = {};
    for (const p of points) {
      const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
      const a =
        avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
        avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
      if (h != null && a != null) merged[String(p)] = { home: h, away: a };
    }
    return Object.keys(merged).length > 0 ? merged : null;
  };
  {
    const cardH = teamHcpBucket("card_handicap"); if (cardH) out.card_handicap_lines = cardH;
  }

  // ── Group 3 — European handicap 1H (home/draw/away by point) ────────────
  {
    const rows = liveRows.filter((r) => r.market === "european_handicap_ht" && (allowSuspendedRows || !r.suspended));
    if (rows.length > 0) {
      const points = new Set<number>();
      for (const r of rows) points.add(Number(r.point ?? 0));
      const merged: Record<string, { home: number | null; draw: number | null; away: number | null }> = {};
      for (const p of points) {
        const h = avg(rows.filter((r) => Number(r.point) === p && isHomeSel(r.selection)));
        const d = avg(rows.filter((r) => Number(r.point) === p && isDrawSel(r.selection)));
        const a =
          avg(rows.filter((r) => Number(r.point) === -p && isAwaySel(r.selection))) ??
          avg(rows.filter((r) => Number(r.point) === p && isAwaySel(r.selection)));
        if (h != null && a != null) merged[String(p)] = { home: h, draw: d, away: a };
      }
      if (Object.keys(merged).length > 0) out.european_handicap_ht_lines = merged;
    }
  }

  // ── Half-time / Full-time (provider key: half_time_/_full_time) ─────────
  // Provider selections are `1/1`, `1/X`, `X/2`, etc. Normalize to canonical
  // "Home/Draw"/"Home/Away"/... labels the UI already understands.
  {
    const rows = liveRows.filter(
      (r) => r.market === "half_time_/_full_time" && (allowSuspendedRows || !r.suspended),
    );
    if (rows.length > 0) {
      const codeToWord = (c: string): string | null => {
        const k = c.trim().toUpperCase();
        if (k === "1") return "Home";
        if (k === "2") return "Away";
        if (k === "X") return "Draw";
        return null;
      };
      const byLabel = new Map<string, LiveOddRow[]>();
      for (const r of rows) {
        const parts = String(r.selection ?? "").split("/").map((s) => s.trim());
        if (parts.length !== 2) continue;
        const a = codeToWord(parts[0]);
        const b = codeToWord(parts[1]);
        if (!a || !b) continue;
        const label = `${a}/${b}`;
        if (!byLabel.has(label)) byLabel.set(label, []);
        byLabel.get(label)!.push(r);
      }
      const merged: Array<{ label: string; price: number }> = [];
      for (const [label, group] of byLabel) {
        const price = avg(group);
        if (price != null) merged.push({ label, price });
      }
      if (merged.length > 0) out.ht_ft = merged;
    }
  }

  // ── Anytime Goalscorer (provider key: anytime_scorer) ───────────────────
  // Post-v2.0.34 the bridge routes anytime_scorer through the player-prop
  // extractor, so selections arrive as either bare `${player}` (single-side
  // anytime shape) or `${player}|Yes` (paired Yes/No shape). Legacy bare
  // "Yes"/"No"/"Over"/"Under" rows without a player are ignored — they were
  // the pre-patch stripped rows that caused the display blocker.
  {
    const rows = liveRows.filter(
      (r) => r.market === "anytime_scorer" && (allowSuspendedRows || !r.suspended),
    );
    if (rows.length > 0) {
      const byPlayer = new Map<string, LiveOddRow[]>();
      for (const r of rows) {
        let sel = String(r.selection ?? "").trim();
        if (!sel) continue;
        // Strip `|Yes` suffix if present; drop `|No` rows entirely (the
        // display group only surfaces the affirmative "to score" side).
        if (sel.includes("|")) {
          const [p, side] = sel.split("|");
          if (!/^yes$/i.test(String(side ?? "").trim())) continue;
          sel = p.trim();
        }
        if (!sel) continue;
        if (/^(over|under|yes|no)$/i.test(sel)) continue;
        if (!byPlayer.has(sel)) byPlayer.set(sel, []);
        byPlayer.get(sel)!.push(r);
      }
      const list: Array<{ player: string; price: number }> = [];
      for (const [player, group] of byPlayer) {
        const price = avg(group);
        if (price != null) list.push({ player, price });
      }
      if (list.length > 0) out.player_anytime_goalscorer = list.sort((a, b) => a.price - b.price);
    }
  }


  // ── Player props (display-only) ─────────────────────────────────────────
  // Bridge now encodes player identity into `selection` as
  // `${player}|Over` / `${player}|Under` / `${player}|Yes` / `${player}|No`
  // / `${player}` (anytime). Group rows by (player, side, point) across
  // bookmakers, average price, and emit per-canonical-bucket display
  // structures. Rows without a player identity (`selection` is bare
  // Over/Under/Yes/No) are ignored — they were the pre-patch stripped rows.
  //
  // NOTE: These groups are DISPLAY ONLY. They are intentionally not
  // wired into any betting allowlist / guard / settlement path. Bet-enable
  // needs player-event settlement + already-decided guard + heartbeat.
  {
    // dbMarket -> canonical UI bucket key on `out`
    const PLAYER_MARKET_TO_OUT_KEY: Record<string, string> = {
      // Soccer
      player_shots: "player_shots_ou",
      player_shots_on_target: "player_shots_on_target_ou",
      player_shots_on_target_outside_box: "player_shots_on_target_ou",
      player_headed_shots_on_target: "player_shots_on_target_ou",
      player_passes: "player_passes_ou",
      player_tackles: "player_tackles_ou",
      player_fouls: "player_fouls_committed_ou",
      player_fouls_committed: "player_fouls_committed_ou",
      player_to_be_fouled: "player_to_be_fouled_ou",
      player_cards: "player_cards",
      player_to_be_booked: "player_to_be_booked",
      player_to_assist: "player_to_assist",
      player_to_score_or_assist: "player_to_score_or_assist",
      player_goals_milestones: "player_goals_milestones",
      player_assists_milestones: "player_assists_milestones",
      goalkeeper_saves: "goalkeeper_saves_ou",
      goalkeeper_saves_home: "goalkeeper_saves_ou",
      goalkeeper_saves_away: "goalkeeper_saves_ou",
      // Basketball
      player_points: "player_points_ou",
      player_rebounds: "player_rebounds_ou",
      player_assists: "player_assists_ou",
      player_threes: "player_threes_ou",
      player_steals: "player_steals_ou",
      player_blocks: "player_blocks_ou",
      player_points_milestones: "player_points_milestones",
      player_rebounds_milestones: "player_rebounds_milestones",
      player_threes_milestones: "player_threes_milestones",
      player_first_basket: "player_first_basket",
      // AFL / catch-all
      total_player_disposals: "player_disposals_ou",
      player_props: "player_props_generic",
    };

    type PlayerRow = {
      player: string;
      side: "Over" | "Under" | "Yes" | "No" | "Anytime";
      point: number;
      price: number;
    };
    const groupsByOutKey = new Map<string, PlayerRow[]>();

    for (const r of liveRows) {
      const outKey = PLAYER_MARKET_TO_OUT_KEY[r.market];
      if (!outKey) continue;
      if (!allowSuspendedRows && r.suspended) continue;
      const rawSel = String(r.selection ?? "").trim();
      if (!rawSel) continue;
      // Reject legacy bare-side rows that lack player identity.
      let player: string;
      let side: PlayerRow["side"];
      const pipeIdx = rawSel.indexOf("|");
      if (pipeIdx > 0) {
        player = rawSel.slice(0, pipeIdx).trim();
        const sideRaw = rawSel.slice(pipeIdx + 1).trim().toLowerCase();
        if (sideRaw === "over") side = "Over";
        else if (sideRaw === "under") side = "Under";
        else if (sideRaw === "yes") side = "Yes";
        else if (sideRaw === "no") side = "No";
        else continue;
      } else {
        // Bare selection — must NOT be Over/Under/Yes/No (legacy stripped row).
        if (/^(over|under|yes|no)$/i.test(rawSel)) continue;
        player = rawSel;
        side = "Anytime";
      }
      if (!player) continue;
      const price = Number(r.price);
      if (!Number.isFinite(price) || price <= 1) continue;
      const point = Number(r.point ?? 0);
      const list = groupsByOutKey.get(outKey) ?? [];
      list.push({ player, side, point: Number.isFinite(point) ? point : 0, price });
      groupsByOutKey.set(outKey, list);
    }

    // Collapse duplicates across bookmakers by (player, side, point) — avg price.
    for (const [outKey, rows] of groupsByOutKey) {
      const bucket = new Map<string, { row: PlayerRow; prices: number[] }>();
      for (const row of rows) {
        const k = `${row.player}\u0001${row.side}\u0001${row.point}`;
        const cur = bucket.get(k);
        if (cur) cur.prices.push(row.price);
        else bucket.set(k, { row, prices: [row.price] });
      }
      const merged = Array.from(bucket.values()).map(({ row, prices }) => ({
        player: row.player,
        side: row.side,
        point: row.point,
        price: Number(
          (prices.reduce((a, b) => a + b, 0) / prices.length).toFixed(3),
        ),
      }));
      if (merged.length > 0) {
        (out as Record<string, unknown>)[outKey] = merged.sort((a, b) => {
          if (a.player !== b.player) return a.player.localeCompare(b.player);
          if (a.point !== b.point) return a.point - b.point;
          return a.side.localeCompare(b.side);
        });
      }
    }
  }



  // ── Live-result-market freshness gate ───────────────────────────────────
  // When the event is in-play we MUST NOT serve prematch fallback prices for
  // result-defining soccer markets (1X2 / Double Chance / Draw No Bet). These
  // are settled by the scoreboard — a stale pre-match line at minute 90 lets
  // users lock in a sure-win (e.g. "Morocco or Tie" at 2.87 when the score
  // is 1-1). Policy:
  //   • Visual stale-cell carry-over (6s) is OK for a missing outcome inside
  //     a still-fresh market (handled in BettingMarkets/useStableMainGroups).
  //   • A whole market that hasn't received a fresh, non-suspended live row
  //     within the live TTL is REMOVED — no prematch fallback, no cached
  //     last-seen price. The market disappears immediately.
  if (opts.isLive) {
    const LIVE_RESULT_FRESH_MS = 20_000; // 20s grace beyond a single tick
    const now = Date.now();
    const hasFreshLiveRow = (market: string): boolean =>
      liveRows.some((r) => {
        if (r.market !== market) return false;
        if (r.suspended) return false;
        const ts = rowTimestampMs(r);
        return Number.isFinite(ts) && now - ts <= LIVE_RESULT_FRESH_MS;
      });
    if (!hasFreshLiveRow("h2h")) {
      delete (out as Record<string, unknown>).h2h;
      delete (out as Record<string, unknown>).h2h_best;
    }
    if (!hasFreshLiveRow("double_chance")) {
      delete (out as Record<string, unknown>).double_chance;
    }
    if (!hasFreshLiveRow("draw_no_bet")) {
      delete (out as Record<string, unknown>).draw_no_bet;
    }
  }

  return out;
}
