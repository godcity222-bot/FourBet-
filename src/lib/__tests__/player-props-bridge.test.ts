/**
 * Player-props bridge & display-merger tests.
 *
 * Covers the v2.0.33 blocker: `live_odds` composite PK has no player
 * column, so the bridge must encode player identity into `selection` as
 * `${player}|Side` — otherwise player prop cards render empty because
 * every row is a bare "Over"/"Under" with no way to group by player.
 *
 *  Bridge-side (services/odds-api-bridge/src/normalize.js):
 *    - extractPlayerName pulls player from any of the known payload keys
 *    - Over/Under/Yes/No labels are NOT treated as players
 *    - buildPlayerPropRows emits `${player}|Over` / `${player}|Under`
 *    - Paired {over, under, hdp}, {yes, no}, single-side listed shapes
 *    - Rows with no player identity are dropped
 *    - Taxonomy covers the 12 required market families
 *
 *  App-side (src/lib/liveOddsMerge.ts):
 *    - Player-prop rows group by (player, side, point) into canonical
 *      output buckets on RealMarkets
 *    - Legacy stripped rows (bare "Over"/"Under" with no `|`) are ignored
 *    - Non-player markets are unaffected by the new merger block
 */
import { describe, it, expect } from 'vitest';
import {
  extractPlayerName,
  extractOverUnderSide,
  buildPlayerPropRows,
  PLAYER_PROP_MARKETS,
  PLAYER_PROP_TAXONOMY,
} from '../../../services/odds-api-bridge/src/normalize.js';
import { mergeLiveOdds } from '@/lib/liveOddsMerge';

describe('bridge: extractPlayerName', () => {
  it('returns the player when payload uses `player`', () => {
    expect(extractPlayerName({ player: 'Erling Haaland', name: 'Over' })).toBe('Erling Haaland');
  });
  it('checks participant/runner/description fallbacks', () => {
    expect(extractPlayerName({ participant: 'M. Salah' })).toBe('M. Salah');
    expect(extractPlayerName({ runner: 'K. De Bruyne' })).toBe('K. De Bruyne');
    expect(extractPlayerName({ description: 'Bukayo Saka' })).toBe('Bukayo Saka');
  });
  it('ignores Over/Under/Yes/No as fake players', () => {
    expect(extractPlayerName({ name: 'Over' })).toBeNull();
    expect(extractPlayerName({ label: 'Under' })).toBeNull();
    expect(extractPlayerName({ selection: 'Yes' })).toBeNull();
    expect(extractPlayerName({ outcome: 'no' })).toBeNull();
  });
  it('falls back to name/label when they are not O/U/Y/N', () => {
    expect(extractPlayerName({ name: 'Rodri' })).toBe('Rodri');
  });
  it('returns null when the payload has no identity', () => {
    expect(extractPlayerName({ price: 2.1 })).toBeNull();
    expect(extractPlayerName(null)).toBeNull();
  });
});

describe('bridge: extractOverUnderSide', () => {
  it.each([
    ['Over', 'Over'],
    ['under', 'Under'],
    ['Yes', 'Yes'],
    ['n', 'No'],
    ['o', 'Over'],
    ['u', 'Under'],
  ])('normalizes "%s" -> "%s"', (raw, expected) => {
    expect(extractOverUnderSide({ selection: raw })).toBe(expected);
  });
  it('returns null for non-side selections', () => {
    expect(extractOverUnderSide({ selection: 'Salah' })).toBeNull();
  });
});

describe('bridge: buildPlayerPropRows', () => {
  it('encodes paired Over/Under with the point', () => {
    const rows = buildPlayerPropRows({
      player: 'Haaland',
      over: '1.85',
      under: '1.95',
      hdp: 2.5,
    });
    expect(rows).toEqual([
      { selection: 'Haaland|Over', price: '1.85', point: 2.5 },
      { selection: 'Haaland|Under', price: '1.95', point: 2.5 },
    ]);
  });
  it('encodes paired Yes/No with point=0', () => {
    const rows = buildPlayerPropRows({ player: 'Vinicius Jr', yes: 3.4, no: 1.28 });
    expect(rows).toEqual([
      { selection: 'Vinicius Jr|Yes', price: 3.4, point: 0 },
      { selection: 'Vinicius Jr|No', price: 1.28, point: 0 },
    ]);
  });
  it('encodes single-side listed shape using extractOverUnderSide', () => {
    const rows = buildPlayerPropRows({
      participant: 'Rodrygo',
      name: 'Over',
      price: 2.1,
      point: 0.5,
    });
    expect(rows).toEqual([{ selection: 'Rodrygo|Over', price: 2.1, point: 0.5 }]);
  });
  it('emits bare player for anytime shape (no O/U marker)', () => {
    const rows = buildPlayerPropRows({ player: 'Kane', odds: 4.5 });
    expect(rows).toEqual([{ selection: 'Kane', price: 4.5, point: 0 }]);
  });
  it('drops rows with no extractable player identity', () => {
    expect(buildPlayerPropRows({ name: 'Over', over: 1.9, under: 1.9 })).toEqual([]);
    expect(buildPlayerPropRows({ name: 'Under', price: 1.9 })).toEqual([]);
  });
});

describe('bridge: PLAYER_PROP_MARKETS taxonomy', () => {
  it('covers every required market family from the directive', () => {
    for (const key of [
      'anytime_scorer',
      'first_scorer',
      'last_scorer',
      'player_shots',
      'player_shots_on_target',
      'player_passes',
      'player_fouls_committed',
      'player_to_be_fouled',
      'player_cards',
      'player_to_be_booked',
      'goalkeeper_saves',
      'player_goals_milestones',
      'player_to_score_or_assist',
      'player_props',
      'total_player_disposals',
    ]) {
      expect(PLAYER_PROP_MARKETS.has(key)).toBe(true);
      expect(PLAYER_PROP_TAXONOMY[key]).toBeTruthy();
    }
  });
  it('maps scorer markets to canonical PLAYER_* buckets', () => {
    expect(PLAYER_PROP_TAXONOMY.anytime_scorer).toBe('PLAYER_ANYTIME_SCORER');
    expect(PLAYER_PROP_TAXONOMY.first_scorer).toBe('PLAYER_FIRST_SCORER');
    expect(PLAYER_PROP_TAXONOMY.last_scorer).toBe('PLAYER_LAST_SCORER');
  });
});

describe('bridge: anytime_scorer routing', () => {
  it('extracts player name from single-side scorer odd (bare shape)', () => {
    const rows = buildPlayerPropRows({ player: 'Harry Kane', price: 3.25 });
    expect(rows).toEqual([{ selection: 'Harry Kane', price: 3.25, point: 0 }]);
  });
  it('extracts player name from Yes/No paired scorer odd', () => {
    const rows = buildPlayerPropRows({ player: 'James Rodriguez', yes: 2.8, no: 1.35 });
    expect(rows).toEqual([
      { selection: 'James Rodriguez|Yes', price: 2.8, point: 0 },
      { selection: 'James Rodriguez|No', price: 1.35, point: 0 },
    ]);
  });
  it('drops bare Yes/No scorer rows with no player identity', () => {
    expect(buildPlayerPropRows({ name: 'Yes', price: 2.5 })).toEqual([]);
    expect(buildPlayerPropRows({ selection: 'No', price: 1.4 })).toEqual([]);
  });
});


// ── App-side display merger ──────────────────────────────────────────────
function row(over: Partial<{
  event_id: string;
  market: string;
  selection: string;
  point: number;
  price: number;
  bookmaker: string;
  suspended: boolean;
  provider_ts: string;
  received_at: string;
}> = {}) {
  const now = new Date().toISOString();
  return {
    event_id: 'e1',
    market: over.market ?? 'player_shots',
    selection: over.selection ?? 'Haaland|Over',
    point: over.point ?? 2.5,
    price: over.price ?? 1.9,
    bookmaker: over.bookmaker ?? 'Bet365',
    suspended: over.suspended ?? false,
    provider_ts: over.provider_ts ?? now,
    received_at: over.received_at ?? now,
  };
}

describe('liveOddsMerge: player-prop display grouping', () => {
  it('groups Over/Under by (player, point) across bookmakers', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ bookmaker: 'Bet365', price: 1.90 }),
      row({ bookmaker: 'Betano', price: 2.00 }),
      row({ selection: 'Haaland|Under', price: 1.85 }),
      row({ selection: 'Salah|Over', price: 2.20, point: 1.5 }),
    ], 'Home', 'Away', { isLive: true });
    const bucket = (merged as Record<string, unknown>).player_shots_ou as Array<{
      player: string; side: string; point: number; price: number;
    }>;
    expect(bucket).toBeTruthy();
    expect(bucket.length).toBe(3);
    const haalandOver = bucket.find(x => x.player === 'Haaland' && x.side === 'Over');
    expect(haalandOver?.price).toBeCloseTo(1.95, 2); // (1.90 + 2.00) / 2
  });

  it('routes goalkeeper_saves_home/away into a single output bucket', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ market: 'goalkeeper_saves_home', selection: 'Alisson|Over', price: 1.9 }),
      row({ market: 'goalkeeper_saves_away', selection: 'Ederson|Under', price: 2.1 }),
    ], 'Home', 'Away', { isLive: true });
    const bucket = (merged as Record<string, unknown>).goalkeeper_saves_ou as unknown[];
    expect(bucket).toBeTruthy();
    expect((bucket as unknown[]).length).toBe(2);
  });

  it('ignores legacy bare Over/Under rows with no player identity', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ selection: 'Over' }),
      row({ selection: 'Under' }),
    ], 'Home', 'Away', { isLive: true });
    expect((merged as Record<string, unknown>).player_shots_ou).toBeUndefined();
  });

  it('handles bare-player Anytime shape (no `|`)', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ market: 'player_first_basket', selection: 'Curry', price: 4.5 }),
    ], 'Home', 'Away', { isLive: true });
    const bucket = (merged as Record<string, unknown>).player_first_basket as Array<{
      player: string; side: string;
    }>;
    expect(bucket).toBeTruthy();
    expect(bucket[0].player).toBe('Curry');
    expect(bucket[0].side).toBe('Anytime');
  });

  it('leaves non-player markets unaffected', () => {
    const initial = { h2h: { home: 1.9, draw: 3.4, away: 4.2 } };
    const merged = mergeLiveOdds(initial as never, [
      row({ market: 'h2h', selection: 'home', price: 1.85 }),
    ], 'Home', 'Away', { isLive: true });
    // No player buckets should exist.
    expect((merged as Record<string, unknown>).player_shots_ou).toBeUndefined();
    expect((merged as Record<string, unknown>).player_passes_ou).toBeUndefined();
  });
});

describe('liveOddsMerge: anytime_scorer p_anytime display', () => {
  it('populates player_anytime_goalscorer from bare-player rows', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ market: 'anytime_scorer', selection: 'Harry Kane', price: 3.2, point: 0 }),
      row({ market: 'anytime_scorer', selection: 'Bukayo Saka', price: 4.5, point: 0 }),
    ], 'Home', 'Away', { isLive: true });
    const bucket = (merged as Record<string, unknown>).player_anytime_goalscorer as Array<{
      player: string; price: number;
    }>;
    expect(bucket).toBeTruthy();
    expect(bucket.length).toBe(2);
    expect(bucket[0].player).toBe('Harry Kane'); // sorted asc
  });

  it('populates player_anytime_goalscorer from `Player|Yes` rows and drops `|No`', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ market: 'anytime_scorer', selection: 'James Rodriguez|Yes', price: 2.8, point: 0 }),
      row({ market: 'anytime_scorer', selection: 'James Rodriguez|No', price: 1.35, point: 0 }),
      row({ market: 'anytime_scorer', selection: 'Vinicius Jr|Yes', price: 2.2, point: 0 }),
    ], 'Home', 'Away', { isLive: true });
    const bucket = (merged as Record<string, unknown>).player_anytime_goalscorer as Array<{
      player: string; price: number;
    }>;
    expect(bucket.length).toBe(2);
    expect(bucket.find(x => x.player === 'James Rodriguez')?.price).toBeCloseTo(2.8, 2);
    expect(bucket.find(x => x.player === 'Vinicius Jr')).toBeTruthy();
  });

  it('ignores bare Yes/No/Over/Under rows without player identity', () => {
    const merged = mergeLiveOdds({} as never, [
      row({ market: 'anytime_scorer', selection: 'Yes', price: 2.5, point: 0 }),
      row({ market: 'anytime_scorer', selection: 'No', price: 1.4, point: 0 }),
      row({ market: 'anytime_scorer', selection: 'Over', price: 1.9, point: 0.5 }),
    ], 'Home', 'Away', { isLive: true });
    expect((merged as Record<string, unknown>).player_anytime_goalscorer).toBeUndefined();
  });
});

