// Ambient types for the plain-JS bridge normalize helpers.
// Kept intentionally loose (any-shaped returns) so existing @ts-expect-error
// call sites in tests continue to compile without ceremony.
export const SIDE_HANDICAP_MARKETS: Set<string>;
export function canonicalizeMatchSide(
  raw: unknown,
  homeName: string,
  awayName: string,
): 'home' | 'away' | null;
export function isQuarterLine(point: unknown): boolean;
export const EXACT_GOAL_BUCKETS: string[];
export const MULTI_GOAL_BANDS: string[];
export const BUCKET_MARKETS: Set<string>;
export function normalizeBucketSelection(
  market: string,
  raw: unknown,
): string | null;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function makeBucketDiagnostics(): any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function normalizeClock(raw: unknown): any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function normalizeScores(raw: unknown): any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildBridgeEventRow(evt: unknown, ctx: unknown): any;
