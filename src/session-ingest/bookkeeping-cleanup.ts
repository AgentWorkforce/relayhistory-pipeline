/** Operator-only driver for the DRAFT ingest bookkeeping cleanup (docs/design/ingest-bookkeeping-cleanup.md).
 *
 * Each page is ONE short transaction: BEGIN; SET LOCAL statement_timeout (1..5000 ms, which the SQL
 * also requires); one sessions.ingest_bookkeeping_cleanup_<phase>(...) call; COMMIT. Between pages it
 * sleeps pauseMs, so cleanup IO never piles onto the ingest ACK path. Dry run is the default. --execute
 * stays a refusal (55000) until the owner has also enabled the phase in SQL. Two switches, both required.
 * A busy phase lock stops the run: it never waits for another cleaner. Any malformed result, or a
 * cursor that does not strictly advance, stops the run before the next page.
 * Output is counts and the resume cursor only. No row content is ever returned or logged.
 */
export type CleanupPhase = "decisions" | "seats" | "custody" | "frozen";
export type CleanupQuery = (sql: string, params: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>;
export interface CleanupSql { transaction<T>(work: (query: CleanupQuery) => Promise<T>): Promise<T> }
export interface CleanupOptions {
  sql: CleanupSql; phase: CleanupPhase; limit: number; maxPages: number; execute: boolean;
  pauseMs: number; statementTimeoutMs: number; after?: unknown; sleep?: (ms: number) => Promise<void>;
}
export interface CleanupReport {
  phase: CleanupPhase; mode: "dry-run" | "execute" | "census"; pages: number; scannedRows: number; eligibleRows: number;
  deleted: number; busy: boolean; done: boolean; next: unknown; blocked: Record<string, number>;
}
export class BookkeepingCleanupError extends Error { constructor(readonly code: string) { super("bookkeeping cleanup refused: " + code); } }
const no = (code: string): never => { throw new BookkeepingCleanupError(code); };
const FN: Record<CleanupPhase, string> = { decisions: "decisions", seats: "seats", custody: "custody", frozen: "frozen_census" };
const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/, SEAT = /^(0|[1-9][0-9]{0,18})$/, HEX = /^[0-9a-f]{64}$/;

/** The cursor each phase accepts. Shape-checked here as well as in SQL. */
export function assertCursor(phase: CleanupPhase, v: unknown): void {
  if (v === null || v === undefined) return;
  if (phase === "decisions" && (typeof v !== "string" || !UUID.test(v))) no("cursor");
  if (phase === "seats" && (typeof v !== "string" || !SEAT.test(v))) no("cursor");
  if (phase === "custody" && (typeof v !== "string" || !HEX.test(v))) no("cursor");
  if (phase === "frozen" && (!Array.isArray(v) || v.length !== 4 || v.some((x) => typeof x !== "string"))) no("cursor");
}
/** Strict advance in the SQL keyset order (uuid and hex: byte order; seat: numeric; frozen: SQL tuple order, checked only for change). */
function advanced(phase: CleanupPhase, prev: unknown, next: unknown): boolean {
  if (prev === null || prev === undefined) return true;
  if (phase === "seats") return BigInt(next as string) > BigInt(prev as string);
  if (phase === "frozen") return JSON.stringify(next) !== JSON.stringify(prev);
  return (next as string) > (prev as string);
}
function body(o: CleanupOptions, after: unknown): Record<string, unknown> {
  const a = after === undefined ? null : after;
  if (o.phase === "decisions") return { after: a, rowBudget: o.limit, execute: o.execute };
  if (o.phase === "frozen") return { after: a, limit: o.limit };
  return { after: a, limit: o.limit, execute: o.execute };
}
const nat = (v: unknown) => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;

export async function runBookkeepingCleanup(o: CleanupOptions): Promise<CleanupReport> {
  const bounds = o.phase === "decisions" ? [500, 5000] : o.phase === "frozen" ? [1, 5000] : [1, 1000];
  if (!(o.phase in FN) || !Number.isInteger(o.limit) || o.limit < bounds[0]! || o.limit > bounds[1]! || !Number.isInteger(o.maxPages) || o.maxPages < 1
    || o.maxPages > 100000 || !Number.isInteger(o.pauseMs) || o.pauseMs < 0 || o.pauseMs > 60000 || !Number.isInteger(o.statementTimeoutMs)
    || o.statementTimeoutMs < 1 || o.statementTimeoutMs > 5000 || (o.phase === "frozen" && o.execute)) no("options");
  assertCursor(o.phase, o.after);
  const sleep = o.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const report: CleanupReport = { phase: o.phase, mode: o.phase === "frozen" ? "census" : o.execute ? "execute" : "dry-run", pages: 0, scannedRows: 0,
    eligibleRows: 0, deleted: 0, busy: false, done: false, next: o.after ?? null, blocked: {} };
  for (let i = 0; i < o.maxPages; i++) {
    if (i > 0 && o.pauseMs > 0) await sleep(o.pauseMs);
    const r = await o.sql.transaction(async (q) => {
      await q(`SET LOCAL statement_timeout=${o.statementTimeoutMs}`, []);
      return (await q(`SELECT sessions.ingest_bookkeeping_cleanup_${FN[o.phase]}($1::jsonb) AS r`, [JSON.stringify(body(o, report.next))])).rows[0]?.r;
    }) as Record<string, unknown> | undefined;
    if (!r || typeof r !== "object" || r.phase !== o.phase) no("result_shape");
    if (r!.busy === true) { report.busy = true; break; }
    if (r!.busy !== undefined && r!.busy !== false) no("result_shape");
    if (r!.mode !== report.mode || !nat(r!.scannedRows) || typeof r!.done !== "boolean") no("result_shape");
    const eligible = o.phase === "frozen" ? r!.bothConditions : r!.eligibleRows, deleted = o.phase === "frozen" ? 0 : r!.deleted;
    if (!nat(eligible) || !nat(deleted) || (!o.execute && deleted !== 0) || (deleted as number) > (eligible as number)) no("result_counts");
    if (r!.next !== null) { assertCursor(o.phase, r!.next); if (!advanced(o.phase, report.next, r!.next)) no("cursor_order"); }
    report.pages++; report.scannedRows += r!.scannedRows as number; report.eligibleRows += eligible as number; report.deleted += deleted as number;
    const blocked = (o.phase === "frozen" ? { slotInitialized: r!.slotInitialized, ownedCovered: r!.ownedCovered } : r!.blocked) as Record<string, unknown> | undefined;
    for (const [k, v] of Object.entries(blocked ?? {})) { if (!nat(v)) no("result_counts"); report.blocked[k] = (report.blocked[k] ?? 0) + (v as number); }
    if (r!.done === true || r!.next === null) { report.done = true; report.next = null; break; }
    report.next = r!.next;
  }
  return report;
}
