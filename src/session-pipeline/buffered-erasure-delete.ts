/**
 * Later-erasure delete lane for buffered ClickHouse rows
 * (INGEST_BUFFERED_ERASURE_DELETE=off|on, default off; schedule {"bufferedErasureDelete": true}).
 *
 * The feeder (buffered-search-feed.ts) re-checks a stage right after it writes it. Erasure that
 * happens LATER (the buffered GC retiring and redacting a stage, a GC lease refusing retention, a
 * batch leaving the buffer) is this lane: it walks the buffered rows already in
 * `transcript_events`, asks the 043870 `stage` RPC what retention keeps NOW, and deletes the rows
 * it no longer keeps. Until it runs (and for rows it cannot map) the read-side query-time gate
 * (buffered-search-gate.ts) keeps erased sessions out of results.
 *
 * Claim token -> stage. A buffered row's `origin_claim_token` is the stage header's ORIGINAL claim
 * token (a GC replacement keeps it); `stage` accepts only a stage token or stage key and the key
 * cannot be rebuilt from a row (it hashes the batch digest and the object's content digest, and a
 * GC replacement has its own). The feeder therefore writes the stage token and the record's
 * original claim ordinals into the fingerprint it already owns: `bf<body>:<stageToken>:<ords>`
 * (bufferedFingerprint). This lane enumerates DISTINCT (origin_claim_token, fingerprint stage
 * token, org_id) over `origin = 'buffered'` rows, bounded, in keyset order. A row without the
 * stage suffix (written before it) is counted `legacy_unmapped` and left alone. No new migration,
 * RPC, grant or column: only the feeder login's `stage` RPC is called.
 *
 * Decision per (claim, stage). The RPC's answer must name the same claim token and org, else the
 * pair is skipped (`stage_claim_mismatch` / `stage_tenant_mismatch`).
 *   live, pending, deferred  -> nothing is deleted (deferred and pending decide later).
 *   found = false            -> nothing is deleted (`stage_absent`): stage intents are never
 *                               deleted (every reference is ON DELETE RESTRICT), so an unknown token
 *                               means this ClickHouse and this ledger disagree, not an erasure.
 *   gone, refused            -> delete the pair's rows whose ordinals are all outside `retained`
 *                               (the batch's ACTIVE object's kept ordinals, [] for refused and for a
 *                               batch that is no longer buffered). Rows of retained ordinals stay:
 *                               a GC-retired or redacted parent's kept records are the active
 *                               child's records too. This is the feeder's own re-check rule.
 *
 * Deletes. Only through deleteBufferedRows, with every row's full event identity and provenance
 * (origin 'buffered', its exact origin_claim_token, fingerprint and version), read back from
 * ClickHouse and re-verified here first. A canonical row (origin 'canonical', `s3`/md5
 * fingerprint, its own version) can never match: this module never issues any other DELETE and
 * never matches canonical rows. Another claim's or another stage's row for the same event is never
 * matched either. A row that does not carry a bound buffered provenance refuses the whole pair.
 *
 * Bounds. Page size (pairs per ClickHouse page), rate (stage checks per second) and a wall budget
 * per invocation; the keyset cursor persists in `search_ingest_state` (name
 * BUFFERED_ERASURE_CURSOR) after every page and restarts from the beginning once a pass is caught
 * up. The lane is idempotent: a re-run re-checks and deletes nothing more. An error on one pair is
 * counted (`failed`, reason code) and the tick moves on; the pair is retried next pass.
 *
 * Arming. INGEST_BUFFERED_ERASURE_DELETE=on (only that literal) and the same ordering guard as the
 * feeder (BUFFERED_SEARCH_FEED_GATE_DECLARED = the read gate's contract id). Config:
 * INGEST_BUFFERED_ERASURE_PAGE (pairs per page, 1-200, default 25), INGEST_BUFFERED_ERASURE_RATE
 * (stage checks per second, 1-20, default 2), INGEST_BUFFERED_ERASURE_BUDGET_MS (5000-240000,
 * default 40000). Logs carry counts and reason codes only.
 */
import { layoutComponent } from "../shared/layout-component.ts";
import {
  BufferedFeedError,
  FINGERPRINT_ORDINALS_SQL,
  FINGERPRINT_STAGE_SQL,
  ORIGIN_BUFFERED,
  assertOriginColumns,
  bufferedFeedGateRefusal,
  deleteBufferedRows,
  parseBufferedFingerprint,
  type BufferedFeedSource,
  type BufferedRowKey,
} from "./buffered-search-feed.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { readState, writeState } from "./vendor/search-index/reconcile.ts";
import { EVENTS_TABLE } from "./vendor/search-index/schema.ts";

export const BUFFERED_ERASURE_CURSOR = "buffered_erasure_cursor";
/** Candidate rows read back per ClickHouse select within one pair. */
export const BUFFERED_ERASURE_ROWS_PER_SELECT = 2_000;

export class BufferedErasureError extends BufferedFeedError {
  constructor(code: string, retryable = false) {
    super(code, retryable);
    this.name = "BufferedErasureError";
    this.message = `buffered erasure delete: ${code}`;
  }
}

export interface BufferedErasureConfig {
  enabled: boolean;
  /** (claim, stage) pairs per ClickHouse page. 1-200, default 25. */
  pageSize: number;
  /** Stage checks per second across a run. 1-20, default 2. */
  stagesPerSecond: number;
  /** Wall budget of one invocation; under the one-minute schedule. 5000-240000, default 40000. */
  budgetMs: number;
}

const PREFIX = "INGEST_BUFFERED_ERASURE_";

/** Only the literals `off` (default) and `on`; `on` also needs the feeder's ordering guard. */
export function bufferedErasureConfig(env: NodeJS.ProcessEnv): BufferedErasureConfig {
  const flag = env.INGEST_BUFFERED_ERASURE_DELETE ?? "off";
  if (flag !== "off" && flag !== "on") throw new BufferedErasureError("config_flag_invalid");
  const refusal = flag === "on" ? bufferedFeedGateRefusal(env) : null;
  if (refusal) throw new BufferedErasureError(refusal);
  const int = (suffix: string, fallback: number, min: number, max: number) => {
    const raw = env[PREFIX + suffix];
    if (raw === undefined || raw === "") return fallback;
    if (!/^\d{1,6}$/.test(raw) || Number(raw) < min || Number(raw) > max) throw new BufferedErasureError(`config_${suffix.toLowerCase()}_invalid`);
    return Number(raw);
  };
  return {
    enabled: flag === "on",
    pageSize: int("PAGE", 25, 1, 200),
    stagesPerSecond: int("RATE", 2, 1, 20),
    budgetMs: int("BUDGET_MS", 40_000, 5_000, 240_000),
  };
}

export interface BufferedErasureDeps {
  /** The feeder login's 043870 source; only `stage` is called. */
  source: Pick<BufferedFeedSource, "stage">;
  ch: ClickHouseClient;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  rowsPerSelect?: number;
}

/** Keyset position: the last fully handled (claim, stage, org). */
export interface ErasureCursor { claim: string; stage: string; org: string }

export interface ErasureSummary {
  pages: number;
  /** Pairs listed and handled (including legacy and skipped ones). */
  pairs: number;
  /** Pairs whose stage was checked through the RPC. */
  checked: number;
  /** Pairs left alone: live, pending, deferred, stage_absent, or nothing outside retention. */
  kept: number;
  /** Pairs with at least one row deleted. */
  erased: number;
  /** Rows sent to deleteBufferedRows (each re-verified first; a row seen again refuses its pair). */
  deleted: number;
  /** Pairs not checked: legacy_unmapped, malformed, claim/tenant mismatch. */
  skipped: number;
  /** Pairs whose check or delete failed (counted, retried next pass). */
  failed: number;
  /** The pass reached the last pair; the cursor restarts next run. */
  caughtUp: boolean;
  cursor: ErasureCursor | null;
  reasons: Record<string, number>;
}

const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const CH_TS = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/;

function parseCursor(value: string | null): ErasureCursor | null | "invalid" {
  if (value === null || value === "") return null;
  try {
    const v = JSON.parse(value) as Partial<ErasureCursor> | null;
    if (v && typeof v.claim === "string" && typeof v.stage === "string" && typeof v.org === "string" && v.claim && v.org) return { claim: v.claim, stage: v.stage, org: v.org };
  } catch { /* fall through */ }
  return "invalid";
}

interface Pair { claim: string; stage: string; org: string }

async function listPairs(ch: ClickHouseClient, after: ErasureCursor | null, limit: number): Promise<Pair[]> {
  const rows = await ch.select<{ claim: string; stage: string; org: string }>(
    `SELECT origin_claim_token AS claim, ${FINGERPRINT_STAGE_SQL} AS stage, org_id AS org
     FROM ${EVENTS_TABLE}
     WHERE origin = '${ORIGIN_BUFFERED}'${after ? `
       AND (origin_claim_token, ${FINGERPRINT_STAGE_SQL}, org_id) > ({after_claim:String}, {after_stage:String}, {after_org:String})` : ""}
     GROUP BY claim, stage, org
     ORDER BY claim, stage, org
     LIMIT {limit:UInt32}`,
    { params: { limit, ...(after ? { after_claim: after.claim, after_stage: after.stage, after_org: after.org } : {}) }, timeoutMs: 30_000 });
  if (rows.length > limit) throw new BufferedErasureError("pairs_overflow");
  return rows.map((r) => {
    if (typeof r.claim !== "string" || typeof r.stage !== "string" || typeof r.org !== "string") throw new BufferedErasureError("pairs_row");
    return { claim: r.claim, stage: r.stage, org: r.org };
  });
}

type RowOut = { [K in keyof BufferedRowKey]: K extends "version" ? string : BufferedRowKey[K] };

/**
 * The pair's rows retention no longer keeps, read back and re-verified: every one must carry the
 * pair's exact buffered provenance and none of the kept ordinals, else the pair is refused.
 */
async function candidates(ch: ClickHouseClient, pair: Pair, workspace: string, keep: ReadonlySet<number>, limit: number): Promise<BufferedRowKey[]> {
  const rows = await ch.select<RowOut>(
    `SELECT org_id, session_id, workspace_id, machine_id, source, kind, event_id, toString(ts) AS ts,
            origin, origin_claim_token, fingerprint, toString(version) AS version
     FROM ${EVENTS_TABLE}
     WHERE org_id = {org:String} AND workspace_id = {workspace:String}
       AND origin = '${ORIGIN_BUFFERED}' AND origin_claim_token = {claim:String}
       AND ${FINGERPRINT_STAGE_SQL} = {stage:String}
       AND NOT hasAny(${FINGERPRINT_ORDINALS_SQL}, {keep:Array(String)})
     LIMIT {limit:UInt32}`,
    { params: { org: pair.org, workspace, claim: pair.claim, stage: pair.stage, keep: [...keep].map(String), limit }, timeoutMs: 30_000 });
  if (rows.length > limit) throw new BufferedErasureError("rows_overflow");
  return rows.map((r) => {
    const bound = typeof r.fingerprint === "string" ? parseBufferedFingerprint(r.fingerprint) : null;
    const version = typeof r.version === "string" && /^\d{1,16}$/.test(r.version) ? Number(r.version) : NaN;
    if (r.origin !== ORIGIN_BUFFERED || r.origin_claim_token !== pair.claim || r.org_id !== pair.org || r.workspace_id !== workspace ||
        !bound || bound.stageToken !== pair.stage || bound.ordinals.some((o) => keep.has(o)) ||
        !Number.isSafeInteger(version) || version <= 0 || typeof r.ts !== "string" || !CH_TS.test(r.ts) ||
        [r.session_id, r.machine_id, r.source, r.kind, r.event_id].some((v) => typeof v !== "string"))
      throw new BufferedErasureError("row_unbound");
    return { ...r, origin: ORIGIN_BUFFERED, version };
  });
}

/**
 * One bounded pass. Resumes from the persisted cursor, persists it after every page, and resets it
 * when the pass is caught up. Throws only for a tick-level failure (origin columns, listing, cursor
 * write); a pair's failure is counted.
 */
export async function runBufferedErasureDelete(deps: BufferedErasureDeps, config: Pick<BufferedErasureConfig, "pageSize" | "stagesPerSecond" | "budgetMs">): Promise<ErasureSummary> {
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const deadline = now() + config.budgetMs;
  const interval = 1_000 / config.stagesPerSecond;
  const perSelect = deps.rowsPerSelect ?? BUFFERED_ERASURE_ROWS_PER_SELECT;
  const summary: ErasureSummary = { pages: 0, pairs: 0, checked: 0, kept: 0, erased: 0, deleted: 0, skipped: 0, failed: 0, caughtUp: false, cursor: null, reasons: {} };
  const count = (reason: string) => (summary.reasons[reason] = (summary.reasons[reason] ?? 0) + 1);
  await assertOriginColumns(deps.ch);
  const stored = parseCursor(await readState(deps.ch, BUFFERED_ERASURE_CURSOR));
  if (stored === "invalid") count("cursor_invalid");
  let cursor: ErasureCursor | null = stored === "invalid" ? null : stored;
  let persisted = stored === "invalid" ? "invalid" : cursor ? JSON.stringify(cursor) : "";
  const save = async (to: ErasureCursor | null) => {
    const value = to ? JSON.stringify({ claim: to.claim, stage: to.stage, org: to.org }) : "";
    if (value === persisted) return;
    await writeState(deps.ch, BUFFERED_ERASURE_CURSOR, value, new Date(now()));
    persisted = value;
  };
  let next = now();

  /** Handle one pair; false when the budget ran out before or part-way through it (the cursor stays before it). */
  const handle = async (pair: Pair): Promise<boolean> => {
    if (pair.stage === "") { count("legacy_unmapped"); summary.skipped += 1; return true; }
    if (!UUID.test(pair.stage) || !UUID.test(pair.claim)) { count("fingerprint_stage_malformed"); summary.skipped += 1; return true; }
    // The rate wait never runs past the budget: stop before the pair instead (the cursor stays before it).
    if (next >= deadline) return false;
    const wait = next - now();
    if (wait > 0) await sleep(wait);
    next = Math.max(next, now()) + interval;
    try {
      const state = await deps.source.stage({ stageToken: pair.stage });
      summary.checked += 1;
      if (!state.found) { count(state.reason ?? "stage_absent"); summary.kept += 1; return true; }
      if (state.claimToken !== pair.claim) { count("stage_claim_mismatch"); summary.skipped += 1; return true; }
      if (state.tenant?.orgId !== pair.org) { count("stage_tenant_mismatch"); summary.skipped += 1; return true; }
      if (state.status !== "gone" && state.status !== "refused") { count(state.reason ?? state.status); summary.kept += 1; return true; }
      const keep = new Set(state.retained);
      const workspace = layoutComponent(state.tenant.workspaceId);
      let deleted = 0;
      const sent = new Set<string>();
      for (;;) {
        const rows = await candidates(deps.ch, pair, workspace, keep, perSelect);
        // A row read back after its delete was sent means the delete did not apply: refuse the
        // pair instead of re-reading the same page until the budget runs out.
        for (const r of rows) {
          const id = JSON.stringify([r.session_id, r.machine_id, r.source, r.kind, r.event_id, r.ts, r.fingerprint, r.version]);
          if (sent.has(id)) throw new BufferedErasureError("delete_not_applied");
          sent.add(id);
        }
        if (rows.length) deleted += await deleteBufferedRows(deps.ch, rows);
        if (rows.length < perSelect) break;
        if (now() >= deadline) { summary.deleted += deleted; count("budget_mid_pair"); return false; }
      }
      summary.deleted += deleted;
      count(state.reason ?? state.status);
      if (deleted) summary.erased += 1; else summary.kept += 1;
    } catch (error) {
      summary.failed += 1;
      count(error instanceof BufferedFeedError ? error.code : "erase_failed");
    }
    return true;
  };

  while (now() < deadline) {
    const page = await listPairs(deps.ch, cursor, config.pageSize);
    summary.pages += 1;
    let stopped = false;
    for (const pair of page) {
      if (now() >= deadline || !(await handle(pair))) { stopped = true; break; }
      summary.pairs += 1;
      cursor = pair;
    }
    if (!stopped && page.length < config.pageSize) {
      summary.caughtUp = true;
      cursor = null;
      await save(null);
      break;
    }
    await save(cursor);
    if (stopped) break;
  }
  summary.cursor = cursor;
  return summary;
}
