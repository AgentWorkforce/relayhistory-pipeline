/**
 * Buffered-stage search feed (INGEST_BUFFERED_SEARCH_FEED=off|on, default off).
 *
 * Since the buffered cutover (2026-11-05 05:33:45Z) uploads are accepted as buffered stages
 * (Supabase ledger + one S3 object under `buffered-batches/`) and never published to session
 * manifests, so `indexSearchScope` never sees them and ClickHouse `transcript_events` stops at
 * the cutover. This feeds each accepted stage's RETAINED lines into the same table, with the
 * same row shape the canonical indexer writes for the same lines.
 *
 * Projection. The calls `readCanonicalScope` makes, in its order, on one stage group (one
 * session): `parseRelayhistoryRecord` -> `normalizeRelayhistoryRecords` ->
 * `projectRelayhistorySession` -> `projectedToNeonRow` -> `toIndexRow`, with the scope and
 * identity a live manifest written from the same group would carry (writer.ts writeGroup:
 * layout workspace/harness/session, `liveMachineId`, `96bulls.canonical_session_id` and
 * `user_id`). The parity test writes the same group through `writeGroups` and compares with
 * `indexSearchScope`'s rows.
 *
 * Provenance marker. Every row written here has `origin = 'buffered'` and
 * `origin_claim_token` = the stage's ORIGINAL claim token (a GC replacement keeps it), the two
 * columns the read-side gate (#338, buffered-search-gate.ts) reads; every other writer leaves
 * the column default `origin = 'canonical'`. The columns come from
 * docs/proposals/buffered-search-feed.clickhouse.draft.sql (the vendored schema.ts is not
 * edited); the feeder checks system.columns and refuses to write without them. The fingerprint
 * also carries the prefix `bf` (canonical S3 `s3`, Neon tail bare md5): that is the writer-side
 * marker indexSearchScope reads in its existing scan, so its delete pass leaves buffered-only
 * rows alone. Because `bf` never equals `s3`, the first canonical pass over the same events
 * rewrites them once with its newer `version` (origin back to canonical).
 *
 * Arming. INGEST_BUFFERED_SEARCH_FEED=on is refused unless the deployment also declares
 * BUFFERED_SEARCH_FEED_GATE_DECLARED equal to the read gate's contract id
 * (BUFFERED_SEARCH_GATE_CONTRACT, #338 buffered-search-gate.ts; exact match, default unset):
 * buffered rows served by a reader without the gate would bypass the per-hit privacy check, so
 * the read Worker must be deployed with READ_API_BUFFERED_SEARCH_GATE=on first. A missing or
 * stale declaration is a logged refusal (reason code only) and the feed does nothing.
 *
 * Idempotency. The row identity is ClickHouse's sort key (org, workspace, ts, session, source,
 * machine, kind, event_id), all derived from the record; `version` is the stage's
 * `received_at` (epoch ms), not the wall clock. A replay, a backfill re-run or a GC
 * replacement of the same batch re-inserts byte-identical rows that ReplacingMergeTree folds,
 * and any later canonical pass (version = its wall clock) wins over them.
 *
 * Privacy and erasure. Only what the buffered GC's own retention authority keeps is indexed
 * (`sessions.ingest_buffer_gc_ordinals`, via the feed RPC in
 * supabase/migrations/20261105043870_buffered_search_feed_cursor.sql); a stage that is not its batch's active,
 * acknowledged, live outbox object is never indexed; a GC lease in flight or a fenced privacy
 * authority defers. After every ClickHouse write the stage is re-checked and rows of any
 * record retention no longer keeps are deleted: only the exact physical rows this claim wrote
 * (origin, origin_claim_token, fingerprint and version all bound; never a prefix match), so a
 * stale claim's cleanup never removes a newer claim's row for the same event. Deletes for
 * erasure that happens LATER (session delete/reset/GC) are a separate lane; until it lands the
 * query-time check keeps deleted sessions out.
 *
 * Nothing here logs content; errors and summaries carry codes and counts only.
 */
import { zstdDecompressSync } from "node:zlib";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { projectRelayhistorySession } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { projectedToNeonRow } from "../session-read-api/canonical-projection.ts";
import { BUFFER_STAGE_MAX_BYTES, readBufferedStage, type DecodedBufferedStage } from "../session-ingest/staging.ts";
import type { BufferedStageSpec } from "../session-ingest/buffered-protocol.ts";
import { liveMachineId } from "../session-ingest/writer.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { isObjectStoreError, type ObjectStore } from "../shared/vendor.ts";
import { arrayLiteral, tuple, type ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import { rowBytes, toIndexRow, tsMillis, type IndexRow } from "./vendor/search-index/rows.ts";
import { EVENTS_TABLE } from "./vendor/search-index/schema.ts";
import { windowStart } from "./vendor/search-index/reconcile.ts";
import type { SessionScope } from "./pointer.ts";
import { BUFFERED_SEARCH_GATE_CONTRACT } from "../session-read-api/buffered-search-gate.ts";

/** Fingerprint prefix of every buffered-origin row (`s3` canonical S3, bare md5 Neon tail). */
export const BUFFERED_FINGERPRINT_PREFIX = "bf";
/** The read-side gate's marker values and columns (#338 buffered-search-gate.ts ORIGIN_*). */
export const ORIGIN_BUFFERED = "buffered";
export const ORIGIN_COLUMNS = { origin: { type: "LowCardinality(String)", default: "'canonical'" }, origin_claim_token: { type: "String", default: "''" } } as const;
/** The buffered cutover: the first buffered acceptance in production. */
export const BUFFERED_FEED_DEFAULT_SINCE = "2026-11-05T05:33:45.000Z";
export const BUFFERED_FEED_DEFAULT_CURSOR = "backfill-20261105";

export const isBufferedOriginFingerprint = (fingerprint: string) => fingerprint.startsWith(BUFFERED_FINGERPRINT_PREFIX);
export const isBufferedStageKey = (key: string) => key.includes("/buffered-batches/v1/");

export class BufferedFeedError extends Error {
  constructor(readonly code: string, readonly retryable = false) {
    super(`buffered search feed: ${code}`);
    this.name = "BufferedFeedError";
  }
}

export interface BufferedFeedConfig {
  enabled: boolean;
  /** Stages per page (one SQL page, one batched ClickHouse insert). 1-200, default 50. */
  pageSize: number;
  /** Stage checks per second across a run (SQL + S3 read each). 1-50, default 5. */
  stagesPerSecond: number;
  /** Lower bound of the backfill; a cursor is bound to it. */
  since: string;
  cursor: string;
  /** Wall budget of one invocation; stays under the one-minute schedule so runs do not overlap. */
  budgetMs: number;
}

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/;

export { BUFFERED_SEARCH_GATE_CONTRACT };

/**
 * The ordering guard: why the feed must not run, or null when the deployment declares the read
 * gate's exact contract id in BUFFERED_SEARCH_FEED_GATE_DECLARED. `gate_not_declared` when unset
 * or empty, `gate_contract_mismatch` for any other value (a stale or mistyped declaration).
 */
export function bufferedFeedGateRefusal(env: NodeJS.ProcessEnv): "gate_not_declared" | "gate_contract_mismatch" | null {
  const declared = env.BUFFERED_SEARCH_FEED_GATE_DECLARED;
  if (declared === undefined || declared === "") return "gate_not_declared";
  return declared === BUFFERED_SEARCH_GATE_CONTRACT ? null : "gate_contract_mismatch";
}

/**
 * Only the literals `off` (default) and `on`; every other value refuses, so a typo never arms it.
 * `on` is refused unless BUFFERED_SEARCH_FEED_GATE_DECLARED declares the read gate's contract.
 */
export function bufferedFeedConfig(env: NodeJS.ProcessEnv): BufferedFeedConfig {
  const flag = env.INGEST_BUFFERED_SEARCH_FEED ?? "off";
  if (flag !== "off" && flag !== "on") throw new BufferedFeedError("config_flag_invalid");
  const refusal = flag === "on" ? bufferedFeedGateRefusal(env) : null;
  if (refusal) throw new BufferedFeedError(refusal);
  const int = (name: string, fallback: number, min: number, max: number) => {
    const raw = env[name];
    if (raw === undefined || raw === "") return fallback;
    if (!/^\d{1,6}$/.test(raw) || Number(raw) < min || Number(raw) > max) throw new BufferedFeedError(`config_${name.slice("INGEST_BUFFERED_SEARCH_FEED_".length).toLowerCase()}_invalid`);
    return Number(raw);
  };
  const since = env.INGEST_BUFFERED_SEARCH_FEED_SINCE || BUFFERED_FEED_DEFAULT_SINCE;
  if (!ISO.test(since) || !Number.isFinite(Date.parse(since))) throw new BufferedFeedError("config_since_invalid");
  const cursor = env.INGEST_BUFFERED_SEARCH_FEED_CURSOR || BUFFERED_FEED_DEFAULT_CURSOR;
  if (!/^[a-z][a-z0-9-]{0,62}$/.test(cursor)) throw new BufferedFeedError("config_cursor_invalid");
  return {
    enabled: flag === "on",
    pageSize: int("INGEST_BUFFERED_SEARCH_FEED_PAGE", 50, 1, 200),
    stagesPerSecond: int("INGEST_BUFFERED_SEARCH_FEED_RATE", 5, 1, 50),
    since,
    cursor,
    budgetMs: int("INGEST_BUFFERED_SEARCH_FEED_BUDGET_MS", 50_000, 5_000, 240_000),
  };
}

// ---------------------------------------------------------------- SQL source (043870 RPCs)

export interface FeedTenant { orgId: string; workspaceId: string; userId: string }
export interface StageCandidate { stageToken: string; createdAt: string; tenant: FeedTenant; originId: string; batchId: string }
export type StageStatus = "live" | "pending" | "deferred" | "refused" | "gone";
export interface StageState {
  found: boolean;
  status: StageStatus;
  reason: string | null;
  /** Original claim ordinals retention keeps NOW for the stage's batch (its active object). */
  retained: number[];
  tenant?: FeedTenant;
  originId?: string;
  batchId?: string;
  /** The stage's original admission (ingest_claim_attempts); the read gate looks it up. */
  claimToken?: string;
  stage?: BufferedStageSpec & { stageToken: string; createdAt: string };
}
export interface FeedPage { after: { createdAt: string; stageToken: string } | null; stages: StageCandidate[] }

export interface BufferedFeedSource {
  page(cursor: string, since: string, limit: number): Promise<FeedPage>;
  stage(ref: { stageToken: string } | { stageKey: string }): Promise<StageState>;
  advance(cursor: string, to: { createdAt: string; stageToken: string }, counts: { indexed: number; skipped: number }): Promise<boolean>;
}

const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const STATUSES: readonly StageStatus[] = ["live", "pending", "deferred", "refused", "gone"];
const obj = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const str = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const bad = (code: string): never => { throw new BufferedFeedError(code); };

function checkTenant(v: unknown): FeedTenant {
  if (!obj(v) || !str(v.orgId) || !str(v.workspaceId) || !str(v.userId)) bad("sql_tenant");
  const t = v as Record<string, string>;
  return { orgId: t.orgId!, workspaceId: t.workspaceId!, userId: t.userId! };
}
function checkOrdinals(v: unknown): number[] {
  if (!Array.isArray(v) || v.some((o) => !Number.isInteger(o) || o < 0 || o > 999)) bad("sql_ordinals");
  return v as number[];
}

/** The 043870 feed RPCs, one autocommit statement each (each its own short transaction). */
export function sqlBufferedFeedSource(db: SqlExecutor): BufferedFeedSource {
  const call = async (name: "page" | "stage" | "advance", p: unknown): Promise<Record<string, unknown>> => {
    let value: unknown;
    try { value = (await db.query<{ r: unknown }>(`SELECT sessions.buffered_search_feed_${name}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0]?.r; }
    catch (error) {
      // A refused feeder login (buffered-feed-login.ts) keeps its own code and is not retried.
      if (error instanceof BufferedFeedError) throw error;
      const code = (error as { code?: unknown } | null)?.code;
      // Serialization, lock and statement timeouts are transient: the page or message retries.
      const sqlstate = typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : "unknown";
      throw new BufferedFeedError(`sql_${sqlstate}`, ["40001", "40P01", "55P03", "57014", "08006", "08003", "unknown"].includes(sqlstate));
    }
    if (!obj(value)) bad("sql_result");
    return value as Record<string, unknown>;
  };
  return {
    async page(cursor, since, limit) {
      const r = await call("page", { cursor, since, limit });
      if (!Array.isArray(r.stages) || r.stages.length > limit) bad("sql_page");
      const stages = (r.stages as unknown[]).map((s) => {
        if (!obj(s) || !str(s.stageToken) || !UUID.test(s.stageToken) || !str(s.createdAt) || !ISO.test(s.createdAt) || !str(s.originId) || !str(s.batchId)) bad("sql_page_row");
        const row = s as Record<string, unknown>;
        return { stageToken: row.stageToken as string, createdAt: row.createdAt as string, tenant: checkTenant(row.tenant), originId: row.originId as string, batchId: row.batchId as string };
      });
      for (let i = 1; i < stages.length; i++)
        if (stages[i - 1]!.createdAt > stages[i]!.createdAt) bad("sql_page_order");
      const after = r.after === null ? null : obj(r.after) && str(r.after.createdAt) && str(r.after.stageToken) ? { createdAt: r.after.createdAt, stageToken: r.after.stageToken } : bad("sql_page_after");
      return { after, stages };
    },
    async stage(ref) {
      const r = await call("stage", ref);
      if (typeof r.found !== "boolean" || !STATUSES.includes(r.status as StageStatus) || (r.reason !== null && typeof r.reason !== "string")) bad("sql_stage");
      const state: StageState = { found: r.found as boolean, status: r.status as StageStatus, reason: (r.reason as string | null) ?? null, retained: checkOrdinals(r.retained) };
      if (!state.found) return state;
      const s = r.stage;
      if (!obj(s) || !str(s.stageToken) || !UUID.test(s.stageToken) || !str(s.key) || !str(s.sha256) || !/^[0-9a-f]{64}$/.test(s.sha256) ||
          !Number.isSafeInteger(s.bytes) || (s.bytes as number) < 1 || (s.bytes as number) > BUFFER_STAGE_MAX_BYTES ||
          !Array.isArray(s.groupOrdinals) || !str(s.createdAt) || !str(r.originId) || !str(r.batchId) || !str(r.claimToken) || !UUID.test(r.claimToken)) bad("sql_stage_spec");
      const spec = s as Record<string, unknown>;
      const groupOrdinals = (spec.groupOrdinals as unknown[]).map(checkOrdinals);
      if ("stageToken" in ref && spec.stageToken !== ref.stageToken) bad("sql_stage_identity");
      if ("stageKey" in ref && spec.key !== ref.stageKey) bad("sql_stage_identity");
      return {
        ...state,
        tenant: checkTenant(r.tenant),
        originId: r.originId as string,
        batchId: r.batchId as string,
        claimToken: r.claimToken as string,
        stage: { stageToken: spec.stageToken as string, key: spec.key as string, sha256: spec.sha256 as string, bytes: spec.bytes as number, groupOrdinals, createdAt: spec.createdAt as string },
      };
    },
    async advance(cursor, to, counts) {
      const r = await call("advance", { cursor, createdAt: to.createdAt, stageToken: to.stageToken, indexed: counts.indexed, skipped: counts.skipped });
      if (typeof r.advanced !== "boolean") bad("sql_advance");
      return r.advanced as boolean;
    },
  };
}

// ---------------------------------------------------------------- projection

/** An index row as the feeder writes it: the canonical shape plus the read gate's origin marker. */
export type BufferedIndexRow = IndexRow & { origin: typeof ORIGIN_BUFFERED; origin_claim_token: string };
/** One projected row and the original claim ordinals of the record it came from. */
export interface BufferedRow { row: BufferedIndexRow; ordinals: number[] }

export interface StageProjection {
  rows: BufferedRow[];
  /** Groups not projected (codes only): a fold or projection refusal never indexes a subset. */
  reasons: string[];
  version: number;
}

/** writer.ts identity validation (manifest-identity.ts sourceId / manifestUserId): <=255 code points, no controls. */
const admitted = (v: string) => v.length > 0 && Array.from(v).length <= 255 && !/[\u0000-\u001f\u007f]/.test(v);

/**
 * The index rows a live manifest written from this stage's retained lines would give
 * `indexSearchScope`, fingerprinted as buffered-origin. Pure: no IO.
 */
export function projectBufferedStage(decoded: DecodedBufferedStage, retained: ReadonlySet<number>, keyRoot: string): StageProjection {
  const { header, groups, spec } = decoded;
  if (!spec.key.startsWith(`${keyRoot}/${header.org_id}/buffered-batches/v1/`)) throw new BufferedFeedError("stage_key_tenant_mismatch");
  const version = Date.parse(header.received_at);
  if (!Number.isSafeInteger(version) || version <= 0) throw new BufferedFeedError("stage_received_at_invalid");
  const workspace = layoutComponent(header.workspace_id);
  const machineId = liveMachineId(header.workspace_id, header.origin_id, header.user_id);
  const userId = admitted(header.user_id) ? header.user_id : "";
  const out: BufferedRow[] = [];
  const reasons: string[] = [];
  groups.forEach((group, g) => {
    const ordinals = spec.groupOrdinals[g]!;
    const kept = group.lines.flatMap((line, j) => (retained.has(ordinals[j]!) ? [{ line, ord: ordinals[j]! }] : []));
    if (!kept.length) return;
    const scope: SessionScope = { orgId: header.org_id, workspaceId: workspace, harness: layoutComponent(group.source), sessionId: layoutComponent(group.sessionId) };
    // canonicalManifestIdentity: the admitted pre-layout id when it encodes to the physical one.
    const sessionId = admitted(group.sessionId) && layoutComponent(group.sessionId) === scope.sessionId ? group.sessionId : scope.sessionId;
    let records: RelayhistoryStreamRecord[];
    let projected;
    try {
      records = normalizeRelayhistoryRecords(kept.map((k) => parseRelayhistoryRecord(k.line)));
      projected = projectRelayhistorySession(records, { workspaceId: scope.workspaceId, source: scope.harness, sessionId, machineId, userId });
    } catch (error) {
      reasons.push((error as { code?: unknown })?.code && typeof (error as { code: unknown }).code === "string" ? (error as { code: string }).code : "projection_failed");
      return;
    }
    const byRecord = new Map<string, number[]>();
    for (const k of kept) {
      const id = (JSON.parse(k.line) as { record_id?: unknown }).record_id;
      if (typeof id === "string") byRecord.set(id, [...(byRecord.get(id) ?? []), k.ord]);
    }
    for (const { table, row } of projected.rows) {
      if (table !== "convergence_events") continue;
      const canonicalRow = { ...row, session_id: row.delivery_record_id ? sessionId : row.session_id, user_id: row.user_id ?? userId };
      const base = toIndexRow(projectedToNeonRow(canonicalRow, scope, userId), version);
      const indexRow: BufferedIndexRow = { ...base, fingerprint: BUFFERED_FINGERPRINT_PREFIX + base.fingerprint.replace(/^s3/, ""), origin: ORIGIN_BUFFERED, origin_claim_token: header.claim_token };
      const recordId = String(row.delivery_record_id ?? row.event_id);
      out.push({ row: indexRow, ordinals: byRecord.get(recordId) ?? [] });
    }
  });
  return { rows: out, reasons, version };
}

// ---------------------------------------------------------------- ClickHouse

/**
 * The origin columns exist with exactly the read gate's definitions. Without them an insert
 * would either fail or (on a server that drops unknown JSON fields) land rows as canonical.
 */
export async function assertOriginColumns(ch: ClickHouseClient): Promise<void> {
  const rows = await ch.select<{ name: string; type: string; default_expression: string }>(
    `SELECT name, type, default_expression FROM system.columns
     WHERE database = currentDatabase() AND table = {table:String} AND name IN ('origin', 'origin_claim_token')`,
    { params: { table: EVENTS_TABLE }, timeoutMs: 10_000 });
  for (const [name, want] of Object.entries(ORIGIN_COLUMNS)) {
    const got = rows.find((r) => r.name === name);
    if (!got || got.type !== want.type || got.default_expression !== want.default) throw new BufferedFeedError("origin_columns_missing");
  }
}

/**
 * Delete exactly the physical rows one claim wrote, never another writer's. Each key binds the
 * event identity AND the full buffered provenance the row was written with: origin 'buffered',
 * its exact `origin_claim_token`, its exact `fingerprint` and its exact `version`. A newer claim's
 * row for the same event (a different claim token, or a different fingerprint/version) and a
 * canonical row with the same identity are never matched, so a stale claim's cleanup cannot
 * remove a row a newer claim has already written and advanced the cursor past. Rows that do not
 * carry a buffered provenance are refused (no unbound delete is ever issued).
 */
export async function deleteBufferedRows(ch: ClickHouseClient, rows: readonly BufferedIndexRow[], maxKeys = 500): Promise<number> {
  const { createHash } = await import("node:crypto");
  for (const r of rows)
    if (r.origin !== ORIGIN_BUFFERED || !UUID.test(r.origin_claim_token) || !isBufferedOriginFingerprint(r.fingerprint) || r.fingerprint.length <= BUFFERED_FINGERPRINT_PREFIX.length || !Number.isSafeInteger(r.version) || r.version <= 0)
      throw new BufferedFeedError("delete_provenance_unbound");
  for (let i = 0; i < rows.length; i += maxKeys) {
    const literals = rows.slice(i, i + maxKeys).map((r) =>
      arrayLiteral([tuple(r.org_id, r.session_id, r.workspace_id, r.machine_id, r.source, r.kind,
        createHash("sha256").update(r.event_id).digest("hex"), tsMillis(`${r.ts.replace(" ", "T")}Z`),
        r.origin_claim_token, r.fingerprint, r.version)]).slice(1, -1));
    await ch.command(
      `DELETE FROM ${EVENTS_TABLE}
       WHERE (org_id, session_id, workspace_id, machine_id, source, kind, lower(hex(SHA256(event_id))), toUnixTimestamp64Milli(ts),
              origin_claim_token, fingerprint, version)
         IN [${literals.join(",")}] AND origin = '${ORIGIN_BUFFERED}'`,
      { settings: { lightweight_deletes_sync: 2, max_query_size: 1_048_576 }, timeoutMs: 60_000 },
    );
  }
  return rows.length;
}

// ---------------------------------------------------------------- feeder

export interface BufferedFeedDeps {
  source: BufferedFeedSource;
  store: ObjectStore;
  ch: ClickHouseClient;
  /** Deployment key root, e.g. `96bulls`. */
  keyRoot: string;
  decompress?: (bytes: Uint8Array) => Uint8Array;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  insertBatch?: number;
  /** Projected bytes held before a page writes what it has (rows.ts rowBytes). */
  flushBytes?: number;
}

const originChecked = new WeakMap<ClickHouseClient, Promise<void>>();
/** Once per client: a failed check is not cached, so a later invocation re-checks after the DDL. */
function originReady(ch: ClickHouseClient): Promise<void> {
  let held = originChecked.get(ch);
  if (!held) {
    held = assertOriginColumns(ch).catch((error) => { originChecked.delete(ch); throw error; });
    originChecked.set(ch, held);
  }
  return held;
}

const defaultDecompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));

interface Loaded { state: StageState; projection: StageProjection }

/** Check the stage's tenant on every axis before its bytes are projected. */
function assertStageTenant(state: StageState, decoded: DecodedBufferedStage, keyRoot: string) {
  const t = state.tenant!, h = decoded.header;
  if (h.claim_token !== state.claimToken) throw new BufferedFeedError("stage_claim_mismatch");
  const prefix = `${keyRoot}/${t.orgId}/buffered-batches/v1/ws=${layoutComponent(t.workspaceId)}/m=${liveMachineId(t.workspaceId, state.originId!, t.userId)}/`;
  if (h.org_id !== t.orgId || h.workspace_id !== t.workspaceId || h.user_id !== t.userId || h.origin_id !== state.originId || !state.stage!.key.startsWith(prefix))
    throw new BufferedFeedError("stage_tenant_mismatch");
}

/**
 * Read and project one live stage. A skip (with its code) when the object is gone or does not
 * match the ledger: never indexed, and never a reason to stall the cursor. Transient reads throw.
 */
async function loadLive(state: StageState, deps: BufferedFeedDeps, fromMs: number): Promise<Loaded | { skip: string }> {
  let decoded: DecodedBufferedStage;
  try {
    decoded = await readBufferedStage(deps.store, state.stage!, deps.decompress ?? defaultDecompress);
  } catch (error) {
    if (isObjectStoreError(error, "transient")) throw new BufferedFeedError("stage_read_transient", true);
    // A redaction barrier or a GC removal since the check reads as a mismatch or a missing object.
    const again = await deps.source.stage({ stageToken: state.stage!.stageToken });
    if (again.status !== "live") return { skip: "stage_not_live" };
    return { skip: error instanceof Error && /^buffered_[a-z_]+$/.test(error.message) ? error.message : isObjectStoreError(error, "not_found") ? "stage_object_missing" : "stage_read_failed" };
  }
  try {
    assertStageTenant(state, decoded, deps.keyRoot);
  } catch (error) {
    if (error instanceof BufferedFeedError) return { skip: error.code };
    throw error;
  }
  const projection = projectBufferedStage(decoded, new Set(state.retained), deps.keyRoot);
  // The canonical indexer's window: older rows are not indexed by either path.
  projection.rows = projection.rows.filter((r) => tsMillis(`${r.row.ts.replace(" ", "T")}Z`) >= fromMs);
  return { state, projection };
}

/** Insert, then re-check each stage and delete rows of records retention no longer keeps. */
async function writeAndRecheck(loaded: Loaded[], deps: BufferedFeedDeps): Promise<{ rows: number; deleted: number }> {
  await originReady(deps.ch);
  const batch = deps.insertBatch ?? 5_000;
  const all = loaded.flatMap((l) => l.projection.rows.map((r) => r.row));
  for (let i = 0; i < all.length; i += batch) await deps.ch.insert(EVENTS_TABLE, all.slice(i, i + batch), { timeoutMs: 60_000 });
  // Only this invocation's own rows (its claim token, fingerprint and version) are ever cleaned up.
  const stale: BufferedIndexRow[] = [];
  for (const l of loaded) {
    if (!l.projection.rows.length) continue;
    const now = await deps.source.stage({ stageToken: l.state.stage!.stageToken });
    const keep = new Set(now.retained);
    for (const r of l.projection.rows) if (!r.ordinals.some((o) => keep.has(o))) stale.push(r.row);
  }
  const deleted = stale.length ? await deleteBufferedRows(deps.ch, stale) : 0;
  return { rows: all.length, deleted };
}

export type KeepUpOutcome = { outcome: "indexed"; rows: number; deleted: number } | { outcome: "requeue" | "skipped"; reason: string };

/** KEEP-UP: one stage named by its object key (an S3 event on the pipeline queue, or a requeue). */
export async function feedBufferedStageByKey(key: string, deps: BufferedFeedDeps): Promise<KeepUpOutcome> {
  if (!isBufferedStageKey(key)) return { outcome: "skipped", reason: "not_a_buffered_stage" };
  const state = await deps.source.stage({ stageKey: key });
  // A preparing stage (bounded by its intent expiry) or a GC lease (minutes) resolves soon: look
  // again. A fenced privacy authority can last: the stage is left to the backfill/tail cursor,
  // which cannot pass an undecided stage.
  if (state.status === "pending" || (state.status === "deferred" && state.reason === "gc_in_flight")) return { outcome: "requeue", reason: state.reason ?? state.status };
  if (state.status !== "live") return { outcome: "skipped", reason: state.reason ?? state.status };
  const fromMs = Date.parse(await windowStart(deps.ch, new Date((deps.now ?? Date.now)())));
  const loaded = await loadLive(state, deps, fromMs);
  if ("skip" in loaded) return { outcome: "skipped", reason: loaded.skip };
  const written = await writeAndRecheck([loaded], deps);
  return { outcome: "indexed", ...written };
}

export interface BackfillSummary {
  pages: number;
  stages: number;
  indexed: number;
  skipped: number;
  rows: number;
  deleted: number;
  /** The cursor reached the newest accepted stage. */
  caughtUp: boolean;
  /** A stage could not be decided yet (GC in flight, privacy fenced): the cursor waits before it. */
  blocked?: string;
  reasons: Record<string, number>;
}

/**
 * BACKFILL and tail: bounded, resumable pages over a named cursor. The cursor advances only
 * past stages that were indexed (and re-checked) or are terminally not indexable, and only after
 * the ClickHouse writes, so a crash replays at most one page (idempotent).
 */
export async function runBufferedFeed(deps: BufferedFeedDeps, config: Pick<BufferedFeedConfig, "pageSize" | "stagesPerSecond" | "since" | "cursor" | "budgetMs">): Promise<BackfillSummary> {
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const deadline = now() + config.budgetMs;
  const interval = 1_000 / config.stagesPerSecond;
  const summary: BackfillSummary = { pages: 0, stages: 0, indexed: 0, skipped: 0, rows: 0, deleted: 0, caughtUp: false, reasons: {} };
  const count = (reason: string) => (summary.reasons[reason] = (summary.reasons[reason] ?? 0) + 1);
  const fromMs = Date.parse(await windowStart(deps.ch, new Date(now())));
  let next = now();
  while (now() < deadline) {
    const page = await deps.source.page(config.cursor, config.since, config.pageSize);
    summary.pages += 1;
    let loaded: Loaded[] = [];
    let held = 0;
    const flush = async () => {
      if (!loaded.length) return;
      const written = await writeAndRecheck(loaded, deps);
      summary.rows += written.rows;
      summary.deleted += written.deleted;
      loaded = [];
      held = 0;
    };
    let last: StageCandidate | null = null;
    let indexed = 0, skipped = 0;
    for (const candidate of page.stages) {
      if (now() >= deadline) break;
      const wait = next - now();
      if (wait > 0) await sleep(wait);
      next = Math.max(next, now()) + interval;
      const state = await deps.source.stage({ stageToken: candidate.stageToken });
      summary.stages += 1;
      if (state.status === "deferred" || state.status === "pending") { summary.blocked = state.reason ?? state.status; break; }
      if (state.status === "live") {
        const got = await loadLive(state, deps, fromMs);
        if ("skip" in got) { count(got.skip); skipped += 1; }
        else {
          for (const reason of got.projection.reasons) count(reason);
          loaded.push(got);
          indexed += 1;
          // Bound memory: a page of large stages writes as it goes (the cursor still moves once, after).
          held += got.projection.rows.reduce((n, r) => n + rowBytes(r.row), 0);
          if (held >= (deps.flushBytes ?? 64 * 1024 * 1024)) await flush();
        }
      } else { count(state.reason ?? state.status); skipped += 1; }
      last = candidate;
    }
    await flush();
    if (last) await deps.source.advance(config.cursor, { createdAt: last.createdAt, stageToken: last.stageToken }, { indexed, skipped });
    summary.indexed += indexed;
    summary.skipped += skipped;
    if (summary.blocked) break;
    if (page.stages.length < config.pageSize && (page.stages.length === 0 || last === page.stages.at(-1))) { summary.caughtUp = true; break; }
  }
  return summary;
}
