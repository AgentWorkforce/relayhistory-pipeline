/** Actual bounded READ ONLY legacy-source capture and frontier composition. */
import type {
  CanonicalFrontierCapture,
  FrontierAuthority,
  FrontierOriginCapture,
  FrontierRecordMaximum,
  FrontierScanEvidence,
  FrontierScanKind,
  FrontierTarget,
  FrontierTenant,
} from "./frontier-plan.ts";
import {
  FRONTIER_CAPTURE_PROTOCOL,
  frontierCaptureId,
  frontierCanonicalJson,
  frontierScanItemsSha256,
  frontierSha256,
} from "./frontier-plan.ts";
import type { SeedScopeExportEvidence } from "./frontier-plan-source.ts";

const HEX64 = /^[a-f0-9]{64}$/;
const IDENT = /^[a-z_][a-z0-9_]{0,62}$/;
const CONTROL = /[\u0000-\u001f\u007f]/u;

export class FrontierCaptureSourceError extends Error {
  constructor(readonly code: string) { super(code); }
}
function fail(code: string): never { throw new FrontierCaptureSourceError(code); }

export interface FrontierCaptureSql {
  query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export interface LegacySourceCaptureOptions {
  schema?: string;
  pageLimit?: number;
  maxPages?: number;
  maxOrigins?: number;
  maxBytes?: number;
  statementTimeoutMs?: number;
  lockTimeoutMs?: number;
  now?: () => Date;
}

export interface LegacySourceRecord {
  tenant: FrontierTenant;
  recordId: string;
  revision: string;
  revisionId: string;
  digest: string;
  kind: string;
  source: string;
  sessionId: string | null;
  operation: "upsert" | "delete";
  projectionAcceptedSeq: string | null;
  catalogAcceptedSeq: string | null;
}

export interface LegacyPrivacyRow {
  kind: "deletion" | "reset";
  key: string[];
  value: string | null;
}

export interface LegacyOriginSourceCapture {
  tenant: FrontierTenant;
  opening: FrontierAuthority;
  closing: FrontierAuthority;
  records: LegacySourceRecord[];
  privacyRows: LegacyPrivacyRow[];
  recordScan: FrontierScanEvidence;
  privacyScan: FrontierScanEvidence;
}

export interface LegacyFrontierSourceCapture {
  startedAt: string;
  capturedAt: string;
  scope: { orgId: string; workspaceId: string };
  expectedOrigins: Array<{ userId: string; originId: string }>;
  inventoryScan: FrontierScanEvidence;
  origins: LegacyOriginSourceCapture[];
}

function mergeClosureScan(opening: FrontierScanEvidence, closing: FrontierScanEvidence, openingStateSha256: string, closingStateSha256: string): FrontierScanEvidence {
  if (opening.kind !== closing.kind || opening.adapter !== closing.adapter || opening.querySha256 !== closing.querySha256 ||
      opening.pageLimit !== closing.pageLimit || opening.maxPages !== closing.maxPages || frontierCanonicalJson(opening.pageItemCounts) !== frontierCanonicalJson(closing.pageItemCounts) ||
      opening.itemCount !== closing.itemCount || opening.itemsSha256 !== closing.itemsSha256 || opening.complete !== true || closing.complete !== true ||
      opening.terminalCursor !== null || closing.terminalCursor !== null) fail("frontier_capture_source_changed");
  return {
    ...opening,
    finishedAt: closing.finishedAt,
    byteCount: opening.byteCount + closing.byteCount,
    snapshotSha256: frontierSha256({ opening: opening.snapshotSha256, closing: closing.snapshotSha256 }),
    pageChainSha256: frontierSha256({ opening: opening.pageChainSha256, closing: closing.pageChainSha256 }),
    openingStateSha256,
    closingStateSha256,
  };
}

/**
 * Binds two complete source scans around slower immutable-object proof work.
 * The returned evidence spans both scans and refuses any intervening content,
 * privacy, schema, function, role, or authority change.
 */
export function reconcileLegacyFrontierClosure(opening: LegacyFrontierSourceCapture, closing: LegacyFrontierSourceCapture): LegacyFrontierSourceCapture {
  if (frontierCanonicalJson(opening.scope) !== frontierCanonicalJson(closing.scope) ||
      frontierCanonicalJson(opening.expectedOrigins) !== frontierCanonicalJson(closing.expectedOrigins) || opening.origins.length !== closing.origins.length) fail("frontier_capture_source_changed");
  const origins = opening.origins.map((first, index): LegacyOriginSourceCapture => {
    const second = closing.origins[index]!;
    if (frontierCanonicalJson(first.tenant) !== frontierCanonicalJson(second.tenant) ||
        frontierCanonicalJson(first.records) !== frontierCanonicalJson(second.records) || frontierCanonicalJson(first.privacyRows) !== frontierCanonicalJson(second.privacyRows) ||
        frontierCanonicalJson(first.opening) !== frontierCanonicalJson(first.closing) || frontierCanonicalJson(first.closing) !== frontierCanonicalJson(second.opening) ||
        frontierCanonicalJson(second.opening) !== frontierCanonicalJson(second.closing)) fail("frontier_capture_source_changed");
    return {
      ...first,
      closing: second.closing,
      recordScan: mergeClosureScan(first.recordScan, second.recordScan, frontierSha256(first.opening), frontierSha256(second.closing)),
      privacyScan: mergeClosureScan(first.privacyScan, second.privacyScan, frontierSha256(first.opening), frontierSha256(second.closing)),
    };
  });
  const inventoryOpening = frontierSha256(origins.map(item => ({ tenant: item.tenant, authority: item.opening })));
  const inventoryClosing = frontierSha256(origins.map(item => ({ tenant: item.tenant, authority: item.closing })));
  return {
    ...opening,
    capturedAt: closing.capturedAt,
    inventoryScan: mergeClosureScan(opening.inventoryScan, closing.inventoryScan, inventoryOpening, inventoryClosing),
    origins,
  };
}

interface PageTrace {
  counts: number[];
  hashes: string[];
  bytes: number;
}
interface CaptureBudget { used: number; max: number; }

const ORIGIN_QUERY = (s: string) => `SELECT user_id,origin_id,count(*) OVER(PARTITION BY origin_id) AS owner_count FROM(
 SELECT user_id,origin_id FROM ${s}.delivery_records WHERE org_id=$1 AND workspace_id=$2
 UNION SELECT user_id,origin_id FROM ${s}.deleted_upload_sessions WHERE org_id=$1 AND workspace_id=$2
)q WHERE ($3::text IS NULL OR (user_id COLLATE "C",origin_id COLLATE "C")>($3 COLLATE "C",$4 COLLATE "C"))
ORDER BY user_id COLLATE "C",origin_id COLLATE "C" LIMIT $5`;

const RECORD_QUERY = (s: string) => `SELECT r.record_id,r.revision::text AS revision,r.revision_id,r.digest,r.kind,r.source,r.session_id,r.operation,
 count(*) OVER(PARTITION BY r.record_id) AS identity_count,
 p.accepted_seq::text AS projection_accepted_seq,c.accepted_seq::text AS catalog_accepted_seq
FROM ${s}.delivery_records r
LEFT JOIN ${s}.delivery_projection_acceptance p ON p.org_id=r.org_id AND p.workspace_id=r.workspace_id AND p.origin_id=r.origin_id AND p.record_id=r.record_id
LEFT JOIN ${s}.delivery_catalog_acceptance c ON c.org_id=r.org_id AND c.workspace_id=r.workspace_id AND c.origin_id=r.origin_id AND c.record_id=r.record_id
WHERE r.org_id=$1 AND r.workspace_id=$2 AND r.user_id=$3 AND r.origin_id=$4
 AND($5::text IS NULL OR r.record_id COLLATE "C">$5 COLLATE "C")
ORDER BY r.record_id COLLATE "C" LIMIT $6`;

const PRIVACY_QUERY = (s: string) => `SELECT kind,k1,k2,k3,value,count(*) OVER(PARTITION BY kind,k1,k2,k3) AS key_count FROM(
 SELECT 'deletion'::text kind,origin_id k1,source k2,coalesce(session_id,'') k3,NULL::text value FROM ${s}.deleted_upload_sessions
  WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
 UNION ALL
 SELECT 'reset'::text kind,request_id k1,''::text k2,''::text k3,deleted::text value FROM ${s}.session_resets
  WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3
)q WHERE($4::text IS NULL OR(kind COLLATE "C",k1 COLLATE "C",k2 COLLATE "C",k3 COLLATE "C")>
 ($4 COLLATE "C",$5 COLLATE "C",$6 COLLATE "C",$7 COLLATE "C"))
ORDER BY kind COLLATE "C",k1 COLLATE "C",k2 COLLATE "C",k3 COLLATE "C" LIMIT $8`;

const AUTHORITY_QUERY = (s: string) => `SELECT ${s}.delivery_cutover_status() AS status,
 jsonb_build_object('database',current_database(),'databaseOid',(SELECT oid::text FROM pg_database WHERE datname=current_database()),
 'serverVersion',current_setting('server_version_num'),'readOnly',current_setting('transaction_read_only'),
 'role',(SELECT jsonb_build_object('name',rolname,'superuser',rolsuper,'inherit',rolinherit,'createRole',rolcreaterole,'createDb',rolcreatedb,'replication',rolreplication,'bypassRls',rolbypassrls) FROM pg_roles WHERE rolname=current_user),
 'collation',(SELECT jsonb_build_object('collate',datcollate,'ctype',datctype,'provider',datlocprovider::text,'locale',coalesce(to_jsonb(d)->>'datlocale','')) FROM pg_database d WHERE datname=current_database()),
 'relations',(SELECT jsonb_agg(jsonb_build_object('name',c.relname,'oid',c.oid::text,'owner',pg_get_userbyid(c.relowner),'kind',c.relkind::text,'persistence',c.relpersistence::text,'rls',c.relrowsecurity,'replicaIdentity',c.relreplident::text) ORDER BY c.relname)
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$3 AND c.relname=ANY($4::text[]) AND c.relkind IN('r','p')),
 'sequences',(SELECT jsonb_object_agg(c.relname,c.oid::text ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname=$3 AND c.relname=ANY($5::text[]) AND c.relkind='S'),
 'functions',(SELECT jsonb_agg(jsonb_build_object('identity',n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')','owner',pg_get_userbyid(p.proowner),'securityDefiner',p.prosecdef,'definitionSha256',encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) ORDER BY p.proname,pg_get_function_identity_arguments(p.oid))
   FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$3 AND p.proname=ANY($6::text[])),
 'controlHeads',(SELECT coalesce(jsonb_agg(jsonb_build_object('userId',user_id,'epoch',epoch,'version',version::text,'digest',digest,'pending',pending)
   ORDER BY user_id COLLATE "C"),'[]'::jsonb) FROM ${s}.delivery_cutover_control_heads WHERE org_id=$1 AND workspace_id=$2)) AS facts,
 txid_current_snapshot()::text AS snapshot`;

const ORPHAN_QUERY = (s: string) => `SELECT key FROM(
 SELECT 'origin:'||o.origin_id AS key FROM ${s}.delivery_origins o
 WHERE o.org_id=$1 AND o.workspace_id=$2 AND NOT EXISTS(SELECT 1 FROM ${s}.delivery_records r
  WHERE r.org_id=o.org_id AND r.workspace_id=o.workspace_id AND r.origin_id=o.origin_id)
 AND NOT EXISTS(SELECT 1 FROM ${s}.deleted_upload_sessions d
  WHERE d.org_id=o.org_id AND d.workspace_id=o.workspace_id AND d.origin_id=o.origin_id)
 UNION ALL
 SELECT 'reset-user:'||x.user_id FROM ${s}.session_resets x
 WHERE x.org_id=$1 AND x.workspace_id=$2 AND NOT EXISTS(SELECT 1 FROM ${s}.delivery_records r
  WHERE r.org_id=x.org_id AND r.workspace_id=x.workspace_id AND r.user_id=x.user_id)
 AND NOT EXISTS(SELECT 1 FROM ${s}.deleted_upload_sessions d
  WHERE d.org_id=x.org_id AND d.workspace_id=x.workspace_id AND d.user_id=x.user_id)
 UNION ALL
 SELECT 'control-user:'||h.user_id FROM ${s}.delivery_cutover_control_heads h
 WHERE h.org_id=$1 AND h.workspace_id=$2 AND NOT EXISTS(SELECT 1 FROM ${s}.delivery_records r
  WHERE r.org_id=h.org_id AND r.workspace_id=h.workspace_id AND r.user_id=h.user_id)
 AND NOT EXISTS(SELECT 1 FROM ${s}.deleted_upload_sessions d
  WHERE d.org_id=h.org_id AND d.workspace_id=h.workspace_id AND d.user_id=h.user_id)
)q ORDER BY key COLLATE "C" LIMIT 1`;

const CONTROL_QUERY = (s: string) => `SELECT epoch,version::text AS version,digest,pending
FROM ${s}.delivery_cutover_control_heads WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3`;

const RELATIONS = ["delivery_records", "delivery_origins", "delivery_projection_acceptance", "delivery_catalog_acceptance", "deleted_upload_sessions", "session_resets", "delivery_cutover_state", "delivery_cutover_control_heads"];
const SEQUENCES = ["delivery_projection_acceptance_seq", "delivery_catalog_acceptance_seq"];
const FUNCTIONS = ["delivery_cutover_status", "snapshot_delivery_cutover_privacy_v1", "guard_delivery_cutover_v1", "record_delivery_projection_acceptance", "project_delivery_catalog"];

function validId(value: unknown, empty = false): value is string {
  return typeof value === "string" && (empty || value.length > 0) && Buffer.byteLength(value) <= 512 && !CONTROL.test(value);
}
function validDecimal(value: unknown, zero = false): value is string {
  return typeof value === "string" && new RegExp(zero ? "^(0|[1-9][0-9]{0,18})$" : "^[1-9][0-9]{0,18}$").test(value);
}
function iso(date: Date): string {
  if (!Number.isFinite(date.getTime())) fail("frontier_capture_clock");
  return date.toISOString();
}
function tracePage(trace: PageTrace, rows: unknown[], budget: CaptureBudget): void {
  const text = frontierCanonicalJson(rows);
  const bytes = Buffer.byteLength(text);
  if (bytes > budget.max - budget.used) fail("frontier_capture_byte_bound");
  budget.used += bytes;
  trace.counts.push(rows.length);
  trace.hashes.push(frontierSha256(rows));
  trace.bytes += bytes;
}
function scan(kind: FrontierScanKind, adapter: string, query: string, snapshot: string, startedAt: string, finishedAt: string, pageLimit: number, maxPages: number, trace: PageTrace, items: unknown, opening: FrontierAuthority, closing: FrontierAuthority): FrontierScanEvidence {
  return {
    kind, adapter, querySha256: frontierSha256(query), snapshotSha256: frontierSha256(snapshot), startedAt, finishedAt,
    pageLimit, maxPages, pagesRead: trace.counts.length, pageItemCounts: trace.counts, itemCount: trace.counts.reduce((a, b) => a + b, 0),
    byteCount: trace.bytes, checkpointCount: trace.counts.filter(count => count > 0).length,
    pageChainSha256: frontierSha256(trace.hashes), itemsSha256: frontierSha256(items),
    openingStateSha256: frontierSha256(opening), closingStateSha256: frontierSha256(closing), terminalCursor: null, complete: true,
  };
}

interface AuthorityRead { authority: FrontierAuthority; snapshot: string; raw: unknown; controlVersions: Map<string, string>; }
async function readAuthority(db: FrontierCaptureSql, schema: string, orgId: string, workspaceId: string): Promise<AuthorityRead> {
  const rows = (await db.query<{ status: Record<string, unknown>; facts: Record<string, unknown>; snapshot: string }>(AUTHORITY_QUERY(schema), [orgId, workspaceId, schema, RELATIONS, SEQUENCES, FUNCTIONS])).rows;
  if (rows.length !== 1) fail("frontier_capture_authority");
  const row = rows[0]!;
  if (row.status?.phase !== "active" || !HEX64.test(String(row.status.epoch ?? "")) || !HEX64.test(String(row.status.privacyBaselineSha256 ?? "")) || row.facts?.readOnly !== "on" || !validId(row.snapshot)) fail("frontier_capture_authority");
  if (!Array.isArray(row.facts.relations) || row.facts.relations.length !== RELATIONS.length || !row.facts.sequences || typeof row.facts.sequences !== "object" || Object.keys(row.facts.sequences).length !== SEQUENCES.length || !Array.isArray(row.facts.functions) || row.facts.functions.length !== FUNCTIONS.length || !Array.isArray(row.facts.controlHeads)) fail("frontier_capture_authority");
  const role = row.facts.role;
  if (!role || typeof role !== "object" || !validId((role as Record<string, unknown>).name) || ["superuser", "inherit", "createRole", "createDb", "replication", "bypassRls"].some(key => (role as Record<string, unknown>)[key] !== false)) fail("frontier_capture_source_role");
  const controlVersions = new Map<string, string>();
  for (const value of row.facts.controlHeads) {
    if (!value || typeof value !== "object") fail("frontier_capture_control");
    const head = value as Record<string, unknown>;
    if (!validId(head.userId) || String(head.epoch) !== String(row.status.epoch) || !validDecimal(head.version) || !HEX64.test(String(head.digest ?? "")) || head.pending !== false || controlVersions.has(head.userId as string)) fail("frontier_capture_control");
    controlVersions.set(head.userId as string, head.version as string);
  }
  const raw = { status: row.status, facts: row.facts };
  const sourceIdentitySha256 = frontierSha256(raw);
  return { raw, snapshot: row.snapshot, controlVersions, authority: {
    sourceIdentitySha256, sourceGenerationSha256: frontierSha256({ relations: row.facts.relations, sequences: row.facts.sequences, functions: row.facts.functions }),
    sourceHead: sourceIdentitySha256.slice(0, 40), controlEpoch: String(row.status.epoch), privacyBaselineSha256: String(row.status.privacyBaselineSha256), privacyVersion: null,
  } };
}

async function beginReadOnly(db: FrontierCaptureSql, statementMs: number, lockMs: number): Promise<void> {
  await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await db.query(`SET LOCAL statement_timeout = ${statementMs}; SET LOCAL lock_timeout = ${lockMs}; SET LOCAL TimeZone = 'UTC'`);
    const row = (await db.query<{ ro: string; tz: string }>("SELECT current_setting('transaction_read_only') AS ro,current_setting('TimeZone') AS tz")).rows[0];
    if (row?.ro !== "on" || row.tz !== "UTC") fail("frontier_capture_not_readonly");
  } catch (error) { await db.query("ROLLBACK").catch(() => undefined); throw error; }
}

/** Fixed-query, complete legacy metadata capture. It never selects payload. */
export async function captureLegacyFrontierSource(db: FrontierCaptureSql, scope: { orgId: string; workspaceId: string }, options: LegacySourceCaptureOptions = {}): Promise<LegacyFrontierSourceCapture> {
  const schema = options.schema ?? "sessions", pageLimit = options.pageLimit ?? 500, maxPages = options.maxPages ?? 10_000;
  const maxOrigins = options.maxOrigins ?? 100_000, maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
  const statementMs = options.statementTimeoutMs ?? 20_000, lockMs = options.lockTimeoutMs ?? 2_000, now = options.now ?? (() => new Date());
  if (!IDENT.test(schema) || !validId(scope.orgId) || !validId(scope.workspaceId, true) || !Number.isSafeInteger(pageLimit) || pageLimit < 1 || pageLimit > 10_000 || !Number.isSafeInteger(maxPages) || maxPages < 1 || maxPages > 1_000_000 || !Number.isSafeInteger(maxOrigins) || maxOrigins < 1 || maxOrigins > 100_000 || !Number.isSafeInteger(maxBytes) || maxBytes < 1024 || maxBytes > 1024 * 1024 * 1024 || !Number.isSafeInteger(statementMs) || statementMs < 1 || statementMs > 20_000 || !Number.isSafeInteger(lockMs) || lockMs < 1 || lockMs > 5_000) fail("frontier_capture_input");
  const startedAt = iso(now());
  await beginReadOnly(db, statementMs, lockMs);
  let opening: AuthorityRead;
  const originTrace: PageTrace = { counts: [], hashes: [], bytes: 0 };
  const origins: Array<{ userId: string; originId: string }> = [];
  const originCaptures: Array<Omit<LegacyOriginSourceCapture, "closing">> = [];
  const budget: CaptureBudget = { used: 0, max: maxBytes };
  try {
    opening = await readAuthority(db, schema, scope.orgId, scope.workspaceId);
    if ((await db.query(ORPHAN_QUERY(schema), [scope.orgId, scope.workspaceId])).rows.length) fail("frontier_capture_origin_owner_unknown");
    let after: [string, string] | null = null;
    for (;;) {
      if (originTrace.counts.length >= maxPages) fail("frontier_capture_page_bound");
      const rows: Array<{ user_id: string; origin_id: string; owner_count: string }> = (await db.query<{ user_id: string; origin_id: string; owner_count: string }>(ORIGIN_QUERY(schema), [scope.orgId, scope.workspaceId, after?.[0] ?? null, after?.[1] ?? null, pageLimit])).rows;
      tracePage(originTrace, rows, budget);
      if (!rows.length) break;
      for (const row of rows) {
        if (!validId(row.user_id) || !validId(row.origin_id) || row.owner_count !== "1") fail("frontier_capture_origin_shape");
        origins.push({ userId: row.user_id, originId: row.origin_id });
        if (origins.length > maxOrigins) fail("frontier_capture_origin_bound");
      }
      const last = rows.at(-1)!; after = [last.user_id, last.origin_id];
    }
    if (new Set(origins.map(frontierCanonicalJson)).size !== origins.length) fail("frontier_capture_origin_duplicate");
    if (origins.length === 0) fail("frontier_capture_origin_empty");

    for (const identity of origins) {
      const tenant = { ...scope, ...identity };
      const controlRows = (await db.query<{ epoch: string; version: string; digest: string; pending: boolean }>(CONTROL_QUERY(schema), [scope.orgId, scope.workspaceId, identity.userId])).rows;
      if (controlRows.length > 1 || controlRows.some(row => row.pending || row.epoch !== opening.authority.controlEpoch || !validDecimal(row.version) || !HEX64.test(row.digest))) fail("frontier_capture_control");
      const privacyVersion = controlRows[0]?.version ?? "0";
      if ((opening.controlVersions.get(identity.userId) ?? "0") !== privacyVersion) fail("frontier_capture_control");
      const authority: FrontierAuthority = { ...opening.authority, privacyVersion };
      const records: LegacySourceRecord[] = [], recordTrace: PageTrace = { counts: [], hashes: [], bytes: 0 };
      let recordAfter: string | null = null;
      for (;;) {
        if (recordTrace.counts.length >= maxPages) fail("frontier_capture_page_bound");
        const rows = (await db.query<Record<string, unknown>>(RECORD_QUERY(schema), [scope.orgId, scope.workspaceId, identity.userId, identity.originId, recordAfter, pageLimit])).rows;
        tracePage(recordTrace, rows, budget);
        if (!rows.length) break;
        for (const row of rows) {
          if (row.identity_count !== "1" || !validId(row.record_id) || !validDecimal(row.revision) || !validId(row.revision_id) || !HEX64.test(String(row.digest ?? "")) || !validId(row.kind) || !validId(row.source) || !(row.session_id === null || validId(row.session_id)) || !["upsert", "delete"].includes(String(row.operation)) || !(row.projection_accepted_seq === null || validDecimal(row.projection_accepted_seq, true)) || !(row.catalog_accepted_seq === null || validDecimal(row.catalog_accepted_seq, true))) fail("frontier_capture_record_shape");
          records.push({ tenant, recordId: row.record_id as string, revision: row.revision as string, revisionId: row.revision_id as string, digest: row.digest as string, kind: row.kind as string, source: row.source as string, sessionId: row.session_id as string | null, operation: row.operation as "upsert" | "delete", projectionAcceptedSeq: row.projection_accepted_seq as string | null, catalogAcceptedSeq: row.catalog_accepted_seq as string | null });
        }
        recordAfter = rows.at(-1)!.record_id as string;
      }
      const privacyRows: LegacyPrivacyRow[] = [], privacyTrace: PageTrace = { counts: [], hashes: [], bytes: 0 };
      let privacyAfter: [string, string, string, string] | null = null;
      for (;;) {
        if (privacyTrace.counts.length >= maxPages) fail("frontier_capture_page_bound");
        const rows: Array<{ kind: string; k1: string; k2: string; k3: string; value: string | null; key_count: string }> = (await db.query<{ kind: string; k1: string; k2: string; k3: string; value: string | null; key_count: string }>(PRIVACY_QUERY(schema), [scope.orgId, scope.workspaceId, identity.userId, privacyAfter?.[0] ?? null, privacyAfter?.[1] ?? null, privacyAfter?.[2] ?? null, privacyAfter?.[3] ?? null, pageLimit])).rows;
        tracePage(privacyTrace, rows, budget);
        if (!rows.length) break;
        for (const row of rows) {
          if (row.key_count !== "1" || !["deletion", "reset"].includes(row.kind) || ![row.k1, row.k2, row.k3].every(value => validId(value, true)) || !(row.value === null || validDecimal(row.value, true))) fail("frontier_capture_privacy_shape");
          privacyRows.push({ kind: row.kind as "deletion" | "reset", key: [row.k1, row.k2, row.k3], value: row.value });
        }
        const last = rows.at(-1)!; privacyAfter = [last.kind, last.k1, last.k2, last.k3];
      }
      const finished = iso(now());
      originCaptures.push({ tenant, opening: authority, records, privacyRows,
        recordScan: scan("record-maxima", "legacy-record-maxima-v1", RECORD_QUERY(schema), opening.snapshot, startedAt, finished, pageLimit, maxPages, recordTrace, records, authority, authority),
        privacyScan: scan("privacy-controls", "ordered-privacy-controls-v1", PRIVACY_QUERY(schema), opening.snapshot, startedAt, finished, pageLimit, maxPages, privacyTrace, privacyRows, authority, authority) });
    }
    await db.query("COMMIT");
  } catch (error) { await db.query("ROLLBACK").catch(() => undefined); throw error; }

  await beginReadOnly(db, statementMs, lockMs);
  let closing: AuthorityRead;
  try { closing = await readAuthority(db, schema, scope.orgId, scope.workspaceId); await db.query("COMMIT"); }
  catch (error) { await db.query("ROLLBACK").catch(() => undefined); throw error; }
  const capturedAt = iso(now());
  const closedOrigins = originCaptures.map(origin => ({ ...origin, closing: { ...closing.authority, privacyVersion: closing.controlVersions.get(origin.tenant.userId) ?? "0" } }));
  const inventoryOpening = frontierSha256(closedOrigins.map(item => ({ tenant: item.tenant, authority: item.opening })));
  const inventoryClosing = frontierSha256(closedOrigins.map(item => ({ tenant: item.tenant, authority: item.closing })));
  return { startedAt, capturedAt, scope, expectedOrigins: origins,
    inventoryScan: { ...scan("origin-inventory", "legacy-origin-inventory-v1", ORIGIN_QUERY(schema), opening!.snapshot, startedAt, capturedAt, pageLimit, maxPages, originTrace, origins, opening!.authority, closing.authority), openingStateSha256: inventoryOpening, closingStateSha256: inventoryClosing },
    origins: closedOrigins };
}

function closureCounts(items: number, pageLimit = 1000): number[] {
  const out: number[] = [];
  let left = items;
  while (left > 0) { const count = Math.min(left, pageLimit); out.push(count); left -= count; }
  out.push(0); return out;
}

/** Composes source rows only when every one has exact verified immutable-root evidence. */
export function composeCanonicalFrontierCapture(source: LegacyFrontierSourceCapture, target: FrontierTarget, exports: SeedScopeExportEvidence[]): CanonicalFrontierCapture {
  const byTenant = new Map<string, SeedScopeExportEvidence[]>();
  for (const evidence of exports) for (const root of evidence.roots) {
    const key = frontierCanonicalJson(root.tenant);
    const list = byTenant.get(key) ?? [];
    if (!list.includes(evidence)) list.push(evidence);
    byTenant.set(key, list);
  }
  const consumed = new Set<SeedScopeExportEvidence>();
  const origins: FrontierOriginCapture[] = source.origins.map(origin => {
    const evidence = byTenant.get(frontierCanonicalJson(origin.tenant)) ?? [];
    for (const item of evidence) consumed.add(item);
    const recordEvidence = evidence.flatMap(item => item.records);
    const roots = evidence.flatMap(item => item.roots).filter(root => frontierCanonicalJson(root.tenant) === frontierCanonicalJson(origin.tenant));
    const maxima: FrontierRecordMaximum[] = origin.records.map(record => {
      const matches = recordEvidence.filter(item => item.evidence.org_id === record.tenant.orgId && item.evidence.workspace_id === record.tenant.workspaceId && item.evidence.user_id === record.tenant.userId && item.evidence.origin_id === record.tenant.originId && item.evidence.record_id === record.recordId && item.evidence.revision === record.revision && item.evidence.kind === record.kind && item.evidence.source === record.source && item.evidence.session_id === record.sessionId && item.evidence.operation === record.operation);
      if (matches.length !== 1) fail("frontier_capture_export_lineage");
      return { tenant: record.tenant, recordId: record.recordId, revision: record.revision, revisionId: record.revisionId, digest: record.digest,
        source: record.source, sessionId: record.sessionId, operation: record.operation, authority: "seed-proof", exportRootSha256: matches[0]!.exportRootSha256 };
    });
    if (recordEvidence.length !== maxima.length || roots.reduce((n, root) => n + root.recordCount, 0) !== maxima.length) fail("frontier_capture_export_coverage");
    const exportCounts = closureCounts(roots.length);
    const make = (kind: FrontierScanKind, adapter: string, counts: number[], itemsSha256: string, snapshot: unknown): FrontierScanEvidence => ({
      kind, adapter, querySha256: frontierSha256("captureSeedScopeExportEvidence-v1"),
      snapshotSha256: frontierSha256(snapshot), startedAt: source.startedAt, finishedAt: source.capturedAt, pageLimit: 1000, maxPages: 1_000_000,
      pagesRead: counts.length, pageItemCounts: counts, itemCount: counts.reduce((a, b) => a + b, 0), byteCount: Buffer.byteLength(frontierCanonicalJson(snapshot)),
      checkpointCount: counts.filter(count => count > 0).length, pageChainSha256: frontierSha256(counts), itemsSha256,
      openingStateSha256: frontierSha256(origin.opening), closingStateSha256: frontierSha256(origin.closing), terminalCursor: null, complete: true,
    });
    const composed: FrontierOriginCapture = { tenant: origin.tenant, opening: origin.opening, closing: origin.closing, sourceRecordCount: maxima.length,
      privacyRowCount: origin.privacyRows.length, scans: [], exportRoots: roots, recordMaxima: maxima, receiptDecisions: [] };
    composed.scans = [
      { ...origin.recordScan, itemsSha256: frontierScanItemsSha256("record-maxima", composed), openingStateSha256: frontierSha256(origin.opening), closingStateSha256: frontierSha256(origin.closing) },
      make("export-roots", "ingest-seed-s3-export-roots-v1", exportCounts, frontierScanItemsSha256("export-roots", composed), evidence.map(item => ({
        snapshotSha256: item.snapshotSha256, sourceEpoch: item.sourceEpoch, scopeFingerprint: item.scopeFingerprint,
        pointerCount: item.pointerCount, recordCount: item.recordCount,
        versioningOpeningSha256: item.versioningOpeningSha256, versioningClosingSha256: item.versioningClosingSha256,
      }))),
      { ...origin.privacyScan, itemsSha256: frontierScanItemsSha256("privacy-controls", composed), openingStateSha256: frontierSha256(origin.opening), closingStateSha256: frontierSha256(origin.closing) },
    ];
    return composed;
  });
  if (consumed.size !== exports.length) fail("frontier_capture_export_coverage");
  const capture: CanonicalFrontierCapture = { protocol: FRONTIER_CAPTURE_PROTOCOL, captureId: "0".repeat(64), startedAt: source.startedAt, capturedAt: source.capturedAt,
    scope: source.scope, target, expectedOrigins: source.expectedOrigins, inventoryScan: source.inventoryScan, origins };
  capture.inventoryScan.itemsSha256 = frontierScanItemsSha256("origin-inventory", capture);
  capture.captureId = frontierCaptureId(capture);
  return capture;
}
