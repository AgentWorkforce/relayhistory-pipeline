/**
 * Bounded client for the proposed owner-installed receipt capture capability.
 * This is NOT an installer. The capability and its reviewed function hash must
 * exist independently; there is no table-read or owner-credential fallback.
 */
import { createHash } from "node:crypto";
import { frontierUtf8TupleCompare } from "./frontier-order.ts";
import type { FrontierAuthority, FrontierReceiptDecision, FrontierScanEvidence, FrontierTenant } from "./frontier-plan.ts";
import { frontierCanonicalJson, frontierSha256, frontierReceiptDecisionsSha256 } from "./frontier-plan.ts";
import type { FrontierCaptureSql } from "./frontier-capture-source.ts";

export const RECEIPT_CAPTURE_PROTOCOL = "first-ack-seat-capture-v1";
export const RECEIPT_CAPTURE_PAGE_SQL = "SELECT sessions.ingest_scoped_publication_receipt_capture_v1($1::jsonb) AS capture";
export const RECEIPT_CAPTURE_IDENTITY_SQL = `SELECT current_setting('transaction_read_only') AS read_only,
 current_setting('transaction_isolation') AS isolation,r.rolname AS role,
 r.rolsuper AS superuser,r.rolinherit AS inherit,r.rolcreaterole AS create_role,
 r.rolcreatedb AS create_db,r.rolreplication AS replication,r.rolbypassrls AS bypass_rls,r.rolcanlogin AS can_login,
 p.prosecdef AS security_definer,p.provolatile::text AS volatility,p.proconfig AS config,
 pg_get_userbyid(p.proowner) AS function_owner,
 encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') AS definition_sha256
 FROM pg_roles r CROSS JOIN pg_proc p
 WHERE r.rolname=current_user AND p.oid=to_regprocedure('sessions.ingest_scoped_publication_receipt_capture_v1(jsonb)')`;
const HASH = /^[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const INT64 = 9223372036854775807n;
const CONTROL = /[\u0000-\u001f\u007f]/u;
export const FRONTIER_RECEIPT_READER_ROLE = "bulls96_frontier_receipt_reader";
const ROLE = FRONTIER_RECEIPT_READER_ROLE;
const ROLE_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const RUNTIME_OWNERS = new Set([ROLE, "bulls96_session_ingest_seeder", "bulls96_session_ingest", "anon", "authenticated", "service_role", "public"]);
type Cursor = [string, string, string, string];
export class FrontierReceiptSourceError extends Error {
  constructor(readonly code: string) { super(code); }
}
function fail(code: string): never { throw new FrontierReceiptSourceError(code); }
function obj(v: unknown): v is Record<string, unknown> { return v !== null && typeof v === "object" && !Array.isArray(v); }
function keys(v: Record<string, unknown>, names: string[]): boolean { return Object.keys(v).sort().join(",") === names.sort().join(","); }
function id(v: unknown, empty = false): v is string { return typeof v === "string" && (empty || v.length > 0) && Buffer.byteLength(v) <= 512 && !CONTROL.test(v) && /^[!-~]*$/.test(v); }
function hash(v: unknown): v is string { return typeof v === "string" && HASH.test(v); }
function decimal(v: unknown): v is string { return typeof v === "string" && /^[1-9][0-9]{0,18}$/.test(v) && BigInt(v) <= INT64; }
function equal(a: unknown, b: unknown): boolean { return frontierCanonicalJson(a) === frontierCanonicalJson(b); }
function tenant(v: unknown): v is FrontierTenant {
  return obj(v) && keys(v, ["orgId", "workspaceId", "userId", "originId"]) && id(v.orgId) && id(v.workspaceId, true) && id(v.userId) && id(v.originId);
}
function authority(v: unknown): v is FrontierAuthority {
  return obj(v) && keys(v, ["sourceIdentitySha256", "sourceGenerationSha256", "sourceHead", "controlEpoch", "privacyBaselineSha256", "privacyVersion"]) &&
    [v.sourceIdentitySha256, v.sourceGenerationSha256, v.controlEpoch, v.privacyBaselineSha256].every(x => typeof x === "string" && HASH.test(x)) &&
    typeof v.sourceHead === "string" && /^[a-f0-9]{40}$/.test(v.sourceHead) && typeof v.privacyVersion === "string" && /^(0|[1-9][0-9]{0,18})$/.test(v.privacyVersion) && BigInt(v.privacyVersion) <= INT64;
}
function cursor(v: FrontierReceiptDecision): Cursor { return [v.recordId, v.revision, v.revisionId, v.candidateDigest]; }
function decision(v: unknown, wanted: FrontierTenant): v is FrontierReceiptDecision {
  return obj(v) && keys(v, ["tenant", "recordId", "revision", "revisionId", "source", "sessionId", "seat", "bodyAccepted", "candidateDigest", "firstDigest"]) &&
    tenant(v.tenant) && equal(v.tenant, wanted) && id(v.recordId) && decimal(v.revision) && id(v.revisionId) && id(v.source) &&
    (v.sessionId === null || id(v.sessionId)) && decimal(v.seat) && typeof v.bodyAccepted === "boolean" &&
    typeof v.candidateDigest === "string" && HASH.test(v.candidateDigest) && typeof v.firstDigest === "string" && HASH.test(v.firstDigest) &&
    (!v.bodyAccepted || v.candidateDigest === v.firstDigest);
}
/**
 * SQL-portable domain digest in RPC cursor order. UTF-8 byte-length framing
 * distinguishes null/empty values; no JSONB text or JavaScript key order is
 * part of this database/client boundary. Rows must already be validated.
 */
export function frontierReceiptDomainSha256(rows: FrontierReceiptDecision[]): string {
  const digest = createHash("sha256").update("first-ack-seat-domain-v1\n");
  for (const d of rows) {
    digest.update("row:");
    const fields = [d.tenant.orgId, d.tenant.workspaceId, d.tenant.userId, d.tenant.originId, d.recordId,
      d.revision, d.revisionId, d.source, d.sessionId, d.seat, d.bodyAccepted ? "true" : "false", d.candidateDigest, d.firstDigest];
    for (const field of fields) {
      if (field === null) digest.update("-1:");
      else digest.update(`${Buffer.byteLength(field)}:`).update(field);
    }
  }
  return digest.digest("hex");
}

export interface FrontierReceiptReadRequest {
  tenant: FrontierTenant;
  generation: string;
  /** Independently installed immutable capture profile, never self-registered. */
  profileId: string;
  profileSha256: string;
  /** From the independently reviewed source freeze, not the RPC response. */
  functionSha256: string;
  /** Independently qualified deployment/schema owner, never learned from the RPC. */
  functionOwner: string;
  bounds: { pageLimit: number; maxPages: number; maxDecisions: number; maxBytes: number; maxDurationMs: number };
}
export interface FrontierReceiptSourceCapture {
  protocol: typeof RECEIPT_CAPTURE_PROTOCOL;
  tenant: FrontierTenant;
  generation: string;
  profileId: string;
  profileSha256: string;
  functionSha256: string;
  functionOwner: string;
  opening: FrontierAuthority;
  closing: FrontierAuthority;
  decisions: FrontierReceiptDecision[];
  scan: FrontierScanEvidence;
}

interface Page {
  protocol: string; profileId: string; profileSha256: string; tenant: FrontierTenant; generation: string;
  authority: FrontierAuthority; snapshot: string; decisionCount: number; decisionsSha256: string;
  rows: FrontierReceiptDecision[]; next: Cursor | null;
}

/**
 * Runs only fixed SELECTs in owned read-only transactions. A final EMPTY page,
 * independently reported domain count+digest, opening/closing authority and a
 * fresh second-transaction closure are all mandatory. A limit is never EOF.
 * On failure the eventual caller must discard an uncertain/broken connection;
 * best-effort ROLLBACK is not proof that its transport is healthy and idle.
 * The SQL capability is still a required source/installation prerequisite.
 */
export async function captureFrontierReceiptSource(db: FrontierCaptureSql, input: FrontierReceiptReadRequest): Promise<FrontierReceiptSourceCapture> {
  let b = input?.bounds;
  if (!obj(input) || !keys(input as unknown as Record<string, unknown>, ["tenant", "generation", "profileId", "profileSha256", "functionSha256", "functionOwner", "bounds"]) || !tenant(input.tenant) || !hash(input.generation) || (typeof input.profileId !== "string" || !UUID.test(input.profileId)) || !hash(input.profileSha256) || !hash(input.functionSha256) || typeof input.functionOwner !== "string" || !ROLE_NAME.test(input.functionOwner) || RUNTIME_OWNERS.has(input.functionOwner) || !obj(b) || !keys(b, ["pageLimit", "maxPages", "maxDecisions", "maxBytes", "maxDurationMs"]) ||
      !Number.isSafeInteger(b.pageLimit) || b.pageLimit < 1 || b.pageLimit > 1000 || !Number.isSafeInteger(b.maxPages) || b.maxPages < 1 || b.maxPages > 10_000 ||
      !Number.isSafeInteger(b.maxDecisions) || b.maxDecisions < 0 || b.maxDecisions > 1_000_000 || !Number.isSafeInteger(b.maxBytes) || b.maxBytes < 1024 || b.maxBytes > 64 * 1024 * 1024 ||
      !Number.isSafeInteger(b.maxDurationMs) || b.maxDurationMs < 1 || b.maxDurationMs > 120_000) fail("frontier_receipt_input");
  // Freeze the qualified request before the first await; callers cannot retarget
  // an in-flight capture by mutating a shared request object.
  input = { ...input, tenant: { ...input.tenant }, bounds: { ...b } };
  b = input.bounds;
  const startedAt = new Date().toISOString(), start = performance.now();
  const check = () => { if (performance.now() - start > b.maxDurationMs) fail("frontier_receipt_deadline"); };
  let transactionAttempted = false, byteCount = 0;
  const begin = async () => {
    check();
    // BEGIN may take effect even if its response is lost. Cleanup must be
    // attempted after any send, not only after an acknowledged response.
    transactionAttempted = true;
    await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await db.query("SET LOCAL statement_timeout = 3000; SET LOCAL lock_timeout = 250; SET LOCAL TimeZone = 'UTC'; SET LOCAL search_path = pg_catalog, pg_temp");
    const rows = (await db.query<Record<string, unknown>>(RECEIPT_CAPTURE_IDENTITY_SQL)).rows;
    const r = rows[0];
    if (rows.length !== 1 || !r || r.read_only !== "on" || r.isolation !== "repeatable read" || r.role !== ROLE ||
        ["superuser", "inherit", "create_role", "create_db", "replication", "bypass_rls", "can_login"].some(k => r[k] !== false) ||
        r.security_definer !== true || r.volatility !== "s" || !equal(r.config, ["search_path=pg_catalog, pg_temp"]) ||
        r.function_owner !== input.functionOwner || r.definition_sha256 !== input.functionSha256) fail("frontier_receipt_capability");
    check();
  };
  const page = async (after: Cursor | null, limit: number): Promise<Page> => {
    check();
    const rows = (await db.query<{ capture: unknown }>(RECEIPT_CAPTURE_PAGE_SQL, [JSON.stringify({ protocol: RECEIPT_CAPTURE_PROTOCOL,
      profileId: input.profileId, profileSha256: input.profileSha256, tenant: input.tenant, generation: input.generation, after, limit })])).rows;
    const v = rows[0]?.capture;
    if (rows.length !== 1 || !obj(v) || !keys(v, ["protocol", "profileId", "profileSha256", "tenant", "generation", "authority", "snapshot", "decisionCount", "decisionsSha256", "rows", "next"]) ||
        v.protocol !== RECEIPT_CAPTURE_PROTOCOL || v.profileId !== input.profileId || v.profileSha256 !== input.profileSha256 || !equal(v.tenant, input.tenant) || v.generation !== input.generation ||
        !authority(v.authority) || !id(v.snapshot) || !Number.isSafeInteger(v.decisionCount) || (v.decisionCount as number) < 0 || (v.decisionCount as number) > b.maxDecisions ||
        typeof v.decisionsSha256 !== "string" || !HASH.test(v.decisionsSha256) || !Array.isArray(v.rows) || v.rows.length > limit ||
        v.rows.some(row => !decision(row, input.tenant))) fail("frontier_receipt_page");
    byteCount += Buffer.byteLength(frontierCanonicalJson(v));
    if (byteCount > b.maxBytes) fail("frontier_receipt_byte_bound");
    const p = v as unknown as Page;
    let prior = after;
    for (const row of p.rows) { const at = cursor(row); if (prior && frontierUtf8TupleCompare(at, prior) <= 0) fail("frontier_receipt_order"); prior = at; }
    if (!equal(p.next, p.rows.length ? cursor(p.rows.at(-1)!) : null)) fail("frontier_receipt_cursor");
    check(); return p;
  };
  const signature = (p: Page) => ({ authority: p.authority, count: p.decisionCount, digest: p.decisionsSha256 });
  try {
    await begin();
    let opening: Page | undefined, after: Cursor | null = null;
    const decisions: FrontierReceiptDecision[] = [], counts: number[] = [], hashes: string[] = [];
    for (;;) {
      if (counts.length >= b.maxPages) fail("frontier_receipt_page_bound");
      const p = await page(after, b.pageLimit);
      if (!opening) opening = p;
      if (!equal(signature(p), signature(opening)) || p.snapshot !== opening.snapshot) fail("frontier_receipt_changed");
      counts.push(p.rows.length); hashes.push(frontierSha256({ after, rows: p.rows, next: p.next }));
      decisions.push(...p.rows);
      if (decisions.length > b.maxDecisions || decisions.length > opening.decisionCount) fail("frontier_receipt_count");
      if (!p.rows.length) break;
      after = p.next;
    }
    if (decisions.length !== opening!.decisionCount || frontierReceiptDomainSha256(decisions) !== opening!.decisionsSha256) fail("frontier_receipt_coverage");
    await db.query("COMMIT"); transactionAttempted = false;
    // A repeatable-read snapshot alone cannot prove the authority still holds.
    await begin();
    const closing = await page(null, 1);
    if (!equal(signature(opening!), signature(closing))) fail("frontier_receipt_changed");
    await db.query("COMMIT"); transactionAttempted = false; check();
    const finishedAt = new Date().toISOString();
    if (Date.parse(finishedAt) < Date.parse(startedAt)) fail("frontier_receipt_clock");
    return { protocol: RECEIPT_CAPTURE_PROTOCOL, tenant: input.tenant, generation: input.generation, profileId: input.profileId,
      profileSha256: input.profileSha256, functionSha256: input.functionSha256, functionOwner: input.functionOwner, opening: opening!.authority, closing: closing.authority, decisions,
      scan: { kind: "receipt-decisions", adapter: "first-ack-seat-decisions-v1", querySha256: frontierSha256({ sql: RECEIPT_CAPTURE_PAGE_SQL, functionSha256: input.functionSha256, functionOwner: input.functionOwner, readerRole: ROLE, profileSha256: input.profileSha256 }),
        snapshotSha256: frontierSha256({ opening: opening!.snapshot, closing: closing.snapshot }), startedAt, finishedAt, pageLimit: b.pageLimit, maxPages: b.maxPages,
        pagesRead: counts.length, pageItemCounts: counts, itemCount: decisions.length, byteCount, checkpointCount: counts.filter(n => n > 0).length,
        pageChainSha256: frontierSha256(hashes), itemsSha256: frontierReceiptDecisionsSha256(decisions), openingStateSha256: frontierSha256(opening!.authority),
        closingStateSha256: frontierSha256(closing.authority), terminalCursor: null, complete: true } };
  } catch (error) {
    if (transactionAttempted) await db.query("ROLLBACK").catch(() => undefined);
    if (error instanceof FrontierReceiptSourceError) throw error;
    // Connection/RPC/permission failures never leak raw server text or fall back.
    fail("frontier_receipt_unavailable");
  }
}
