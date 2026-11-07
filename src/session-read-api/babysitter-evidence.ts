/**
 * Babysitter evidence reader: the new store's equivalent of relayhistory-cloud's Neon
 * `GET /v1/delivery/records` (packages/relayhistory/src/lib/delivery.ts `listDelivery`)
 * for the `session_lineage` / `turn_receipt` kinds, whose payloads now live only in the
 * retention buckets (src/session-ingest/babysitter-retention.ts).
 *
 * Same contract as Neon: filters kind, source, session_id, include_deleted, cursor and
 * limit (1..100, default 100); unknown or repeated filters refused; Neon's NATIVE order
 * (origin_id, record_id) in C collation; an opaque cursor bound to the filter scope whose
 * position is (origin_id, record_id); the envelope `{protocolVersion: 1, listing: "live",
 * records, nextCursor}` and Neon's record shape.
 *
 * Bounded page work, no materialization: the order and every visibility decision come from
 * the content-free key index in SQL (sessions.babysitter_evidence_page_for: limit+1 rows,
 * at most 1000 records scanned). The reader then GETs only the exact objects the page's
 * live rows point at (at most MAX_OBJECTS_PER_PAGE distinct objects, by exact key, never a
 * listing) and takes the indexed line. A page cut by any bound returns a cursor at the last
 * record it fully decided, so it resumes exactly there and never claims completeness.
 *
 * Visibility (Neon's, per record by its newest indexed revision; see the SQL):
 *  - tenant-pinned (read API role, verified binding, session_directory_read_api_scope);
 *  - delete -> Neon's tombstone (operation "delete", payload null, the delete's revision)
 *    exactly once under include_deleted; hidden otherwise; never a superseded payload;
 *  - expiry: payload.expires_at_ms <= now (or the bucket hour passed) is unreadable, and
 *    include_deleted shows a payload-free tombstone with the line's revision;
 *  - delete / reset / disable: a fenced bucket is hidden at once, before any physical
 *    erase, and stays hidden after a re-enable; erased buckets have no index rows;
 *  - the newest revision must be the committed one (ingest_record_revisions), else the
 *    record is not served (never a stale payload, never a guessed tombstone).
 * Default off: the route answers 501 unless the read worker wires these deps
 * (READ_API_BABYSITTER_EVIDENCE=on plus the raw store).
 */
import { createHash } from "node:crypto";
import { layoutComponent } from "../shared/layout-component.ts";

export const BABYSITTER_EVIDENCE_KINDS: ReadonlySet<string> = new Set(["session_lineage", "turn_receipt"]);
const FILTERS = ["kind", "source", "session_id", "include_deleted", "cursor", "limit"];
const MAX_PAGE_BYTES = 2 * 1024 * 1024;
const BUCKET_SEGMENT = /^_bx[0-9]{10}-[0-9a-f]{16}$/;
const MACHINE = /^srv_[0-9a-f]{32}$/;
const OBJECT = /^b=[0-9a-f]{32}\.ndjson\.zst$/;
/** Distinct bucket objects one page may GET (each by exact key). */
export const MAX_OBJECTS_PER_PAGE = 32;
const OBJECT_CONCURRENCY = 8;

export interface EvidenceBucket {
  machineId: string;
  ref: string;
}

/** One row of sessions.babysitter_evidence_page_for, in native order. */
export type EvidencePageRow =
  | { originId: string; recordId: string; revision: string; revisionId: string; operation: "delete"; source: string; sessionId: string | null }
  | { originId: string; recordId: string; revision: string; revisionId: string; operation: "upsert"; source: string; sessionId: string | null;
      machineId: string; ref: string; object: string; line: number };

export interface EvidencePageRequest {
  kind: string;
  source: string | null;
  sessionId: string | null;
  after: [string, string] | null;
  /** limit + 1 (1..101): the extra row proves there is more. */
  limit: number;
  includeDeleted: boolean;
}

export interface EvidenceDeps {
  /** One native-order page of the bound tenant (read role): <= limit rows, scan-bounded. */
  page(scope: { orgId: string; workspaceId: string }, req: EvidencePageRequest): Promise<{ rows: EvidencePageRow[]; more: boolean; scannedTo: [string, string] | null }>;
  /** One object's decompressed body, by exact key; bounded by `signal`. */
  object(key: string, signal: AbortSignal): Promise<Uint8Array>;
  keyRoot: string;
  /** Below Cloud's 4 s read timeout. Default 3000. */
  deadlineMs?: number;
}

export class EvidenceRequestError extends Error {
  constructor(readonly status: 400 | 403 | 413 | 501 | 503, readonly code: string, message: string, readonly reason?: string) {
    super(message);
  }
}
const invalid = (m: string) => new EvidenceRequestError(400, "invalid_delivery", m);
const unavailable = () => new EvidenceRequestError(503, "backend_unavailable", "read backend unavailable");

export interface EvidenceRecord {
  schema_version: 1;
  origin_id: string;
  record_id: string;
  revision_id: string;
  revision: number;
  kind: string;
  source: string;
  session_id: string | null;
  operation: "upsert" | "delete";
  payload: Record<string, unknown> | null;
}

export interface EvidencePage {
  protocolVersion: 1;
  listing: "live";
  records: EvidenceRecord[];
  nextCursor: string | null;
}

const identifier = (v: string) => v.length > 0 && v.length <= 512 && !/[\x00-\x1f\x7f]/u.test(v);

function scopeDigest(parts: unknown[]): string {
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

function b64url(text: string): string {
  return Buffer.from(text, "utf8").toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function decodeCursor(raw: string, scope: string): [string, string] {
  if (raw.length > 4096 || !/^[A-Za-z0-9_-]+$/.test(raw)) throw invalid("Invalid delivery cursor");
  let cursor: unknown;
  try {
    cursor = JSON.parse(Buffer.from(raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8"));
  } catch {
    throw invalid("Invalid delivery cursor");
  }
  const c = cursor as { v?: unknown; scope?: unknown; position?: unknown };
  if (!c || c.v !== 1 || c.scope !== scope || !Array.isArray(c.position) || c.position.length !== 2 || !c.position.every((p) => typeof p === "string" && identifier(p)))
    throw invalid("Invalid delivery cursor scope");
  return c.position as [string, string];
}

/** The exact bucket directory, rebuilt from its coordinates (same rule as the sweeper). */
export function evidenceBucketPrefix(keyRoot: string, scope: { orgId: string; workspaceId: string }, b: EvidenceBucket): string {
  if (!MACHINE.test(b.machineId) || !BUCKET_SEGMENT.test(b.ref)) throw unavailable();
  const y = b.ref.slice(3, 7), m = b.ref.slice(7, 9), d = b.ref.slice(9, 11);
  return `${keyRoot}/${scope.orgId}/v1/ws=${layoutComponent(scope.workspaceId)}/m=${b.machineId}/h=_babysitter/d=${y}/${m}/${d}/s=${b.ref}/`;
}

interface Line {
  kind: string;
  origin_id: string;
  record_id: string;
  revision_id: string;
  revision: string;
  source: string;
  session_id: string | null;
  operation: string;
  payload: Record<string, unknown> | null;
}

/**
 * Neon's delivery account (relayhistory delivery.ts `deliveryAccount`): "relayhistory:" +
 * sha256 of the canonical JSON of [orgId, workspaceId], which for two strings is exactly
 * JSON.stringify. A client assertion only; tenancy still comes from the verified token.
 */
export function evidenceAccount(scope: { orgId: string; workspaceId: string }): string {
  return "relayhistory:" + createHash("sha256").update(JSON.stringify([scope.orgId, scope.workspaceId ?? ""])).digest("hex");
}

/**
 * Neon `requireDeliveryAccount`: the X-RelayHistory-Expected-Account header must equal the
 * verified tenant's account. A missing header is the same refusal as a mismatched one:
 * 403 `delivery_account_mismatch`, "Delivery account does not match authenticated tenant".
 */
export function requireEvidenceAccount(scope: { orgId: string; workspaceId: string }, expected: string | undefined): void {
  if (expected !== evidenceAccount(scope))
    throw new EvidenceRequestError(403, "delivery_account_mismatch", "Delivery account does not match authenticated tenant");
}

/**
 * Neon `listDelivery` for Babysitter kinds. The account assertion is checked FIRST, before
 * any evidence database or S3 access (Neon checks it before listDelivery runs).
 */
export async function readBabysitterEvidence(deps: EvidenceDeps, scope: { orgId: string; workspaceId: string }, query: Record<string, string>, expectedAccount: string | undefined): Promise<EvidencePage> {
  requireEvidenceAccount(scope, expectedAccount);
  for (const key of Object.keys(query)) if (!FILTERS.includes(key)) throw invalid("Unknown delivery filter");
  if (query.kind === undefined || !BABYSITTER_EVIDENCE_KINDS.has(query.kind))
    throw new EvidenceRequestError(501, "unsupported_by_backend", "not served by this backend: delivery_kind", "delivery_kind");
  for (const key of ["source", "session_id"]) if (query[key] !== undefined && !identifier(query[key]!)) throw invalid("Invalid delivery filter");
  if (query.include_deleted !== undefined && !["true", "false"].includes(query.include_deleted)) throw invalid("include_deleted must be true or false");
  const limit = query.limit === undefined ? 100 : /^\d+$/.test(query.limit) ? Number(query.limit) : NaN;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw invalid("limit must be 1..100");
  const includeDeleted = query.include_deleted === "true";
  const digest = scopeDigest([scope.orgId, scope.workspaceId, query.kind, query.source ?? "", query.session_id ?? "", includeDeleted]);
  const after = query.cursor === undefined ? null : decodeCursor(query.cursor, digest);
  const signal = AbortSignal.timeout(deps.deadlineMs ?? 3000);

  const page = await deps.page(scope, { kind: query.kind, source: query.source ?? null, sessionId: query.session_id ?? null, after, limit: limit + 1, includeDeleted });
  // Take rows in native order until the limit or the distinct-object bound.
  const taken: EvidencePageRow[] = [];
  const keys = new Map<string, string>();   // object key per row index
  const objects = new Set<string>();
  let cutByObjects = false;
  for (const row of page.rows.slice(0, limit)) {
    if (row.operation === "upsert") {
      if (!OBJECT.test(row.object) || !Number.isSafeInteger(row.line) || row.line < 0) throw unavailable();
      const key = evidenceBucketPrefix(deps.keyRoot, scope, row) + "raw/" + row.object;
      if (!objects.has(key) && objects.size >= MAX_OBJECTS_PER_PAGE) { cutByObjects = true; break; }
      objects.add(key);
      keys.set(`${taken.length}`, key);
    }
    taken.push(row);
  }
  // GET exactly those objects (bounded concurrency, the request deadline).
  const bodies = new Map<string, string[]>();
  const queue = [...objects];
  await Promise.all(Array.from({ length: Math.min(OBJECT_CONCURRENCY, queue.length) }, async () => {
    for (let key = queue.shift(); key !== undefined; key = queue.shift())
      bodies.set(key, new TextDecoder().decode(await deps.object(key, signal)).split("\n"));
  }));
  const records: EvidenceRecord[] = taken.map((row, i) => {
    const base = { schema_version: 1 as const, origin_id: row.originId, record_id: row.recordId, kind: query.kind!, source: row.source, session_id: row.sessionId ?? null,
      revision_id: row.revisionId, revision: Number(row.revision) };
    if (row.operation === "delete") return { ...base, operation: "delete", payload: null };
    let l: Line;
    try { l = JSON.parse(bodies.get(keys.get(`${i}`)!)![row.line] ?? "") as Line; } catch { throw unavailable(); }
    // The indexed line must be exactly this revision of this record (fail closed otherwise).
    if (!l || l.origin_id !== row.originId || l.record_id !== row.recordId || l.revision !== row.revision || l.revision_id !== row.revisionId
        || l.kind !== query.kind || l.source !== row.source || (l.session_id ?? null) !== (row.sessionId ?? null) || l.operation !== "upsert")
      throw unavailable();
    return { ...base, operation: "upsert", payload: l.payload };
  });
  // The next cursor: the last record decided, whenever any bound (limit+1, objects, scan) cut the page.
  const last = records[records.length - 1];
  // Scan-bound cut: scannedTo is the last record SQL decided (>= every emitted row).
  const at: [string, string] | null = cutByObjects || page.rows.length > limit
    ? (last ? [last.origin_id, last.record_id] : null)
    : page.more ? page.scannedTo : null;
  if ((cutByObjects || page.rows.length > limit || page.more) && !at) throw unavailable();
  const build = (recs: EvidenceRecord[], cursorAt: [string, string] | null): EvidencePage => ({ protocolVersion: 1, listing: "live", records: recs,
    nextCursor: cursorAt ? b64url(JSON.stringify({ v: 1, scope: digest, position: cursorAt })) : null });
  let out = build(records, at);
  let kept = records.length;
  while (Buffer.byteLength(JSON.stringify(out)) > MAX_PAGE_BYTES) {
    if (kept <= 1) throw new EvidenceRequestError(413, "delivery_too_large", "Retained delivery record exceeds the readback byte limit");
    kept--;
    const r = records[kept - 1]!;
    out = build(records.slice(0, kept), [r.origin_id, r.record_id]);
  }
  return out;
}

const str = (v: unknown): v is string => typeof v === "string";

/** Strict shape of one SQL page (anything else is a 503, never a partial page). */
function parsePage(v: unknown): { rows: EvidencePageRow[]; more: boolean; scannedTo: [string, string] | null } {
  const p = v as { rows?: unknown; more?: unknown; scannedTo?: unknown };
  if (!p || !Array.isArray(p.rows) || typeof p.more !== "boolean") throw unavailable();
  const scannedTo = p.scannedTo === null || p.scannedTo === undefined ? null : p.scannedTo;
  if (scannedTo !== null && !(Array.isArray(scannedTo) && scannedTo.length === 2 && scannedTo.every(str))) throw unavailable();
  for (const r of p.rows as Record<string, unknown>[]) {
    if (!r || !str(r.originId) || !str(r.recordId) || !str(r.revision) || !/^[1-9][0-9]{0,17}$/.test(r.revision) || !str(r.revisionId) || !str(r.source)
        || (r.sessionId !== null && !str(r.sessionId)) || (r.operation !== "upsert" && r.operation !== "delete"))
      throw unavailable();
    if (r.operation === "upsert" && (!str(r.machineId) || !str(r.ref) || !str(r.object) || !Number.isSafeInteger(r.line))) throw unavailable();
  }
  return { rows: p.rows as EvidencePageRow[], more: p.more, scannedTo: scannedTo as [string, string] | null };
}

/**
 * The read worker's deps over the read role's tenant runner and a raw store built PER CALL
 * with a fetcher bound to the request's abort signal (so every GET honors the deadline).
 * Backend failures surface as 503 `backend_unavailable` (never a payload, key or message).
 */
export function sqlEvidenceDeps(
  run: <T>(orgId: string, workspaceId: string, work: (q: { query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: R[] }> }) => Promise<T>) => Promise<T>,
  storeFor: (signal: AbortSignal) => { getObject(key: never): Promise<{ body: unknown }> } | undefined,
  read: (body: unknown, signal: AbortSignal) => Promise<Uint8Array>,
  decompress: (bytes: Uint8Array) => Uint8Array,
  objectKey: (key: string) => never,
  keyRoot: string,
): EvidenceDeps {
  const guard = async <T>(work: () => Promise<T>): Promise<T> => {
    try { return await work(); } catch (e) { if (e instanceof EvidenceRequestError) throw e; throw unavailable(); }
  };
  return {
    keyRoot,
    page: (scope, req) => guard(async () => {
      const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
        q.query<{ v: unknown }>("SELECT sessions.babysitter_evidence_page_for($1, $2::jsonb) AS v", [scope.workspaceId, JSON.stringify(req)]));
      return parsePage(rows[0]?.v);
    }),
    object: (key, signal) => guard(async () => {
      signal.throwIfAborted();
      const s = storeFor(signal);
      if (!s) throw unavailable();
      return decompress(await read((await s.getObject(objectKey(key))).body, signal));
    }),
  };
}

/** The bucket hour a ref names (exported for tests). */
export function refHour(ref: string): Date {
  return new Date(Date.UTC(Number(ref.slice(3, 7)), Number(ref.slice(7, 9)) - 1, Number(ref.slice(9, 11)), Number(ref.slice(11, 13))));
}
