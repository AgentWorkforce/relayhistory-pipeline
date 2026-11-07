/**
 * Babysitter evidence reader: the new store's equivalent of relayhistory-cloud's Neon
 * `GET /v1/delivery/records` (packages/relayhistory/src/lib/delivery.ts `listDelivery`)
 * for the `session_lineage` / `turn_receipt` kinds, whose payloads now live only in the
 * retention buckets (src/session-ingest/babysitter-retention.ts).
 *
 * Same query contract as Neon: filters kind, source, session_id, include_deleted, cursor
 * and limit (1..100, default 100); unknown or repeated filters refused; an opaque cursor
 * bound to the filter scope; the same envelope `{protocolVersion: 1, listing: "live",
 * records, nextCursor}` and record shape. It is a real PAGE: buckets come from SQL by
 * keyset and objects are read one at a time, stopping at limit + 1 visible records (or a
 * work bound), so a page reads O(limit) objects and a continuation never rereads earlier
 * ones. Order is (bucket machine, bucket ref, object key, line), stable across pages; Neon
 * orders by (origin_id, record_id), which only a full scan could reproduce here, and the
 * cursor is opaque either way.
 *
 * Visibility (Neon-equivalent, and stricter where privacy requires):
 *  - tenant-pinned: the bucket and revision reads run as the read API role under the
 *    verified binding (sessions.babysitter_evidence_*_for, session_directory_read_api_scope);
 *  - expiry: a record whose payload.expires_at_ms <= now is hidden (include_deleted shows
 *    it as a payload-free tombstone, as Neon's read-only expiry predicate does); a bucket
 *    whose expiry hour passed is not read at all;
 *  - delete / reset: a bucket whose session was disabled or whose user reset after its
 *    claim is hidden AT ONCE (logical delete, before any physical erase) and stays hidden
 *    after a re-enable; erased buckets are never read;
 *  - supersession: a line older than the committed revision (ingest_record_revisions: a
 *    newer upsert or a tombstone) is never served, not even under include_deleted: a page
 *    cannot tell whether the newer revision is a live line in a later bucket, and a
 *    delete@N beside an upsert@N would contradict itself. include_deleted tombstones come
 *    from expiry only (a deliberate deviation from Neon, which keeps tombstone rows).
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

export interface EvidenceBucket {
  machineId: string;
  ref: string;
  source: string;
  sessionId: string | null;
}

export interface EvidenceDeps {
  /** One keyset page (<= limit) of live, unfenced, unexpired, unerased buckets of the bound tenant (read role). */
  buckets(scope: { orgId: string; workspaceId: string }, filter: { source: string | null; sessionId: string | null },
    after: { machineId: string; ref: string; inclusive: boolean } | null, limit: number): Promise<EvidenceBucket[]>;
  /** Committed revisions of the given (origin, record) keys (read role). */
  revisions(scope: { orgId: string; workspaceId: string }, keys: [string, string][]): Promise<Map<string, { revision: bigint; revisionId: string }>>;
  /** The object keys under one bucket directory, in key order; bounded by `signal`. */
  objectKeys(prefix: string, signal: AbortSignal): Promise<string[]>;
  /** One object's decompressed body; bounded by `signal`. */
  object(key: string, signal: AbortSignal): Promise<Uint8Array>;
  keyRoot: string;
  /** Below Cloud's 4 s read timeout. Default 3000. */
  deadlineMs?: number;
  now?: () => number;
}

/** Work bounds of one page: a page may end short of `limit` with a cursor when they bind. */
export const PAGE_BUCKETS = 64;
export const MAX_BUCKETS_PER_PAGE = 256;
export const MAX_OBJECTS_PER_PAGE = 32;

export class EvidenceRequestError extends Error {
  constructor(readonly status: 400 | 403 | 413 | 501 | 503, readonly code: string, message: string, readonly reason?: string) {
    super(message);
  }
}
const invalid = (m: string) => new EvidenceRequestError(400, "invalid_delivery", m);

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
const keyOf = (origin: string, record: string) => JSON.stringify([origin, record]);

function scopeDigest(parts: unknown[]): string {
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

function b64url(text: string): string {
  return Buffer.from(text, "utf8").toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

type Position = [machineId: string, ref: string, key: string, line: number];

function decodeCursor(raw: string, scope: string): Position {
  if (raw.length > 4096 || !/^[A-Za-z0-9_-]+$/.test(raw)) throw invalid("Invalid delivery cursor");
  let cursor: unknown;
  try {
    cursor = JSON.parse(Buffer.from(raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8"));
  } catch {
    throw invalid("Invalid delivery cursor");
  }
  const c = cursor as { v?: unknown; scope?: unknown; position?: unknown };
  const p = c?.position as unknown[];
  if (!c || c.v !== 2 || c.scope !== scope || !Array.isArray(p) || p.length !== 4 || !MACHINE.test(String(p[0])) || !BUCKET_SEGMENT.test(String(p[1]))
      || typeof p[2] !== "string" || p[2].length > 2048 || !Number.isSafeInteger(p[3]) || (p[3] as number) < -1)
    throw invalid("Invalid delivery cursor scope");
  return p as Position;
}

/** The C-collation order Neon uses (byte order of the UTF-8 text). */
function compareC(a: string, b: string): number {
  return Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
}

/** The exact bucket directory to read, rebuilt from its coordinates (same rule as the sweeper). */
export function evidenceBucketPrefix(keyRoot: string, scope: { orgId: string; workspaceId: string }, b: EvidenceBucket): string {
  if (!MACHINE.test(b.machineId) || !BUCKET_SEGMENT.test(b.ref)) throw new EvidenceRequestError(503, "backend_unavailable", "read backend unavailable");
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

function parseLines(body: Uint8Array): { line: Line; index: number }[] {
  const out: { line: Line; index: number }[] = [];
  new TextDecoder().decode(body).split("\n").forEach((text, index) => {
    if (!text) return;
    let l: Line;
    try { l = JSON.parse(text) as Line; } catch { return; }
    if (!l || typeof l.origin_id !== "string" || typeof l.record_id !== "string" || typeof l.revision !== "string" || !/^[1-9][0-9]{0,18}$/.test(l.revision)
        || typeof l.revision_id !== "string" || typeof l.kind !== "string" || l.operation !== "upsert") return;
    out.push({ line: l, index });
  });
  return out;
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
 * Neon `listDelivery` for Babysitter kinds over the retention buckets. The account
 * assertion is checked FIRST, before any evidence database or S3 access (Neon checks it
 * before listDelivery runs).
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
  const position = query.cursor === undefined ? null : decodeCursor(query.cursor, digest);
  const now = (deps.now ?? Date.now)();
  const signal = AbortSignal.timeout(deps.deadlineMs ?? 3000);

  const filter = { source: query.source ?? null, sessionId: query.session_id ?? null };
  const rows: { record: EvidenceRecord; at: Position }[] = [];
  let after: { machineId: string; ref: string; inclusive: boolean } | null = position ? { machineId: position[0], ref: position[1], inclusive: true } : null;
  let last: Position | null = position;   // the last position fully processed
  let bucketsSeen = 0, objectsRead = 0, exhausted = false, bound = false;
  outer: for (;;) {
    const page = await deps.buckets(scope, filter, after, PAGE_BUCKETS);
    for (const b of page) {
      if (++bucketsSeen > MAX_BUCKETS_PER_PAGE) { bound = true; break outer; }
      const resume = position && b.machineId === position[0] && b.ref === position[1] ? position : null;
      const keys = (await deps.objectKeys(evidenceBucketPrefix(deps.keyRoot, scope, b), signal)).sort(compareC)
        .filter((k) => !resume || compareC(k, resume[2]) >= 0);
      for (const key of keys) {
        if (objectsRead >= MAX_OBJECTS_PER_PAGE) { bound = true; break outer; }
        objectsRead++;
        const fromLine = resume && key === resume[2] ? resume[3] : -1;
        const lines = parseLines(await deps.object(key, signal)).filter((x) => x.index > fromLine && x.line.kind === query.kind
          // The bucket row is the authority for (source, session); a line that disagrees is not served.
          && x.line.source === b.source && (x.line.session_id ?? null) === b.sessionId);
        const committed = lines.length === 0 ? new Map() : await deps.revisions(scope, lines.map((x) => [x.line.origin_id, x.line.record_id] as [string, string]));
        for (const { line: l, index } of lines) {
          const at: Position = [b.machineId, b.ref, key, index];
          const r = visible(l, committed.get(keyOf(l.origin_id, l.record_id)), includeDeleted, now);
          if (r) rows.push({ record: r, at });
          if (rows.length > limit) break outer;
          last = at;
        }
        last = [b.machineId, b.ref, key, Number.MAX_SAFE_INTEGER];
      }
      last = [b.machineId, b.ref, "\uffff", Number.MAX_SAFE_INTEGER];
    }
    if (page.length < PAGE_BUCKETS) { exhausted = true; break; }
    const tail = page[page.length - 1]!;
    after = { machineId: tail.machineId, ref: tail.ref, inclusive: false };
  }
  // More: a (limit+1)th visible record was found, or a work bound stopped the scan.
  const more = rows.length > limit || (bound && !exhausted);
  const records = rows.slice(0, limit);
  // `last` is the last position fully processed before the (limit+1)th record (or the bound):
  // at or past the limit-th record, possibly the end of its object or bucket, so a
  // continuation never rereads a finished object; anything skipped in between was invisible.
  const cursorAt = last;
  const build = (recs: EvidenceRecord[], at: Position | null): EvidencePage => ({ protocolVersion: 1, listing: "live", records: recs,
    nextCursor: at ? b64url(JSON.stringify({ v: 2, scope: digest, position: at })) : null });
  let out = build(records.map((r) => r.record), more ? cursorAt : null);
  let kept = records.length;
  while (Buffer.byteLength(JSON.stringify(out)) > MAX_PAGE_BYTES) {
    if (kept <= 1) throw new EvidenceRequestError(413, "delivery_too_large", "Retained delivery record exceeds the readback byte limit");
    kept--;
    out = build(records.slice(0, kept).map((r) => r.record), records[kept - 1]!.at);
  }
  return out;
}

/** Neon's visibility for one line: live, a tombstone (include_deleted), or nothing. */
function visible(l: Line, c: { revision: bigint; revisionId: string } | undefined, includeDeleted: boolean, now: number): EvidenceRecord | null {
  const base = { schema_version: 1 as const, origin_id: l.origin_id, record_id: l.record_id, kind: l.kind, source: l.source, session_id: l.session_id ?? null };
  // Superseded or tombstoned since (a newer committed revision): never served (see header).
  if (c && c.revision > BigInt(l.revision)) return null;
  const expires = l.payload?.expires_at_ms;
  if (typeof expires === "number" && expires <= now)
    return includeDeleted ? { ...base, revision_id: l.revision_id, revision: Number(l.revision), operation: "delete", payload: null } : null;
  return { ...base, revision_id: l.revision_id, revision: Number(l.revision), operation: "upsert", payload: l.payload };
}

/**
 * The read worker's deps over the read role's tenant runner and a raw store built PER CALL
 * with a fetcher bound to the request's abort signal (so list and get honor the deadline).
 * Backend failures surface as 503 `backend_unavailable` (never a payload, key or message).
 */
export function sqlEvidenceDeps(
  run: <T>(orgId: string, workspaceId: string, work: (q: { query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: R[] }> }) => Promise<T>) => Promise<T>,
  storeFor: (signal: AbortSignal) => { list(prefix: never, cursor?: string): Promise<{ items: { key: unknown }[]; nextCursor?: string }>; getObject(key: never): Promise<{ body: unknown }> } | undefined,
  read: (body: unknown, signal: AbortSignal) => Promise<Uint8Array>,
  decompress: (bytes: Uint8Array) => Uint8Array,
  objectKey: (key: string) => never,
  keyRoot: string,
): EvidenceDeps {
  const unavailable = () => new EvidenceRequestError(503, "backend_unavailable", "read backend unavailable");
  const guard = async <T>(work: () => Promise<T>): Promise<T> => {
    try { return await work(); } catch (e) { if (e instanceof EvidenceRequestError) throw e; throw unavailable(); }
  };
  const store = (signal: AbortSignal) => { signal.throwIfAborted(); const s = storeFor(signal); if (!s) throw unavailable(); return s; };
  return {
    keyRoot,
    buckets: (scope, filter, after, limit) => guard(async () => {
      const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
        q.query<{ v: unknown }>("SELECT sessions.babysitter_evidence_buckets_for($1, $2::jsonb, $3::jsonb, $4) AS v", [scope.workspaceId, JSON.stringify(filter), JSON.stringify(after), limit]));
      const v = rows[0]?.v;
      if (!Array.isArray(v)) throw unavailable();
      return v as EvidenceBucket[];
    }),
    revisions: (scope, keys) => guard(async () => {
      const out = new Map<string, { revision: bigint; revisionId: string }>();
      for (let i = 0; i < keys.length; i += 2000) {
        const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
          q.query<{ v: unknown }>("SELECT sessions.babysitter_evidence_revisions_for($1, $2::jsonb) AS v", [scope.workspaceId, JSON.stringify(keys.slice(i, i + 2000))]));
        const v = rows[0]?.v;
        if (!Array.isArray(v)) throw unavailable();
        for (const e of v as [string, string, string, string][]) out.set(keyOf(e[0], e[1]), { revision: BigInt(e[2]), revisionId: e[3] });
      }
      return out;
    }),
    objectKeys: (prefix, signal) => guard(async () => {
      const s = store(signal);
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        signal.throwIfAborted();
        const page = await s.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => String(i.key)).filter((k) => k.startsWith(prefix)));
        cursor = page.nextCursor;
      } while (cursor);
      return keys;
    }),
    object: (key, signal) => guard(async () => decompress(await read((await store(signal).getObject(objectKey(key))).body, signal))),
  };
}

/** The bucket hour a ref names (exported for tests). */
export function refHour(ref: string): Date {
  return new Date(Date.UTC(Number(ref.slice(3, 7)), Number(ref.slice(7, 9)) - 1, Number(ref.slice(9, 11)), Number(ref.slice(11, 13))));
}
