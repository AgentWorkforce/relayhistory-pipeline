/**
 * Babysitter evidence reader: the new store's equivalent of relayhistory-cloud's Neon
 * `GET /v1/delivery/records` (packages/relayhistory/src/lib/delivery.ts `listDelivery`)
 * for the `session_lineage` / `turn_receipt` kinds, whose payloads now live only in the
 * retention buckets (src/session-ingest/babysitter-retention.ts).
 *
 * Same query contract as Neon: filters kind, source, session_id, include_deleted, cursor
 * and limit (1..100, default 100); unknown or repeated filters refused; ordered by
 * (origin_id, record_id); an opaque cursor bound to the filter scope; the same envelope
 * `{protocolVersion: 1, listing: "live", records, nextCursor}` and record shape.
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
 *  - supersession: the newest bucket line per (origin_id, record_id) wins, and a line older
 *    than the committed revision (ingest_record_revisions: a newer upsert or a tombstone)
 *    is hidden (include_deleted shows the committed revision as a tombstone).
 * Default off: the route answers 501 unless the read worker wires these deps
 * (READ_API_BABYSITTER_EVIDENCE=on plus the raw store).
 */
import { createHash } from "node:crypto";
import { layoutComponent } from "../shared/layout-component.ts";

export const BABYSITTER_EVIDENCE_KINDS: ReadonlySet<string> = new Set(["session_lineage", "turn_receipt"]);
const FILTERS = ["kind", "source", "session_id", "include_deleted", "cursor", "limit"];
const MAX_PAGE_BYTES = 2 * 1024 * 1024;
const MAX_BUCKET_READS = 4;
const BUCKET_SEGMENT = /^_bx[0-9]{10}-[0-9a-f]{16}$/;
const MACHINE = /^srv_[0-9a-f]{32}$/;

export interface EvidenceBucket {
  machineId: string;
  ref: string;
  source: string;
  sessionId: string | null;
}

export interface EvidenceDeps {
  /** Live, unfenced, unexpired, unerased buckets of the bound tenant (read role). */
  buckets(scope: { orgId: string; workspaceId: string }, filter: { source: string | null; sessionId: string | null }): Promise<EvidenceBucket[]>;
  /** Committed revisions of the given (origin, record) keys (read role). */
  revisions(scope: { orgId: string; workspaceId: string }, keys: [string, string][]): Promise<Map<string, { revision: bigint; revisionId: string }>>;
  /** The decompressed bodies of every object under one bucket directory. */
  objects(prefix: string, signal: AbortSignal): Promise<Uint8Array[]>;
  keyRoot: string;
  /** Below Cloud's 4 s read timeout. Default 3000. */
  deadlineMs?: number;
  now?: () => number;
}

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

function parseLines(body: Uint8Array): Line[] {
  const out: Line[] = [];
  for (const text of new TextDecoder().decode(body).split("\n")) {
    if (!text) continue;
    let l: Line;
    try { l = JSON.parse(text) as Line; } catch { continue; }
    if (!l || typeof l.origin_id !== "string" || typeof l.record_id !== "string" || typeof l.revision !== "string" || !/^[1-9][0-9]{0,18}$/.test(l.revision)
        || typeof l.revision_id !== "string" || typeof l.kind !== "string" || l.operation !== "upsert") continue;
    out.push(l);
  }
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

  const buckets = await deps.buckets(scope, { source: query.source ?? null, sessionId: query.session_id ?? null });
  // Newest line per (origin, record) across every visible bucket.
  const newest = new Map<string, Line>();
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(MAX_BUCKET_READS, buckets.length) }, async () => {
    while (next < buckets.length) {
      const b = buckets[next++]!;
      for (const body of await deps.objects(evidenceBucketPrefix(deps.keyRoot, scope, b), signal))
        for (const l of parseLines(body)) {
          if (l.kind !== query.kind) continue;
          // The bucket row is the authority for (source, session); a line that disagrees is not served.
          if (l.source !== b.source || (l.session_id ?? null) !== b.sessionId) continue;
          const k = keyOf(l.origin_id, l.record_id);
          const held = newest.get(k);
          if (!held || BigInt(l.revision) > BigInt(held.revision)) newest.set(k, l);
        }
    }
  }));
  const keys = [...newest.values()].map((l) => [l.origin_id, l.record_id] as [string, string]);
  const committed = keys.length === 0 ? new Map() : await deps.revisions(scope, keys);

  const rows: EvidenceRecord[] = [];
  for (const l of newest.values()) {
    if (position && (compareC(l.origin_id, position[0]) || compareC(l.record_id, position[1])) <= 0) continue;
    const c = committed.get(keyOf(l.origin_id, l.record_id));
    const base = { schema_version: 1 as const, origin_id: l.origin_id, record_id: l.record_id, kind: l.kind, source: l.source, session_id: l.session_id ?? null };
    if (c && c.revision > BigInt(l.revision)) {
      // Superseded or tombstoned since: never the stale payload.
      if (includeDeleted) rows.push({ ...base, revision_id: c.revisionId, revision: Number(c.revision), operation: "delete", payload: null });
      continue;
    }
    const expires = l.payload?.expires_at_ms;
    const expired = typeof expires === "number" && expires <= now;
    if (expired) {
      if (includeDeleted) rows.push({ ...base, revision_id: l.revision_id, revision: Number(l.revision), operation: "delete", payload: null });
      continue;
    }
    rows.push({ ...base, revision_id: l.revision_id, revision: Number(l.revision), operation: "upsert", payload: l.payload });
  }
  rows.sort((a, b) => compareC(a.origin_id, b.origin_id) || compareC(a.record_id, b.record_id));
  const page = rows.slice(0, limit);
  const more = rows.length > page.length;
  const build = (records: EvidenceRecord[], hasMore: boolean): EvidencePage => {
    const last = records[records.length - 1];
    return { protocolVersion: 1, listing: "live", records,
      nextCursor: hasMore && last ? b64url(JSON.stringify({ v: 1, scope: digest, position: [last.origin_id, last.record_id] })) : null };
  };
  let out = build(page, more);
  while (Buffer.byteLength(JSON.stringify(out)) > MAX_PAGE_BYTES) {
    if (out.records.length <= 1) throw new EvidenceRequestError(413, "delivery_too_large", "Retained delivery record exceeds the readback byte limit");
    out = build(out.records.slice(0, -1), true);
  }
  return out;
}

/** The read worker's deps over the read role's tenant runner and the raw store. */
export function sqlEvidenceDeps(
  run: <T>(orgId: string, workspaceId: string, work: (q: { query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: R[] }> }) => Promise<T>) => Promise<T>,
  store: { list(prefix: never, cursor?: string): Promise<{ items: { key: unknown }[]; nextCursor?: string }>; getObject(key: never): Promise<{ body: unknown }> },
  read: (body: unknown, signal: AbortSignal) => Promise<Uint8Array>,
  decompress: (bytes: Uint8Array) => Uint8Array,
  objectKey: (key: string) => never,
  keyRoot: string,
): EvidenceDeps {
  return {
    keyRoot,
    async buckets(scope, filter) {
      const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
        q.query<{ v: unknown }>("SELECT sessions.babysitter_evidence_buckets_for($1, $2::jsonb) AS v", [scope.workspaceId, JSON.stringify(filter)]));
      const v = rows[0]?.v;
      if (!Array.isArray(v)) throw new EvidenceRequestError(503, "backend_unavailable", "read backend unavailable");
      return v as EvidenceBucket[];
    },
    async revisions(scope, keys) {
      const out = new Map<string, { revision: bigint; revisionId: string }>();
      for (let i = 0; i < keys.length; i += 2000) {
        const { rows } = await run(scope.orgId, scope.workspaceId, (q) =>
          q.query<{ v: unknown }>("SELECT sessions.babysitter_evidence_revisions_for($1, $2::jsonb) AS v", [scope.workspaceId, JSON.stringify(keys.slice(i, i + 2000))]));
        const v = rows[0]?.v;
        if (!Array.isArray(v)) throw new EvidenceRequestError(503, "backend_unavailable", "read backend unavailable");
        for (const e of v as [string, string, string, string][]) out.set(keyOf(e[0], e[1]), { revision: BigInt(e[2]), revisionId: e[3] });
      }
      return out;
    },
    async objects(prefix, signal) {
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        const page = await store.list(objectKey(prefix.slice(0, -1)), cursor);
        keys.push(...page.items.map((i) => String(i.key)));
        cursor = page.nextCursor;
      } while (cursor);
      const bodies: Uint8Array[] = [];
      for (const k of keys) bodies.push(decompress(await read((await store.getObject(objectKey(k))).body, signal)));
      return bodies;
    },
  };
}

/** The bucket hour a ref names (exported for tests). */
export function refHour(ref: string): Date {
  return new Date(Date.UTC(Number(ref.slice(3, 7)), Number(ref.slice(7, 9)) - 1, Number(ref.slice(9, 11)), Number(ref.slice(11, 13))));
}
