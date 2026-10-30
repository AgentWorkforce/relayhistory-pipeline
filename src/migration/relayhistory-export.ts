/**
 * Neon to S3 session export, core (docs/relayhistory-export-design.md, P2).
 * Reads RelayHistory's delivery records for one session from Neon and turns
 * them into the session's raw stream: one ndjson line per live record, ordered
 * by event time, split at record boundaries into chunks of at most 4 MiB.
 * Upload goes through the courier protocol (ingest scrub hop, v1 manifest,
 * pipeline notification), added once identity (O2) is settled. Nothing here
 * writes anywhere; the reader runs on the read-only Neon role.
 */
export interface Queryable {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
}

/** The record kinds a session's stream carries (every delivery kind with a session). */
export const RECORD_KINDS = [
  "session",
  "session_event",
  "tool_call",
  "file_edit",
  "history",
  "session_marker",
  "relationship",
  "presence",
  "source_observation",
  "observation_evidence",
] as const;

export const MAX_CHUNK_BYTES = 4 * 1024 * 1024;

export interface DeliveryRecord {
  kind: string;
  origin_id: string;
  record_id: string;
  revision: string;
  operation: "upsert" | "delete";
  received_at: string;
  payload: Record<string, unknown> | null;
}

/** Event time of a record, in ms: the payload's own clock, else when it was received. */
export function eventTimeMs(record: DeliveryRecord): number {
  const payload = record.payload ?? {};
  for (const field of ["ts_ms", "timestamp_ms", "created_ms", "spawned_at_ms", "updated_ms", "first_activity_ms", "last_activity_ms"]) {
    const value = payload[field];
    if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
    if (typeof value === "string" && /^\d{10,16}$/.test(value)) return Number(value);
  }
  return Date.parse(record.received_at);
}

export interface SessionStream {
  bytes: Uint8Array;
  /** Byte offset where each record's line starts, plus the total length at the end. */
  lineStarts: number[];
  records: number;
  firstMs: number | null;
  lastMs: number | null;
}

/**
 * The session's raw stream: live records only (a record whose latest revision
 * is a delete is left out, so deleted content is never exported), ordered by
 * event time then origin and record id, one JSON line each. Deterministic, so
 * a re-export produces identical chunk hashes.
 */
export function buildSessionStream(records: readonly DeliveryRecord[]): SessionStream {
  const live = records
    .filter((record) => record.operation === "upsert" && record.payload !== null)
    .map((record) => ({ record, at: eventTimeMs(record) }))
    .sort(
      (a, b) =>
        a.at - b.at ||
        (a.record.origin_id < b.record.origin_id ? -1 : a.record.origin_id > b.record.origin_id ? 1 : 0) ||
        (a.record.record_id < b.record.record_id ? -1 : a.record.record_id > b.record.record_id ? 1 : 0),
    );
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  const lineStarts: number[] = [];
  let length = 0;
  for (const { record } of live) {
    const line = encoder.encode(
      `${JSON.stringify({
        kind: record.kind,
        origin_id: record.origin_id,
        record_id: record.record_id,
        revision: record.revision,
        received_at: record.received_at,
        payload: record.payload,
      })}\n`,
    );
    lineStarts.push(length);
    parts.push(line);
    length += line.byteLength;
  }
  lineStarts.push(length);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return {
    bytes,
    lineStarts,
    records: live.length,
    firstMs: live.length ? live[0]!.at : null,
    lastMs: live.length ? live.at(-1)!.at : null,
  };
}

export interface ChunkRange {
  start: number;
  end: number;
  records: number;
}

/**
 * Split at record boundaries into ranges of at most maxBytes. A single record
 * larger than maxBytes becomes its own range (a chunk never splits a record).
 */
export function chunkRanges(stream: SessionStream, maxBytes = MAX_CHUNK_BYTES): ChunkRange[] {
  const ranges: ChunkRange[] = [];
  const starts = stream.lineStarts;
  let first = 0;
  while (first < starts.length - 1) {
    let last = first + 1;
    while (last < starts.length - 1 && starts[last + 1]! - starts[first]! <= maxBytes) last += 1;
    ranges.push({ start: starts[first]!, end: starts[last]!, records: last - first });
    first = last;
  }
  return ranges;
}

export interface SessionKey {
  org_id: string;
  workspace_id: string;
  source: string;
  session_id: string;
  last_ts: string;
}

/**
 * Sessions newest first, by their latest activity across rollups, keyset-paged
 * on (last_ts, org, workspace, source, session) descending.
 */
export async function sessionsNewestFirst(
  db: Queryable,
  after: SessionKey | null,
  limit: number,
): Promise<SessionKey[]> {
  const { rows } = await db.query<SessionKey>(
    `SELECT org_id, workspace_id, source, session_id, max(last_ts)::text AS last_ts
       FROM sessions.session_rollups
      GROUP BY org_id, workspace_id, source, session_id
     ${after ? "HAVING (max(last_ts), org_id, workspace_id, source, session_id) < ($2::timestamptz, $3, $4, $5, $6)" : ""}
      ORDER BY max(last_ts) DESC, org_id DESC, workspace_id DESC, source DESC, session_id DESC
      LIMIT $1`,
    after
      ? [limit, after.last_ts, after.org_id, after.workspace_id, after.source, after.session_id]
      : [limit],
  );
  return rows;
}

/** A session's delivery records (one row per record: its latest revision). */
export async function sessionRecords(db: Queryable, key: SessionKey): Promise<DeliveryRecord[]> {
  const { rows } = await db.query<DeliveryRecord>(
    `SELECT kind, origin_id, record_id, revision::text, operation, received_at::text, payload
       FROM sessions.delivery_records
      WHERE org_id = $1 AND workspace_id = $2 AND kind = ANY ($3::text[])
        AND source = $4 AND session_id = $5`,
    [key.org_id, key.workspace_id, [...RECORD_KINDS], key.source, key.session_id],
  );
  return rows;
}

/**
 * The machine each origin's records came from, read from the session's events
 * (which carry machine_id and link back to their delivery record). An origin
 * with no linked event maps to `unknown-origin-<origin_id>`, so its records
 * never merge into another machine's stream.
 */
export async function originMachines(db: Queryable, key: SessionKey): Promise<Map<string, string>> {
  const { rows } = await db.query<{ origin_id: string; machine_id: string }>(
    `SELECT DISTINCT ON (d.origin_id) d.origin_id, e.machine_id
       FROM sessions.convergence_events e
       JOIN sessions.delivery_records d
         ON d.org_id = e.org_id AND d.workspace_id = e.workspace_id AND d.record_id = e.delivery_record_id
      WHERE e.org_id = $1 AND e.session_id = $2 AND e.workspace_id = $3 AND e.source = $4
      ORDER BY d.origin_id, e.machine_id`,
    [key.org_id, key.session_id, key.workspace_id, key.source],
  );
  return new Map(rows.map((row) => [row.origin_id, row.machine_id]));
}

export const unknownOriginMachine = (originId: string) =>
  `unknown-origin-${originId.replace(/[^A-Za-z0-9._-]/g, "_")}`.slice(0, 128);

/** Group a session's records by machine: one exported stream per machine. */
export function byMachine(
  records: readonly DeliveryRecord[],
  machines: ReadonlyMap<string, string>,
): Map<string, DeliveryRecord[]> {
  const groups = new Map<string, DeliveryRecord[]>();
  for (const record of records) {
    const machine = machines.get(record.origin_id) ?? unknownOriginMachine(record.origin_id);
    const list = groups.get(machine) ?? [];
    list.push(record);
    groups.set(machine, list);
  }
  return groups;
}
