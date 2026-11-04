// GENERATED from relayhistory-cloud packages/relayhistory/src/lib/search-feature-journal.ts
// Source commit: d953f2b2974438dd74c7cd4b7daaddbd05a6cf24; SHA256: ee32443826d955142d7279008cbabe68c0fcdfc094cd843a917dae2172eb2026
// Regenerate/check: bun scripts/sync-search-feature-source.ts --source <RHC-worktree> [--check]
/** Readonly legacy feature source API. No connection, token, model or writes. */
export type FeatureKind = "event" | "brief";
export interface FeatureReader {
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}
export interface FeatureReference {
  kind: FeatureKind;
  key: Record<string, string | null>;
  revision: string;
  sourceHash: string;
  deleted: boolean;
}
export interface FeatureSnapshot {
  reference: FeatureReference;
  row: Record<string, unknown> | null;
}
export interface FeatureJournalPage {
  changes: FeatureReference[];
  nextRevision: string;
  observedRevision: string;
}
export class FeatureJournalError extends Error {
  constructor(
    readonly reason: string,
    readonly code?: string,
  ) {
    super("Semantic feature source is unavailable");
    this.name = "FeatureJournalError";
  }
}
async function sourceQuery<T>(
  db: FeatureReader,
  query: string,
  params?: unknown[],
): Promise<{ rows: T[] }> {
  try {
    return await db.query<T>(query, params);
  } catch (error) {
    const raw = (error as { code?: unknown })?.code;
    throw new FeatureJournalError(
      "query_failed",
      typeof raw === "string" && /^[0-9A-Z]{5}$/.test(raw) ? raw : undefined,
    );
  }
}
const fail = (r: string): never => {
  throw new FeatureJournalError(r);
};
const decimal = (v: unknown): v is string =>
  typeof v === "string" &&
  /^(0|[1-9][0-9]{0,18})$/.test(v) &&
  BigInt(v) <= 9223372036854775807n;
function kind(k: FeatureKind) {
  if (k !== "event" && k !== "brief") return fail("kind_invalid");
}
function limit(n: number) {
  if (!Number.isInteger(n) || n < 1 || n > 200) return fail("batch_invalid");
}
function reference(v: any): FeatureReference {
  if (
    !v ||
    typeof v !== "object" ||
    !v.key ||
    typeof v.key !== "object" ||
    Array.isArray(v.key) ||
    !decimal(v.revision) ||
    typeof v.sourceHash !== "string" ||
    !/^[0-9a-f]{64}$/.test(v.sourceHash) ||
    typeof v.deleted !== "boolean"
  )
    return fail("reference_invalid");
  kind(v.kind);
  const fields =
    v.kind === "event"
      ? [
          "orgId",
          "workspaceId",
          "source",
          "sessionId",
          "machineId",
          "kind",
          "eventId",
          "ts",
        ]
      : ["orgId", "workspaceId", "source", "sessionId"];
  if (
    Object.keys(v.key).length !== fields.length ||
    fields.some(
      (k) =>
        !(k in v.key) ||
        !(
          typeof v.key[k] === "string" ||
          (k === "workspaceId" && v.key[k] === null)
        ),
    )
  )
    return fail("key_invalid");
  return {
    ...v,
    key: Object.fromEntries(
      Object.entries(v.key).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
    ),
  };
}
export function featureVersion(revision: string): string {
  if (!decimal(revision)) return fail("revision_invalid");
  return (2n * BigInt(revision) + 1n).toString();
}
export async function readFeatureJournalPage(
  db: FeatureReader,
  k: FeatureKind,
  afterRevision: string,
  batch = 100,
): Promise<FeatureJournalPage> {
  kind(k);
  limit(batch);
  if (!decimal(afterRevision)) return fail("revision_invalid");
  const result = (
    await sourceQuery<{ p: any }>(
      db,
      `WITH observed AS MATERIALIZED(SELECT revision FROM sessions.search_feature_clock WHERE singleton),
 selected AS MATERIALIZED(SELECT c.* FROM sessions.search_feature_changes c,observed o WHERE c.feature_type=$1 AND c.revision>$2::bigint AND c.revision<=o.revision ORDER BY c.revision LIMIT $3)
 SELECT jsonb_build_object('observedRevision',o.revision::text,'rows',coalesce((SELECT jsonb_agg(jsonb_build_object('kind',s.feature_type,'key',s.feature_key,'revision',s.revision::text,'sourceHash',s.source_hash,'deleted',s.deleted) ORDER BY s.revision) FROM selected s),'[]'::jsonb)) p FROM observed o`,
      [k, afterRevision, batch + 1],
    )
  ).rows[0]?.p;
  if (
    !result ||
    !decimal(result.observedRevision) ||
    BigInt(result.observedRevision) < BigInt(afterRevision) ||
    !Array.isArray(result.rows) ||
    result.rows.length > batch + 1
  )
    return fail("page_invalid");
  const all = result.rows.map(reference),
    changes = all.slice(0, batch);
  let previous = BigInt(afterRevision);
  for (const r of all) {
    if (
      r.kind !== k ||
      BigInt(r.revision) <= previous ||
      BigInt(r.revision) > BigInt(result.observedRevision)
    )
      return fail("page_invalid");
    previous = BigInt(r.revision);
  }
  return {
    changes,
    observedRevision: result.observedRevision,
    nextRevision:
      all.length > batch ? changes.at(-1)!.revision : result.observedRevision,
  };
}
const payloadHashSql =
  "encode(sha256(convert_to(coalesce(payload,jsonb_build_array('deleted',kind,key))::text,'UTF8')),'hex')";
export async function readFeatureSnapshots(
  db: FeatureReader,
  refs: FeatureReference[],
): Promise<FeatureSnapshot[]> {
  if (refs.length > 200) return fail("batch_invalid");
  if (!refs.length) return [];
  const wanted = refs.map(reference),
    keys = new Set<string>();
  for (const r of wanted) {
    const k = JSON.stringify([r.kind, r.key]);
    if (keys.has(k)) return fail("duplicate");
    keys.add(k);
  }
  const rows = (
    await sourceQuery<any>(
      db,
      `WITH wanted AS MATERIALIZED(SELECT x.kind,x.key,x.ordinal FROM jsonb_to_recordset($1::jsonb) AS x(kind text,key jsonb,ordinal integer)),
 current AS MATERIALIZED(SELECT w.*,h.revision,h.source_hash,h.deleted,sessions.search_feature_payload(w.kind,w.key) payload
 FROM wanted w LEFT JOIN sessions.search_feature_heads h ON h.feature_type=w.kind AND h.feature_key=w.key)
 SELECT kind,key,ordinal,revision::text,source_hash,deleted,payload,${payloadHashSql} actual_hash FROM current ORDER BY ordinal`,
      [
        JSON.stringify(
          wanted.map((r, ordinal) => ({ kind: r.kind, key: r.key, ordinal })),
        ),
      ],
    )
  ).rows;
  if (rows.length !== wanted.length) return fail("snapshot_missing");
  return rows.map((r, n) => {
    const original = wanted[n]!;
    if (
      r.ordinal !== n ||
      r.kind !== original.kind ||
      JSON.stringify(
        Object.fromEntries(
          Object.entries(r.key).sort(([a], [b]) =>
            a < b ? -1 : a > b ? 1 : 0,
          ),
        ),
      ) !== JSON.stringify(original.key)
    )
      return fail("snapshot_invalid");
    if (r.revision === null) {
      if (original.revision !== "0") return fail("snapshot_missing");
      r.revision = "0";
      r.source_hash = r.actual_hash;
      r.deleted = r.payload === null;
    }
    if (r.source_hash !== r.actual_hash || r.deleted !== (r.payload === null))
      return fail("source_changed");
    const ref = reference({
      kind: r.kind,
      key: r.key,
      revision: r.revision,
      sourceHash: r.source_hash,
      deleted: r.deleted,
    });
    if (
      r.payload !== null &&
      (typeof r.payload !== "object" || Array.isArray(r.payload))
    )
      return fail("snapshot_invalid");
    return { reference: ref, row: r.payload };
  });
}
const eventColumns = [
    "org_id",
    "machine_id",
    "source",
    "session_id",
    "kind",
    "event_id",
  ] as const,
  briefColumns = ["org_id", "session_id"] as const;
export async function readFeatureBaselinePage(
  db: FeatureReader,
  k: FeatureKind,
  cursor: string | null,
  batch = 100,
): Promise<{ rows: FeatureSnapshot[]; nextCursor: string | null }> {
  kind(k);
  limit(batch);
  const columns = k === "event" ? eventColumns : briefColumns;
  let after: string[] | null = null;
  if (cursor !== null) {
    try {
      const v = JSON.parse(cursor);
      if (
        v.kind !== k ||
        !Array.isArray(v.after) ||
        v.after.length !== columns.length ||
        v.after.some((s: unknown) => typeof s !== "string")
      )
        return fail("cursor_invalid");
      after = v.after;
    } catch {
      return fail("cursor_invalid");
    }
  }
  const table = k === "event" ? "convergence_events" : "session_brief_search",
    keyFn =
      k === "event" ? "search_feature_event_key" : "search_feature_brief_key",
    payloadFn =
      k === "event"
        ? "search_feature_event_payload"
        : "search_feature_brief_payload";
  const tuple = columns.map((c) => `s.${c}`).join(","),
    params: unknown[] = [batch + 1];
  const where = after
    ? `AND (${tuple})>(${after
        .map((v, n) => {
          params.push(v);
          return `$${n + 2}::text`;
        })
        .join(",")})`
    : "";
  const rows = (
    await sourceQuery<any>(
      db,
      `WITH source AS MATERIALIZED(SELECT sessions.${keyFn}(s) key,sessions.${payloadFn}(s) payload,${columns.map((c) => `s.${c}`).join(",")} FROM sessions.${table} s WHERE true ${where} ORDER BY ${tuple} LIMIT $1)
 SELECT s.*,encode(sha256(convert_to(coalesce(payload,jsonb_build_array('deleted','${k}',key))::text,'UTF8')),'hex') actual_hash FROM source s ORDER BY ${tuple}`,
      params,
    )
  ).rows;
  if (rows.length > batch + 1) return fail("baseline_invalid");
  const selected = rows.slice(0, batch);
  const refs = selected.map((r) =>
    reference({
      kind: k,
      key: r.key,
      revision: "0",
      sourceHash: r.actual_hash,
      deleted: r.payload === null,
    }),
  );
  const snapshots = await readFeatureSnapshots(db, refs);
  const last = selected.at(-1),
    nextCursor =
      rows.length > batch && last
        ? JSON.stringify({ kind: k, after: columns.map((c) => last[c]) })
        : null;
  return { rows: snapshots, nextCursor };
}
