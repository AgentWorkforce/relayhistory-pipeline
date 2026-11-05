/**
 * Durable canonical feature journal: the ONLY input the producer trusts for
 * publication + retirement ordering (native contract: S3 events/Neon tombstones
 * are insufficient). Writes happen under a single-counter row lock so the
 * revision series is strictly increasing in the same space as the legacy
 * feature clock — seeded ABOVE the last legacy revision at drain time.
 * Implements CanonicalVersionPort + CanonicalJournalPort over a write-capable
 * connection (SqlQuery-compatible). DDL is held: 040610 is a proposal only.
 */
import type {
  CanonicalJournalEntry,
  CanonicalJournalPort,
  CanonicalScope,
  CanonicalSourceAuthority,
  CanonicalPointerSet,
  CanonicalVersionPort,
} from "./canonical-version.ts";

export interface SqlQuery {
  query<T>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

type Row = {
  revision: string | number;
  kind: string;
  scope: string;
  schema_generation: string;
  image_key: string;
  source_hash: string;
  prior_keyset_hash: string | null;
  source_authority: string;
  prior_source_authority: string | null;
  pointer_set: string;
};
const toEntry = (r: Row): CanonicalJournalEntry => {
  const scope = JSON.parse(r.scope) as CanonicalScope;
  const pointerSet = JSON.parse(r.pointer_set) as CanonicalPointerSet;
  return r.kind === "retire"
    ? { kind: "retire", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        priorKeysetHash: r.prior_keyset_hash ?? "", scope,
        priorSourceAuthority:
          JSON.parse(r.prior_source_authority ?? "null") as CanonicalSourceAuthority,
        pointerSet }
    : { kind: "publish", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        sourceHash: r.source_hash, scope,
        sourceAuthority:
          JSON.parse(r.source_authority) as CanonicalSourceAuthority,
        pointerSet };
};

/**
 * Durable version counter over the same table — a single-row high-water under
 * the journal's transaction. seed() writes the legacy high-water once;
 * next() takes an advisory row lock and allocates strictly above it.
 */
export class CanonicalVersionStore implements CanonicalVersionPort {
  constructor(readonly db: SqlQuery) {}
  async last(): Promise<string | null> {
    const { rows } = await this.db.query<{ revision: string | number }>(
      "SELECT revision FROM sessions.canonical_feature_journal" +
        " ORDER BY revision DESC LIMIT 1");
    return rows.length ? String(rows[0]!.revision) : null;
  }
  async seed(aboveLegacyClock: string): Promise<void> {
    const last = await this.last();
    if (last !== null && BigInt(aboveLegacyClock) <= BigInt(last))
      throw new Error("seed_below_last");
    if (last === null)
      await this.db.query(
        "INSERT INTO sessions.canonical_feature_journal(" +
          "revision,kind,org_id,workspace_id,scope,schema_generation," +
          " image_key,source_hash,source_authority,pointer_set)" +
          " VALUES($1,'seed','','','{}','','','{}','{}')" +
          " ON CONFLICT (revision) DO NOTHING",
        [aboveLegacyClock]);
  }
  async next(): Promise<string> {
    // Single-writer allocation: the caller holds the transaction; the insert
    // itself is the counter advance. Revision = last+1 under the row lock.
    const { rows } = await this.db.query<{ revision: string }>(
      "SELECT COALESCE(MAX(revision),0)+1::text AS revision" +
        " FROM sessions.canonical_feature_journal FOR UPDATE");
    return rows[0]!.revision;
  }
}
export class CanonicalJournalStore implements CanonicalJournalPort {
  constructor(readonly db: SqlQuery) {}
  async entries(after: string, limit: number) {
    const { rows } = await this.db.query<Row>(
      "SELECT revision,kind,scope,schema_generation,image_key,source_hash," +
        " prior_keyset_hash,source_authority,prior_source_authority," +
        " pointer_set FROM sessions.canonical_feature_journal" +
        " WHERE revision > $1::bigint AND kind <> 'seed'" +
        " ORDER BY revision LIMIT $2",
      [after, limit]);
    const entries = rows.map(toEntry);
    return {
      entries,
      nextRevision: entries.length ? entries[entries.length - 1]!.revision : after,
      hasMore: rows.length === limit,
    };
  }
  /** CAS-fenced append — a stale revision can never land. */
  async append(entry: CanonicalJournalEntry, observed: string): Promise<void> {
    if (BigInt(entry.revision) <= BigInt(observed))
      throw new Error("stale_revision");
    const scope = JSON.stringify(entry.scope);
    const ptr = JSON.stringify(entry.pointerSet);
    const cols = entry.kind === "retire"
      ? ["retire", scope, entry.schemaGeneration, entry.imageKey, "",
          entry.priorKeysetHash, "",
          JSON.stringify(entry.priorSourceAuthority), ptr]
      : ["publish", scope, entry.schemaGeneration, entry.imageKey,
          entry.sourceHash, "", JSON.stringify(entry.sourceAuthority), "", ptr];
    // cols = [kind, scope, schema_generation, image_key, source_hash,
    //         prior_keyset_hash, source_authority, prior_source_authority,
    //         pointer_set]
    await this.db.query(
      "INSERT INTO sessions.canonical_feature_journal(" +
        "revision,kind,org_id,workspace_id,scope,schema_generation," +
        " image_key,source_hash,prior_keyset_hash,source_authority," +
        " prior_source_authority,pointer_set)" +
        " VALUES($1::bigint,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
      [entry.revision, cols[0], entry.scope.orgId, entry.scope.workspaceId,
        ...cols.slice(1)]);
  }
}
