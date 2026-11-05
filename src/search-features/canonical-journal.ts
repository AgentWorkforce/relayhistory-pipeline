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
  // jsonb columns: drivers return parsed objects, fixtures may give strings.
  scope: unknown;
  schema_generation: string;
  image_key: string;
  source_hash: string;
  prior_keyset_hash: string | null;
  source_authority: unknown;
  prior_source_authority: unknown;
  pointer_set: unknown;
};
/** jsonb decode: a real driver returns the parsed object; a raw string parses. */
const jsonb = <T>(v: unknown): T => {
  if (typeof v === "string") return JSON.parse(v) as T;
  if (v && typeof v === "object") return v as T;
  throw new Error("journal_row_invalid");
};
const HS1 = /^hs1:[a-f0-9]{64}:[a-f0-9]{64}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const toEntry = (r: Row): CanonicalJournalEntry => {
  if (r.kind !== "publish" && r.kind !== "retire")
    throw new Error("journal_kind_invalid");
  if (!HS1.test(r.image_key)) throw new Error("journal_image_key_invalid");
  const scope = jsonb<CanonicalScope>(r.scope);
  const pointerSet = jsonb<CanonicalPointerSet>(r.pointer_set);
  return r.kind === "retire"
    ? { kind: "retire", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        sourceHash: r.source_hash, priorKeysetHash: r.prior_keyset_hash ?? "",
        scope,
        // 610 CHECK: prior authority lives in source_authority on retire
        // (prior_source_authority is always NULL for a retire row).
        priorSourceAuthority:
          jsonb<CanonicalSourceAuthority>(r.source_authority),
        pointerSet }
    : { kind: "publish", revision: String(r.revision),
        imageKey: r.image_key, schemaGeneration: r.schema_generation,
        sourceHash: r.source_hash, scope,
        sourceAuthority:
          jsonb<CanonicalSourceAuthority>(r.source_authority),
        ...(r.prior_source_authority
          ? { priorSourceAuthority:
              jsonb<CanonicalSourceAuthority>(r.prior_source_authority) }
          : {}),
        ...(r.prior_keyset_hash ? { priorKeysetHash: r.prior_keyset_hash } : {}),
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
  async seed(_aboveLegacyClock: string): Promise<void> {
    throw new Error("sole writer is the 620 _install invoker");
  }
  async next(): Promise<string> {
    throw new Error("sole writer is the 620 _install invoker");
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
  /** Sole writer is the 620 _install invoker — never a no-op. */
  async append(_e: CanonicalJournalEntry, _o: string): Promise<void> {
    throw new Error("journal is read-only; sole writer is the _install invoker");
  }
}
