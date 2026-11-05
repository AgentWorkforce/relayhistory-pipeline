/**
 * Read-only adapter for the canonical feature journal installed by 040610.
 * The owner-only 620 final-CAS transaction is the sole live writer; a producer
 * must never allocate revisions or append rows itself. S3 events and legacy
 * Neon tombstones are not publication authority.
 */
import type {
  CanonicalJournalEntry,
  CanonicalJournalPort,
  CanonicalScope,
  CanonicalSourceAuthority,
  CanonicalPointerSet,
} from "./canonical-version.ts";

export interface SqlQuery {
  query<T>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

type Row = {
  revision: string | number;
  kind: string;
  scope: unknown;
  schema_generation: string;
  image_key: string;
  source_hash: string;
  prior_keyset_hash: string | null;
  source_authority: unknown;
  prior_source_authority: unknown | null;
  pointer_set: unknown;
};
const jsonObject = (value: unknown): Record<string, unknown> => {
  let parsed: unknown;
  try {
    parsed = typeof value === "string" ? JSON.parse(value) : value;
  } catch {
    throw new Error("canonical_journal_invalid_row");
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    throw new Error("canonical_journal_invalid_row");
  return parsed as Record<string, unknown>;
};
const stringField = (value: unknown) => {
  if (typeof value !== "string" || value.length === 0)
    throw new Error("canonical_journal_invalid_row");
  return value;
};
const hashField = (value: unknown) => {
  const text = stringField(value);
  if (!/^[a-f0-9]{64}$/.test(text))
    throw new Error("canonical_journal_invalid_row");
  return text;
};
const authority = (value: unknown): CanonicalSourceAuthority => {
  const parsed = jsonObject(value);
  const epoch = stringField(parsed.epoch);
  if (!/^[1-9][0-9]{0,18}$/.test(epoch))
    throw new Error("canonical_journal_invalid_row");
  return { epoch, fingerprint: hashField(parsed.fingerprint) };
};
const pointerSet = (value: unknown): CanonicalPointerSet => {
  const parsed = jsonObject(value);
  const rawCount = parsed.count;
  const count = typeof rawCount === "number" ? rawCount
    : typeof rawCount === "string" && /^(0|[1-9][0-9]{0,9})$/.test(rawCount)
      ? Number(rawCount) : Number.NaN;
  if (!Number.isSafeInteger(count) || count < 0 || count > 1_000_000_000)
    throw new Error("canonical_journal_invalid_row");
  return { hash: hashField(parsed.hash), count };
};
const toEntry = (r: Row): CanonicalJournalEntry => {
  const rawScope = jsonObject(r.scope);
  const scope: CanonicalScope = {
    orgId: stringField(rawScope.orgId),
    workspaceId: stringField(rawScope.workspaceId),
    harness: stringField(rawScope.harness),
    sessionId: stringField(rawScope.sessionId),
  };
  const scopeGeneration = hashField(rawScope.generation);
  const revision = String(r.revision);
  if (!/^[1-9][0-9]*$/.test(revision))
    throw new Error("canonical_journal_invalid_row");
  const common = {
    revision,
    imageKey: stringField(r.image_key),
    schemaGeneration: hashField(r.schema_generation),
    scope,
    pointerSet: pointerSet(r.pointer_set),
  };
  if (common.imageKey !== `${scope.orgId}:${scope.sessionId}` ||
      common.schemaGeneration !== scopeGeneration)
    throw new Error("canonical_journal_invalid_row");
  return r.kind === "retire"
    ? (() => {
        const priorSourceAuthority = authority(r.prior_source_authority);
        const priorKeysetHash = hashField(r.prior_keyset_hash);
        if (priorSourceAuthority.fingerprint !== priorKeysetHash)
          throw new Error("canonical_journal_invalid_row");
        return { kind: "retire" as const, ...common, priorKeysetHash,
          priorSourceAuthority };
      })()
    : r.kind === "publish"
      ? (() => {
          const sourceAuthority = authority(r.source_authority);
          const sourceHash = hashField(r.source_hash);
          if (sourceAuthority.fingerprint !== sourceHash)
            throw new Error("canonical_journal_invalid_row");
          return { kind: "publish" as const, ...common, sourceHash,
            sourceAuthority };
        })()
      : (() => { throw new Error("canonical_journal_invalid_row"); })();
};
export class CanonicalJournalStore implements CanonicalJournalPort {
  constructor(readonly db: SqlQuery) {}
  async entries(after: string, limit: number) {
    if (!/^(0|[1-9][0-9]*)$/.test(after) ||
        !Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("canonical_journal_invalid_page");
    const { rows } = await this.db.query<Row>(
      "SELECT revision::text AS revision,kind,scope,schema_generation,image_key,source_hash," +
        " prior_keyset_hash,source_authority,prior_source_authority," +
        " pointer_set FROM sessions.canonical_feature_journal" +
        " WHERE revision > $1::bigint AND kind <> 'seed'" +
        " ORDER BY revision LIMIT $2",
      [after, limit + 1]);
    const hasMore = rows.length > limit;
    const entries = rows.slice(0, limit).map(toEntry);
    return {
      entries,
      nextRevision: entries.length ? entries[entries.length - 1]!.revision : after,
      hasMore,
    };
  }
}
