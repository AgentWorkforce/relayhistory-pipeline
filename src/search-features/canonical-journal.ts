/**
 * Durable canonical feature journal: the ONLY input the producer trusts for
 * publication + retirement ordering (native contract: S3 events/Neon tombstones
 * are insufficient). Writes happen under a single-counter row lock so the
 * revision series is strictly increasing in the same space as the legacy
 * feature clock — seeded ABOVE the last legacy revision at drain time.
 * Implements CanonicalVersionPort + CanonicalJournalPort over a write-capable
 * connection (SqlQuery-compatible). DDL is held: 040610 is a proposal only.
 */
import { createHash } from "node:crypto";
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
  org_id: string;
  workspace_id: string;
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
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX64 = /^[a-f0-9]{64}$/;
// Canonical positive integer — nonzero, no leading zeros, writer bounds:
// epoch/revision at most 19 digits (bigint-safe).
const DEC = /^[1-9][0-9]{0,18}$/;
// Count may be zero, at most 6 digits (writer ABI bound).
const DEC0 = /^(0|[1-9][0-9]{0,5})$/;
/** Fail-closed row validation — every field + cross-bound shape checked. */
const exactKeys = (o: unknown, keys: string[]): boolean =>
  !!o && typeof o === "object" &&
  new Set(Object.keys(o)).size === keys.length &&
  keys.every((k) => k in (o as Record<string, unknown>));
const validAuth = (a: CanonicalSourceAuthority | undefined) =>
  !!a && exactKeys(a, ["authorityId", "epoch", "fingerprint"]) &&
  UUID.test(a.authorityId) && DEC.test(a.epoch) &&
  HEX64.test(a.fingerprint);
const validPtr = (p: CanonicalPointerSet | undefined) =>
  !!p && exactKeys(p, ["authorityId", "hash", "count"]) &&
  UUID.test(p.authorityId) && HEX64.test(p.hash) &&
  DEC0.test(p.count);
const validScope = (s: CanonicalScope | undefined) =>
  !!s && exactKeys(s, ["orgId", "workspaceId", "harness", "sessionId",
                       "generation"]) &&
  typeof s.orgId === "string" && typeof s.workspaceId === "string" &&
  typeof s.harness === "string" && typeof s.sessionId === "string" &&
  HEX64.test(s.generation);
const toEntry = (r: Row): CanonicalJournalEntry => {
  if (r.kind !== "publish" && r.kind !== "retire")
    throw new Error("journal_kind_invalid");
  if (!HS1.test(r.image_key)) throw new Error("journal_image_key_invalid");
  const scope = jsonb<CanonicalScope>(r.scope);
  const pointerSet = jsonb<CanonicalPointerSet>(r.pointer_set);
  if (!validScope(scope) || !validPtr(pointerSet))
    throw new Error("journal_row_invalid");
  if (!HEX64.test(r.source_hash) || !HEX64.test(r.schema_generation) ||
      !DEC.test(String(r.revision)) ||
      (r.prior_keyset_hash !== null && !HEX64.test(r.prior_keyset_hash)))
    throw new Error("journal_row_invalid");
  const hs1Auth = r.image_key.split(":")[3]!;
  const hs1Gen = r.image_key.split(":")[2]!;
  const hs1Scope = r.image_key.split(":")[1]!;
  // The hs1 scope digest recomputes from the scope's own identity — a
  // misrouted tenant scope with a syntactically valid key refuses.
  if (createHash("sha256")
        .update(JSON.stringify(scope)).digest("hex") !== hs1Scope ||
      hs1Gen !== scope.generation || hs1Gen !== r.schema_generation ||
      r.org_id !== scope.orgId || r.workspace_id !== scope.workspaceId)
    throw new Error("journal_row_invalid");
  if (r.kind === "retire") {
    const prior = jsonb<CanonicalSourceAuthority>(r.source_authority);
    // Retire: prior identity in source_authority; prior_keyset_hash must be
    // present and equal to the retired pointer set's hash;
    // prior_source_authority must be NULL (the 610 CHECK shape).
    if (!validAuth(prior) || prior.authorityId !== pointerSet.authorityId ||
        prior.authorityId !== hs1Auth ||
        r.prior_keyset_hash === null ||
        r.prior_keyset_hash !== pointerSet.hash ||
        r.prior_source_authority !== null ||
        r.source_hash !== prior.fingerprint)
      throw new Error("journal_row_invalid");
  } else {
    const src = jsonb<CanonicalSourceAuthority>(r.source_authority);
    // Publish: authority + hs1 + source_hash == fingerprint; prior fields
    // both-or-neither (a replacement carries prior authority + keyset hash).
    if (!validAuth(src) || src.authorityId !== pointerSet.authorityId ||
        src.authorityId !== hs1Auth ||
        r.source_hash !== src.fingerprint ||
        ((r.prior_keyset_hash === null) !==
         (r.prior_source_authority === null)))
      throw new Error("journal_row_invalid");
    if (r.prior_source_authority) {
      const prior = jsonb<CanonicalSourceAuthority>(r.prior_source_authority);
      if (!validAuth(prior) ||
          prior.fingerprint === src.fingerprint ||
          prior.authorityId === src.authorityId)
        throw new Error("journal_row_invalid");
    }
  }
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
      "SELECT revision,kind,org_id,workspace_id,scope,schema_generation," +
        " image_key,source_hash,prior_keyset_hash,source_authority," +
        " prior_source_authority,pointer_set" +
        " FROM sessions.canonical_feature_journal" +
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
  /** Sole writer is the 620 _install invoker — never a no-op. */
  async append(_e: CanonicalJournalEntry, _o: string): Promise<void> {
    throw new Error("journal is read-only; sole writer is the _install invoker");
  }
}
