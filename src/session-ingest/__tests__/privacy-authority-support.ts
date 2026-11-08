/**
 * C3 fixtures: the full Supabase migration stack (040000 lazy source tables, 041000 privacy authority) plus the post-apply index job,
 * and a minimal "Neon" with relayhistory's exact privacy tables (0038 / 0039 / 0078 DDL).
 */
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { sqlLedger } from "../ledger-sql.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import type { CurrentCustodyPacket } from "../custody-protocol.ts";
import type { Queryable } from "../../migration/relayhistory-export.ts";
import { TENANT, scratchStore } from "./support.ts";

export const ROOT = join(import.meta.dir, "../../..");
export const generation = "a".repeat(64), epoch = "3".repeat(64), identity = "c".repeat(64), baseline = "a".repeat(64);
export const PENDING_SQL = "supabase/migrations/20261105041000_ingest_privacy_authority.sql";
export const OWNER_BUFFERED_SQL = "supabase/migrations/20261105043700_owner_buffered_read.sql";
/** 043860's creator precondition requires 043700's sessions.owner_buffered_parity. */
export const BUFFERED_SEARCH_GATE_SQL = "supabase/migrations/20261105043860_buffered_search_gate.sql";
/** 043870's creator precondition requires 041000's sessions.ingest_privacy_authority. */
export const BUFFERED_SEARCH_FEED_SQL = "supabase/migrations/20261105043870_buffered_search_feed_cursor.sql";
/** 043890 replaces 043860's gate, so its creator precondition requires 043860. */
export const BUFFERED_SEARCH_GATE_RETIRED_SQL = "supabase/migrations/20261105043890_buffered_search_gate_retired_witness.sql";
/** Independent prerequisite fix (also needed by mirror/frozen-supabase without C3). */
export const LAZY_SOURCE_SQL = "supabase/migrations/20261105040000_ingest_lazy_source_tables.sql";
/** C3 post-apply index job (non-transactional in production). */
export const INDEX_SQL = "scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql";
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");

export async function authorityFixture(opts: { pending?: boolean; lazyFix?: boolean; index?: boolean; store?: import("../../shared/vendor.ts").ObjectStore } = {}) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const closers: Array<() => unknown> = [() => db.close()];
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"]) await db.exec(read("supabase/bootstrap/" + f + ".sql"));
  // lazyFix:false reproduces the pre-040000 state (that migration skipped; it touches nothing later ones depend on).
  // pending:false reproduces the state without 041000; 043700's creator precondition requires 041000's ingest_privacy_authority,
  // so it is skipped with it (and 043860, which requires 043700's parity table, 043870, which requires 041000, and 043890, which
  // replaces 043860's gate), while 042000/043000/043500 still apply.
  for (const f of readdirSync(join(ROOT, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) {
    const path = "supabase/migrations/" + f;
    if (opts.pending === false && (path === PENDING_SQL || path === OWNER_BUFFERED_SQL || path === BUFFERED_SEARCH_GATE_SQL || path === BUFFERED_SEARCH_FEED_SQL || path === BUFFERED_SEARCH_GATE_RETIRED_SQL)) continue;
    if (opts.lazyFix === false && path === LAZY_SOURCE_SQL) continue;
    await db.exec(read(path));
  }
  if (opts.pending !== false && opts.index !== false) await db.exec(read(INDEX_SQL));
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','" + generation + "','pass')");
  const owner = async (name: string, p: unknown) => (await db.query("SELECT sessions." + name + "($1::jsonb) AS r", [JSON.stringify(p)])).rows[0] as { r: any };
  await owner("ingest_lazy_policy_set", { baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline, retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 3600000).toISOString(), issuer: "relayhistory-cloud" });
  await owner("ingest_receipt_seat_initialize", { protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z", sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })), allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
  const run = async (s: string, p: unknown[] = []) => {
    await db.exec("BEGIN;SET LOCAL ROLE bulls96_session_ingest");
    try {
      const r = await db.query(s, p as never);
      await db.exec("COMMIT");
      return r as never;
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  };
  const ledger = sqlLedger(run, { acceptanceMode: "buffered", expectedGeneration: generation });
  const fs = scratchStore();
  closers.push(fs.done);
  const deps = { ledger, acceptanceMode: "buffered" as const, target: { store: opts.store ?? fs.store, keyRoot: "96bulls" } };
  const seal = () => db.query("INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')", [identity, epoch, baseline, "e".repeat(64)]);
  const rpc = async (fn: string, p: unknown) => ((await run("SELECT sessions." + fn + "($1::jsonb) AS r", [JSON.stringify(p)])) as any).rows[0].r;
  const ownerRpc = async (fn: string, p?: unknown) => (await db.query("SELECT sessions." + fn + (p === undefined ? "()" : "($1::jsonb)") + " AS r", p === undefined ? [] : [JSON.stringify(p)])).rows[0] as { r: any };
  async function witnessFor(raw: any, kind: "batch" | "selection" | "reset" = "batch", tenant = TENANT) {
    const p = await prepareDelivery(tenant, raw);
    return rpc("ingest_frozen_source_witness", { kind, tenant, originId: p.originId, batchId: p.batchId, batchDigest: p.batchDigest, originalRecordCount: raw.records.length,
      candidates: p.records.map((r, i) => ({ preparedOrd: i, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateDigest: r.digest, source: r.source, sessionId: r.session_id })) }) as Promise<CurrentCustodyPacket>;
  }
  /** A pg-style client over PGlite for the handoff module (BEGIN/COMMIT pass straight through). */
  const queryable: Queryable = { query: async (text, params) => (params?.length ? db.query(text, params as never) : db.query(text)) as never };
  return { db, owner, ownerRpc, run, rpc, ledger, deps, seal, witnessFor, queryable, close: async () => { for (const c of closers.reverse()) await c(); } };
}

/** relayhistory's exact privacy tables (0038 deleted_upload_sessions, 0039 session_resets, 0078 control heads). */
/** relayhistory's privacy tables and the owned-identity sources, for a fake "Neon" (PGlite or a real PG database). */
export function fakeNeonDdl(): string {
  const v38 = read("vendor/relayhistory-cloud/migrations/0038_session_upload_deletion.sql");
  const v39 = read("vendor/relayhistory-cloud/migrations/0039_session_reset.sql");
  const ddl = (src: string, table: string) => {
    const m = new RegExp(`CREATE TABLE sessions\\.${table} \\([\\s\\S]*?\\n\\);`).exec(src)?.[0];
    if (!m) throw new Error(`vendored migration no longer creates ${table}`);
    return m;
  };
  return `CREATE SCHEMA sessions; ${ddl(v38, "deleted_upload_sessions")} ${ddl(v39, "session_resets")}
    -- relayhistory 0078, verbatim.
    CREATE TABLE sessions.delivery_cutover_control_heads (
      org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
      epoch text NOT NULL,version bigint NOT NULL CHECK(version>0),
      digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),
      kind text NOT NULL CHECK(kind IN ('selection','reset')),payload jsonb NOT NULL,
      pending boolean NOT NULL,deleted bigint,
      before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0 CHECK(privacy_version>=0),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      PRIMARY KEY(org_id,workspace_id,user_id)
    );
    -- The tables Neon's owned_uploaded_session_identities() reads (0038 shapes, the columns it uses).
    CREATE TABLE sessions.uploaded_support_identities(org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
      user_id text NOT NULL,source text NOT NULL,session_id text,PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
    CREATE TABLE sessions.delivery_records(org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
      user_id text,source text,session_id text,operation text,PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
    CREATE TABLE sessions.neighborhood_claims(id text PRIMARY KEY,org_id text NOT NULL,support_event_ids jsonb NOT NULL DEFAULT '[]');
    CREATE TABLE sessions.delivery_cutover_state(singleton boolean PRIMARY KEY, phase text, epoch text);
    INSERT INTO sessions.delivery_cutover_state VALUES(true,'active','${epoch}');
    CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql STABLE AS
      $$ SELECT jsonb_build_object('version',1,'phase',phase,'epoch',epoch) FROM sessions.delivery_cutover_state $$;`;
}

export async function fakeNeon() {
  const pg = new PGlite();
  await pg.exec(fakeNeonDdl());
  const queryable: Queryable = { query: async (text, params) => (params?.length ? pg.query(text, params as never) : pg.query(text)) as never };
  const digest = async (kind: string, tenant: typeof TENANT, payload: unknown) =>
    ((await pg.query("SELECT encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',$1::text,$2::text,$3::text,$4::text,$5::text,$6::jsonb)::text,'UTF8')),'hex') AS d",
      [epoch, tenant.orgId, tenant.workspaceId, tenant.userId, kind, JSON.stringify(payload)])).rows[0] as { d: string }).d;
  /** A completed Neon head (what relayhistory leaves after begin -> apply -> fence -> complete). */
  const head = async (tenant: typeof TENANT, version: number, kind: "selection" | "reset", payload: unknown, opts: { pending?: boolean; deleted?: number | null } = {}) => {
    const d = await digest(kind, tenant, payload);
    await pg.query(`INSERT INTO sessions.delivery_cutover_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,deleted,privacy_version)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(org_id,workspace_id,user_id) DO UPDATE SET version=EXCLUDED.version,digest=EXCLUDED.digest,kind=EXCLUDED.kind,
      payload=EXCLUDED.payload,pending=EXCLUDED.pending,deleted=EXCLUDED.deleted,privacy_version=EXCLUDED.privacy_version`,
      [tenant.orgId, tenant.workspaceId, tenant.userId, epoch, version, d, kind, JSON.stringify(payload), opts.pending ?? false, opts.deleted ?? null, version - 1]);
    return d;
  };
  return { pg, queryable, digest, head, close: () => pg.close() };
}

export const err = async (p: Promise<unknown>) => {
  try {
    await p;
    return null;
  } catch (e) {
    return String((e as Error).message);
  }
};
