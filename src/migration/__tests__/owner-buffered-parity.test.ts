import { describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import {
  bufferedParityVisible,
  evaluateParityCensus,
  planProofWrite,
  readParityCensus,
  writeProofRowCas,
  writeCurrentParityProof,
  type OwnerBufferedParityProof,
  type ParityCensusInput,
} from "../owner-buffered-parity.ts";
import type { Queryable } from "../relayhistory-export.ts";

const H = (c: string) => c.repeat(64);
const BASE: ParityCensusInput = {
  tenant: { orgId: "fixture-org", workspaceId: "fixture-workspace", userId: "fixture-user" },
  controlEpoch: H("1"),
  privacyBaselineSha256: H("2"),
  head: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection", pending: false },
  sourceDeletionSetSha256: H("5"),
  sourceResetSetSha256: H("6"),
  targetControlEpoch: H("1"),
  targetPrivacyBaselineSha256: H("2"),
  targetGeneration: H("4"),
  targetLatestReceipt: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection" },
  targetReceiptVersionsContiguousFromOne: true,
  targetDeletionSetSha256: H("5"),
  targetResetSetSha256: H("6"),
  targetLineageDigest: H("7"),
};

const clone = <T>(value: T): T => structuredClone(value);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (path: string) => read(path).split("\n").filter((line) => !line.startsWith("\\")).join("\n");

describe("Option C G1 census", () => {
  test("emits a proof tuple only for exact parity", () => {
    const receipt = evaluateParityCensus(BASE);
    expect(receipt.equal).toBe(true);
    expect(receipt.mismatches).toEqual([]);
    expect(receipt.proof).toMatchObject({
      contract: "owner-buffered-parity/v1",
      explicitNoHead: false,
      controlEpoch: H("1"),
      privacyBaselineSha256: H("2"),
      deletionSetSha256: H("5"),
      resetSetSha256: H("6"),
      targetLineageDigest: H("7"),
      targetGeneration: H("4"),
    });
  });

  test("database census uses read-only transactions and returns only hashes/tuples", async () => {
    const statements: { source: string[]; target: string[] } = { source: [], target: [] };
    const sourceRow = {
      cutover_status: "active", control_epoch: H("1"), privacy_baseline_sha256: H("2"),
      head: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection", pending: false },
      deletion_count: "2", deletion_set_sha256: H("5"), reset_count: "1", reset_set_sha256: H("6"),
    };
    const targetRow = {
      control_epoch: H("1"), privacy_baseline_sha256: H("2"), target_generation: H("4"),
      receipts_contiguous: true,
      latest_receipt: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection" },
      target_lineage_digest: H("7"), deletion_count: "2", deletion_set_sha256: H("5"),
      reset_count: "1", reset_set_sha256: H("6"),
    };
    const fake = (side: "source" | "target", row: Record<string, unknown>): Queryable => ({
      async query<T>(sql: string) {
        statements[side].push(sql);
        return { rows: (sql.includes("CENSUS") ? [] : sql.includes("SELECT") || sql.includes("WITH") ? [row] : []) as T[] };
      },
    });
    const receipt = await readParityCensus(fake("source", sourceRow), fake("target", targetRow), BASE.tenant);
    expect(receipt.equal).toBe(true);
    for (const side of ["source", "target"] as const) {
      expect(statements[side][0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      expect(statements[side][1]).toBe("SET LOCAL statement_timeout = '2s'");
      expect(statements[side].at(-1)).toBe("COMMIT");
      expect(statements[side].some((sql) => /\b(?:INSERT|UPDATE|DELETE|TRUNCATE|ALTER|CREATE|DROP)\b/iu.test(sql))).toBe(false);
    }
  });

  test.each([
    ["pending source head", (x: ParityCensusInput) => { x.head!.pending = true; }, "source_head_pending"],
    ["source head epoch", (x: ParityCensusInput) => { x.head!.epoch = H("8"); }, "source_head_epoch_mismatch"],
    ["target epoch", (x: ParityCensusInput) => { x.targetControlEpoch = H("8"); }, "control_epoch_mismatch"],
    ["privacy baseline", (x: ParityCensusInput) => { x.targetPrivacyBaselineSha256 = H("8"); }, "privacy_baseline_mismatch"],
    ["receipt gap", (x: ParityCensusInput) => { x.targetReceiptVersionsContiguousFromOne = false; }, "target_lineage_gap"],
    ["receipt behind", (x: ParityCensusInput) => { x.targetLatestReceipt!.version = "6"; }, "target_receipt_version_mismatch"],
    ["receipt digest", (x: ParityCensusInput) => { x.targetLatestReceipt!.digest = H("9"); }, "target_receipt_digest_mismatch"],
    ["deletion set", (x: ParityCensusInput) => { x.targetDeletionSetSha256 = H("9"); }, "deletion_set_mismatch"],
    ["reset set", (x: ParityCensusInput) => { x.targetResetSetSha256 = H("9"); }, "reset_set_mismatch"],
  ] as const)("refuses %s mismatch", (_name, patch, code) => {
    const input = clone(BASE);
    patch(input);
    expect(evaluateParityCensus(input)).toMatchObject({ equal: false, proof: null, mismatches: expect.arrayContaining([code]) });
  });
});

describe("pipeline-lead G2 write ruling", () => {
  const next = () => clone(evaluateParityCensus(BASE).proof!);

  test("first proof inserts; byte-equivalent rerun is unchanged", () => {
    const proof = next();
    expect(planProofWrite(null, proof)).toBe("insert");
    expect(planProofWrite(clone(proof), proof)).toBe("unchanged");
  });

  test("higher head version or changed generation permits a CAS update", () => {
    const prior = next();
    const head = next(); head.head!.version = "8";
    expect(planProofWrite(prior, head)).toBe("update");
    const generation = next(); generation.targetGeneration = H("8");
    expect(planProofWrite(prior, generation)).toBe("update");
  });

  test("same-head set or lineage changes are never relabeled", () => {
    for (const field of ["deletionSetSha256", "resetSetSha256", "targetLineageDigest"] as const) {
      const prior = next(), changed = next(); changed[field] = H("9");
      expect(() => planProofWrite(prior, changed)).toThrow("same_head_digest_divergence");
    }
  });

  test("same/lower version tuple changes without generation advance refuse", () => {
    const prior = next(), changed = next(); changed.head!.digest = H("9");
    expect(() => planProofWrite(prior, changed)).toThrow("proof_not_advanced");
  });

  test("SQL uses insert-only first write and exact prior tuple CAS replacement", async () => {
    const calls: { sql: string; params?: unknown[] }[] = [];
    const db: Queryable = { async query<T>(sql: string, params?: unknown[]) {
      calls.push({ sql, params }); return { rows: [{ written_at: "now" }] as T[] };
    } };
    const first = next();
    expect(await writeProofRowCas(db, null, first)).toMatchObject({ outcome: "insert" });
    expect(calls[0]!.sql).toContain("ON CONFLICT (org_id,workspace_id,user_id) DO NOTHING");
    expect(calls[0]!.sql).not.toContain("DO UPDATE");

    const advanced = next(); advanced.head!.version = "8";
    expect(await writeProofRowCas(db, first, advanced)).toMatchObject({ outcome: "update" });
    expect(calls[1]!.sql).toContain("IS NOT DISTINCT FROM");
    expect(calls[1]!.sql).toContain("deletion_set_digest,reset_set_digest,target_lineage_digest");
    expect(calls[1]!.params).toHaveLength(24);
  });

  test("0 affected rows refuses insert races and CAS races", async () => {
    const db: Queryable = { async query<T>() { return { rows: [] as T[] }; } };
    const prior = next(), advanced = next(); advanced.head!.version = "8";
    await expect(writeProofRowCas(db, null, prior)).rejects.toThrow("proof_insert_conflict");
    await expect(writeProofRowCas(db, prior, advanced)).rejects.toThrow("proof_update_cas_conflict");
  });

  test("G2 opens the target snapshot before two unchanged Neon reads, then commits", async () => {
    const order: string[] = [];
    const sourceRow = {
      cutover_status: "active", control_epoch: H("1"), privacy_baseline_sha256: H("2"),
      head: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection", pending: false },
      deletion_count: "2", deletion_set_sha256: H("5"), reset_count: "1", reset_set_sha256: H("6"),
    };
    const targetRow = {
      control_epoch: H("1"), privacy_baseline_sha256: H("2"), target_generation: H("4"), receipts_contiguous: true,
      latest_receipt: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection" },
      target_lineage_digest: H("7"), deletion_count: "2", deletion_set_sha256: H("5"), reset_count: "1", reset_set_sha256: H("6"),
    };
    const neon: Queryable = { async query<T>(sql: string) {
      order.push(`neon:${sql.trim().split(/\s/u)[0]}`);
      return { rows: (sql.includes("delivery_cutover_status") ? [sourceRow] : []) as T[] };
    } };
    const target: Queryable = { async query<T>(sql: string) {
      order.push(`target:${sql.trim().split(/\s/u)[0]}`);
      if (sql.includes("WITH policy")) return { rows: [targetRow] as T[] };
      if (sql.includes("INSERT INTO sessions.owner_buffered_parity")) return { rows: [{ written_at: "now" }] as T[] };
      return { rows: [] as T[] };
    } };
    expect(await writeCurrentParityProof(neon, target, BASE.tenant)).toMatchObject({ outcome: "insert", census: { equal: true } });
    expect(order[0]).toBe("target:BEGIN");
    expect(order.filter((x) => x.startsWith("neon:WITH"))).toHaveLength(2);
    expect(order.at(-1)).toBe("target:COMMIT");
  });

  test("G2 rolls back when Neon changes between its two reads", async () => {
    let reads = 0; const targetStatements: string[] = [];
    const source = () => ({
      cutover_status: "active", control_epoch: H("1"), privacy_baseline_sha256: H("2"),
      head: { epoch: H("1"), version: reads === 1 ? "7" : "8", digest: H("3"), kind: "selection", pending: false },
      deletion_count: "0", deletion_set_sha256: H("5"), reset_count: "0", reset_set_sha256: H("6"),
    });
    const neon: Queryable = { async query<T>(sql: string) {
      if (sql.includes("delivery_cutover_status")) { reads++; return { rows: [source()] as T[] }; }
      return { rows: [] as T[] };
    } };
    const target: Queryable = { async query<T>(sql: string) {
      targetStatements.push(sql.trim().split(/\s/u)[0]!);
      if (sql.includes("WITH policy")) return { rows: [{
        control_epoch: H("1"), privacy_baseline_sha256: H("2"), target_generation: H("4"), receipts_contiguous: true,
        latest_receipt: null, target_lineage_digest: H("7"), deletion_count: "0", deletion_set_sha256: H("5"),
        reset_count: "0", reset_set_sha256: H("6"),
      }] as T[] };
      return { rows: [] as T[] };
    } };
    await expect(writeCurrentParityProof(neon, target, BASE.tenant)).rejects.toThrow("neon_changed_during_census");
    expect(targetStatements.at(-1)).toBe("ROLLBACK");
  });

  test("G2 generation pin is checked inside the transaction before any proof write", async () => {
    const statements: string[] = [];
    const sourceRow = {
      cutover_status: "active", control_epoch: H("1"), privacy_baseline_sha256: H("2"),
      head: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection", pending: false },
      deletion_count: "0", deletion_set_sha256: H("5"), reset_count: "0", reset_set_sha256: H("6"),
    };
    const targetRow = {
      control_epoch: H("1"), privacy_baseline_sha256: H("2"), target_generation: H("4"), receipts_contiguous: true,
      latest_receipt: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection" },
      target_lineage_digest: H("7"), deletion_set_sha256: H("5"), reset_set_sha256: H("6"),
    };
    const neon: Queryable = { async query<T>(sql: string) {
      return { rows: (sql.includes("delivery_cutover_status") ? [sourceRow] : []) as T[] };
    } };
    const target: Queryable = { async query<T>(sql: string) {
      statements.push(sql);
      if (sql.includes("WITH policy")) return { rows: [targetRow] as T[] };
      return { rows: [] as T[] };
    } };
    await expect(writeCurrentParityProof(neon, target, BASE.tenant, H("8")))
      .rejects.toThrow("target_generation_mismatch");
    expect(statements.some((sql) => sql.includes("INSERT INTO sessions.owner_buffered_parity"))).toBeFalse();
    expect(statements.some((sql) => sql.includes("UPDATE sessions.owner_buffered_parity"))).toBeFalse();
    expect(statements.at(-1)).toBe("ROLLBACK");
  });

  test("real 043700 helper matches the Neon census bytes and accepts insert/CAS update", async () => {
    const target = new PGlite({ extensions: { btree_gin, pg_trgm } });
    const neon = new PGlite();
    try {
      await target.exec(read("supabase/tests/supabase_shim.sql"));
      for (const file of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles", "session_read_api_roles"])
        await target.exec(read(`supabase/bootstrap/${file}.sql`));
      for (const file of readdirSync(join(root, "supabase/migrations")).filter((name) => name.endsWith(".sql")).sort())
        await target.exec(read(`supabase/migrations/${file}`));
      await target.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
      await target.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
        INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${H("4")}','pass');
        INSERT INTO sessions.ingest_lazy_policy(singleton,baseline_mode,generation,source_identity_sha256,control_epoch,
          privacy_baseline_sha256,retention_contract_sha256,source_proof_sha256,issuer,valid_until)
        VALUES(true,'lazy-current','${H("4")}','${H("c")}','${H("1")}','${H("2")}','${H("b")}','${H("d")}',
          'relayhistory-cloud',clock_timestamp()+interval '1 hour');
        INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
        VALUES('fixture-org','fixture-workspace','fixture-user','origin-1','claude','session-1');
        INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id)
        VALUES('fixture-org','fixture-workspace','fixture-user','request-1');
        INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation)
        SELECT 'fixture-org','fixture-workspace','fixture-user','${H("1")}',n,
               CASE WHEN n=7 THEN '${H("3")}' ELSE '${H("8")}' END,'selection','{}'::jsonb,'${H("4")}'
          FROM generate_series(1,7) AS n;`);

      await neon.exec(`CREATE SCHEMA sessions;
        CREATE TABLE sessions.delivery_cutover_control_heads(org_id text,workspace_id text,user_id text,epoch text,
          version bigint,digest text,kind text,pending boolean);
        CREATE TABLE sessions.deleted_upload_sessions(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text);
        CREATE TABLE sessions.session_resets(org_id text,workspace_id text,user_id text,request_id text,deleted bigint);
        CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql STABLE AS $$
          SELECT jsonb_build_object('phase','active','epoch','${H("1")}','privacyBaselineSha256','${H("2")}') $$;
        INSERT INTO sessions.delivery_cutover_control_heads VALUES('fixture-org','fixture-workspace','fixture-user','${H("1")}',7,'${H("3")}','selection',false);
        INSERT INTO sessions.deleted_upload_sessions VALUES('fixture-org','fixture-workspace','fixture-user','origin-1','claude','session-1');
        INSERT INTO sessions.session_resets VALUES('fixture-org','fixture-workspace','fixture-user','request-1',1);`);
      const queryable = (db: PGlite): Queryable => ({ query: async <T>(sql: string, params?: unknown[]) =>
        (params?.length ? db.query(sql, params as never) : db.query(sql)) as Promise<{ rows: T[] }> });
      const n = queryable(neon), t = queryable(target);
      expect(await readParityCensus(n, t, BASE.tenant)).toMatchObject({ equal: true, proof: { head: { version: "7" } } });
      expect(await writeCurrentParityProof(n, t, BASE.tenant)).toMatchObject({ outcome: "insert" });
      expect(await writeCurrentParityProof(n, t, BASE.tenant)).toMatchObject({ outcome: "unchanged" });

      await target.exec(`INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation)
        VALUES('fixture-org','fixture-workspace','fixture-user','${H("1")}',8,'${H("9")}','reset','{}'::jsonb,'${H("4")}')`);
      await neon.exec(`UPDATE sessions.delivery_cutover_control_heads SET version=8,digest='${H("9")}',kind='reset'`);
      expect(await writeCurrentParityProof(n, t, BASE.tenant)).toMatchObject({ outcome: "update", census: { equal: true } });
    } finally {
      await neon.close(); await target.close();
    }
  }, 120_000);
});

describe("f1a31a65 16-vector rule 3 + rule 4 matrix", () => {
  const proof = evaluateParityCensus(BASE).proof!;
  type Vector = { id: string; emit: boolean; patch: (state: { admission: { epoch: string; version: string }; current: ParityCensusInput; proof: OwnerBufferedParityProof | null }) => void };
  const vectors: Vector[] = [
    { id: "exact-current-user-proof-positive", emit: true, patch: () => {} },
    { id: "proof-absent", emit: false, patch: (x) => { x.proof = null; } },
    { id: "proof-foreign-user", emit: false, patch: (x) => { x.proof!.tenant.userId = "foreign-user"; } },
    { id: "proof-foreign-workspace", emit: false, patch: (x) => { x.proof!.tenant.workspaceId = "foreign-workspace"; } },
    { id: "proof-foreign-org", emit: false, patch: (x) => { x.proof!.tenant.orgId = "foreign-org"; } },
    { id: "proof-foreign-epoch", emit: false, patch: (x) => { x.proof!.controlEpoch = H("2"); } },
    { id: "proof-stale-target-generation", emit: false, patch: (x) => { x.current.targetGeneration = H("8"); } },
    { id: "proof-stale-head-after-new-admission", emit: false, patch: (x) => {
      x.admission.version = "8"; x.current.head!.version = "8"; x.current.targetLatestReceipt!.version = "8";
    } },
    { id: "proof-wrong-head-digest", emit: false, patch: (x) => { x.current.head!.digest = H("9"); } },
    { id: "proof-nohead-not-established", emit: false, patch: (x) => {
      x.admission.version = "0"; x.current.head = null; x.current.targetLatestReceipt = null;
      x.proof!.explicitNoHead = false; x.proof!.head = null;
    } },
    { id: "target-control-lineage-gap", emit: false, patch: (x) => { x.current.targetReceiptVersionsContiguousFromOne = false; } },
    { id: "target-receipt-behind-source", emit: false, patch: (x) => { x.current.targetLatestReceipt!.version = "6"; } },
    { id: "target-receipt-digest-mismatch", emit: false, patch: (x) => { x.current.targetLatestReceipt!.digest = H("9"); } },
    { id: "full-deletion-set-mismatch", emit: false, patch: (x) => { x.current.targetDeletionSetSha256 = H("9"); } },
    { id: "full-reset-set-mismatch", emit: false, patch: (x) => { x.current.targetResetSetSha256 = H("9"); } },
    { id: "proof-target-lineage-digest-mismatch", emit: false, patch: (x) => { x.current.targetLineageDigest = H("9"); } },
  ];

  test.each(vectors)("$id", ({ emit, patch }) => {
    const state = {
      admission: { epoch: H("1"), version: "7" },
      current: clone(BASE),
      proof: clone(proof) as OwnerBufferedParityProof | null,
    };
    patch(state);
    expect(bufferedParityVisible(state.admission, state.current, state.proof)).toBe(emit);
  });
});
