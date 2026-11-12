import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import {
  FROZEN_OWNER_BUFFERED_CENSUS_SQL,
  NEON_OWNER_BUFFERED_CENSUS_SQL,
  readParityCensus,
  writeCurrentParityProof,
  type ParityCensusSource,
} from "../owner-buffered-parity.ts";
import type { Queryable } from "../relayhistory-export.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";

// Step-6 edge 5: the owner-buffered census reads its source side from the
// sealed frozen Supabase copy. Fixture identifiers only; no real database.
const H = (c: string) => c.repeat(64);
const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const psql = (path: string) => read(path).split("\n").filter((line) => !line.startsWith("\\")).join("\n");
const TENANT = { orgId: "fixture-org", workspaceId: "fixture-workspace", userId: "fixture-user" };
const T = `'fixture-org','fixture-workspace','fixture-user'`;
const PARAMS = [TENANT.orgId, TENANT.workspaceId, TENANT.userId];

// Mixed case, punctuation and non-ASCII so a locale-aware sort would differ from COLLATE "C".
const DELETIONS: Array<[string, string, string]> = [
  ["origin-b", "claude", "Session-Z"], ["Origin-a", "codex", "session-1"], ["origin-b", "claude", "_session"],
  ["origin-é", "claude", "s-2"], ["origin-b", "Claude", "session-1"], ["origin_a", "codex", "a"],
];
const RESETS = ["request-b", "Request-A", "_request", "request-é", "request-a"];

const queryable = (db: PGlite): Queryable => ({ query: async <R>(sql: string, params?: unknown[]) =>
  (params?.length ? db.query(sql, params as never) : db.query(sql)) as Promise<{ rows: R[] }> });

const deletionValues = (rows: Array<[string, string, string]>) =>
  rows.map(([o, s, id]) => `(${T},'${o}','${s}','${id}')`).join(",");
const resetValues = (rows: string[]) => rows.map((id, i) => `(${T},'${id}',${i + 1})`).join(",");

const AUTHORITY_ACTIVE = `INSERT INTO sessions.ingest_privacy_authority(singleton,state,control_epoch,neon_fingerprint_sha256,activated_at)
  VALUES(true,'active','${H("1")}','${H("f")}',clock_timestamp())`;
const SEAL = `INSERT INTO sessions.ingest_frozen_source_completeness(singleton,source_identity_sha256,control_epoch,
  privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot)
  VALUES(true,'${H("c")}','${H("1")}','${H("2")}',1,0,'${H("e")}','{}'::jsonb)`;

describe("owner-buffered parity census: frozen Supabase source (step-6 edge 5)", () => {
  let target: PGlite;
  let neon: PGlite;
  let t: Queryable;
  let n: Queryable;

  /** Restore the sealed, authority-active baseline every case starts from. */
  async function baseline(): Promise<void> {
    await target.exec(`DELETE FROM sessions.ingest_privacy_authority;
      DELETE FROM sessions.ingest_frozen_source_completeness;
      ${SEAL};
      ${AUTHORITY_ACTIVE};`);
  }

  beforeAll(async () => {
    target = new PGlite({ extensions: { btree_gin, pg_trgm } });
    neon = new PGlite();
    await target.exec(read("supabase/tests/supabase_shim.sql"));
    for (const file of SESSION_BOOTSTRAP_FILES) await target.exec(read(file));
    for (const file of readdirSync(join(root, "supabase/migrations")).filter((name) => name.endsWith(".sql")).sort())
      await target.exec(read(`supabase/migrations/${file}`));
    await target.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
    // Target side (043700 helper inputs), inserted in one order...
    await target.exec(`CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
      INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker','${H("4")}','pass');
      INSERT INTO sessions.ingest_lazy_policy(singleton,baseline_mode,generation,source_identity_sha256,control_epoch,
        privacy_baseline_sha256,retention_contract_sha256,source_proof_sha256,issuer,valid_until)
      VALUES(true,'lazy-current','${H("4")}','${H("c")}','${H("1")}','${H("2")}','${H("b")}','${H("d")}',
        'relayhistory-cloud',clock_timestamp()+interval '1 hour');
      INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
      VALUES ${deletionValues(DELETIONS)};
      INSERT INTO sessions.upload_resets(org_id,workspace_id,user_id,request_id)
      VALUES ${RESETS.map((id) => `(${T},'${id}')`).join(",")};
      INSERT INTO sessions.ingest_control_receipts(org_id,workspace_id,user_id,epoch,version,digest,kind,result,schema_generation)
      SELECT ${T},'${H("1")}',n,CASE WHEN n=7 THEN '${H("3")}' ELSE '${H("8")}' END,'selection','{}'::jsonb,'${H("4")}'
        FROM generate_series(1,7) AS n;`);
    // ...the frozen copy in reverse order, plus a foreign tenant that must not count...
    await target.exec(`INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
        VALUES(${T},'${H("1")}',7,'${H("3")}','selection','{}'::jsonb,false),
              ('fixture-org','fixture-workspace','other-user','${H("1")}',2,'${H("9")}','reset','{}'::jsonb,false);
      INSERT INTO sessions.ingest_frozen_source_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
        VALUES ${deletionValues([...DELETIONS].reverse())},('fixture-org','fixture-workspace','other-user','o','claude','s');
      INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
        VALUES ${resetValues([...RESETS].reverse())},('fixture-org','fixture-workspace','other-user','r',1);`);
    await baseline();
    // ...and the fake Neon in a third order.
    const shuffled = [DELETIONS[3]!, DELETIONS[0]!, DELETIONS[5]!, DELETIONS[1]!, DELETIONS[4]!, DELETIONS[2]!];
    await neon.exec(`CREATE SCHEMA sessions;
      CREATE TABLE sessions.delivery_cutover_control_heads(org_id text,workspace_id text,user_id text,epoch text,
        version bigint,digest text,kind text,pending boolean);
      CREATE TABLE sessions.deleted_upload_sessions(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text);
      CREATE TABLE sessions.session_resets(org_id text,workspace_id text,user_id text,request_id text,deleted bigint);
      CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql STABLE AS $$
        SELECT jsonb_build_object('phase','active','epoch','${H("1")}','privacyBaselineSha256','${H("2")}') $$;
      INSERT INTO sessions.delivery_cutover_control_heads VALUES(${T},'${H("1")}',7,'${H("3")}','selection',false);
      INSERT INTO sessions.deleted_upload_sessions VALUES ${deletionValues(shuffled)};
      INSERT INTO sessions.session_resets VALUES ${resetValues([RESETS[2]!, RESETS[0]!, RESETS[4]!, RESETS[1]!, RESETS[3]!])};`);
    t = queryable(target);
    n = queryable(neon);
  }, 180_000);

  afterAll(async () => {
    await neon?.close();
    await target?.close();
  });

  test("frozen SQL returns the Neon census row byte-for-byte for the same logical rows", async () => {
    const neonRow = (await n.query<Record<string, unknown>>(NEON_OWNER_BUFFERED_CENSUS_SQL, PARAMS)).rows[0]!;
    const frozenRow = (await t.query<Record<string, unknown>>(FROZEN_OWNER_BUFFERED_CENSUS_SQL, PARAMS)).rows[0]!;
    expect(Object.keys(frozenRow)).toEqual(Object.keys(neonRow));
    expect(JSON.stringify(frozenRow)).toBe(JSON.stringify(neonRow));
    expect(frozenRow).toMatchObject({ cutover_status: "active", deletion_count: "6", reset_count: "5" });
    expect(frozenRow.deletion_set_sha256).toMatch(/^[0-9a-f]{64}$/u);
  });

  test("G1 receipts are identical from either source and both prove parity", async () => {
    const fromNeon = await readParityCensus(n, t, TENANT);
    const fromFrozen = await readParityCensus(t, t, TENANT, "frozen-supabase");
    expect(fromNeon).toMatchObject({ equal: true, mismatches: [] });
    expect(JSON.stringify(fromFrozen)).toBe(JSON.stringify(fromNeon));
    expect(fromFrozen.proof?.deletionSetSha256).toBe(fromNeon.proof?.deletionSetSha256);
    expect(fromFrozen.proof?.resetSetSha256).toBe(fromNeon.proof?.resetSetSha256);
  });

  test("refuses frozen_source_unsealed when the completeness seal is absent", async () => {
    await baseline();
    await target.exec("DELETE FROM sessions.ingest_frozen_source_completeness");
    try {
      await expect(readParityCensus(t, t, TENANT, "frozen-supabase")).rejects.toThrow("frozen_source_unsealed");
    } finally { await baseline(); }
  });

  test("refuses frozen_source_unsealed when the seal is not bound to the live policy", async () => {
    await baseline();
    await target.exec(`UPDATE sessions.ingest_frozen_source_completeness SET privacy_baseline_sha256='${H("a")}'`);
    try {
      await expect(readParityCensus(t, t, TENANT, "frozen-supabase")).rejects.toThrow("frozen_source_unsealed");
    } finally { await baseline(); }
  });

  test("refuses privacy_authority_not_active when the authority row is absent", async () => {
    await baseline();
    await target.exec("DELETE FROM sessions.ingest_privacy_authority");
    try {
      await expect(readParityCensus(t, t, TENANT, "frozen-supabase")).rejects.toThrow("privacy_authority_not_active");
    } finally { await baseline(); }
  });

  test("refuses privacy_authority_not_active when the authority is fenced", async () => {
    await baseline();
    await target.exec(`DELETE FROM sessions.ingest_privacy_authority;
      INSERT INTO sessions.ingest_privacy_authority(singleton,state,control_epoch) VALUES(true,'fenced','${H("1")}')`);
    try {
      await expect(readParityCensus(t, t, TENANT, "frozen-supabase")).rejects.toThrow("privacy_authority_not_active");
    } finally { await baseline(); }
  });

  test("refuses privacy_authority_not_active when the authority is active at another epoch", async () => {
    await baseline();
    await target.exec(`UPDATE sessions.ingest_privacy_authority SET control_epoch='${H("9")}'`);
    try {
      await expect(readParityCensus(t, t, TENANT, "frozen-supabase")).rejects.toThrow("privacy_authority_not_active");
    } finally { await baseline(); }
  });

  test("refusals happen on snapshot A, before the target census is read", async () => {
    await baseline();
    await target.exec("DELETE FROM sessions.ingest_privacy_authority");
    const statements: string[] = [];
    const watchedTarget: Queryable = { query: async <R>(sql: string, params?: unknown[]) => {
      statements.push(sql);
      return t.query<R>(sql, params);
    } };
    try {
      await expect(readParityCensus(t, watchedTarget, TENANT, "frozen-supabase")).rejects.toThrow("privacy_authority_not_active");
      expect(statements).toEqual([]);
    } finally { await baseline(); }
  });

  test("refuses frozen_source_changed_during_census when snapshot B differs from A", async () => {
    await baseline();
    let commits = 0;
    // An authority control lands after snapshot A commits and before snapshot B.
    const drifting: Queryable = { query: async <R>(sql: string, params?: unknown[]) => {
      const result = await t.query<R>(sql, params);
      if (sql === "COMMIT" && ++commits === 1)
        await target.exec(`SET bulls96.privacy_authority_writer='authority';
          INSERT INTO sessions.ingest_frozen_source_resets VALUES(${T},'request-drift',1);
          RESET bulls96.privacy_authority_writer;`);
      return result;
    } };
    try {
      await expect(readParityCensus(drifting, t, TENANT, "frozen-supabase"))
        .rejects.toThrow("frozen_source_changed_during_census");
    } finally {
      await target.exec(`SET bulls96.privacy_authority_writer='authority';
        DELETE FROM sessions.ingest_frozen_source_resets WHERE request_id='request-drift';
        RESET bulls96.privacy_authority_writer;`);
    }
    expect(await readParityCensus(t, t, TENANT, "frozen-supabase")).toMatchObject({ equal: true });
  });

  test("a target that differs from the frozen source still reports a parity mismatch", async () => {
    await baseline();
    await target.exec(`DELETE FROM sessions.upload_deletions WHERE session_id='_session'`);
    try {
      const receipt = await readParityCensus(t, t, TENANT, "frozen-supabase");
      expect(receipt).toMatchObject({ equal: false, proof: null });
      expect(receipt.mismatches).toContain("deletion_set_mismatch");
      const neonReceipt = await readParityCensus(n, t, TENANT);
      expect(receipt.mismatches).toEqual(neonReceipt.mismatches);
    } finally {
      await target.exec(`INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id)
        VALUES(${T},'origin-b','claude','_session')`);
    }
  });

  test("an unknown census source refuses before any read", async () => {
    const statements: string[] = [];
    const watch: Queryable = { query: async <R>(sql: string) => { statements.push(sql); return { rows: [] as R[] }; } };
    await expect(readParityCensus(watch, watch, TENANT, "neon-replica" as ParityCensusSource))
      .rejects.toThrow("census_source_invalid");
    await expect(writeCurrentParityProof(watch, watch, TENANT, undefined, "neon-replica" as ParityCensusSource))
      .rejects.toThrow("census_source_invalid");
    expect(statements).toEqual([]);
  });
});

describe("G2 with the frozen source (mocked sessions)", () => {
  const sourceRow = (version: string, status = "active") => ({
    cutover_status: status, control_epoch: H("1"), privacy_baseline_sha256: H("2"),
    head: { epoch: H("1"), version, digest: H("3"), kind: "selection", pending: false },
    deletion_count: "0", deletion_set_sha256: H("5"), reset_count: "0", reset_set_sha256: H("6"),
  });
  const targetRow = {
    control_epoch: H("1"), privacy_baseline_sha256: H("2"), target_generation: H("4"), receipts_contiguous: true,
    latest_receipt: { epoch: H("1"), version: "7", digest: H("3"), kind: "selection" },
    target_lineage_digest: H("7"), deletion_set_sha256: H("5"), reset_set_sha256: H("6"),
  };
  const mockTarget = (statements: string[]): Queryable => ({ async query<R>(sql: string) {
    statements.push(sql);
    if (sql.includes("owner_buffered_parity_expected")) return { rows: [targetRow] as R[] };
    return { rows: [] as R[] };
  } });

  test("G2 reads the frozen SQL and refuses when the copy moves inside the write transaction", async () => {
    const sourceSql: string[] = [];
    const statements: string[] = [];
    let reads = 0;
    const source: Queryable = { async query<R>(sql: string) {
      if (!sql.includes("ingest_frozen_source_completeness")) return { rows: [] as R[] };
      sourceSql.push(sql);
      return { rows: [sourceRow(++reads === 1 ? "7" : "8")] as R[] };
    } };
    await expect(writeCurrentParityProof(source, mockTarget(statements), TENANT, H("4"), "frozen-supabase"))
      .rejects.toThrow("frozen_source_changed_during_census");
    expect(sourceSql).toEqual([FROZEN_OWNER_BUFFERED_CENSUS_SQL, FROZEN_OWNER_BUFFERED_CENSUS_SQL]);
    expect(statements.some((sql) => /INSERT INTO|UPDATE sessions\.owner_buffered_parity/u.test(sql))).toBeFalse();
    expect(statements.at(-1)).toBe("ROLLBACK");
  });

  test.each(["frozen_source_unsealed", "privacy_authority_not_active"])(
    "G2 refuses %s on snapshot A and never reads the target census", async (status) => {
      const statements: string[] = [];
      const source: Queryable = { async query<R>(sql: string) {
        return { rows: (sql.includes("ingest_frozen_source_completeness") ? [sourceRow("7", status)] : []) as R[] };
      } };
      await expect(writeCurrentParityProof(source, mockTarget(statements), TENANT, H("4"), "frozen-supabase"))
        .rejects.toThrow(status);
      expect(statements.some((sql) => sql.includes("owner_buffered_parity_expected"))).toBeFalse();
      expect(statements.at(-1)).toBe("ROLLBACK");
    });

  test("neon remains the default source for G1 and G2", async () => {
    const seen: string[] = [];
    const source: Queryable = { async query<R>(sql: string) {
      if (sql.includes("delivery_cutover_status")) { seen.push("neon"); return { rows: [sourceRow("7")] as R[] }; }
      if (sql.includes("ingest_frozen_source_completeness")) seen.push("frozen");
      return { rows: [] as R[] };
    } };
    await readParityCensus(source, mockTarget([]), TENANT);
    await writeCurrentParityProof(source, mockTarget([]), TENANT, H("4")).catch(() => undefined);
    expect(seen.length).toBeGreaterThan(0);
    expect(seen.every((s) => s === "neon")).toBeTrue();
  });
});
