/**
 * Lazy-current adoption must ignore SOURCE-side tables (supabase/migrations/20261105040000_ingest_lazy_source_tables.sql).
 * Before the fix, the Supabase-first mirror head (written before the ingest fence) and the frozen Neon copy both
 * look like "prior target authority" to ingest_lazy_user / ingest_lazy_begin, so mirror-mode controls and
 * frozen-supabase uploads of affected users fail closed forever.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { createIngestApp } from "../app.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, err } from "./privacy-authority-support.ts";

setDefaultTimeout(60_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});

test("without the exclusion a mirror head blocks the user's first fence forever, and a frozen deletion blocks uploads; with it both succeed", async () => {
  const f = await authorityFixture({ lazyFix: false, pending: false });
  open.push(f.close);
  await f.seal();
  const head = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "a" } });
  const w = await f.rpc("ingest_frozen_source_witness", { kind: "reset", tenant: TENANT, originId: null, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
  const control = { epoch, version: head.version, digest: head.digest, kind: "reset" };
  const apply = () => f.rpc("ingest_reset_uploaded_sessions", { tenant: TENANT, requestId: "a", control, custody: w, generation: "a".repeat(64) });
  expect(await err(apply())).toContain("requires recovery");
  // A user whose pre-cutover deletion is in the frozen copy cannot upload either.
  const u2 = { ...TENANT, userId: "user-2" };
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,'user-2','o','claude','s')", [TENANT.orgId, TENANT.workspaceId]);
  const raw = await batch([record({ record_id: "z" })], { batch_id: "u2", account_id: await deliveryAccount(u2) });
  expect((await createIngestApp({ identify: () => ({ ...u2, scopes: ["rth:sync"] }), deliver: f.deps, log: () => {}, custodySource: "frozen-supabase" })
    .request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ protocolVersion: 1, batch: raw }) })).status).toBe(503);
  // With the fix both succeed.
  await f.db.exec(await Bun.file(new URL("../../../supabase/migrations/20261105040000_ingest_lazy_source_tables.sql", import.meta.url)).text());
  expect((await apply()).control).toMatchObject({ version: "1", outcome: "applied" });
  expect((await createIngestApp({ identify: () => ({ ...u2, scopes: ["rth:sync"] }), deliver: f.deps, log: () => {}, custodySource: "frozen-supabase" })
    .request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ protocolVersion: 1, batch: raw }) })).status).toBe(200);
});

test("negative: a user with a real TARGET row (ingest_seed_fence_baselines) is still refused 55000 by lazy adoption, with the fix", async () => {
  // The exclusion covers only source-side tables (ingest_frozen_source_*, ingest_source_control_heads). A genuine
  // prior target-authority row must still fail closed into reviewed recovery. Control: the identical control on an
  // identical fixture WITHOUT that row is applied, so only the target row made the difference.
  for (const withTargetRow of [true, false]) {
    const f = await authorityFixture({ pending: false });
    open.push(f.close);
    await f.seal();
    if (withTargetRow)
      await f.db.query("INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint) VALUES($1,$2,$3,$4)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "c".repeat(64)]);
    const head = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "t" } });
    const w = await f.rpc("ingest_frozen_source_witness", { kind: "reset", tenant: TENANT, originId: null, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
    const control = { epoch, version: head.version, digest: head.digest, kind: "reset" };
    const r = await f.rpc("ingest_reset_uploaded_sessions", { tenant: TENANT, requestId: "t", control, custody: w, generation: "a".repeat(64) }).then((v: any) => v, (x: any) => x);
    if (withTargetRow) {
      expect(r?.code).toBe("55000");
      expect(String(r?.message)).toContain("requires recovery");
    } else expect(r.control).toMatchObject({ version: "1", outcome: "applied" });
  }
});

/**
 * 041000 tightens 040000's prefix exclusion to the EXACT source-table list (lead ruling 2026-11-07,
 * c3-041-exact-source-exclusions). Causal in both directions: under 041000 a hypothetical TARGET table sharing the
 * ingest_frozen_source_ prefix is still prior authority (55000) in both the user and the org adoption path; with the
 * 040000 bodies its rollback restores, the same table is (wrongly) ignored; re-applying 041000 closes it again.
 */
const MIG041 = "../../../supabase/migrations/20261105041000_ingest_privacy_authority.sql";
const MIG040 = "../../../supabase/migrations/20261105040000_ingest_lazy_source_tables.sql";
async function lazyBodies(rel: string): Promise<string> {
  const src = await Bun.file(new URL(rel, import.meta.url)).text();
  return [/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_user\(p_w jsonb,p_kind text,p_control jsonb\)[\s\S]*?\$lazy_user\$;/, /CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_begin\(p jsonb,p_kind text\)[\s\S]*?\$lazy_begin\$;/]
    .map((re) => {
      const m = src.match(re);
      if (!m) throw new Error("lazy body missing in " + rel);
      return m[0];
    })
    .join("\n");
}

type Fx = Awaited<ReturnType<typeof authorityFixture>>;
async function world() {
  const f = await authorityFixture({});
  open.push(f.close);
  await f.seal();
  return f;
}
/** One mirror reset control for `tenant`: the first one for an org runs the org path, then the user path. */
async function firstControl(f: Fx, tenant: typeof TENANT, id: string): Promise<"applied" | string> {
  try {
    const head = await f.rpc("ingest_source_control_begin", { epoch, tenant, kind: "reset", payload: { request_id: id } });
    const w = await f.rpc("ingest_frozen_source_witness", { kind: "reset", tenant, originId: null, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
    const r = await f.rpc("ingest_reset_uploaded_sessions", { tenant, requestId: id, control: { epoch, version: head.version, digest: head.digest, kind: "reset" }, custody: w, generation: "a".repeat(64) });
    return r.control.outcome === "applied" ? "applied" : "outcome:" + r.control.outcome;
  } catch (e: any) {
    return `${e.code}:${String(e.message).includes("prior org authority") ? "org" : String(e.message).includes("prior user authority") ? "user" : e.message}`;
  }
}
const seedAs = async (f: Fx, sql: string, p: unknown[] = []) => {
  // Seeding only (owner session, triggers off): the rows are what a loader/sync would have written.
  await f.db.exec("SET session_replication_role = replica");
  try {
    await f.db.query(sql, p as never);
  } finally {
    await f.db.exec("SET session_replication_role = origin");
  }
};
const T2 = { ...TENANT, userId: "user-0" };
/** Admits the org through another user's first control, so the next control for TENANT exercises the USER path only. */
const admitOrg = async (f: Fx) => expect(await firstControl(f, T2, "admit")).toBe("applied");

test("041000 exact list: rows in every documented source table (six 010000 + owned-set copy) and the control heads still permit first org and user adoption", async () => {
  const f = await world();
  const { orgId: o, workspaceId: w, userId: u } = TENANT;
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id,user_id,source,session_id) VALUES($1,$2,'o','r',$3,'claude','s')", [o, w, u]);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,'o','claude','s')", [o, w, u]);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_resets VALUES($1,$2,$3,'old',0)", [o, w, u]);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_org_seals(org_id,record_rows,record_sum,privacy_rows,privacy_sum) VALUES($1,0,0,0,0)", [o]);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_receipts VALUES($1,$2,'o','b',$3,'{}')", [o, w, "a".repeat(64)]);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,'o','claude','s')", [o, w, u]);
  // completeness is a singleton without tenant columns (seeded by f.seal()); the control head is written by begin itself.
  expect(await firstControl(f, TENANT, "first")).toBe("applied");
});

test("041000 exact list: a TARGET table named ingest_frozen_source_target_fixture refuses 55000 in BOTH the org and the user path; tenant-isolated", async () => {
  const ddl = "CREATE TABLE sessions.ingest_frozen_source_target_fixture(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL)";
  // Org path: a fresh org with a prefixed target row.
  const a = await world();
  await a.db.exec(ddl);
  await a.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES($1,$2,'someone-else')", [TENANT.orgId, TENANT.workspaceId]);
  expect(await firstControl(a, TENANT, "x")).toBe("55000:org");
  // User path: the org admitted first, then the user's prefixed target row.
  const b = await world();
  await b.db.exec(ddl);
  await admitOrg(b);
  await b.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
  expect(await firstControl(b, TENANT, "x")).toBe("55000:user");
  // Tenant isolation: another org's row in the same table refuses nothing here.
  const c = await world();
  await c.db.exec(ddl);
  await c.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES('org-other',$1,$2)", [TENANT.workspaceId, TENANT.userId]);
  expect(await firstControl(c, TENANT, "x")).toBe("applied");
});

test("041000 exact list: real target rows and control-head SIBLINGS refuse 55000 in both paths", async () => {
  for (const [ddl, table] of [
    ["CREATE TABLE sessions.ingest_source_control_heads_fixture(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL)", "ingest_source_control_heads_fixture"],
    ["CREATE TABLE sessions.ingest_source_control_heads2(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL)", "ingest_source_control_heads2"],
    [null, "ingest_seed_fence_baselines"],
  ] as const) {
    const org = await world();
    if (ddl) await org.db.exec(ddl);
    const cols = table === "ingest_seed_fence_baselines" ? "(org_id,workspace_id,user_id,fingerprint) VALUES($1,$2,$3,'" + "c".repeat(64) + "')" : " VALUES($1,$2,$3)";
    await org.db.query(`INSERT INTO sessions.${table}${cols}`, [TENANT.orgId, TENANT.workspaceId, "someone-else"]);
    expect([table, await firstControl(org, TENANT, "x")]).toEqual([table, "55000:org"]);
    const user = await world();
    if (ddl) await user.db.exec(ddl);
    await admitOrg(user);
    await user.db.query(`INSERT INTO sessions.${table}${cols}`, [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect([table, await firstControl(user, TENANT, "x")]).toEqual([table, "55000:user"]);
  }
});

test("041 up -> the bodies its rollback restores (exact 040000) -> 041 up: the prefixed target table is refused, ignored, refused", async () => {
  const f = await world();
  await f.db.exec("CREATE TABLE sessions.ingest_frozen_source_target_fixture(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL)");
  await admitOrg(f);
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
  const prosrc = async () => (await f.db.query("SELECT string_agg(prosrc,'') AS s FROM pg_proc WHERE oid IN(to_regprocedure('sessions.ingest_lazy_user(jsonb,text,jsonb)'),to_regprocedure('sessions.ingest_lazy_begin(jsonb,text)'))")).rows[0] as { s: string };
  expect((await prosrc()).s).not.toContain("NOT LIKE 'ingest");
  expect(await firstControl(f, TENANT, "a")).toBe("55000:user");
  // Down: exactly what the generated 041000 rollback restores (the 040000 bodies, prefix exclusion).
  await f.db.exec(await lazyBodies(MIG040));
  expect((await prosrc()).s).toContain("NOT LIKE 'ingest\\_frozen\\_source\\_%'");
  const u4 = { ...TENANT, userId: "user-4" };
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, u4.userId]);
  expect(await firstControl(f, u4, "b")).toBe("applied");
  // Up again: closed.
  const u3 = { ...TENANT, userId: "user-3" };
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_target_fixture VALUES($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, u3.userId]);
  await f.db.exec(await lazyBodies(MIG041));
  expect(await firstControl(f, u3, "c")).toBe("55000:user");
});

test("why the owned-set copy is on the list: with only the six 010000 tables, a user whose Neon owned set was synced cannot be adopted", async () => {
  const f = await world();
  await admitOrg(f);
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,'o','claude','s')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
  const sixOnly = (await lazyBodies(MIG041)).replace(/,\s*'ingest_frozen_source_owned_sessions'\)/g, ")");
  expect(sixOnly).not.toContain("ingest_frozen_source_owned_sessions");
  await f.db.exec(sixOnly);
  expect(await firstControl(f, TENANT, "x")).toBe("55000:user");
  await f.db.exec(await lazyBodies(MIG041));
  const u5 = { ...TENANT, userId: "user-5" };
  await seedAs(f, "INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,'o','claude','s')", [u5.orgId, u5.workspaceId, u5.userId]);
  expect(await firstControl(f, u5, "y")).toBe("applied");
});
