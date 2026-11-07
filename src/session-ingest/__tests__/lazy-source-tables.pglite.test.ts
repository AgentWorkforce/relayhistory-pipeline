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
  // prior target-authority row for the user must still fail closed into reviewed recovery.
  const f = await authorityFixture({ pending: false });
  open.push(f.close);
  await f.seal();
  await f.db.query("INSERT INTO sessions.ingest_seed_fence_baselines(org_id,workspace_id,user_id,fingerprint) VALUES($1,$2,$3,$4)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "c".repeat(64)]);
  const head = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "t" } });
  const w = await f.rpc("ingest_frozen_source_witness", { kind: "reset", tenant: TENANT, originId: null, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
  const control = { epoch, version: head.version, digest: head.digest, kind: "reset" };
  const e = await f.rpc("ingest_reset_uploaded_sessions", { tenant: TENANT, requestId: "t", control, custody: w, generation: "a".repeat(64) }).then(() => null, (x: any) => x);
  expect(e?.code).toBe("55000");
  expect(String(e?.message)).toContain("requires recovery");
  // Control: the same control for a user WITHOUT a target row is applied (only the baseline row made the difference).
  const u2 = { ...TENANT, userId: "user-2" };
  const h2 = await f.rpc("ingest_source_control_begin", { epoch, tenant: u2, kind: "reset", payload: { request_id: "t2" } });
  const w2 = await f.rpc("ingest_frozen_source_witness", { kind: "reset", tenant: u2, originId: null, batchId: null, batchDigest: null, originalRecordCount: 0, candidates: [] });
  expect((await f.rpc("ingest_reset_uploaded_sessions", { tenant: u2, requestId: "t2", control: { epoch, version: h2.version, digest: h2.digest, kind: "reset" }, custody: w2, generation: "a".repeat(64) })).control).toMatchObject({ outcome: "applied" });
});
