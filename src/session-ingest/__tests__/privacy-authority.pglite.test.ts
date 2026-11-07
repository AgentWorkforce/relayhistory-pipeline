/**
 * C3: Supabase-first privacy authority (supabase/migrations/20261105041000_ingest_privacy_authority.sql), through the real
 * migration stack, the unchanged ingest fence and lazy-current target, and the handoff module against a Neon with
 * relayhistory's exact privacy tables. Real-PG races are in privacy-authority.postgres.test.ts.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { createIngestApp } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../custody-protocol.ts";
import { activate, census, fence, release, readNeonSnapshot, syncOwned, HandoffRefusal } from "../../migration/privacy-authority-handoff.ts";
import { TENANT, batch, record } from "./support.ts";
import { authorityFixture, epoch, err, fakeNeon } from "./privacy-authority-support.ts";

setDefaultTimeout(60_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
async function setup() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  return f;
}
type F = Awaited<ReturnType<typeof setup>>;
const origin = async () => (await prepareDelivery(TENANT, await batch([record({ record_id: "x" })]))).originId;

/** The ingest Worker as relayhistory reaches it, with an explicit identity (control or not). */
function app(f: F, privacyAuthority: "neon" | "supabase", control?: { epoch: string; version: string; digest: string; kind: "selection" | "reset" }) {
  return createIngestApp({ identify: () => ({ ...TENANT, scopes: ["rth:sync"], ...(control ? { control } : {}) }), deliver: f.deps, log: () => {}, custodySource: "frozen-supabase", privacyAuthority });
}
const post = (a: ReturnType<typeof app>, path: string, body: unknown, type = "application/json") =>
  a.request(path, { method: "POST", headers: { "content-type": type }, body: JSON.stringify(body) });

/** One selection/reset exactly as relayhistory's supabase-authority mode drives it: begin, ordered fence, complete. */
async function authorityControl(f: F, kind: "selection" | "reset", payload: any) {
  const b = await post(app(f, "supabase"), "/v1/custody/authority/begin", { epoch, kind, payload });
  if (b.status !== 200) throw new Error("begin " + b.status + " " + (await b.text()));
  const head = (await b.json()) as any;
  const control = { epoch, version: head.version, digest: head.digest, kind };
  const account_id = await deliveryAccount(TENANT);
  const body = kind === "selection" ? { account_id, ...payload } : { account_id, request_id: payload.request_id, confirmation: "DELETE ALL SESSIONS" };
  const r = await post(app(f, "supabase", control), kind === "selection" ? "/v1/delivery/sessions/selection" : "/v1/delivery/sessions/reset", body);
  if (r.status !== 200) throw new Error("fence " + r.status + " " + (await r.text()));
  const ack = (await r.json()) as any;
  const c = await post(app(f, "supabase"), "/v1/custody/authority/complete", { ...control, outcome: ack.control.outcome });
  if (c.status !== 200) throw new Error("complete " + c.status);
  return { head, ack };
}

/** The legacy (Neon-first) mirror path minus Neon itself: what mirror mode leaves in Supabase. */
async function mirrorControl(f: F, kind: "selection" | "reset", payload: any) {
  const head = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind, payload });
  const control = { epoch, version: head.version, digest: head.digest, kind };
  const account_id = await deliveryAccount(TENANT);
  const body = kind === "selection" ? { account_id, ...payload } : { account_id, request_id: payload.request_id, confirmation: "DELETE ALL SESSIONS" };
  const r = await post(app(f, "neon", control), kind === "selection" ? "/v1/delivery/sessions/selection" : "/v1/delivery/sessions/reset", body);
  expect(r.status).toBe(200);
  await f.rpc("ingest_source_control_complete", { epoch, tenant: TENANT, version: head.version, digest: head.digest, kind, outcome: "applied" });
  return head;
}

async function activateFrom(f: F, n: Awaited<ReturnType<typeof fakeNeon>>) {
  await fence(f.queryable, epoch);
  await syncOwned(n.queryable, f.queryable);
  const c = await census(n.queryable, f.queryable, epoch);
  return activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 });
}
const uploadsOf = async (f: F, sid: string) =>
  ((await f.db.query("SELECT count(*)::int AS n FROM sessions.upload_deletions WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 AND session_id=$4", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, sid])).rows[0] as any).n;
const frozenDeleted = async (f: F, sid: string) =>
  ((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_deletions WHERE org_id=$1 AND user_id=$2 AND session_id=$3", [TENANT.orgId, TENANT.userId, sid])).rows[0] as any).n;

test("default-off: with no authority row the legacy mirror path is unchanged and the authority path refuses", async () => {
  const f = await setup();
  const head = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "r0" } });
  expect(head).toMatchObject({ version: "1", apply: true });
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "r1" } }))).toContain("supabase privacy authority not active");
  expect((await f.ownerRpc("ingest_privacy_authority_state")).r).toMatchObject({ authority: "neon", heads: 1, pendingHeads: 1 });
  // The HTTP authority routes are closed in the default (neon) worker mode too.
  expect((await post(app(f, "neon"), "/v1/custody/authority/begin", { epoch, kind: "reset", payload: { request_id: "r1" } })).status).toBe(503);
});

test("fence: requires the sealed copy; closes the legacy begin before it writes; in-flight legacy completions still land", async () => {
  const f0 = await authorityFixture();
  open.push(f0.close);
  expect(await err(f0.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch }))).toContain("copy not sealed");
  const f = await setup();
  expect(await err(f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: "4".repeat(64) }))).toContain("epoch mismatch");
  const inflight = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "r0" } });
  expect((await f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch })).r).toEqual({ state: "fenced", changed: true });
  expect((await f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch })).r).toEqual({ state: "fenced", changed: false });
  const other = { ...TENANT, userId: "user-2" };
  expect(await err(f.rpc("ingest_source_control_begin", { epoch, tenant: other, kind: "reset", payload: { request_id: "x" } }))).toContain("privacy authority moved");
  expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_source_control_heads WHERE user_id='user-2'")).rows[0] as any).n).toBe(0);
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch, tenant: other, kind: "reset", payload: { request_id: "x" } }))).toContain("not active");
  // A straggler that began before the fence completes normally (its Neon side completes too).
  expect(await f.rpc("ingest_source_control_complete", { epoch, tenant: TENANT, version: "1", digest: inflight.digest, kind: "reset", outcome: "applied" })).toEqual({ completed: true, superseded: false });
  // Release from fenced is always allowed and restores the legacy path.
  expect((await f.ownerRpc("ingest_privacy_authority_release", { controlEpoch: epoch })).r).toMatchObject({ authority: "neon", changed: true, from: "fenced" });
  expect(await f.rpc("ingest_source_control_begin", { epoch, tenant: other, kind: "reset", payload: { request_id: "x" } })).toMatchObject({ version: "1" });
});

test("handoff: the Neon-computed fingerprint equals Supabase's byte for byte, and activation makes Supabase exactly Neon", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  const o = await origin();
  // Neon era (mirror mode): one disable through the mirror and the target; Neon holds the same head and fence.
  const payload = { origin_id: o, sessions: [{ source: "claude", session_id: "S1" }], enabled: false };
  await mirrorControl(f, "selection", payload);
  await n.head(TENANT, 1, "selection", payload);
  await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,$4,'claude','S1')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, o]);
  // Neon also holds a pre-freeze deletion and reset the copy has but the frozen tables lost (drift), and a
  // frozen-only deletion Neon re-enabled: activation must converge exactly on Neon.
  await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES('org-z','ws','u','o','claude','old')");
  await n.pg.query("INSERT INTO sessions.session_resets VALUES('org-z','ws','u','req-old',4)");
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES('org-z','ws','u','o','claude','reenabled-on-neon')");
  const before = await census(n.queryable, f.queryable, epoch);
  expect(before.equal).toBe(false);
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: before.neon.fingerprint, settleMs: 0 }))).toContain("not_fenced");
  await fence(f.queryable, epoch);
  const r = await activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: before.neon.fingerprint, settleMs: 0 });
  expect(r.neonStableAfter).toBe(true);
  expect(r.activated).toMatchObject({ state: "active", deletionsInserted: 2, deletionsRemoved: 1, resetsInserted: 1, resetsRemoved: 0, headsWritten: 0 });
  const after = await census(n.queryable, f.queryable, epoch);
  expect(after.equal).toBe(true);
  expect(after.supabase).toMatchObject({ authority: "active", heads: 1, pendingHeads: 0, deletions: 2, resets: 1, activatedFingerprintSha256: after.neon.fingerprint });
});

test("activation refusals: unbound or unstable snapshot, pending heads, divergent mirror, target receipts", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await fence(f.queryable, epoch);
  const c = await census(n.queryable, f.queryable, epoch);
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: "f".repeat(64), settleMs: 0 }))).toContain("neon_changed_since_census");
  // A Neon write during the settle window (a straggler that bypassed the fence) refuses before any write.
  const sleep = async () => void (await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES('o','w','u','or','claude','late')"));
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0, sleep }))).toContain("neon_not_stable");
  expect((await f.ownerRpc("ingest_privacy_authority_state")).r.authority).toBe("fenced");
  // The SQL binds the rows to the fingerprint itself (a tampered snapshot refuses).
  const snap = await readNeonSnapshot(n.queryable, epoch);
  expect(await err(f.ownerRpc("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: snap.fingerprint, heads: [], deletions: [], resets: snap.resets, neonOwned: (await f.ownerRpc("ingest_privacy_authority_owned_digest")).r }))).toContain("not the neon fingerprint");
  // A pending Neon head refuses.
  await n.pg.query("DELETE FROM sessions.deleted_upload_sessions");
  await n.head(TENANT, 1, "reset", { request_id: "p" }, { pending: true });
  const c2 = await census(n.queryable, f.queryable, epoch);
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c2.neon.fingerprint, settleMs: 0 }))).toContain("neon_control_pending");
  // A completed Neon head the target never received refuses (the fence was not mandatory for it).
  await n.head(TENANT, 1, "reset", { request_id: "p" }, { deleted: 0 });
  const c3 = await census(n.queryable, f.queryable, epoch);
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c3.neon.fingerprint, settleMs: 0 }))).toContain("do not match the target control receipts");
  // A mirror head newer than Neon refuses (H-2: never roll the mirror back).
  await f.ownerRpc("ingest_privacy_authority_release", { controlEpoch: epoch });
  await mirrorControl(f, "reset", { request_id: "p" });
  await mirrorControl(f, "reset", { request_id: "q" });
  await fence(f.queryable, epoch);
  const c4 = await census(n.queryable, f.queryable, epoch);
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c4.neon.fingerprint, settleMs: 0 }))).toContain("diverges from neon");
  expect((await f.ownerRpc("ingest_privacy_authority_state")).r.authority).toBe("fenced");
});

test("activation refuses until the frozen user/session index exists (no whole-origin scans under the authority)", async () => {
  const f = await authorityFixture({ index: false });
  open.push(f.close);
  await f.seal();
  const n = await fakeNeon();
  open.push(n.close);
  expect(await err(activateFrom(f, n))).toContain("user session index missing");
  await f.db.exec(await Bun.file(new URL("../../../scripts/ops/c3-privacy-authority/ingest_privacy_authority_index.sql", import.meta.url)).text());
  const c = await census(n.queryable, f.queryable, epoch);
  expect((await activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 })).activated.state).toBe("active");
});

test("activation repairs a stale (older) mirror head to Neon's, bound to the target receipts", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await mirrorControl(f, "reset", { request_id: "a" });
  // Neon completed version 2 (its mirror completion was lost); the target received it through the fence.
  const h2 = await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "b" } });
  const account_id = await deliveryAccount(TENANT);
  expect((await post(app(f, "neon", { epoch, version: "2", digest: h2.digest, kind: "reset" }), "/v1/delivery/sessions/reset", { account_id, request_id: "b", confirmation: "DELETE ALL SESSIONS" })).status).toBe(200);
  // Model the stale mirror: an operator-repaired older head (version 1, completed) while Neon is at 2.
  await f.db.query("UPDATE sessions.ingest_source_control_heads SET version=1,digest=(SELECT digest FROM sessions.ingest_control_receipts WHERE version=1),payload='{\"request_id\":\"a\"}',pending=false");
  await n.head(TENANT, 2, "reset", { request_id: "b" }, { deleted: 0 });
  const r = await activateFrom(f, n);
  expect(r.activated).toMatchObject({ state: "active", headsWritten: 1 });
  expect((await f.db.query("SELECT version::text,pending,deleted::text FROM sessions.ingest_source_control_heads")).rows).toEqual([{ version: "2", pending: false, deleted: "0" }]);
});

test("active: every legacy writer is refused (mirror begin/complete, parity/handoff tools, copy re-seed)", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  expect(await err(f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "x" } }))).toContain("privacy authority moved");
  // The legacy completion refuses explicitly, even if a caller forges the guard's session setting.
  await f.db.exec("SET bulls96.privacy_authority_writer='authority'");
  expect(await err(f.rpc("ingest_source_control_complete", { epoch, tenant: TENANT, version: "1", digest: "0".repeat(64), kind: "reset", outcome: "applied" }))).toContain("privacy authority moved");
  await f.db.exec("RESET bulls96.privacy_authority_writer");
  for (const sql of [
    "INSERT INTO sessions.ingest_frozen_source_deletions VALUES('o','w','u','or','claude','s')",
    "DELETE FROM sessions.ingest_frozen_source_deletions",
    "INSERT INTO sessions.ingest_frozen_source_resets VALUES('o','w','u','r',1)",
    "INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id) VALUES('o','w','or','r') ON CONFLICT DO NOTHING",
    "UPDATE sessions.ingest_source_control_heads SET pending=false",
    "TRUNCATE sessions.ingest_frozen_source_deletions",
  ])
    expect(await err(f.db.query(sql))).toContain("legacy writer refused");
  // The HTTP legacy mirror routes and any relayhistory (Neon) witness envelope refuse in supabase mode.
  const a = app(f, "supabase");
  expect((await post(a, "/v1/custody/control/begin", { epoch, kind: "reset", payload: { request_id: "x" } })).status).toBe(503);
  expect((await post(a, "/v1/custody/control/complete", { epoch, version: "1", digest: "0".repeat(64), kind: "reset", outcome: "applied" })).status).toBe(503);
  expect((await post(a, "/v1/delivery/batches", { protocol: "relay-ingest-custody-v1" }, CURRENT_CUSTODY_CONTENT_TYPE)).status).toBe(503);
  expect(() => createIngestApp({ deliver: f.deps, privacyAuthority: "supabase", custodySource: "relayhistory" })).toThrow("requires custodySource=frozen-supabase");
});

test("selection under Supabase authority: disable fences and erases the frozen copy; re-enable is NOT undone by the frozen witness", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  const o = await origin();
  // A pre-freeze record of session S (copied from Neon), and a pre-freeze deletion of session D.
  const old = record({ record_id: "old-s", session_id: "S" });
  const p = (await prepareDelivery(TENANT, await batch([old]))).records[0]!;
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_records VALUES($1,$2,$3,'old-s',$4,1,$5,$6,$7,'S','upsert',$8,NULL,NULL)", [TENANT.orgId, TENANT.workspaceId, o, p.revision_id, p.digest, p.kind, p.source, TENANT.userId]);
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES($1,$2,$3,$4,'claude','D')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, o]);
  await n.pg.query("INSERT INTO sessions.deleted_upload_sessions VALUES($1,$2,$3,$4,'claude','D')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, o]);
  await activateFrom(f, n);
  const neonBefore = (await readNeonSnapshot(n.queryable, epoch)).fingerprint;

  const off = await authorityControl(f, "selection", { origin_id: o, sessions: [{ source: "claude", session_id: "S" }], enabled: false });
  expect(off.head).toMatchObject({ version: "1", pending: true, apply: true, deleted: null });
  expect(off.ack.control).toMatchObject({ version: "1", outcome: "applied" });
  expect(await frozenDeleted(f, "S")).toBe(1);
  expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_records WHERE session_id='S'")).rows[0] as any).n).toBe(0);
  // The target imported the frozen PRE (D) at the first control and applied the disable (S).
  expect(await uploadsOf(f, "S")).toBe(1);
  expect(await uploadsOf(f, "D")).toBe(1);
  // Re-enable the pre-freeze deletion D.
  await authorityControl(f, "selection", { origin_id: o, sessions: [{ source: "claude", session_id: "D" }], enabled: true });
  expect(await frozenDeleted(f, "D")).toBe(0);
  expect(await uploadsOf(f, "D")).toBe(0);
  // An upload of D afterwards: the frozen witness no longer reports it fenced, so the target does not re-fence it.
  const up = await post(app(f, "supabase"), "/v1/delivery/batches", { protocolVersion: 1, batch: await batch([record({ record_id: "d1", session_id: "D" })], { batch_id: "after-reenable" }) });
  expect(up.status).toBe(200);
  expect((await up.json() as any).acceptedRevisionIds).toEqual(["d1:r1"]);
  expect(await uploadsOf(f, "D")).toBe(0);
  // ... while an upload of the disabled S stays fenced.
  const s = await post(app(f, "supabase"), "/v1/delivery/batches", { protocolVersion: 1, batch: await batch([record({ record_id: "s2", session_id: "S" })], { batch_id: "after-disable" }) });
  expect(s.status).toBe(200);
  expect(await uploadsOf(f, "S")).toBe(1);
  // Neon was never written.
  expect((await readNeonSnapshot(n.queryable, epoch)).fingerprint).toBe(neonBefore);
  // One-way: release now refuses.
  expect(await err(f.ownerRpc("ingest_privacy_authority_release", { controlEpoch: epoch }))).toContain("diverged from neon");
});

test("reset under Supabase authority: fences every owned frozen session, counts like Neon, replays its count", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  const o = await origin();
  for (const [rid, sid] of [["r1", "A"], ["r2", "A"], ["r3", "B"]]) {
    const p = (await prepareDelivery(TENANT, await batch([record({ record_id: rid!, session_id: sid! })]))).records[0]!;
    await f.db.query("INSERT INTO sessions.ingest_frozen_source_records VALUES($1,$2,$3,$4,$5,1,$6,$7,$8,$9,'upsert',$10,NULL,NULL)", [TENANT.orgId, TENANT.workspaceId, o, rid, p.revision_id, p.digest, p.kind, p.source, sid, TENANT.userId]);
    // Neon keeps a support identity per uploaded record (synced into the owned copy at activation).
    await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,$3,$4,$5,'claude',$6)", [TENANT.orgId, TENANT.workspaceId, o, rid, TENANT.userId, sid]);
  }
  // Another user's record in the same origin is untouched.
  const q = (await prepareDelivery(TENANT, await batch([record({ record_id: "other", session_id: "A" })]))).records[0]!;
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_records VALUES($1,$2,$3,'other',$4,1,$5,$6,$7,'A','upsert','user-2',NULL,NULL)", [TENANT.orgId, TENANT.workspaceId, o, q.revision_id, q.digest, q.kind, q.source]);
  await activateFrom(f, n);
  const { head } = await authorityControl(f, "reset", { request_id: "req-1" });
  expect(head).toMatchObject({ version: "1", deleted: "2", apply: true });
  expect(await frozenDeleted(f, "A")).toBe(1);
  expect(await frozenDeleted(f, "B")).toBe(1);
  expect((await f.db.query("SELECT user_id FROM sessions.ingest_frozen_source_records")).rows).toEqual([{ user_id: "user-2" }]);
  expect((await f.db.query("SELECT request_id,deleted::text FROM sessions.ingest_frozen_source_resets")).rows).toEqual([{ request_id: "req-1", deleted: "2" }]);
  // The same request id again: a new head version, Neon's "previous count" answer, nothing re-applied.
  const again = await authorityControl(f, "reset", { request_id: "req-1" });
  expect(again.head).toMatchObject({ version: "2", deleted: "2" });
  expect((await f.db.query("SELECT version::text,deleted::text,pending FROM sessions.ingest_source_control_heads")).rows).toEqual([{ version: "2", deleted: "2", pending: false }]);
});

test("pending authority control: blocks the user's uploads and legacy receipts until completed; a retry replays, another payload is refused", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  const begin = await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "p1" } });
  expect(begin).toMatchObject({ version: "1", pending: true, apply: true, deleted: "0" });
  const raw = await batch([record({ record_id: "u1" })], { batch_id: "while-pending" });
  expect(await err(f.witnessFor(raw))).toContain("privacy control pending");
  expect((await post(app(f, "supabase"), "/v1/delivery/batches", { protocolVersion: 1, batch: raw })).status).toBe(503);
  expect((await post(app(f, "supabase"), "/v1/custody/legacy-receipt", { originId: "o1", batchId: "b1" })).status).toBe(503);
  // Crash-retry with the same payload replays (apply:false, stored count); a different control is refused.
  expect(await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "p1" } })).toMatchObject({ version: "1", apply: false, deleted: "0" });
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "p2" } }))).toContain("control pending");
  // A completion for a different digest or a future version is refused; the right one unblocks uploads.
  expect(await err(f.rpc("ingest_privacy_authority_complete", { epoch, tenant: TENANT, version: "1", digest: "0".repeat(64), kind: "reset", outcome: "applied" }))).toContain("invalid control");
  expect(await err(f.rpc("ingest_privacy_authority_complete", { epoch, tenant: TENANT, version: "2", digest: begin.digest, kind: "reset", outcome: "applied" }))).toContain("invalid control");
  // Complete before the fence: the target has not applied version 1, so the next batch's witness (version 1) refuses at the target.
  await f.rpc("ingest_privacy_authority_complete", { epoch, tenant: TENANT, version: "1", digest: begin.digest, kind: "reset", outcome: "applied" });
  expect((await post(app(f, "supabase"), "/v1/delivery/batches", { protocolVersion: 1, batch: raw })).status).toBe(503);
});

test("epochs: an authority control for another epoch refuses before any write", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch: "4".repeat(64), tenant: TENANT, kind: "reset", payload: { request_id: "e" } }))).toContain("invalid control epoch");
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "selection", payload: { origin_id: "o", sessions: [], enabled: false } }))).toContain("invalid selection");
  expect(await err(f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "selection", payload: { origin_id: "o", sessions: [{ source: "c", session_id: "s", extra: 1 }], enabled: false } }))).not.toBeNull();
  expect((await f.ownerRpc("ingest_privacy_authority_state")).r).toMatchObject({ heads: 0 });
});

test("release from active is allowed only while nothing changed since activation", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  expect((await release(f.queryable, epoch))).toMatchObject({ authority: "neon", changed: true, from: "active" });
  expect(await f.rpc("ingest_source_control_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "x" } })).toMatchObject({ version: "1" });
});

test("the authority lock refuses a non-read-committed transaction", async () => {
  const f = await setup();
  await f.db.exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
  try {
    expect(await err(f.db.query("SELECT sessions.ingest_privacy_authority_fence($1::jsonb)", [JSON.stringify({ controlEpoch: epoch })]))).toContain("requires read committed");
  } finally {
    await f.db.exec("ROLLBACK");
  }
  expect(HandoffRefusal).toBeDefined();
});

/**
 * Reset coverage (no narrowing): Neon's reset fences every session owned_uploaded_session_identities() returns,
 * including sessions known only through uploaded_support_identities and session ids recovered from null-session
 * delete tombstones via neighborhood_claims. The Supabase authority's reset must fence at least that set.
 */
async function ownedWorld() {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  const o = await origin();
  const ws = TENANT.workspaceId, machine = async () => ((await n.pg.query("SELECT 'delivery:'||md5(jsonb_build_array($1::text,$2::text)::text) AS m", [ws, o])).rows[0] as any).m as string;
  const m = await machine();
  // (a) a support identity whose frozen delivery record is gone (no frozen record carries this session);
  await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,$3,'r-a',$4,'claude','ONLY-IDENTITY')", [TENANT.orgId, ws, o, TENANT.userId]);
  // (b) a null-session delete tombstone whose session is recovered through a claim's support event;
  await n.pg.query("INSERT INTO sessions.delivery_records VALUES($1,$2,$3,'r-b',$4,'claude',NULL,'delete')", [TENANT.orgId, ws, o, TENANT.userId]);
  await n.pg.query("INSERT INTO sessions.neighborhood_claims VALUES('c1',$1,$2::jsonb)", [TENANT.orgId, JSON.stringify([{ machineId: m, source: "claude", eventId: m + ":r-b", sessionId: "FROM-CLAIM" }])]);
  // (c) another user's identity in the same origin (must stay untouched by this user's reset).
  await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,$3,'r-c','user-2','claude','OTHER-USER')", [TENANT.orgId, ws, o]);
  return { f, n, o };
}

test("reset coverage: activation refuses until the owned copy is exactly Neon's; sync-owned makes it exact (and prunes)", async () => {
  const { f, n } = await ownedWorld();
  await fence(f.queryable, epoch);
  // A stale extra row in the copy, and the two missing Neon rows.
  await f.db.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES('org-z','ws','u','o','claude','stale')");
  const c = await census(n.queryable, f.queryable, epoch);
  expect(c.ownedEqual).toBe(false);
  expect(c.neon.owned.rows).toBe("3");
  expect(await err(activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 }))).toContain("owned sessions differ from neon");
  expect(await syncOwned(n.queryable, f.queryable, { pageRows: 1 })).toEqual({ rows: 3, inserted: 3, removed: 1 });
  const c2 = await census(n.queryable, f.queryable, epoch);
  expect(c2.ownedEqual).toBe(true);
  expect(c2.supabase.owned).toEqual(c2.neon.owned);
  expect(await syncOwned(n.queryable, f.queryable)).toEqual({ rows: 3, inserted: 0, removed: 0 });
  expect((await activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c2.neon.fingerprint, settleMs: 0 })).activated.state).toBe("active");
  // Once active the copy is the authority: sync-owned refuses, and so does any direct write.
  expect(await err(syncOwned(n.queryable, f.queryable))).toContain("supabase_authority_active");
  expect(await err(f.db.query("DELETE FROM sessions.ingest_frozen_source_owned_sessions"))).toContain("legacy writer refused");
});

test("reset coverage: a Supabase-authority reset fences identity-only and claim-recovered sessions, counts like Neon, leaves other users", async () => {
  const { f, n } = await ownedWorld();
  await fence(f.queryable, epoch);
  await syncOwned(n.queryable, f.queryable);
  const c = await census(n.queryable, f.queryable, epoch);
  await activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 });
  const head = await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "cover" } });
  // Neon's count: distinct (source, session_id) of the user's owned identities = 2.
  expect(head).toMatchObject({ deleted: "2", apply: true });
  expect(await frozenDeleted(f, "ONLY-IDENTITY")).toBe(1);
  expect(await frozenDeleted(f, "FROM-CLAIM")).toBe(1);
  expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_deletions WHERE user_id='user-2'")).rows[0] as any).n).toBe(0);
  expect((await f.db.query("SELECT user_id,session_id FROM sessions.ingest_frozen_source_owned_sessions")).rows).toEqual([{ user_id: "user-2", session_id: "OTHER-USER" }]);
});

test("reset coverage: a disable removes the session's owned identity (as Neon deletes uploaded_support_identities)", async () => {
  const { f, n, o } = await ownedWorld();
  await fence(f.queryable, epoch);
  await syncOwned(n.queryable, f.queryable);
  const c = await census(n.queryable, f.queryable, epoch);
  await activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 });
  await authorityControl(f, "selection", { origin_id: o, sessions: [{ source: "claude", session_id: "ONLY-IDENTITY" }], enabled: false });
  expect(await frozenDeleted(f, "ONLY-IDENTITY")).toBe(1);
  expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions WHERE session_id='ONLY-IDENTITY'")).rows[0] as any).n).toBe(0);
});

/**
 * "C3 must purge digest narratives": the F4 narrative cache (#299, migration 045000, lands after 041000) is purged in the
 * authority's head transaction on every disable and reset, through #299's own purge function (vendored verbatim in the
 * fixture). Without 045000 the guarded call is a no-op.
 */
async function withNarratives(f: F) {
  await f.db.exec(await Bun.file(new URL("./fixtures/digest-narrative-cache-045000.sql", import.meta.url)).text());
  const add = (ws: string) =>
    f.db.query("INSERT INTO sessions.daily_digest_narratives(org_id,workspace_id,day,tz,scope_key,stats_hash,narrative_text,narrative_generated_at) VALUES($1,$2,'2026-11-05','UTC',$3,$4,'cached text',now())",
      [TENANT.orgId, ws, "workspace:" + JSON.stringify([ws, null]), "a".repeat(64)]);
  await add(TENANT.workspaceId);
  await add("other-ws");
  return async () => (await f.db.query("SELECT workspace_id FROM sessions.daily_digest_narratives ORDER BY 1")).rows.map((r: any) => r.workspace_id);
}

test("digest narratives: a Supabase-authority reset purges the workspace's cached narratives (other workspaces kept)", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  const left = await withNarratives(f);
  expect(await left()).toEqual(["other-ws", TENANT.workspaceId].sort());
  await authorityControl(f, "reset", { request_id: "purge-1" });
  expect(await left()).toEqual(["other-ws"]);
});

test("digest narratives: a Supabase-authority disable purges them; a re-enable does not", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  const o = await origin();
  const left = await withNarratives(f);
  await authorityControl(f, "selection", { origin_id: o, sessions: [{ source: "claude", session_id: "S" }], enabled: true });
  expect(await left()).toEqual(["other-ws", TENANT.workspaceId].sort());
  await authorityControl(f, "selection", { origin_id: o, sessions: [{ source: "claude", session_id: "S" }], enabled: false });
  expect(await left()).toEqual(["other-ws"]);
});

test("digest narratives: without migration 045000 the guarded purge is a no-op (reset still succeeds)", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  expect((await authorityControl(f, "reset", { request_id: "no-045" })).head).toMatchObject({ apply: true });
});

test("analysis purge (guarded, ordering): a reset calls purge_session_analysis_for_owned while the user's owned set is still present", async () => {
  // STAND-IN until the analysis lane's head lands (046000/#292): records what the owned set holds at call time.
  // It proves only the guarded call and its ordering; the real function's semantics get their own fixture test.
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1','r1',$3,'claude','NEON-ERA')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
  await activateFrom(f, n);
  await f.db.exec(`CREATE TABLE sessions.__c3_analysis_purge_calls(org_id text,user_id text,owned_seen bigint);
    CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text) RETURNS bigint LANGUAGE sql AS $$
      INSERT INTO sessions.__c3_analysis_purge_calls SELECT p_org,p_user,(SELECT count(*) FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user) RETURNING 0::bigint $$;
    CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text,p_sessions jsonb) RETURNS bigint LANGUAGE sql AS $$ SELECT 0::bigint $$;
    CREATE FUNCTION sessions.session_analysis_purge_grant(p_org text,p_workspace text,p_user text) RETURNS void LANGUAGE sql AS $$ SELECT $$;`);
  await authorityControl(f, "reset", { request_id: "analysis-1" });
  expect((await f.db.query("SELECT org_id,user_id,owned_seen::int FROM sessions.__c3_analysis_purge_calls")).rows).toEqual([{ org_id: TENANT.orgId, user_id: TENANT.userId, owned_seen: 1 }]);
  expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions")).rows[0] as any).n).toBe(0);
});

/** #292's workspace-scoped purge_session_analysis_for_owned (both forms) + its per-transaction grant, VERBATIM from #292 fe120007 (046000). */
async function withAnalysis(f: F) {
  await f.db.exec("DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_analysis_admit') THEN CREATE ROLE bulls96_analysis_admit NOLOGIN NOINHERIT; END IF; END$$;");
  await f.db.exec(await Bun.file(new URL("./fixtures/analysis-jobs-046000.sql", import.meta.url)).text());
  const job = (ws: string, sid: string) =>
    f.db.query("INSERT INTO sessions.session_analysis_jobs_v2(org_id,workspace_id,session_id,analysis_version,schema_version,model_version) VALUES($1,$2,$3,'session-analysis-v2.4',2,'gpt-5.4')", [TENANT.orgId, ws, sid]);
  const owned = (ws: string, user: string, sid: string, o: string) =>
    f.db.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES($1,$2,$3,$4,'claude',$5)", [TENANT.orgId, ws, user, o, sid]);
  const jobs = async () => (await f.db.query("SELECT workspace_id,session_id FROM sessions.session_analysis_jobs_v2 ORDER BY 1,2")).rows.map((r: any) => `${r.workspace_id}/${r.session_id}`);
  return { job, owned, jobs };
}

async function analysisWorld() {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  const o = await origin();
  const a = await withAnalysis(f);
  // user-1 owns S1, S2 in ws-a; user-2 owns a same-id S1 in ws-b. Neon has the same owned identities.
  // Same user, same session id, OTHER workspace (W2 = ws-c): a W1 reset/disable must leave it.
  // user-3 owns S3 in W1 (same org and workspace, other user); org-2 has a same-workspace, same-id S1 job (other org).
  for (const [ws, user, sid] of [[TENANT.workspaceId, TENANT.userId, "S1"], [TENANT.workspaceId, TENANT.userId, "S2"], ["ws-b", "user-2", "S1"], ["ws-c", TENANT.userId, "S1"], [TENANT.workspaceId, "user-3", "S3"]] as const)
    await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,$3,$4,$5,'claude',$6)", [TENANT.orgId, ws, o, `r-${ws}-${user}-${sid}`, user, sid]);
  await activateFrom(f, n);
  for (const [ws, sid] of [[TENANT.workspaceId, "S1"], [TENANT.workspaceId, "S2"], ["ws-b", "S1"], ["ws-c", "S1"], [TENANT.workspaceId, "S3"]] as const) await a.job(ws, sid);
  await f.db.query("INSERT INTO sessions.session_analysis_jobs_v2(org_id,workspace_id,session_id,analysis_version,schema_version,model_version) VALUES('org-2',$1,'S1','session-analysis-v2.4',2,'gpt-5.4')", [TENANT.workspaceId]);
  // Every purge-fence write by the REAL function records the caller's role, the writer marker and how much of the
  // user's owned set / frozen records still existed: the call runs under the authority, before the deletion.
  await f.db.exec(`CREATE TABLE sessions.__c3_purge_ctx(role text, marker text, owned bigint, records bigint);
    CREATE FUNCTION sessions.__c3_purge_ctx_fn() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN
      INSERT INTO sessions.__c3_purge_ctx SELECT current_setting('role',true), current_setting('bulls96.privacy_authority_writer',true),
        (SELECT count(*) FROM sessions.ingest_frozen_source_owned_sessions WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND session_id=NEW.session_id),
        (SELECT count(*) FROM sessions.ingest_frozen_source_records WHERE org_id=NEW.org_id AND workspace_id=NEW.workspace_id AND session_id=NEW.session_id);
      RETURN NEW; END $t$;
    CREATE TRIGGER __c3_purge_ctx BEFORE INSERT ON sessions.session_analysis_purge_fences FOR EACH ROW EXECUTE FUNCTION sessions.__c3_purge_ctx_fn();`);
  const ctx = async () => (await f.db.query("SELECT role, marker, owned::int AS owned FROM sessions.__c3_purge_ctx")).rows as { role: string; marker: string; owned: number }[];
  const org2 = async () => (await f.db.query("SELECT session_id FROM sessions.session_analysis_jobs_v2 WHERE org_id='org-2'")).rows.map((r: any) => r.session_id);
  return { f, o, ctx, org2, ...a };
}

test("analysis purge (real #292 function): a reset in W1 purges the user's W1 owned set; W2 and other users are untouched", async () => {
  const w = await analysisWorld();
  expect(await w.jobs()).toEqual([`${TENANT.workspaceId}/S1`, `${TENANT.workspaceId}/S2`, `${TENANT.workspaceId}/S3`, "ws-b/S1", "ws-c/S1", `${TENANT.workspaceId}/S1`].sort());
  await authorityControl(w.f, "reset", { request_id: "analysis-reset" });
  // W1's analysis for THIS user is purged; the same user's same-id session in W2 (ws-c), another user's session in
  // W1 (S3) and in ws-b, and another org's same-workspace same-id job are all untouched.
  expect(await w.jobs()).toEqual([`${TENANT.workspaceId}/S1`, `${TENANT.workspaceId}/S3`, "ws-b/S1", "ws-c/S1"]);
  expect(await w.org2()).toEqual(["S1"]);
  expect((await w.f.db.query("SELECT org_id, session_id FROM sessions.session_analysis_purge_fences ORDER BY 2")).rows).toEqual([{ org_id: TENANT.orgId, session_id: "S1" }, { org_id: TENANT.orgId, session_id: "S2" }]);
  // Both fences were written by the real function as the ingest role, under the authority marker, while the owned
  // set was still present (before the deletion).
  expect(await w.ctx()).toEqual([1, 1].map((owned) => ({ role: "bulls96_session_ingest", marker: "authority", owned })));
  expect(((await w.f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions WHERE user_id=$1 AND workspace_id=$2", [TENANT.userId, TENANT.workspaceId])).rows[0] as any).n).toBe(0);
});

test("analysis purge (real #292 function): a disable purges only the listed sessions", async () => {
  const w = await analysisWorld();
  await authorityControl(w.f, "selection", { origin_id: w.o, sessions: [{ source: "claude", session_id: "S1" }], enabled: false });
  // Only W1's S1: S2 in W1, the same user's S1 in W2 (ws-c), other users' sessions and the other org are untouched.
  expect(await w.jobs()).toEqual([`${TENANT.workspaceId}/S1`, `${TENANT.workspaceId}/S2`, `${TENANT.workspaceId}/S3`, "ws-b/S1", "ws-c/S1"]);
  expect(await w.org2()).toEqual(["S1"]);
  expect(await w.ctx()).toEqual([{ role: "bulls96_session_ingest", marker: "authority", owned: 1 }]);
  // A re-enable purges nothing.
  await authorityControl(w.f, "selection", { origin_id: w.o, sessions: [{ source: "claude", session_id: "S2" }], enabled: true });
  expect(await w.jobs()).toEqual([`${TENANT.workspaceId}/S1`, `${TENANT.workspaceId}/S2`, `${TENANT.workspaceId}/S3`, "ws-b/S1", "ws-c/S1"]);
});

test("analysis purge (real #292 fe120007): a reset and a disable each mint the grant for EXACTLY the scope, consumed by the purge in the SAME transaction", async () => {
  const w = await analysisWorld();
  // Every grant row inserted, with the transaction that minted it; every purge fence write, with ITS transaction and
  // whether the grant row for its scope was already consumed (the purge deletes it before writing fences).
  await w.f.db.exec(`CREATE TABLE sessions.__c3_grants(txid bigint, org_id text, workspace_id text, user_id text);
    CREATE FUNCTION sessions.__c3_grant_fn() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN
      INSERT INTO sessions.__c3_grants VALUES (NEW.txid, NEW.org_id, NEW.workspace_id, NEW.user_id); RETURN NEW; END $t$;
    CREATE TRIGGER __c3_grant AFTER INSERT ON sessions.session_analysis_purge_authority FOR EACH ROW EXECUTE FUNCTION sessions.__c3_grant_fn();
    CREATE TABLE sessions.__c3_fence_tx(txid bigint, pending bigint);
    CREATE FUNCTION sessions.__c3_fence_tx_fn() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN
      INSERT INTO sessions.__c3_fence_tx SELECT txid_current(), (SELECT count(*) FROM sessions.session_analysis_purge_authority); RETURN NEW; END $t$;
    CREATE TRIGGER __c3_fence_tx BEFORE INSERT ON sessions.session_analysis_purge_fences FOR EACH ROW EXECUTE FUNCTION sessions.__c3_fence_tx_fn();`);
  const grants = async () => (await w.f.db.query("SELECT txid::text AS txid, org_id, workspace_id, user_id FROM sessions.__c3_grants ORDER BY txid")).rows as { txid: string; org_id: string; workspace_id: string; user_id: string }[];
  const fences = async () => (await w.f.db.query("SELECT txid::text AS txid, pending::int AS pending FROM sessions.__c3_fence_tx")).rows as { txid: string; pending: number }[];
  await authorityControl(w.f, "selection", { origin_id: w.o, sessions: [{ source: "claude", session_id: "S1" }], enabled: false });
  const g1 = await grants();
  expect(g1.map(({ org_id, workspace_id, user_id }) => ({ org_id, workspace_id, user_id }))).toEqual([{ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId }]);
  expect((await fences()).map((r) => r.txid)).toEqual([g1[0]!.txid]); // the purge ran in the grant's transaction
  expect((await fences())[0]!.pending).toBe(0); // and consumed the grant before writing its fence
  await authorityControl(w.f, "reset", { request_id: "grant-reset" });
  const g2 = await grants();
  expect(g2.length).toBe(2);
  expect(g2[1]).toMatchObject({ org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId });
  expect(new Set((await fences()).slice(1).map((r) => r.txid))).toEqual(new Set([g2[1]!.txid]));
  // No proof outlives its transaction.
  expect(((await w.f.db.query("SELECT count(*)::int AS n FROM sessions.session_analysis_purge_authority")).rows[0] as any).n).toBe(0);
});

test("analysis purge (real #292 fe120007): without the authority's grant, a purge in the ingest role is refused even under the writer marker", async () => {
  const w = await analysisWorld();
  const e = await w.f.db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE bulls96_session_ingest");
    await tx.query("SELECT set_config('bulls96.privacy_authority_writer','authority',true)");
    return tx.query("SELECT sessions.purge_session_analysis_for_owned($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]).then(() => null, (x: any) => x.code);
  });
  expect(e).toBe("42501");
});

test("analysis purge grant (real #292 fe120007): the ingest role cannot MINT a proof directly, even under the writer marker; only the owner-context authority can", async () => {
  const w = await analysisWorld();
  const e = await w.f.db.transaction(async (tx) => {
    await tx.exec("SET LOCAL ROLE bulls96_session_ingest");
    await tx.query("SELECT set_config('bulls96.privacy_authority_writer','authority',true)");
    return tx.query("SELECT sessions.session_analysis_purge_grant($1,$2,$3)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]).then(() => null, (x: any) => x.code);
  });
  expect(e).toBe("42501");
  expect(((await w.f.db.query("SELECT count(*)::int AS n FROM sessions.session_analysis_purge_authority")).rows[0] as any).n).toBe(0);
  // The positive path is the REAL C3 apply as the ingest role: begin (definer) -> mint -> purge consumes (above test).
  await authorityControl(w.f, "reset", { request_id: "owner-context-mint" });
  // This tenant's W1 analysis (S1, S2) is purged; the other org's same-id W1 job, user-3's S3 and other workspaces stay.
  expect(await w.jobs()).toEqual([`${TENANT.workspaceId}/S1`, `${TENANT.workspaceId}/S3`, "ws-b/S1", "ws-c/S1"]);
  expect(await w.org2()).toEqual(["S1"]);
});

test("analysis purge (real #292 function): the ingest role cannot call it outside the authority (writer marker required)", async () => {
  const w = await analysisWorld();
  for (const sql of ["SELECT sessions.purge_session_analysis_for_owned($1,$2,$3)", "SELECT sessions.purge_session_analysis_for_owned($1,$2,$3,'[]'::jsonb)"])
    expect(await w.f.run(sql, [TENANT.orgId, TENANT.workspaceId, TENANT.userId]).then(() => "ok", (e: any) => e.code)).toBe("42501");
  expect((await w.jobs()).length).toBe(6);
});

/** 046000 present in any form, but without BOTH exact workspace-scoped signatures (e.g. #292's older f7f49308 ABI). */
async function staleAnalysisAbi(f: F, variant: "old-signatures" | "table-only" | "reset-form-only" | "no-grant") {
  if (variant !== "reset-form-only") await f.db.exec("CREATE TABLE sessions.session_analysis_jobs_v2(org_id text, workspace_id text, session_id text)");
  if (variant === "old-signatures")
    await f.db.exec(`CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_user text) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;
      CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_user text,p_sessions jsonb) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;`);
  if (variant === "no-grant")
    // Both exact workspace-scoped purges, but 046000's grant helper is missing (its purge-authority table is present).
    await f.db.exec(`CREATE TABLE sessions.session_analysis_purge_authority(txid bigint, org_id text, workspace_id text, user_id text);
      CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;
      CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text,p_sessions jsonb) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;`);
  if (variant === "reset-form-only")
    await f.db.exec("CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;");
}

for (const variant of ["old-signatures", "table-only", "reset-form-only", "no-grant"] as const)
  test(`analysis ABI fail-closed (${variant}): a reset and a disable ABORT, nothing applied`, async () => {
    const f = await setup();
    const n = await fakeNeon();
    open.push(n.close);
    await n.pg.query("INSERT INTO sessions.uploaded_support_identities VALUES($1,$2,'o1','r1',$3,'claude','NEON-ERA')", [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    await activateFrom(f, n);
    const o = await origin();
    await staleAnalysisAbi(f, variant);
    const owned = async () => ((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_owned_sessions")).rows[0] as any).n;
    expect(await owned()).toBe(1);
    for (const [kind, payload] of [["reset", { request_id: "abi-reset" }], ["selection", { origin_id: o, sessions: [{ source: "claude", session_id: "NEON-ERA" }], enabled: false }]] as const) {
      const b = await post(app(f, "supabase"), "/v1/custody/authority/begin", { epoch, kind, payload });
      expect(b.status).not.toBe(200);
    }
    // Nothing was applied: the owned set, frozen deletions and resets are unchanged.
    expect(await owned()).toBe(1);
    expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_resets")).rows[0] as any).n).toBe(0);
    expect(((await f.db.query("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_deletions")).rows[0] as any).n).toBe(0);
  });

test("analysis ABI: the begin's refusal is the ABI mismatch itself (55000), called directly as the ingest role", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  await staleAnalysisAbi(f, "old-signatures");
  const e = await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "abi-direct" } }).then(() => null, (x: any) => x);
  expect([e?.code, e?.message]).toEqual(["55000", "frozen source: analysis purge ABI mismatch"]);
});

/** Issue #310 (relayhistory-pipeline): each 046000 footprint clause in isolation (and the no-grant variant) refuses
 * with exactly 55000 at begin, for a RESET and for a DISABLE. */
for (const [name, ddl] of [
  ["no-grant (both purges, no grant helper)", `CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;
    CREATE FUNCTION sessions.purge_session_analysis_for_owned(p_org text,p_workspace text,p_user text,p_sessions jsonb) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;`],
  ["purge-authority table only", "CREATE TABLE sessions.session_analysis_purge_authority(txid bigint, org_id text, workspace_id text, user_id text)"],
  ["grant-name only (another overload)", "CREATE FUNCTION sessions.session_analysis_purge_grant(p_x jsonb) RETURNS void LANGUAGE sql AS $$ SELECT $$"],
] as const)
  test(`#310 analysis ABI footprint (${name}): a reset AND a disable begin refuse with exactly 55000 "analysis purge ABI mismatch"`, async () => {
    const f = await setup();
    const n = await fakeNeon();
    open.push(n.close);
    await activateFrom(f, n);
    const o = await origin();
    await f.db.exec(ddl);
    for (const [kind, payload] of [["reset", { request_id: "footprint" }], ["selection", { origin_id: o, sessions: [{ source: "claude", session_id: "S" }], enabled: false }]] as const) {
      const e = await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind, payload }).then(() => null, (x: any) => x);
      expect([kind, e?.code, e?.message]).toEqual([kind, "55000", "frozen source: analysis purge ABI mismatch"]);
    }
  });

test("digest ABI fail-closed: 045000's table present without purge_daily_digest_narratives(text,text) aborts the reset", async () => {
  const f = await setup();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f, n);
  await f.db.exec("CREATE TABLE sessions.daily_digest_narratives(org_id text, workspace_id text)");
  const e = await f.rpc("ingest_privacy_authority_begin", { epoch, tenant: TENANT, kind: "reset", payload: { request_id: "digest-abi" } }).then(() => null, (x: any) => x);
  expect([e?.code, e?.message]).toEqual(["55000", "frozen source: digest narrative purge ABI mismatch"]);
});

/**
 * Buffered/staged copies (coordinator follow-up): a batch staged under buffered-batches/ and reset BEFORE promotion
 * must leave no version of its content: the real buffered GC (claim -> redaction barrier -> erase every other version)
 * over a versioned store, driven by the target fences the authority reset produces.
 */
async function bufferedWorld() {
  const { VersionedMemoryStore } = await import("./versioned-store.ts");
  const { sqlBufferedGcLedger, bufferedGcTick } = await import("../buffered-gc.ts");
  const store = new VersionedMemoryStore();
  const f = await authorityFixture({ store: store as never });
  open.push(f.close);
  await f.seal();
  const n = await fakeNeon();
  open.push(n.close);
  await activateFrom(f as F, n);
  const gc = (limit: number) => bufferedGcTick({ ledger: sqlBufferedGcLedger({ query: (s, p) => f.run(s, p as unknown[]) as never }, "a".repeat(64)), store: store as never, eraser: store.eraser(), keyRoot: "96bulls" }, limit);
  const upload = async (tenant: typeof TENANT, sid: string, batchId: string, text: string) => {
    const a = createIngestApp({ identify: () => ({ ...tenant, scopes: ["rth:sync"] }), deliver: f.deps, log: () => {}, custodySource: "frozen-supabase", privacyAuthority: "supabase" });
    const r = await a.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: batchId + "-r", session_id: sid, payload: { text, ts_ms: 1_761_955_200_000 } })], { batch_id: batchId, account_id: await deliveryAccount(tenant) }) }) });
    if (r.status !== 200) throw new Error("upload " + r.status + " " + (await r.text()));
  };
  const { zstdDecompressSync } = await import("node:zlib");
  /** Every stored version under buffered-batches/ whose (decompressed) bytes contain `text`. */
  const holding = (text: string) => store.versions("96bulls/").filter((v) => v.key.includes("/buffered-batches/") && v.body).filter((v) => {
    const raw = Buffer.from(v.body!);
    let plain = raw;
    try { plain = zstdDecompressSync(raw); } catch {}
    return plain.includes(text);
  });
  return { f, store, gc, upload, holding };
}

test("buffered: a batch staged and reset before promotion leaves no version of its content (real GC, versioned store)", async () => {
  const w = await bufferedWorld();
  await w.upload(TENANT, "STAGED", "b-staged", "C3-STAGED-SECRET");
  expect(w.holding("C3-STAGED-SECRET").length).toBe(1);
  // Healthy and unreset: the GC keeps it.
  await w.gc(5);
  expect(w.holding("C3-STAGED-SECRET").length).toBe(1);
  await authorityControl(w.f as F, "reset", { request_id: "staged-reset" });
  const r = await w.gc(5);
  expect(r.finished).toBeGreaterThanOrEqual(1);
  expect(w.holding("C3-STAGED-SECRET")).toEqual([]);
  expect(((await w.f.db.query("SELECT cleanup_state FROM sessions.ingest_stage_intents WHERE batch_id='b-staged'")).rows[0] as any).cleanup_state).toBe("redacted");
});

test("buffered: a reset user's stages jump the GC queue (examined in the very next claim, ahead of older healthy stages)", async () => {
  const w = await bufferedWorld();
  const other = { ...TENANT, userId: "user-2" };
  for (let i = 0; i < 6; i++) await w.upload(other, `O${i}`, `b-other-${i}`, `OTHER-${i}`);
  await w.upload(TENANT, "LATE", "b-late", "C3-LATE-SECRET");
  await authorityControl(w.f as F, "reset", { request_id: "late-reset" });
  // One single-candidate claim: it must be the reset user's stage, not the oldest healthy one.
  await w.gc(1);
  expect(w.holding("C3-LATE-SECRET")).toEqual([]);
  for (let i = 0; i < 6; i++) expect(w.holding(`OTHER-${i}`).length).toBe(1);
});

/**
 * Registry guard: the analysis fixture is a COPY of #292's 046000 migration, which keeps changing. Drift fails loudly:
 * once 046000 is in the tree, the fixture's two purge_session_analysis_for_owned definitions must equal it byte for
 * byte; until then the fixture body must still hash to the source-sha256 its header pins (the reviewed #292 commit).
 */
test("analysis fixture: the purge definitions match #292's 046000 source byte for byte (or the pinned source hash)", async () => {
  const { createHash } = await import("node:crypto");
  const { existsSync, readFileSync } = await import("node:fs");
  const fixture = readFileSync(new URL("./fixtures/analysis-jobs-046000.sql", import.meta.url), "utf8");
  const header = fixture.split("\n").slice(0, 2).join("\n");
  const body = fixture.split("\n").slice(2).join("\n");
  const pinned = /source-sha256: ([0-9a-f]{64})/.exec(header)?.[1];
  expect(pinned).toBeDefined();
  expect(createHash("sha256").update(body).digest("hex")).toBe(pinned!);
  const defs = (sql: string) => [
    /CREATE FUNCTION sessions\.purge_session_analysis_for_owned\(p_org text, p_workspace text, p_user text, p_sessions jsonb\)[\s\S]*?\$purge_owned\$;/,
    /CREATE FUNCTION sessions\.purge_session_analysis_for_owned\(p_org text, p_workspace text, p_user text\)[\s\S]*?\$purge_owned_all\$;/,
  ].map((re) => re.exec(sql)?.[0]);
  const mine = defs(body);
  expect(mine.every((d) => typeof d === "string")).toBe(true);
  const source = new URL("../../../supabase/migrations/20261105046000_analysis_jobs.sql", import.meta.url);
  if (existsSync(source)) expect(mine).toEqual(defs(readFileSync(source, "utf8")));
});
