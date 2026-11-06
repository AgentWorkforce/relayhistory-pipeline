/**
 * C3: Supabase-first privacy authority (supabase/pending/ingest_privacy_authority.sql, slot TBD), through the real
 * migration stack, the unchanged ingest fence and lazy-current target, and the handoff module against a Neon with
 * relayhistory's exact privacy tables. Real-PG races are in privacy-authority.postgres.test.ts.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { createIngestApp } from "../app.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import { CURRENT_CUSTODY_CONTENT_TYPE } from "../custody-protocol.ts";
import { activate, census, fence, release, readNeonSnapshot, HandoffRefusal } from "../../migration/privacy-authority-handoff.ts";
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
  expect(await err(f.ownerRpc("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: snap.fingerprint, heads: [], deletions: [], resets: snap.resets }))).toContain("not the neon fingerprint");
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
  await f.db.exec(await Bun.file(new URL("../../../supabase/pending/ingest_privacy_authority_index.sql", import.meta.url)).text());
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
