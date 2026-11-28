/**
 * ISOLATED SYNTHETIC PROOF (lander: go for synthetic proof only; docs/design/org-bridge-gate.md, A recovery and W2 (2)).
 * Real SQL: PGlite with every migration (062320 hold lifecycle, 062340 history). Real S3 ledger: ledger-s3 with
 * `orgBridge: "required"` over a scratch fs store. The canonical layout and the two READERS are toys, labelled:
 *   todayReader  serves every manifest entry (what the read API does today: it never consults ledger state);
 *   w2Reader     serves an entry only within the enrollment baseline, or with a visible effect row (none exist here
 *                for direct writes), the W2 rule (2). Bridge-stamped commits, rule (c), are not built.
 * Cases: the paused installer resumes after an install_failed withdrawal; a crash between the withdrawal's _install
 * and its verified_rollback, then the rerun; a paused direct writer (claimed and bytes written before enrollment)
 * resumes across enrollment and install. Every read is taken right after resumption, before any repair.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import type { ClaimInput, CommitInput, LedgerRecord } from "../ledger.ts";
import { fenceAuthorityKey, orgEnrollmentKey, s3Ledger } from "../ledger-s3.ts";
import { enrollmentFor, enrollOrg, OrgBridgeRefusal, withdrawEnrollment, type BridgeSql } from "../org-bridge-gate.ts";
import { liveMachineId } from "../writer.ts";
import { authorityFixture } from "./privacy-authority-support.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });

const ROOT = "96bulls";
const ORG = "org-proof-w";
const TENANT = { orgId: ORG, workspaceId: "ws-proof", userId: "user-proof" };
const ORIGIN = "origin-proof";
const SLOTS = ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"];
const FIELDS = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 };
const hex = (v: string) => createHash("sha256").update(v).digest("hex");
const enc = new TextEncoder();
const dec = new TextDecoder();

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const s of SLOTS) await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [s]);
  const s = scratchStore();
  open.push(s.done);
  const store = s.store;
  // The org already accepts direct uploads before any hold (the deployment-wide marker), as the T97 scenario.
  await store.put(objectKey(fenceAuthorityKey(ROOT)), enc.encode(JSON.stringify(FIELDS)));
  const sql: BridgeSql = { query: (t, p) => f.db.query(t, p as never) as never };
  const ledger = s3Ledger(store, ROOT, { fenceAuthority: "required", orgBridge: "required", sleep: async () => {} });
  return { f, store, sql, ledger };
}

// ---- the toy canonical layout and readers ----
const manifestKey = (session: string) => `${ROOT}/${ORG}/v1/toy/s=${session}/manifest.json`;
async function readManifest(store: ObjectStore, session: string): Promise<string[]> {
  try { return JSON.parse(dec.decode(await readAll(await store.get(objectKey(manifestKey(session)))))); } catch { return []; }
}
/** The writer's canonical write (chunk, then manifest append): readable from the manifest append on. */
async function writeCanonical(store: ObjectStore, session: string, batch: string) {
  await store.put(objectKey(`${ROOT}/${ORG}/v1/toy/s=${session}/raw/${batch}.json`), enc.encode(JSON.stringify({ batch })));
  await store.put(objectKey(manifestKey(session)), enc.encode(JSON.stringify([...(await readManifest(store, session)), batch])));
}
const todayReader = (store: ObjectStore, session: string) => readManifest(store, session);
const w2Reader = async (store: ObjectStore, session: string, baseline: string[]) => (await readManifest(store, session)).filter((b) => baseline.includes(b));

// ---- the direct writer ----
function rec(recordId: string): LedgerRecord {
  return { source: "claude", sessionId: "sess-1", recordId, revision: 1, revisionId: `${recordId}@1`, digest: hex(recordId) };
}
const claimInput = (batchId: string): ClaimInput => ({ tenant: TENANT, originId: ORIGIN, batchId, batchDigest: hex(batchId), receiptId: `rhr_${hex(`r:${batchId}`)}`,
  sessions: [["claude", "sess-1"]], records: [rec(`rec-${batchId}`)] }) as ClaimInput;
const commitOf = (c: ClaimInput, claimToken: string): CommitInput => ({ tenant: c.tenant, originId: c.originId, batchId: c.batchId, batchDigest: c.batchDigest, claimToken,
  sessions: c.sessions, records: c.records, machineId: liveMachineId(TENANT.workspaceId, ORIGIN, TENANT.userId) });
const code = (e: unknown) => (e instanceof OrgBridgeRefusal ? e.code : String((e as { code?: unknown }).code));
async function directUpload(x: Awaited<ReturnType<typeof fixture>>, batch: string): Promise<string> {
  try {
    const c = claimInput(batch);
    const r = await x.ledger.claim(c);
    if (r.kind !== "claimed") return r.kind;
    await writeCanonical(x.store, "sess-1", batch);
    await x.ledger.commit(commitOf(c, r.claimToken));
    return "committed";
  } catch (e) { return code(e); }
}
const liveHolds = async (x: Awaited<ReturnType<typeof fixture>>) =>
  (await x.f.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.ingest_org_admission_holds WHERE held_org = $1 AND state <> 'released'", [ORG])).rows[0]!.n;

describe("A recovery and W2 (2): what readers see when a paused installer or direct writer resumes (synthetic)", () => {
  test("the paused installer resumes after an install_failed withdrawal: it cannot commit, and readers see only the pre-cutover bytes", async () => {
    const x = await fixture();
    expect(await directUpload(x, "before")).toBe("committed"); // the org already admits direct uploads
    const request = randomUUID();
    expect((await enrollOrg({ sql: x.sql, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, now: () => 5 })).generation).toBe(1);
    // The installer passes its check, then pauses before _install.
    expect(await enrollmentFor(x.store, ROOT, ORG, request)).toEqual({ generation: 1 });
    expect(await directUpload(x, "while-enrolled")).toBe("ledger_fence_authority_bridge_pending"); // fenced at once
    // The operator gives up on this install: the withdrawal consumes the request in SQL, then reopens S3.
    expect(await withdrawEnrollment({ sql: x.sql, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, reason: "install_failed", now: () => 6 })).toEqual({ outcome: "withdrawn" });
    expect(await liveHolds(x)).toBe(0);
    // The paused installer resumes: refused (request reused), no hold, so no held window opens behind open admission.
    await expect(x.f.db.query("SELECT sessions.ingest_org_admission_hold_install($1, $2::uuid)", [ORG, request])).rejects.toMatchObject({ code: "55000" });
    expect(await liveHolds(x)).toBe(0);
    // Reads right after resumption: the pre-cutover upload only (the fenced one never wrote a byte).
    expect(await todayReader(x.store, "sess-1")).toEqual(["before"]);
    // The org is back to its pre-cutover writer (direct, by the deployment-wide marker): one writer class, as before.
    expect(await directUpload(x, "after-withdrawal")).toBe("committed");
  });

  test("a crash between the withdrawal's _install and its verified_rollback: SQL held AND S3 enrolled (both fenced); the rerun completes", async () => {
    const x = await fixture();
    const request = randomUUID();
    await enrollOrg({ sql: x.sql, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, now: () => 5 });
    // The withdrawal dies right after its _install commits.
    const crashing: BridgeSql = { query: async (t, p) => { if (t.includes("ingest_org_admission_hold_release")) throw new Error("killed"); return x.sql.query(t, p); } };
    await expect(withdrawEnrollment({ sql: crashing, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, reason: "install_failed" })).rejects.toMatchObject({ code: "enrollment_consume_failed" });
    expect(await liveHolds(x)).toBe(1); // held in SQL with no seal
    expect(JSON.parse(dec.decode(await readAll(await x.store.get(objectKey(orgEnrollmentKey(ROOT, ORG)))))).state).toBe("enrolled");
    // Both writers fenced meanwhile: the direct writer by enrollment, SQL admission by the hold.
    expect(await directUpload(x, "during-crash")).toBe("ledger_fence_authority_bridge_pending");
    expect((await x.f.db.query<{ s: any }>("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).rows[0]!.s.hold.state).toBe("held");
    expect(await todayReader(x.store, "sess-1")).toEqual([]);
    // The rerun finds the live hold of this request, releases it, and withdraws.
    expect(await withdrawEnrollment({ sql: x.sql, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, reason: "install_failed" })).toEqual({ outcome: "withdrawn" });
    expect(await liveHolds(x)).toBe(0);
    await expect(x.f.db.query("SELECT sessions.ingest_org_admission_hold_install($1, $2::uuid)", [ORG, request])).rejects.toMatchObject({ code: "55000" });
  });

  test("a paused direct writer (claimed and bytes written before enrollment) resumes across enrollment and install: commit refused; today's reader serves its bytes, the W2 rule does not", async () => {
    const x = await fixture();
    expect(await directUpload(x, "before")).toBe("committed");
    // The paused writer: claimed under the deployment-wide marker, then paused.
    const c = claimInput("paused");
    const r = await x.ledger.claim(c);
    expect(r.kind).toBe("claimed");
    // The cutover captures the enrollment baseline (W2 (2)), then enrolls. The paused writer's bytes land after the
    // capture (an entry that landed before it is pre-cutover state, erased through its owner stub like any other).
    const baseline = await readManifest(x.store, "sess-1");
    expect(baseline).toEqual(["before"]);
    await writeCanonical(x.store, "sess-1", "paused");
    const request = randomUUID();
    await enrollOrg({ sql: x.sql, store: x.store, keyRoot: ROOT, orgId: ORG, requestId: request, now: () => 5 });
    const epoch = (await x.f.db.query<{ e: string }>("SELECT sessions.ingest_org_admission_hold_install($1, $2::uuid)::text e", [ORG, request])).rows[0]!.e;
    expect(epoch).toMatch(/^[0-9a-f-]{36}$/);
    // The writer resumes: its commit is refused while held (no index entry, batch pending).
    await expect(x.ledger.commit(commitOf(c, (r as { claimToken: string }).claimToken))).rejects.toMatchObject({ code: "ledger_fence_authority_bridge_pending" });
    // Read right after resumption, before any repair:
    expect(await todayReader(x.store, "sess-1")).toEqual(["before", "paused"]); // THE GAP: readable without commit
    expect(await w2Reader(x.store, "sess-1", baseline)).toEqual(["before"]);    // W2 (2): beyond the baseline, no effect row
  });
});
