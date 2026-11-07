/**
 * S3X_OBLIGATIONS (default OFF) gates the NEW exporter-obligation protocol, its schema and genesis bootstrap. Off: no
 * obligation schema, triggers or genesis on open; leases without generations; the exporter records and claims nothing;
 * the erasure job still uses the STRICT canonical paged eraser (unflagged hardening), without obligations. Used
 * obligation state is refused off. On: the obligation model. Plus the registry's M1 cases on the production default.
 */
import { expect, test } from "bun:test";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { SyncState } from "../neon-s3-sync/state.ts";
import { obligationsEnabled } from "../neon-s3-erasure/main.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const tmp = async (n: string) => join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), n)), "s.sqlite");
const tables = (path: string) => {
  const db = new DatabaseSync(path);
  try {
    return (db.prepare("SELECT name FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY name").all() as { name: string }[]).map((r) => r.name);
  } finally {
    db.close();
  }
};

test("OFF (the default): no obligation schema, triggers, genesis or lease generation; base lease / queue behaviour", async () => {
  expect(obligationsEnabled(() => undefined)).toBe(false);
  expect(obligationsEnabled(() => "on")).toBe(true);
  expect(obligationsEnabled(() => "true")).toBe(false);
  const path = await tmp("e3-flag-off-");
  const st = new SyncState(path, { obligations: false });
  expect(st.obligations).toBe(false);
  expect(st.acquireLease("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", "exporter:1")).toBe(true);
  expect(st.leaseGen("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/")).toBeUndefined();
  st.enqueueErasure([{ key: "96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", kind: "prefix", org_id: "o", reason: "fenced" }]);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).length).toBe(1);
  expect(st.meta("obligations_genesis")).toBeUndefined();
  st.close();
  const names = tables(path);
  for (const t of ["write_invocations", "write_published", "erase_reservations", "write_terminal", "erased_versions", "write_touch", "write_clock"])
    expect(names).not.toContain(t);
  expect(names).toContain("erase_checkpoints"); // the strict eraser's resume state: unflagged hardening
  expect(names.some((n) => n.includes("_touch_"))).toBe(false);
});

test("OFF: the base schema statement is byte-identical to main 341dd5a9 (#289 merged), pinned by digest (no git needed)", async () => {
  const { createHash } = await import("node:crypto");
  const src = await readFile(new URL("../neon-s3-sync/state.ts", import.meta.url), "utf8");
  const cut = src.slice(src.indexOf('this.db.exec("PRAGMA journal_mode'), src.indexOf("CREATE INDEX IF NOT EXISTS groups_last"));
  // sha256 of exactly that span in main 341dd5a9's src/migration/neon-s3-sync/state.ts.
  expect(createHash("sha256").update(cut).digest("hex")).toBe("01af58f5a8c87bd748930abfc15548be192a0993c00262a2b3fcf0514ba89420");
});

test("ON: the obligation schema, triggers, lease generations and an unattested genesis", async () => {
  const path = await tmp("e3-flag-on-");
  const st = new SyncState(path, { obligations: true });
  expect(st.acquireLease("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", "exporter:1")).toBe(true);
  expect(st.leaseGen("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/")).toMatch(/^[0-9a-f-]{36}$/);
  expect(st.obligationGenesis()).toMatchObject({ attestedAt: null, basis: "none" });
  st.close();
  const names = tables(path);
  for (const t of ["write_invocations", "write_published", "erase_reservations", "write_terminal", "erased_versions", "write_touch", "erase_checkpoints"]) expect(names).toContain(t);
});

test("M1, the UNSETTLED production default (obligationsSettled unset): reservations are still retired, so the exporter's claim and cancel are never wedged", async () => {
  const PREFIX = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
  const s = new FakeS3Versions();
  for (let i = 0; i < 30; i++) s.put(`${PREFIX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
  const st = new SyncState(await tmp("e3-flag-m1-"), { obligations: true });
  const clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], clock - 1000);
  const authority: ErasureAuthority = { kind: "supabase", ready: async () => {}, lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }), fencedGroups: async () => [], fencedLivePage: async () => [], stillFenced: async () => true };
  const j = new ErasureJob(authority, st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => clock, async () => {});
  await j.drain();
  expect(s.count(PREFIX)).toBe(0); // erased ...
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).map((e) => e.key)).toEqual([PREFIX]); // ... never reported done (unsettled)
  const reserved = (st as unknown as { db: DatabaseSync }).db.prepare("SELECT count(*) AS n FROM erase_reservations").get() as { n: number };
  expect(reserved.n).toBe(0); // the proven-absent reservations were retired anyway
  // The exporter re-references a key under the prefix: its claim is NOT refused (publish_reserved), and cancel works.
  expect(st.acquireLease(PREFIX, "exporter:1")).toBe(true);
  st.publishWrites([{ key: `${PREFIX}raw/b=000003.ndjson.zst`, versionId: "*" }], { holder: "exporter:1", gens: { [PREFIX]: st.leaseGen(PREFIX)! } });
  st.cancelErasures([PREFIX]);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5)).toEqual([]);
});

const PFX = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
const auth = (): ErasureAuthority => ({ kind: "supabase", ready: async () => {}, lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }), fencedGroups: async () => [], fencedLivePage: async () => [], stillFenced: async () => true });

test("C1: ON -> OFF with USED obligation state is REFUSED (never silently dropped); unused state may be opened off", async () => {
  const used = await tmp("e3-flag-used-");
  const a = new SyncState(used, { obligations: true });
  a.dispatchWrite(`${PFX}raw/x.zst`, "exporter:1");
  a.close();
  expect(() => new SyncState(used, { obligations: false })).toThrow(expect.objectContaining({ code: "obligations_state_in_use" }));
  // ... and the protections are preserved: back on, the outstanding invocation is still there.
  const again = new SyncState(used, { obligations: true });
  expect(again.unresolvedWrites(PFX).length).toBe(1);
  again.close();
  const unused = await tmp("e3-flag-unused-");
  new SyncState(unused, { obligations: true }).close();
  const off = new SyncState(unused, { obligations: false });
  expect(off.obligations).toBe(false);
  off.close();
});

test("C1: OFF dry run and OFF erasure use the STRICT paged eraser and create no obligation state", async () => {
  const path = await tmp("e3-flag-dry-");
  const st = new SyncState(path, { obligations: false });
  const s = new FakeS3Versions();
  for (let i = 0; i < 12_500; i++) s.put(`${PFX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
  const clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PFX, kind: "prefix", org_id: "org-1", reason: "fenced" }], clock - 1000);
  const job = new ErasureJob(auth(), st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => clock, async () => {});
  await job.sweep({ dryRun: true });
  expect(s.count(PFX)).toBe(12_500); // the dry run erases nothing
  // The strict primitive: bounded per run (10k), resumed from its checkpoint, done only after a strict zero inventory.
  await job.drain();
  expect(s.count(PFX)).toBe(2_500);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).map((e) => e.key)).toEqual([PFX]);
  await job.drain();
  expect(s.count(PFX)).toBe(0);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5)).toEqual([]);
  st.close();
  expect(tables(path).filter((n) => n.startsWith("write_") || n === "erase_reservations" || n === "erased_versions")).toEqual([]);
});

test("M1 (a)+(b), production default: a claim hitting publish_reserved is a transient wait (never stuck_with_keys); after the erasure run proves, the claim and cancelErasures go through", async () => {
  const s = new FakeS3Versions();
  for (let i = 0; i < 10; i++) s.put(`${PFX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
  const st = new SyncState(await tmp("e3-flag-m1ab-"), { obligations: true });
  let clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PFX, kind: "prefix", org_id: "org-1", reason: "stale_prefix" }], clock - 1000);
  // Run 1 cannot prove (a new version lands mid-traversal): its reservations for the deleted versions stay.
  let added = false;
  s.onDelete = (k) => { if (!added && k.endsWith("b=000009.ndjson.zst")) { added = true; s.put(`${PFX}raw/b=000000.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z")); } };
  const j = new ErasureJob(auth(), st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => clock, async () => {});
  await j.drain();
  // (a) The exporter re-references a reused chunk: its key-level claim hits publish_reserved -> a transient wait.
  const chunk = `${PFX}raw/b=000003.ndjson.zst`;
  expect(st.acquireLease(PFX, "exporter:1", undefined, clock)).toBe(true);
  const lease = { holder: "exporter:1", gens: { [PFX]: st.leaseGen(PFX)! } };
  let code: string | undefined;
  try { st.publishWrites([{ key: chunk, versionId: "*" }], lease); } catch (e) { code = (e as { code?: string }).code; }
  expect(code).toBe("publish_reserved");
  st.markFailed("org-1", "sess_1", code!, [`${PFX}manifest.json`], 1, clock);
  expect(st.exported("org-1", "sess_1")?.attempts).toBe(0); // not counted
  expect(st.stuckWithKeys()).toBe(0);
  st.releaseLeases("exporter:1", [PFX]);
  // The erasure run proves next time (no genesis settlement, no unknown PUT resolved): absent reservations retire.
  clock += 3_600_000;
  await j.drain();
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).map((e) => e.key)).toEqual([PFX]); // still never "done" (unsettled)
  // (b) The exporter's retry: the claim goes through and cancelErasures removes the queue row.
  expect(st.acquireLease(PFX, "exporter:1", undefined, clock)).toBe(true);
  st.publishWrites([{ key: chunk, versionId: "*" }], { holder: "exporter:1", gens: { [PFX]: st.leaseGen(PFX)! } });
  st.cancelErasures([PFX]);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5)).toEqual([]);
});

test("P1 mixed-mode admission: A opens OFF, THEN B opens ON and records an unknown -> A's lease, queue mutations and erase step are REFUSED", async () => {
  const path = await tmp("e3-flag-mixed-");
  const A = new SyncState(path, { obligations: false });
  A.enqueueErasure([{ key: PFX, kind: "prefix", org_id: "org-1", reason: "fenced" }], 1); // fine: nothing used yet
  const B = new SyncState(path, { obligations: true });
  B.dispatchWrite(`${PFX}raw/x.zst`, "exporter:1"); // ON records a tracked unknown
  const refused = (f: () => unknown) => { try { f(); return null; } catch (e) { return (e as { code?: string }).code; } };
  expect(refused(() => A.acquireLease(PFX, "eraser:A"))).toBe("obligations_state_in_use");
  expect(refused(() => A.renewLease(PFX, "eraser:A"))).toBe("obligations_state_in_use");
  expect(refused(() => A.enqueueErasure([{ key: PFX, kind: "prefix", org_id: "org-1", reason: "fenced" }], 2))).toBe("obligations_state_in_use");
  expect(refused(() => A.cancelErasures([PFX]))).toBe("obligations_state_in_use");
  expect(refused(() => A.erasureDone(PFX, 1))).toBe("obligations_state_in_use");
  expect(refused(() => A.erasureFailed(PFX, "x"))).toBe("obligations_state_in_use");
  // The OFF job's erase step refuses before any external effect: nothing is deleted.
  const s = new FakeS3Versions();
  s.put(`${PFX}raw/b=000001.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
  const job = new ErasureJob(auth(), A, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => Date.now(), async () => {});
  await job.drain().catch(() => undefined);
  expect(s.deletes).toEqual([]);
  expect(B.unresolvedWrites(PFX).length).toBe(1); // the tracked unknown is preserved
  expect(B.nextErasures(Number.MAX_SAFE_INTEGER, 5).map((e) => e.key)).toEqual([PFX]);
  A.close();
  B.close();
});

test("P2: releasing a LARGE, SPARSE-present reservation set reads <= 1000 rows per transaction and resumes from a proof-bound cursor", async () => {
  const st = new SyncState(await tmp("e3-flag-rel-"), { obligations: true });
  const D = `${PFX}raw/`;
  const vs = Array.from({ length: 2_500 }, (_, i) => ({ key: `${D}b=${String(i).padStart(6, "0")}.zst`, versionId: `v${i}` }));
  for (let i = 0; i < vs.length; i += 500) expect(st.reserveErase(D, st.obligationRev(D), "S", vs.slice(i, i + 500)).ok).toBe(true);
  const hw = st.reservationHighWater(D);
  // Only ONE version (the last) is still present: a sparse match.
  const last = vs[vs.length - 1]!;
  let seen = 0;
  const present = (k: string, v: string) => { seen += 1; return k === last.key && v === last.versionId; };
  let rev = st.obligationRev(D);
  const p1 = st.releasePresentPage(D, "S", undefined, present, hw, undefined, rev, 1000, "proof-1");
  expect(p1).toMatchObject({ released: 0, done: false, ok: true });
  expect(seen).toBe(1000); // rows READ per transaction are bounded, not just the releases
  expect(st.releaseCursor("S")).toEqual([vs[999]!.key, "v999"]);
  rev = p1.rev;
  const p2 = st.releasePresentPage(D, "S", undefined, present, hw, undefined, rev, 1000, "proof-1");
  expect(seen).toBe(2000);
  const p3 = st.releasePresentPage(D, "S", undefined, present, hw, undefined, p2.rev, 1000, "proof-1");
  expect(p3).toMatchObject({ released: 1, done: true, ok: true });
  expect(seen).toBe(2500);
  expect(st.releaseCursor("S")).toBeNull();
  expect(st.reservationsAt(last.key)).toEqual([]);
  // A different proof discards a saved cursor (restart), never resuming a foreign workset.
  const q = st.releasePresentPage(D, "S", undefined, () => false, hw, undefined, st.obligationRev(D), 1000, "proof-A");
  expect(q.done).toBe(false);
  let seen2 = 0;
  st.releasePresentPage(D, "S", undefined, () => { seen2 += 1; return false; }, hw, undefined, q.rev, 1000, "proof-B");
  expect(seen2).toBe(1000); // restarted from the start under proof-B
});
