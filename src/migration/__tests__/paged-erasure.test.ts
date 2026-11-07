/**
 * The shared strict, paged, resumable eraser primitive (eraser.ts eraseStep/verifyErased)
 * against an offline versioned provider: > 10k versions across runs, strict later pages,
 * cursor invalidation, crash / lost ACK resume, mid-traversal writes, scope binding.
 */
import { describe, expect, test } from "bun:test";
import { ERASE_RUN_MAX_VERSIONS, type EraseCheckpoint, type EraseStepResult } from "../neon-s3-erasure/eraser.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const P = "96bulls/org/v1/ws=w/m=srv_x/h=_babysitter/d=2026/12/06/s=_bx2026120614-aaaaaaaaaaaaaaaa/";
const k = (i: number) => `${P}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`;
const ok = async () => {};
const scope = "babysitter_bucket_erase:org:ws:srv_x:_bx..:gen1:fence3";

function fill(s: FakeS3Versions, n: number, at = 1_000) { for (let i = 0; i < n; i++) s.put(k(i), at); }

describe("paged eraser: > 10k versions resume across bounded runs", () => {
  test("12,500 versions: run 1 stops at the 10k per-run ceiling with a checkpoint; run 2 finishes; a fresh strict zero inventory proves it", async () => {
    const s = new FakeS3Versions(); fill(s, 12_500);
    const e = s.eraser();
    const r1 = await e.eraseStep(P, { scope, authorize: ok });
    expect(r1).toMatchObject({ status: "incomplete", reason: "versions", deleted: ERASE_RUN_MAX_VERSIONS, listed: ERASE_RUN_MAX_VERSIONS });
    expect(s.count(P)).toBe(2_500);
    // Not erased: a traversal or a checkpoint is never proof.
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "pending", reason: "remaining" });
    const r2 = await e.eraseStep(P, { scope, authorize: ok, checkpoint: (r1 as Extract<EraseStepResult, { status: "incomplete" }>).checkpoint });
    expect(r2).toMatchObject({ status: "traversed", deleted: 2_500 });
    expect(s.count(P)).toBe(0);
    expect(await e.verifyErased(P, { authorize: ok })).toEqual({ status: "erased", listed: 0 });
  });

  test("the page, version and deadline budgets each stop a run with a resumable checkpoint (never partial success)", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 55);
    const e = s.eraser();
    const a = await e.eraseStep(P, { scope, authorize: ok, budget: { pages: 2 } });
    expect(a).toMatchObject({ status: "incomplete", reason: "pages", deleted: 20 });
    let t = 0;
    const b = await e.eraseStep(P, { scope, authorize: ok, checkpoint: (a as never as { checkpoint: EraseCheckpoint }).checkpoint, budget: { deadlineAt: 2 }, now: () => t++ });
    expect(b).toMatchObject({ status: "incomplete", reason: "deadline" });
    let cp: EraseCheckpoint | null = (b as never as { checkpoint: EraseCheckpoint }).checkpoint;
    for (let runs = 0; cp && runs < 20; runs++) {
      const r = await e.eraseStep(P, { scope, authorize: ok, checkpoint: cp, budget: { versions: 7 } });
      cp = r.status === "incomplete" ? r.checkpoint : null;
    }
    expect(cp).toBeNull();
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "erased" });
  });
});

describe("paged eraser: every page strict; no delete from an ambiguous page", () => {
  for (const [name, mutate, code] of [
    ["truncated later page", (x: string) => x.slice(0, x.length - 40), "erasure_inventory"],
    ["missing IsLatest on a later page", (x: string) => x.replace("<IsLatest>true</IsLatest>", ""), "erasure_inventory"],
    ["no pagination markers on a truncated later page", (x: string) => x.replace(/<NextKeyMarker>.*?<\/NextVersionIdMarker>/, ""), "erasure_inventory"],
    ["markers that do not match the page", (x: string) => x.replace(/<NextVersionIdMarker>[^<]*</, "<NextVersionIdMarker>v99999999<"), "erasure_inventory"],
    ["an out-of-prefix key", (x: string) => x.replace(/<Key>[^<]*<\/Key>/, "<Key>96bulls/org/v1/ws=w/other/b=1</Key>"), "erasure_foreign_key"],
    ["a duplicate version in the page", (x: string) => x.replace(/(<Version>.*?<\/Version>)/, "$1$1"), "erasure_inventory"],
  ] as const)
    test(`${name}: pages before it keep their (real) deletes, that page deletes nothing, and the error is unresolved`, async () => {
      const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 35);
      s.rewrite = (call, xml) => (call === 3 ? mutate(xml) : xml);
      const e = s.eraser();
      await expect(e.eraseStep(P, { scope, authorize: ok })).rejects.toMatchObject({ code });
      expect(s.deletes).toHaveLength(20);   // pages 1-2 only; observed progress is retained, not rolled back
      expect(s.count(P)).toBe(15);
      s.rewrite = undefined;
      // The consumer still holds its last persisted checkpoint (none): a clean rerun finishes.
      expect(await e.eraseStep(P, { scope, authorize: ok })).toMatchObject({ status: "traversed" });
      expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "erased" });
    });

  test("a repeated page (cursor did not advance) is a cycle or a backwards cursor, never an endless loop", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 30);
    let first = "";
    s.rewrite = (call, xml) => { if (call === 1) first = xml; return call >= 2 ? first : xml; };
    // Select nothing so the repeat cannot be explained by deletes.
    await expect(s.eraser().eraseStep(P, { scope, authorize: ok, select: () => false })).rejects.toMatchObject({ code: "erasure_inventory" });
    expect(s.deletes).toEqual([]);
  });

  test("an acknowledged delete that left its version is caught by the fresh strict proof (never erased)", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 15);
    const e = s.eraser();
    // A provider that acknowledges a delete but keeps the version.
    const orig = s.remove.bind(s);
    s.remove = (key, id) => { if (key !== k(3)) orig(key, id); };
    await expect(e.eraseStep(P, { scope, authorize: ok, budget: {} })).resolves.toMatchObject({ status: "traversed" });
    // verifyErased (fresh strict listing) sees the survivor: never erased.
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "pending", reason: "remaining", remaining: 1 });
  });
});

describe("paged eraser: authority, crash and lost ACK", () => {
  test("authorize() runs before every page's deletes; a revoked authority stops before that page deletes anything", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 30);
    let calls = 0;
    const authorize = async () => { if (++calls === 2) throw Object.assign(new Error("lease lost"), { code: "fence_refused" }); };
    await expect(s.eraser().eraseStep(P, { scope, authorize })).rejects.toMatchObject({ code: "fence_refused" });
    expect(s.deletes).toHaveLength(10);
  });

  test("crash after deletes but before the checkpoint persisted: rerun from the older checkpoint is idempotent", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 40);
    const e = s.eraser();
    const r1 = await e.eraseStep(P, { scope, authorize: ok, budget: { pages: 1 } });
    // The run deleted page 2 too but "crashed" before persisting (simulate: run again from r1, stop after one page, drop result).
    await e.eraseStep(P, { scope, authorize: ok, checkpoint: (r1 as never as { checkpoint: EraseCheckpoint }).checkpoint, budget: { pages: 1 } });
    const r3 = await e.eraseStep(P, { scope, authorize: ok, checkpoint: (r1 as never as { checkpoint: EraseCheckpoint }).checkpoint });
    expect(r3).toMatchObject({ status: "traversed" });
    expect(s.count(P)).toBe(0);
  });

  test("a lost DELETE acknowledgement fails the run (unresolved); the rerun converges and only then verifies erased", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 25);
    let lost = false;
    s.onDelete = (key) => (key === k(14) && !lost ? ((lost = true), "lose") : undefined);
    const e = s.eraser();
    await expect(e.eraseStep(P, { scope, authorize: ok })).rejects.toMatchObject({ code: "s3_500" });
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "pending" });
    expect(await e.eraseStep(P, { scope, authorize: ok })).toMatchObject({ status: "traversed" });
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "erased" });
  });
});

describe("paged eraser: cursor invalidation and mid-traversal writes", () => {
  test("an in-key provider marker whose version was deleted restarts at the completed-key boundary (no skip, no guessed marker)", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10;
    fill(s, 5, 500);   // old versions of k(0)..k(4)
    for (let i = 0; i < 25; i++) s.put(k(0), 1_000 + i);   // then 25 newer versions of k(0) (> a page; listed first)
    const keepNewest = (v: { key: string; lastModified?: number }) => (v.lastModified ?? 0) < 1_000;
    const e = s.eraser();
    // Run 1 selects only old versions: the first page (all k(0), all new) deletes nothing → in-key cursor.
    const r1 = await e.eraseStep(P, { scope, authorize: ok, select: keepNewest, budget: { pages: 1 } });
    const cp = (r1 as never as { checkpoint: EraseCheckpoint }).checkpoint;
    expect(cp.inKey).toMatchObject({ key: k(0) });
    // Someone deletes the marker version before the resume.
    s.remove(cp.inKey!.key, cp.inKey!.versionId);
    const r2 = await e.eraseStep(P, { scope, authorize: ok, select: keepNewest, checkpoint: cp });
    expect(r2.status).toBe("traversed");
    expect(await e.verifyErased(P, { authorize: ok, select: keepNewest })).toMatchObject({ status: "erased" });
    expect(s.count(P)).toBe(24);   // the 24 surviving new versions of k(0) are preserved
  });

  test("a version written behind the cursor mid-traversal is not proven erased: verify is pending and a fresh pass is needed", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 30);
    const e = s.eraser();
    const r1 = await e.eraseStep(P, { scope, authorize: ok, budget: { pages: 2 } });
    s.put(`${P}raw/b=000000a.ndjson.zst`);   // sorts before the checkpoint
    const r2 = await e.eraseStep(P, { scope, authorize: ok, checkpoint: (r1 as never as { checkpoint: EraseCheckpoint }).checkpoint });
    expect(r2.status).toBe("traversed");
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "pending", reason: "remaining", remaining: 1 });
    expect(await e.eraseStep(P, { scope, authorize: ok })).toMatchObject({ status: "traversed", deleted: 1 });
    expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "erased" });
  });

  test("a successor selected out (written after the cut) is preserved by both the step and the proof", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 12, 1_000);
    s.put(k(5), 9_000);
    const before = (v: { lastModified?: number }) => v.lastModified !== undefined && v.lastModified < 5_000;
    const e = s.eraser();
    expect(await e.eraseStep(P, { scope, authorize: ok, select: before })).toMatchObject({ status: "traversed", deleted: 12 });
    expect(await e.verifyErased(P, { authorize: ok, select: before })).toMatchObject({ status: "erased" });
    expect([...s.keys.keys()]).toEqual([k(5)]);
  });
});

describe("paged eraser: checkpoint binding", () => {
  test("a checkpoint is refused for another scope (operation / generation / fence), prefix or bucket", async () => {
    const s = new FakeS3Versions(); s.pageSize = 10; fill(s, 30);
    const e = s.eraser();
    const r1 = await e.eraseStep(P, { scope, authorize: ok, budget: { pages: 1 } });
    const cp = (r1 as never as { checkpoint: EraseCheckpoint }).checkpoint;
    const before = s.deletes.length;
    for (const [p, sc, c] of [
      [P, "analysis_purge:org:ws:gen1:fence3", cp],
      [P, scope.replace("gen1", "gen2"), cp],
      [P, scope.replace("fence3", "fence4"), cp],
      [`${P}raw/`, scope, cp],
      [P, scope, { ...cp, bucket: "other" }],
      [P, scope, { ...cp, afterKey: "96bulls/elsewhere/x" }],
    ] as const)
      await expect(e.eraseStep(p, { scope: sc, authorize: ok, checkpoint: c as EraseCheckpoint })).rejects.toMatchObject({ code: "erasure_checkpoint_scope" });
    expect(s.deletes.length).toBe(before);
  });
});

describe("paged eraser: unversioned store entries", () => {
  test("storeEraser's paged entries carry lastModified (ms) like the S3 eraser's, so a time-based select works", async () => {
    const { storeEraser } = await import("../neon-s3-erasure/eraser.ts");
    const { scratchStore } = await import("../../session-ingest/__tests__/support.ts");
    const { objectKey } = await import("../../shared/vendor.ts");
    const s = scratchStore();
    try {
      const before = Date.now() - 1_000;
      for (const i of [0, 1, 2]) await s.store.put(objectKey(k(i)), new Uint8Array([i]), { ifNoneMatch: true });
      const seen: { key: string; lastModified?: number }[] = [];
      const e = storeEraser(s.store);
      const r = await e.eraseStep(P, { scope, authorize: ok, select: (v) => { seen.push(v); return (v.lastModified ?? Infinity) < Date.now() + 60_000; } });
      expect(r).toMatchObject({ status: "traversed", deleted: 3 });
      expect(seen.map((v) => v.key)).toEqual([k(0), k(1), k(2)]);
      for (const v of seen) {
        expect(typeof v.lastModified).toBe("number");
        expect(v.lastModified!).toBeGreaterThanOrEqual(before - 60_000);
      }
      expect(await e.verifyErased(P, { authorize: ok })).toMatchObject({ status: "erased" });
    } finally { s.done(); }
  });
});
