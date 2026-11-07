/**
 * The shared unresolved-write obligation primitive (write-obligation.ts) over the shared strict
 * paged eraser and an offline versioned provider: lost response, after-grace commit,
 * successor-safe ownership, owned-only resolution, unrecorded owners, lost DELETE ack,
 * > 10k versions with crash/resume, foreign checkpoints, authority changes, markers.
 */
import { describe, expect, test } from "bun:test";
import type { EraseCheckpoint } from "../neon-s3-erasure/eraser.ts";
import { boundedPut, isObligationComplete, nextSweepDelayMs, obligationScope, reconcileObligation, type ObligationAuthority, type ObligationReason, type ObligationStore, type WriteObligation } from "../neon-s3-erasure/write-obligation.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const KEY = "96bulls/org/v1/ws=w/m=srv_x/h=_babysitter/d=2026/12/06/s=_bx2026120614-aaaaaaaaaaaaaaaa/raw/b=0123456789abcdef0123456789abcdef.ndjson.zst";
const SIBLING = KEY.replace("b=0123", "b=9999");
const ID = ["babysitter_write", "org", "ws", "srv_x", "_bx..", "gen1", "writer-1"];

/** An in-memory lane table row + adapter, with durability observable across "crashes". */
function lane(reason: ObligationReason, over: Partial<ObligationAuthority> = {}) {
  const row = { reason, strayFound: false, checkpoint: null as EraseCheckpoint | null, sweeps: 0, retired: false, owned: [] as string[], ownersWithoutVersion: 0, identity: ID as readonly unknown[], ...over };
  const store: ObligationStore = {
    load: async () => (row.retired ? null : { reason: row.reason, identity: row.identity, ownedVersions: row.owned, ownersWithoutVersion: row.ownersWithoutVersion }),
    markStray: async () => { row.strayFound = true; },
    saveCheckpoint: async (cp) => { row.checkpoint = cp; },
    reschedule: async (stray) => { row.sweeps++; row.checkpoint = null; row.strayFound = stray; },
    retire: async () => { row.retired = true; },
  };
  const obligation = (): WriteObligation => ({ key: KEY, reason: row.reason, identity: ID, checkpoint: row.checkpoint, strayFound: row.strayFound });
  return { row, store, obligation };
}

describe("write obligation: unresolved PUT never retired by absence or time", () => {
  test("a lost response with nothing ever listed stays unknown forever (rescheduled with backoff, never retired)", async () => {
    const s = new FakeS3Versions();
    const l = lane("unknown");
    for (let i = 0; i < 5; i++) expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("rescheduled");
    expect(l.row).toMatchObject({ retired: false, sweeps: 5, strayFound: false });
    expect([0, 1, 2, 3, 11, 12, 50].map(nextSweepDelayMs)).toEqual([60_000, 120_000, 240_000, 480_000, 86_400_000, 86_400_000, 86_400_000]);
  });

  test("a PUT that commits AFTER abort + grace + an initial zero inventory is found, erased and only then retired", async () => {
    const s = new FakeS3Versions();
    const l = lane("unknown");
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("rescheduled");   // initial zero: not proof
    s.put(KEY);                                   // the late provider commit
    s.put(SIBLING);                               // never touched: another key in the directory
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
    expect(s.count(KEY)).toBe(0);
    expect(s.count(SIBLING)).toBe(1);
    expect(l.row.strayFound).toBe(true);
  });

  test("delete markers never count as the write", async () => {
    const s = new FakeS3Versions();
    s.keys.set(KEY, [{ id: "m1", at: 1, marker: true }]);
    const l = lane("unknown");
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("rescheduled");
    expect(l.row.strayFound).toBe(false);
    expect(l.row.retired).toBe(false);
  });
});

describe("write obligation: ownership (successor-safe)", () => {
  test("a version a completed owner holds is never erased, even at the same key; unowned ones are", async () => {
    const s = new FakeS3Versions();
    const owned = s.put(KEY), stray = s.put(KEY);
    const l = lane("forgotten", { owned: [owned] });
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
    expect(s.keys.get(KEY)!.map((v) => v.id)).toEqual([owned]);
    expect(s.deletes).toEqual([`${KEY}@${stray}`]);
  });

  test("an unknown write whose listed version is owned (a successor or the publish) is resolved by ownership, never deleted", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY);
    const l = lane("unknown", { owned: [v] });
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
    expect(s.count(KEY)).toBe(1);
    expect(s.deletes).toEqual([]);
  });

  test("an owner whose exact version is unrecorded blocks every erase at the key: nothing deleted, rescheduled, never resolved", async () => {
    const s = new FakeS3Versions();
    s.put(KEY);
    const l = lane("forgotten", { ownersWithoutVersion: 1 });
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("rescheduled");
    expect(s.deletes).toEqual([]);
    expect(l.row.retired).toBe(false);
  });

  test("ownership that changes mid-run stops before that page's deletes", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 5; i++) s.put(KEY);
    const l = lane("forgotten");
    let loads = 0;
    const store = { ...l.store, load: async () => { loads++; const a = await l.store.load(); return loads >= 3 && a ? { ...a, ownedVersions: ["v99"] } : a; } };
    await expect(reconcileObligation(s.eraser(), store, l.obligation())).rejects.toMatchObject({ code: "authority_changed" });
    expect(l.row.retired).toBe(false);
  });
});

describe("write obligation: durability across failures", () => {
  test("a lost DELETE ack: the step fails, the stray evidence is already durable, the rerun proves zero and retires", async () => {
    const s = new FakeS3Versions();
    s.put(KEY);
    let lose = true;
    s.onDelete = () => (lose ? ((lose = false), "lose") : undefined);
    const l = lane("unknown");
    await expect(reconcileObligation(s.eraser(), l.store, l.obligation())).rejects.toMatchObject({ code: "s3_500" });
    expect(l.row).toMatchObject({ strayFound: true, retired: false });   // markStray ran BEFORE the delete
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
  });

  test("> 10k versions at one key: bounded runs persist the bound checkpoint; a crash reuses the last one; then retired", async () => {
    const s = new FakeS3Versions();
    for (let i = 0; i < 12_500; i++) s.put(KEY);
    const l = lane("unknown");
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("incomplete");
    const saved = l.row.checkpoint!;
    expect(saved.scope).toBe(obligationScope({ key: KEY, identity: ID }, { reason: "unknown", ownedVersions: [] }));
    // "Crash": a run that loses its result; the next run starts from the persisted checkpoint.
    l.row.checkpoint = saved;
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
    expect(s.count(KEY)).toBe(0);
  }, 60_000);

  test("a foreign checkpoint (another reason, identity or owned set) is never adopted: the run restarts", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 3; i++) s.put(KEY);
    const l = lane("unknown");
    l.row.checkpoint = { v: 1, bucket: "b", prefix: KEY.slice(0, KEY.lastIndexOf("/") + 1), scope: "write-obligation:other", afterKey: KEY, inKey: null };
    expect(await reconcileObligation(s.eraser(), l.store, l.obligation())).toBe("retired");
    expect(s.count(KEY)).toBe(0);   // not skipped past by the foreign afterKey
  });

  test("a changed identity (generation / writer) refuses before any delete", async () => {
    const s = new FakeS3Versions();
    s.put(KEY);
    const l = lane("unknown", { identity: [...ID.slice(0, -1), "writer-2"] });
    await expect(reconcileObligation(s.eraser(), l.store, l.obligation())).rejects.toMatchObject({ code: "authority_changed" });
    expect(s.deletes).toEqual([]);
  });

  test("the completion predicate and the bounded writer PUT", async () => {
    expect(isObligationComplete("forgotten", { strayFound: false, ownedSeen: false }, { status: "erased", listed: 0 })).toBe(true);
    expect(isObligationComplete("unknown", { strayFound: false, ownedSeen: false }, { status: "erased", listed: 0 })).toBe(false);
    expect(isObligationComplete("publishing", { strayFound: true, ownedSeen: false }, { status: "pending", reason: "remaining", remaining: 1, listed: 1 })).toBe(false);
    let unknown = 0;
    await expect(boundedPut((signal) => new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason))), async () => { unknown++; }, 20)).rejects.toBeDefined();
    expect(unknown).toBe(1);
    expect(await boundedPut(async () => "v1", async () => { unknown++; })).toBe("v1");
    expect(unknown).toBe(1);
  });
});
