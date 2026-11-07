/**
 * The shared unresolved-write obligation primitive v3 (write-obligation.ts): no content
 * attribution (nonterminal unknown), reserve-every-doomed-version against publish, CAS
 * mutations, an enforced PUT wall deadline with contained late hooks. Offline provider only.
 */
import { describe, expect, test } from "bun:test";
import type { EraseCheckpoint } from "../neon-s3-erasure/eraser.ts";
import { boundedPut, nextSweepDelayMs, PutTimeout, reconcileObligation, terminalInvocations, type Cas, type ObligationState, type ObligationStore, type WriteInvocation } from "../neon-s3-erasure/write-obligation.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const KEY = "96bulls/org/v1/ws=w/m=srv_x/h=_babysitter/d=2026/12/06/s=_bx2026120614-aaaaaaaaaaaaaaaa/raw/b=0123456789abcdef0123456789abcdef.ndjson.zst";
const SIBLING = KEY.replace("b=0123", "b=9999");
const ID = ["babysitter_write", "org", "ws", "srv_x", "_bx..", "gen1"];
const SAME = "5d41402abc4b2a76b9719d911017c592";   // identical content -> identical ETag

/**
 * An in-memory lane: obligation row with a version, reservations, and a writer `publish` that
 * takes the same lock as `reserve` (conflict in either order).
 */
function lane(invocations: WriteInvocation[], over: Partial<ObligationState> = {}) {
  const row: ObligationState & { gone: boolean; sweeps: number; reserved: Set<string> } = { key: KEY, identity: ID, rev: 1, invocations: invocations.map((i) => ({ ...i })),
    ownedVersions: [], ownersWithoutVersion: 0, checkpoint: null, gone: false, sweeps: 0, reserved: new Set(), ...over };
  const bump = (): Cas => ({ ok: true, rev: ++row.rev });
  const guard = (rev: number) => !row.gone && rev === row.rev;
  const store: ObligationStore = {
    load: async () => (row.gone ? null : { ...row, invocations: row.invocations.map((i) => ({ ...i })), ownedVersions: [...row.ownedVersions] }),
    reserve: async (rev, ids) => { if (!guard(rev) || ids.some((v) => row.ownedVersions.includes(v))) return { ok: false }; for (const v of ids) row.reserved.add(v); return bump(); },
    saveCheckpoint: async (rev, cp: EraseCheckpoint) => { if (!guard(rev)) return { ok: false }; row.checkpoint = cp; return bump(); },
    resolve: async (rev, ids) => { if (!guard(rev)) return { ok: false }; row.invocations = row.invocations.filter((i) => !ids.includes(i.id)); return bump(); },
    reschedule: async (rev) => { if (!guard(rev)) return { ok: false }; row.sweeps++; row.checkpoint = null; return bump(); },
    retire: async (rev) => { if (!guard(rev)) return { ok: false }; row.gone = true; return { ok: true, rev: rev + 1 }; },
  };
  /** The writer's publish under the same lock: refused for a reserved version. */
  const publish = (versionId: string) => { if (row.reserved.has(versionId)) return false; row.ownedVersions = [...row.ownedVersions, versionId]; row.rev++; return true; };
  return { row, store, publish };
}
const dispatched = (id: string): WriteInvocation => ({ id, outcome: "dispatched" });

describe("no content attribution: a dispatched invocation is never resolved by a listed version", () => {
  test("a PRE-EXISTING identical-content stray: erased as cleanup, the in-flight invocation stays unresolved", async () => {
    const s = new FakeS3Versions();
    const old = s.put(KEY, 1, SAME);
    const l = lane([dispatched("inv-1")]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(s.deletes).toEqual([`${KEY}@${old}`]);
    expect(l.row.invocations).toEqual([{ id: "inv-1", outcome: "dispatched" }]);
    expect(l.row.gone).toBe(false);
  });

  test("an identical-body version written by a DIFFERENT invocation never resolves this one; only its own acked version does", async () => {
    const s = new FakeS3Versions();
    const other = s.put(KEY, 1, SAME);
    const l = lane([dispatched("mine"), { id: "theirs", outcome: "acked", versionId: other }]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(l.row.invocations.map((i) => i.id)).toEqual(["mine"]);   // "theirs" resolved on its own version; "mine" not
  });

  test("a late commit after zero inventories is erased (cleanup) but the dispatched invocation stays unresolved forever; backoff caps at a day", async () => {
    const s = new FakeS3Versions();
    const l = lane([dispatched("inv-late")]);
    for (let i = 0; i < 5; i++) expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    s.put(KEY, 9, SAME);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(s.count(KEY)).toBe(0);
    expect(l.row.invocations).toHaveLength(1);
    expect([0, 1, 2, 11, 12, 99].map(nextSweepDelayMs)).toEqual([60_000, 120_000, 240_000, 86_400_000, 86_400_000, 86_400_000]);
  });

  test("acked owned -> resolved, version kept; acked not owned -> erased then resolved; refused -> resolved; incomplete inventory proves nothing", async () => {
    const s = new FakeS3Versions();
    const v1 = s.put(KEY, 1, SAME);
    const a = lane([{ id: "pub", outcome: "acked", versionId: v1 }], { ownedVersions: [v1] });
    expect(await reconcileObligation(s.eraser(), a.store)).toBe("retired");
    expect(s.count(KEY)).toBe(1);
    const b = lane([{ id: "gone", outcome: "acked", versionId: v1 }, { id: "nope", outcome: "refused" }]);
    expect(await reconcileObligation(s.eraser(), b.store)).toBe("retired");
    expect(s.count(KEY)).toBe(0);
    expect(terminalInvocations([{ id: "x", outcome: "acked", versionId: "v9" }], new Set(), null)).toEqual([]);
    expect(terminalInvocations([{ id: "d", outcome: "dispatched" }], new Set(), new Set())).toEqual([]);
  });
});

describe("reserve every doomed version against publish (either order)", () => {
  test("publish BEFORE the reserve (after authorize): the reserve is refused and NOTHING of that page is deleted (no matching ETag either)", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1);                    // no ETag at all: unattributable
    const l = lane([{ id: "x", outcome: "acked", versionId: "v-other" }]);
    const inner = l.store.reserve;
    l.store.reserve = async (rev, ids) => { expect(l.publish(v)).toBe(true); return inner(rev, ids); };   // the writer wins the race
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("refused");
    expect(s.deletes).toEqual([]);
    expect(s.count(KEY)).toBe(1);
    expect(l.row.ownedVersions).toEqual([v]);
  });

  test("publish AFTER the reserve (before the DELETE): the publish is refused, so the delete never removes owned bytes", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1);
    const l = lane([]);
    const inner = l.store.reserve;
    let published: boolean | undefined;
    l.store.reserve = async (rev, ids) => { const r = await inner(rev, ids); published = l.publish(v); return r; };   // the writer loses the race
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
    expect(published).toBe(false);
    expect(l.row.ownedVersions).toEqual([]);
    expect(s.deletes).toEqual([`${KEY}@${v}`]);
  });

  test("a sibling key is never reserved or deleted; an owner with an unrecorded version blocks every erase", async () => {
    const s = new FakeS3Versions();
    s.put(KEY, 1); s.put(SIBLING, 1);
    const l = lane([]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
    expect(s.count(SIBLING)).toBe(1);
    expect([...l.row.reserved]).toHaveLength(1);
    const t = new FakeS3Versions(); t.put(KEY, 1);
    const b = lane([dispatched("d")], { ownersWithoutVersion: 1 });
    expect(await reconcileObligation(t.eraser(), b.store)).toBe("rescheduled");
    expect(t.deletes).toEqual([]);
  });
});

describe("compare-and-set and durability", () => {
  test("a publication between the proof and the retirement: refused, the row survives", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1);
    const l = lane([{ id: "x", outcome: "acked", versionId: v }]);
    l.store.resolve = async () => { l.row.rev++; return { ok: false }; };
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("refused");
    expect(l.row.gone).toBe(false);
  });

  test("a new invocation dispatched mid-run stops the run before that page's deletes", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 5; i++) s.put(KEY, i);
    const l = lane([dispatched("inv-1")]);
    let loads = 0;
    const store: ObligationStore = { ...l.store, load: async () => { if (++loads === 3) { l.row.rev++; (l.row.invocations as WriteInvocation[]).push(dispatched("inv-new")); } return l.store.load(); } };
    expect(await reconcileObligation(s.eraser(), store)).toBe("refused");
    expect(l.row.invocations.map((i) => i.id)).toEqual(["inv-1", "inv-new"]);
  });

  test("a lost DELETE ack fails the step (reservation durable); the rerun proves the acked version absent and resolves it", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1);
    let lose = true;
    s.onDelete = () => (lose ? ((lose = false), "lose") : undefined);
    const l = lane([{ id: "a", outcome: "acked", versionId: v }]);
    await expect(reconcileObligation(s.eraser(), l.store)).rejects.toMatchObject({ code: "s3_500" });
    expect(l.row.reserved.has(v)).toBe(true);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
  });

  test("> 10k versions with a checkpoint crash/resume: everything erased, the dispatched invocation still unresolved", async () => {
    const s = new FakeS3Versions();
    for (let i = 0; i < 12_500; i++) s.put(KEY, i);
    const l = lane([dispatched("inv-1")]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("incomplete");
    expect(l.row.checkpoint).not.toBeNull();
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(s.count(KEY)).toBe(0);
    expect(l.row.invocations).toHaveLength(1);
  }, 60_000);

  test("a foreign checkpoint is never adopted (the run restarts and skips nothing)", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 3; i++) s.put(KEY, i);
    const l = lane([], { checkpoint: { v: 1, bucket: "b", prefix: KEY.slice(0, KEY.lastIndexOf("/") + 1), scope: "write-obligation:other", afterKey: KEY, inKey: null } });
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
    expect(s.count(KEY)).toBe(0);
  });
});

describe("boundedPut: an enforced wall deadline with contained late hooks", () => {
  test("a port that IGNORES the abort and succeeds late: timeout at the deadline, unknown marked first, the late success only reported", async () => {
    const order: string[] = [];
    const late: unknown[] = [];
    const started = Date.now();
    await expect(boundedPut(() => new Promise((r) => setTimeout(() => { order.push("late-success"); r("v-late"); }, 40)),
      { onUnknown: async () => { order.push("unknown"); }, onLate: (o) => { late.push(o); } }, 1)).rejects.toBeInstanceOf(PutTimeout);
    expect(Date.now() - started).toBeLessThan(35);
    expect(order).toEqual(["unknown"]);
    await new Promise((r) => setTimeout(r, 60));
    expect(late).toEqual([{ ok: true, value: "v-late" }]);
  });

  test("an onLate that THROWS or REJECTS is contained: no unhandled rejection, the outcome stays unknown", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (e: unknown) => { unhandled.push(e); };
    process.on("unhandledRejection", onUnhandled);
    try {
      let unknown = 0;
      await expect(boundedPut(() => new Promise((r) => setTimeout(() => r("late"), 10)), { onUnknown: async () => { unknown++; }, onLate: () => { throw new Error("hook boom"); } }, 1)).rejects.toBeInstanceOf(PutTimeout);
      await expect(boundedPut(() => new Promise((_, rej) => setTimeout(() => rej(new Error("late err")), 10)), { onUnknown: async () => { unknown++; }, onLate: async () => { throw new Error("async hook boom"); } }, 1)).rejects.toBeInstanceOf(PutTimeout);
      await new Promise((r) => setTimeout(r, 60));
      expect(unknown).toBe(2);
      expect(unhandled).toEqual([]);
    } finally { process.off("unhandledRejection", onUnhandled); }
  });

  test("a stalling port returns at the deadline; refusal vs unknown classification; bounds are validated", async () => {
    let unknown = 0;
    await expect(boundedPut(() => new Promise(() => {}), { onUnknown: async () => { unknown++; } }, 5)).rejects.toBeInstanceOf(PutTimeout);
    expect(unknown).toBe(1);
    const seen: string[] = [];
    const hooks = { onUnknown: async () => { seen.push("unknown"); }, onRefused: async () => { seen.push("refused"); }, isRefusal: (e: unknown) => (e as { code?: string }).code === "precondition_failed" };
    await expect(boundedPut(async () => { throw Object.assign(new Error("412"), { code: "precondition_failed" }); }, hooks)).rejects.toBeDefined();
    await expect(boundedPut(async () => { throw Object.assign(new Error("reset"), { code: "ECONNRESET" }); }, hooks)).rejects.toBeDefined();
    expect(seen).toEqual(["refused", "unknown"]);
    expect(await boundedPut(async () => "ok", hooks)).toBe("ok");
    for (const bad of [0, -1, 1.5, 900_001]) await expect(boundedPut(async () => "x", hooks, bad)).rejects.toMatchObject({ code: "timeout_bounds" });
  });
});
