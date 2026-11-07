/**
 * The shared unresolved-write obligation primitive v2 (write-obligation.ts): per-invocation
 * attribution, nonterminal unknown, compare-and-set mutations, an enforced PUT wall deadline.
 * Offline versioned provider only.
 */
import { describe, expect, test } from "bun:test";
import type { EraseCheckpoint } from "../neon-s3-erasure/eraser.ts";
import { boundedPut, expectedEtag, nextSweepDelayMs, PutTimeout, reconcileObligation, terminalInvocations, type Cas, type ObligationState, type ObligationStore, type WriteInvocation } from "../neon-s3-erasure/write-obligation.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const KEY = "96bulls/org/v1/ws=w/m=srv_x/h=_babysitter/d=2026/12/06/s=_bx2026120614-aaaaaaaaaaaaaaaa/raw/b=0123456789abcdef0123456789abcdef.ndjson.zst";
const SIBLING = KEY.replace("b=0123", "b=9999");
const ID = ["babysitter_write", "org", "ws", "srv_x", "_bx..", "gen1"];
const E1 = expectedEtag(new TextEncoder().encode("body-1")), E2 = expectedEtag(new TextEncoder().encode("body-2"));

/** An in-memory lane table with a row version: every mutation is a compare-and-set. */
function lane(invocations: WriteInvocation[], over: Partial<ObligationState> = {}) {
  const row: ObligationState & { gone: boolean; sweeps: number } = { key: KEY, identity: ID, rev: 1, invocations: invocations.map((i) => ({ ...i })), ownedVersions: [], ownersWithoutVersion: 0, checkpoint: null, gone: false, sweeps: 0, ...over };
  const bump = (): Cas => ({ ok: true, rev: ++row.rev });
  const guard = (rev: number) => !row.gone && rev === row.rev;
  const store: ObligationStore = {
    load: async () => (row.gone ? null : { ...row, invocations: row.invocations.map((i) => ({ ...i })), ownedVersions: [...row.ownedVersions] }),
    observe: async (rev, id, v) => { if (!guard(rev)) return { ok: false }; const i = (row.invocations as WriteInvocation[]).find((x) => x.id === id)!; i.observedVersion = v; return bump(); },
    saveCheckpoint: async (rev, cp: EraseCheckpoint) => { if (!guard(rev)) return { ok: false }; row.checkpoint = cp; return bump(); },
    resolve: async (rev, ids) => { if (!guard(rev)) return { ok: false }; row.invocations = row.invocations.filter((i) => !ids.includes(i.id)); return bump(); },
    reschedule: async (rev) => { if (!guard(rev)) return { ok: false }; row.sweeps++; row.checkpoint = null; return bump(); },
    retire: async (rev) => { if (!guard(rev)) return { ok: false }; row.gone = true; return { ok: true, rev: rev + 1 }; },
  };
  return { row, store };
}
const unknown = (id: string, etag = E1): WriteInvocation => ({ id, outcome: "dispatched", expectedEtag: etag });

describe("attribution: only an invocation's OWN evidence clears it", () => {
  test("an existing OWNED version + a never-landing unknown: never retired (owner success is not terminal for it); a late commit is attributed, erased, then retired", async () => {
    const s = new FakeS3Versions();
    const owned = s.put(KEY, 1, E2);
    const l = lane([unknown("inv-1")], { ownedVersions: [owned] });
    for (let i = 0; i < 3; i++) expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(l.row.invocations).toHaveLength(1);
    expect(s.deletes).toEqual([]);
    const late = s.put(KEY, 2, E1);   // the original PUT commits after all of that
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
    expect(s.deletes).toEqual([`${KEY}@${late}`]);
    expect(s.keys.get(KEY)!.map((v) => v.id)).toEqual([owned]);
  });

  test("two unresolved attempts at one key, only one appears: one is resolved, the other stays unresolved", async () => {
    const s = new FakeS3Versions();
    const l = lane([unknown("inv-a"), unknown("inv-b")]);
    s.put(KEY, 1, E1);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(l.row.invocations.map((i) => i.id)).toHaveLength(1);
    expect(s.count(KEY)).toBe(0);
    s.put(KEY, 2, E1);   // the second one lands later
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
  });

  test("a different stray (another body's ETag) is erased as cleanup but resolves nothing", async () => {
    const s = new FakeS3Versions();
    s.put(KEY, 1, E2);
    s.put(SIBLING, 1, E1);   // another key, never touched
    const l = lane([unknown("inv-1")]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(s.count(KEY)).toBe(0);
    expect(s.count(SIBLING)).toBe(1);
    expect(l.row.invocations).toHaveLength(1);
  });

  test("an unattributable version (no expected ETag recorded) is erased but resolves nothing; markers never count", async () => {
    const s = new FakeS3Versions();
    s.put(KEY, 1, E1);
    s.keys.set(KEY, [{ id: "m1", at: 2, marker: true }, ...s.keys.get(KEY)!]);
    const l = lane([{ id: "inv-1", outcome: "dispatched" }]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(l.row.invocations).toHaveLength(1);
  });

  test("an arbitrarily old unknown with repeated strict-zero inventories stays unresolved; backoff caps at a day", async () => {
    const s = new FakeS3Versions();
    const l = lane([unknown("inv-old")]);
    for (let i = 0; i < 20; i++) expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(l.row.invocations).toHaveLength(1);
    expect(l.row.sweeps).toBe(20);
    expect([0, 1, 2, 3, 11, 12, 99].map(nextSweepDelayMs)).toEqual([60_000, 120_000, 240_000, 480_000, 86_400_000, 86_400_000, 86_400_000]);
  });

  test("acked (its own confirmed version): owned -> resolved, version stays; not owned (erase duty) -> erased, then resolved; refused -> resolved", async () => {
    const s = new FakeS3Versions();
    const v1 = s.put(KEY, 1, E1);
    const a = lane([{ id: "pub", outcome: "acked", versionId: v1 }], { ownedVersions: [v1] });
    expect(await reconcileObligation(s.eraser(), a.store)).toBe("retired");
    expect(s.count(KEY)).toBe(1);
    const b = lane([{ id: "gone", outcome: "acked", versionId: v1 }, { id: "nope", outcome: "refused" }]);
    expect(await reconcileObligation(s.eraser(), b.store)).toBe("retired");
    expect(s.count(KEY)).toBe(0);
    expect(terminalInvocations([{ id: "x", outcome: "acked", versionId: "v9" }], new Set(), null)).toEqual([]);   // incomplete inventory: no proof
  });
});

describe("compare-and-set: a changed row survives", () => {
  test("a publication that lands between the proof and the retirement makes the retire refuse; the row survives", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1, E1);
    const l = lane([{ id: "x", outcome: "acked", versionId: v }]);
    const store: ObligationStore = { ...l.store, resolve: async (rev, ids) => { l.row.rev++; void ids; void rev; return { ok: false }; } };
    expect(await reconcileObligation(s.eraser(), store)).toBe("refused");
    expect(l.row.gone).toBe(false);
  });

  test("a new invocation dispatched mid-run (row version moved) stops the run before that page's deletes: refused, nothing retired", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 5; i++) s.put(KEY, i, E2);
    const l = lane([unknown("inv-1")]);
    let loads = 0;
    const store: ObligationStore = { ...l.store, load: async () => { if (++loads === 3) { l.row.rev++; (l.row.invocations as WriteInvocation[]).push(unknown("inv-new")); } return l.store.load(); } };
    expect(await reconcileObligation(s.eraser(), store)).toBe("refused");
    expect(l.row.gone).toBe(false);
    expect(l.row.invocations.map((i) => i.id)).toEqual(["inv-1", "inv-new"]);
  });

  test("an owner whose version is unrecorded blocks every erase at the key", async () => {
    const s = new FakeS3Versions();
    s.put(KEY, 1, E1);
    const l = lane([unknown("inv-1")], { ownersWithoutVersion: 1 });
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");
    expect(s.deletes).toEqual([]);
  });
});

describe("durability across failures", () => {
  test("a lost DELETE ack: the attribution is durable before the delete; the rerun proves it absent and resolves", async () => {
    const s = new FakeS3Versions();
    const v = s.put(KEY, 1, E1);
    let lose = true;
    s.onDelete = () => (lose ? ((lose = false), "lose") : undefined);
    const l = lane([unknown("inv-1")]);
    await expect(reconcileObligation(s.eraser(), l.store)).rejects.toMatchObject({ code: "s3_500" });
    expect(l.row.invocations[0]!.observedVersion).toBe(v);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
  });

  test("> 10k versions: bounded runs persist the bound checkpoint; a crash reuses it; the obligation survives until complete", async () => {
    const s = new FakeS3Versions();
    for (let i = 0; i < 12_500; i++) s.put(KEY, i, E2);
    const l = lane([unknown("inv-1")]);
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("incomplete");
    expect(l.row.checkpoint).not.toBeNull();
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("rescheduled");   // all erased, but inv-1 never appeared
    expect(s.count(KEY)).toBe(0);
    expect(l.row.invocations).toHaveLength(1);
  }, 60_000);

  test("a foreign checkpoint is never adopted (the run restarts and skips nothing)", async () => {
    const s = new FakeS3Versions(); s.pageSize = 2;
    for (let i = 0; i < 3; i++) s.put(KEY, i, E2);
    const l = lane([], { checkpoint: { v: 1, bucket: "b", prefix: KEY.slice(0, KEY.lastIndexOf("/") + 1), scope: "write-obligation:other", afterKey: KEY, inKey: null } });
    expect(await reconcileObligation(s.eraser(), l.store)).toBe("retired");
    expect(s.count(KEY)).toBe(0);
  });
});

describe("boundedPut: an enforced wall deadline", () => {
  test("a port that IGNORES the abort and succeeds late: the caller sees a timeout at the deadline, unknown is durably marked first, the late success is only reported", async () => {
    const order: string[] = [];
    const late: unknown[] = [];
    const started = Date.now();
    await expect(boundedPut(() => new Promise((r) => setTimeout(() => { order.push("late-success"); r("v-late"); }, 40)),
      { onUnknown: async () => { order.push("unknown"); }, onLate: (o) => late.push(o) }, 1)).rejects.toBeInstanceOf(PutTimeout);
    expect(Date.now() - started).toBeLessThan(35);
    expect(order).toEqual(["unknown"]);
    await new Promise((r) => setTimeout(r, 60));
    expect(order).toEqual(["unknown", "late-success"]);
    expect(late).toEqual([{ ok: true, value: "v-late" }]);
  });

  test("a stalling port that never settles still returns at the deadline; a late rejection is contained", async () => {
    let unknown = 0;
    await expect(boundedPut(() => new Promise(() => {}), { onUnknown: async () => { unknown++; } }, 5)).rejects.toBeInstanceOf(PutTimeout);
    expect(unknown).toBe(1);
    const late: unknown[] = [];
    await expect(boundedPut(() => new Promise((_, rej) => setTimeout(() => rej(new Error("late boom")), 20)), { onUnknown: async () => {}, onLate: (o) => late.push(o) }, 2)).rejects.toBeInstanceOf(PutTimeout);
    await new Promise((r) => setTimeout(r, 40));
    expect(late).toEqual([{ ok: false, error: expect.any(Error) }]);
  });

  test("a definite refusal of this request is reported as refused, any other error as unknown; bounds are validated", async () => {
    const seen: string[] = [];
    const hooks = { onUnknown: async () => { seen.push("unknown"); }, onRefused: async () => { seen.push("refused"); }, isRefusal: (e: unknown) => (e as { code?: string }).code === "precondition_failed" };
    await expect(boundedPut(async () => { throw Object.assign(new Error("412"), { code: "precondition_failed" }); }, hooks)).rejects.toBeDefined();
    await expect(boundedPut(async () => { throw Object.assign(new Error("reset"), { code: "ECONNRESET" }); }, hooks)).rejects.toBeDefined();
    expect(seen).toEqual(["refused", "unknown"]);
    expect(await boundedPut(async () => "ok", hooks)).toBe("ok");
    for (const bad of [0, -1, 1.5, 900_001]) await expect(boundedPut(async () => "x", hooks, bad)).rejects.toMatchObject({ code: "timeout_bounds" });
  });
});
