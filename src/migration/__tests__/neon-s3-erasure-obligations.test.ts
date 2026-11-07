/**
 * E3 on the shared unresolved-write obligation (write-obligation.ts v3, byte-identical from 142761c7) over its own
 * SyncState tables: every doomed version is reserved before its delete (publish-vs-reserve barrier, both orders), a
 * prefix is done only after a fresh strict zero inventory AND no unresolved write invocation, a dispatched PUT is never
 * resolved by time or a zero inventory, a malformed later page / new versions mid-traversal never yield "done", and the
 * exporter's writer side (obligated-store.ts) records dispatch / acked / refused exactly. Offline: fake-s3-versions.ts.
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { obligatedStore } from "../neon-s3-sync/obligated-store.ts";
import { ObjectStoreError, type ObjectStore } from "../../shared/vendor.ts";

setDefaultTimeout(120_000);
const OLD = Date.parse("2026-11-01T00:00:00Z");
const PREFIX = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
const key = (i: number) => `${PREFIX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`;

const authority = (): ErasureAuthority => ({
  kind: "supabase",
  ready: async () => {},
  lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }),
  fencedGroups: async () => [],
  fencedLivePage: async () => [],
  stillFenced: async () => true,
});
async function world(n = 30) {
  const s = new FakeS3Versions();
  const ids: string[] = [];
  for (let i = 0; i < n; i++) ids.push(s.put(key(i), OLD));
  const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-")), "s.sqlite"));
  let clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], clock - 1000);
  const j = new ErasureJob(authority(), st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => clock, async () => {});
  const queued = () => st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key);
  return { s, st, j, ids, queued, advance: (ms: number) => void (clock += ms) };
}

describe("E3 erasure gated on unresolved-write obligations (v3)", () => {
  test("a dispatched PUT (unknown outcome) keeps the prefix queued after a zero inventory, across drains and hours; its own ack of a gone version resolves it", async () => {
    const w = await world();
    const inv = w.st.dispatchWrite(key(3), "exporter:1");
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0); // everything erased...
    expect(w.queued()).toEqual([PREFIX]); // ...but NOT done: the PUT may still commit
    expect(w.j.counters.unresolved).toBe(1);
    for (let i = 0; i < 5; i++) {
      w.advance(2 * 3_600_000); // time never resolves it
      await w.j.drain();
    }
    expect(w.queued()).toEqual([PREFIX]);
    expect(w.st.unresolvedWrites(PREFIX).map((r) => r.id)).toEqual([inv]);
    // The late commit lands, then the writer's own response arrives with that exact version.
    const late = w.s.put(key(3), OLD);
    w.st.ackWrite(inv, late);
    w.advance(2 * 3_600_000);
    await w.j.drain();
    // Erased (it was unowned), then resolved by absence from a fresh complete inventory: done.
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.st.unresolvedWrites(PREFIX)).toEqual([]);
    expect(w.j.counters.prefixesErased).toBe(1);
  });

  test("a listed version never resolves a dispatched invocation (no content attribution)", async () => {
    const w = await world(3);
    w.st.dispatchWrite(key(1), "exporter:1");
    // A version at exactly that key exists; it is erased as cleanup, but the invocation stays unresolved.
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.st.unresolvedWrites(PREFIX).length).toBe(1);
    expect(w.queued()).toEqual([PREFIX]);
  });

  test("publish BEFORE reserve: an owned version refuses the page's reservation; nothing in that page is deleted", async () => {
    const w = await world(5);
    // The exporter published (owns) a version under the queued prefix after it was queued.
    w.st.publishWrites([{ key: key(2), versionId: w.ids[2]! }]);
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(5);
    expect(w.s.deletes).toEqual([]);
    expect(w.j.counters.reservationsRefused).toBe(1);
    expect(w.queued()).toEqual([PREFIX]);
    // Ownership ends when it is queued again (the exporter dropped it): the next drain erases everything.
    w.st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], Date.parse("2026-11-07T00:00:00Z"));
    w.advance(3_600_000);
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
  });

  test("reserve BEFORE publish: a publish of a reserved version is refused (the barrier holds in the other order)", async () => {
    const w = await world(5);
    let refused: string | undefined;
    // While the eraser deletes the page (its versions already reserved), the exporter tries to publish one of them.
    w.s.onDelete = (k, id) => {
      if (refused === undefined && k === key(0))
        try {
          w.st.publishWrites([{ key: key(4), versionId: w.ids[4]! }]);
          refused = "published";
        } catch (e) {
          refused = (e as { code?: string }).code;
        }
    };
    await w.j.drain();
    expect(refused).toBe("publish_reserved");
    expect(w.st.publishedUnder(PREFIX)).toEqual([]);
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
  });

  test("a writer change under the prefix after the load refuses the reservation (compare-and-set on the prefix revision)", async () => {
    const w = await world(5);
    // A dispatch lands between the eraser's load and its first page's reserve: the listing request is the hook.
    let once = false;
    const orig = w.s.rewrite;
    w.s.rewrite = (call, xml) => {
      if (!once) {
        once = true;
        w.st.dispatchWrite(key(9), "exporter:2");
      }
      return orig ? orig(call, xml) : xml;
    };
    await w.j.drain();
    expect(w.s.deletes).toEqual([]);
    expect(w.j.counters.reservationsRefused).toBe(1);
    expect(w.queued()).toEqual([PREFIX]);
  });

  test("a malformed LATER page fails the step: earlier pages may be erased, the prefix is never done; the next drain resumes and finishes", async () => {
    const w = await world(2_500);
    w.s.pageSize = 1000;
    w.s.rewrite = (call, xml) => (call === 2 ? xml.replace("<IsTruncated>true</IsTruncated>", "<IsTruncated>maybe</IsTruncated>") : xml);
    await w.j.drain();
    expect(w.queued()).toEqual([PREFIX]);
    expect(w.s.count(PREFIX)).toBeGreaterThan(0);
    expect(w.j.counters.failed).toBe(1);
    w.s.rewrite = undefined;
    w.advance(3_600_000);
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
  });

  test("new versions written mid-traversal are found by the fresh verification: not done until a later drain proves zero", async () => {
    const w = await world(10);
    let added = false;
    w.s.onDelete = (k) => {
      if (!added && k === key(9)) {
        added = true;
        w.s.put(key(0), OLD); // a new version of an already-erased key
      }
    };
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(1);
    expect(w.queued()).toEqual([PREFIX]);
    expect(w.j.counters.unverified).toBe(1);
    w.advance(3_600_000);
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
  });
});

describe("exporter writer side (obligated-store.ts)", () => {
  const base = (put: ObjectStore["put"]): ObjectStore => ({ backend: "fake", head: async () => undefined, get: async () => { throw new Error("x"); }, getObject: async () => { throw new Error("x"); }, list: async () => ({ items: [] }), presignGet: async () => "", put, delete: async () => {} }) as unknown as ObjectStore;
  async function st() {
    return new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-w-")), "s.sqlite"));
  }
  test("dispatch is durable BEFORE the PUT; its own ack records the exact version; created:false and a definite refusal clear it; anything else stays dispatched", async () => {
    const s = await st();
    const acked = new Map<string, string>();
    let seenDuringPut: number | undefined;
    const ok = obligatedStore(base(async (k) => {
      seenDuringPut = s.unresolvedWrites(PREFIX).length;
      return { key: k, version: { eTag: "e", version: "ver-1" }, contentSha256: "x", size: 1, created: true };
    }), s, "exporter:1", acked);
    await ok.put(key(1) as never, new Uint8Array([1]));
    expect(seenDuringPut).toBe(1);
    expect(s.unresolvedWrites(PREFIX)).toEqual([expect.objectContaining({ key: key(1), outcome: "acked", versionId: "ver-1" })]);
    expect(acked.get(key(1))).toBe("ver-1");
    // Its own publish resolves exactly that invocation.
    s.publishWrites([{ key: key(1), versionId: "ver-1" }]);
    expect(s.unresolvedWrites(PREFIX)).toEqual([]);
    // An unversioned store: "null".
    await obligatedStore(base(async (k) => ({ key: k, version: { eTag: "e" }, contentSha256: "x", size: 1, created: true })), s, "exporter:1", acked).put(key(2) as never, new Uint8Array([1]));
    expect(s.unresolvedWrites(PREFIX)).toEqual([expect.objectContaining({ key: key(2), outcome: "acked", versionId: "null" })]);
    // Identical content already there (created:false): no write, cleared.
    await obligatedStore(base(async (k) => ({ key: k, version: { eTag: "e" }, contentSha256: "x", size: 1, created: false })), s, "exporter:1", acked).put(key(3) as never, new Uint8Array([1]));
    // A definite refusal of this request: cleared. A timeout / transient error: stays dispatched (unknown).
    await obligatedStore(base(async () => { throw new ObjectStoreError("already_exists", "fake", "different content"); }), s, "exporter:1", acked).put(key(4) as never, new Uint8Array([1])).catch(() => {});
    await obligatedStore(base(async () => { throw new ObjectStoreError("transient", "fake", "timeout"); }), s, "exporter:1", acked).put(key(5) as never, new Uint8Array([1])).catch(() => {});
    expect(s.unresolvedWrites(PREFIX).map((r) => [r.key, r.outcome])).toEqual(expect.arrayContaining([[key(2), "acked"], [key(5), "dispatched"]]));
    expect(s.unresolvedWrites(PREFIX).length).toBe(2);
  });
});

describe("prefix membership of the obligation tables (index-friendly, exact)", () => {
  test("exact under byte/code-point order for keys that sort differently under locale collations; the key index is used", async () => {
    const s = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-p-")), "s.sqlite"));
    const P = "96bulls/org-1/v1/ws=W-a/m=x/h=claude/d=2026/10/30/s=Sess_1/";
    const inside = [`${P}a`, `${P}Z`, `${P}-`, `${P}_`, `${P}é/ü.json`, `${P}\u{10FFFF}tail`, `${P}raw/日本.zst`];
    const outside = [
      P.slice(0, -1), // the prefix without its slash
      P.replace("Sess_1", "sess_1"), // case
      P.replace("Sess_1", "Sess-1"), // '_' vs '-'
      P.replace("W-a", "W_a"),
      P.replace("W-a", "W-A"),
      P.replace("s=Sess_1/", "s=Sess_1é/"), // multibyte right after the prefix body
      `${P.slice(0, -1)}\u{10FFFF}/x`,
    ];
    for (const k of [...inside, ...outside]) s.publishWrites([{ key: k, versionId: "v1" }]);
    expect(s.publishedUnder(P).map((r) => r.key).sort()).toEqual([...inside].sort());
    for (const k of [...inside, ...outside]) s.dispatchWrite(k, "exporter:1");
    expect(s.unresolvedWrites(P).map((r) => r.key).sort()).toEqual([...inside].sort());
    // A range-only predicate would still be exact here; the residual test pins exactness at the U+10FFFF edge.
    for (const t of ["write_invocations", "write_published", "erase_reservations"] as const) expect(s.prefixPlan(t, P)).toMatch(/SEARCH .* USING (COVERING )?(INDEX|PRIMARY KEY)/);
    s.unpublish([P]);
    expect(s.publishedUnder(P)).toEqual([]);
    expect(s.publishedUnder(outside[1]!).length).toBe(1);
  });
});

test("prefix membership: a prefix ending in U+10FFFF (no simple successor) is still exact", async () => {
  const s = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-q-")), "s.sqlite"));
  const P = "k/\u{10FFFF}";
  const inside = [P, `${P}\u{10FFFF}\u{10FFFF}x`, `${P}a`];
  const outside = ["k/", "k/\u{FFFF}", "k0", "l"];
  for (const k of [...inside, ...outside]) s.dispatchWrite(k, "h");
  expect(s.unresolvedWrites(P).map((r) => r.key).sort()).toEqual([...inside].sort());
});
