/**
 * E3 on the shared unresolved-write obligation (write-obligation.ts v3, byte-identical from 142761c7) over its own
 * SyncState tables: every doomed version is reserved before its delete (publish-vs-reserve barrier, both orders), a
 * prefix is done only after a fresh strict zero inventory AND no unresolved write invocation, a dispatched PUT is never
 * resolved by time or a zero inventory, a malformed later page / new versions mid-traversal never yield "done", and the
 * exporter's writer side (obligated-store.ts) records dispatch / acked / refused exactly. Offline: fake-s3-versions.ts.
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, eraseScope } from "../neon-s3-erasure/job.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { obligatedStore } from "../neon-s3-sync/obligated-store.ts";
import { ObjectStoreError, type ObjectStore } from "../../shared/vendor.ts";

// These suites exercise the obligation model (S3X_OBLIGATIONS on); the default-off path has its own suite.
process.env.S3X_OBLIGATIONS = "on";

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
/** An exporter lease on `prefix` (generation minted at acquisition): the publication proof. */
function lease(st: SyncState, prefix: string, holder = "exporter:1", at = Date.now()) {
  if (!st.acquireLease(prefix, holder, undefined, at)) throw new Error("lease busy");
  return { holder, gens: { [prefix]: st.leaseGen(prefix)! } };
}
async function world(n = 30) {
  const s = new FakeS3Versions();
  const ids: string[] = [];
  for (let i = 0; i < n; i++) ids.push(s.put(key(i), OLD));
  const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-")), "s.sqlite"));
  let clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], clock - 1000);
  // TEST-ONLY completion seam (production never sets it).
  const j = new ErasureJob(authority(), st, s.eraser(), () => {}, { ...DEFAULT_ERASURE_CONFIG, obligationsSettled: () => true }, () => clock, async () => {});
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
    // (its own publication of a reused version it referenced, at the key level, then its group ended)
    w.st.publishWrites([{ key: key(2), versionId: "*" }], lease(w.st, PREFIX));
    w.st.releaseLeases("exporter:1", [PREFIX]);
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
          // A delayed exporter whose lease the eraser took: refused (lease generation, or the reservation).
          w.st.publishWrites([{ key: key(4), versionId: "*" }], { holder: "exporter:1", gens: { [PREFIX]: "stale-generation" } });
          refused = "published";
        } catch (e) {
          refused = (e as { code?: string }).code;
        }
    };
    await w.j.drain();
    expect(["publish_lease_lost", "publish_reserved"]).toContain(refused);
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
    s.publishWrites([{ key: key(1), versionId: "ver-1" }], lease(s, PREFIX));
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
    const root = lease(s, "96bulls/");
    for (const k of [...inside, ...outside]) s.publishWrites([{ key: k, versionId: "*" }], root);
    expect(s.publishedUnder(P).map((r) => r.key).sort()).toEqual([...inside].sort());
    for (const k of [...inside, ...outside]) s.dispatchWrite(k, "exporter:1");
    expect(s.unresolvedWrites(P).map((r) => r.key).sort()).toEqual([...inside].sort());
    // A range-only predicate would still be exact here; the residual test pins exactness at the U+10FFFF edge.
    for (const t of ["write_invocations", "write_published", "erase_reservations"] as const) expect(s.prefixPlan(t, P)).toMatch(/SEARCH .* USING (COVERING )?(INDEX|PRIMARY KEY)/);
    s.unpublishPrefix(P);
    expect(s.publishedUnder(P)).toEqual([]);
    expect(s.publishedUnder(outside[1]!).length).toBe(1);
  });
});

test("prefix membership: U+10FFFF right before the slash is still exact; a non-/ prefix is refused", async () => {
  const s = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-q-")), "s.sqlite"));
  const P = "k/\u{10FFFF}/";
  const inside = [`${P}x`, `${P}\u{10FFFF}\u{10FFFF}x`, `${P}a/b`];
  const outside = ["k/", "k/\u{10FFFF}", "k/\u{FFFF}/", "k/\u{10FFFF}0", "k0", "l"];
  for (const k of [...inside, ...outside]) s.dispatchWrite(k, "h");
  expect(s.unresolvedWrites(P).map((r) => r.key).sort()).toEqual([...inside].sort());
  expect(() => s.unresolvedWrites("k/\u{10FFFF}")).toThrow();
});

describe("every destructive route is gated; state loss fails closed", () => {
  const OBJ = `${PREFIX}raw/orphan.ndjson.zst`;
  test("queued OBJECT: a pending PUT + empty inventory stays queued; the late PUT is then erased and only then done", async () => {
    const w = await world(0);
    w.s.put(OBJ, OLD);
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], Date.parse("2026-11-06T00:00:00Z"));
    // Only the object item (drop the prefix item world() queued).
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    const inv = w.st.dispatchWrite(OBJ, "exporter:1");
    await w.j.drain();
    expect(w.s.count(OBJ)).toBe(0);
    expect(w.queued()).toEqual([OBJ]); // NOT done: the PUT may still commit
    const late = w.s.put(OBJ, OLD); // ... and it does
    w.st.ackWrite(inv, late);
    w.advance(3_600_000);
    await w.j.drain();
    expect(w.s.count(OBJ)).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.j.counters.objectsErased).toBe(1);
  });

  test("queued OBJECT: an owned (claimed) version refuses the reservation; nothing deleted", async () => {
    const w = await world(0);
    w.s.put(OBJ, OLD);
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], Date.parse("2026-11-06T00:00:00Z"));
    w.st.publishWrites([{ key: OBJ, versionId: "*" }], lease(w.st, PREFIX)); // the exporter claims it again (key level)
    w.st.releaseLeases("exporter:1", [PREFIX]);
    await w.j.drain();
    expect(w.s.count(OBJ)).toBe(1);
    expect(w.s.deletes).toEqual([]);
    expect(w.queued()).toEqual([OBJ]);
  });

  test("a deleted / recreated state DB fails CLOSED: nothing is ever marked erased; there is no attestation unlock", async () => {
    const s = new FakeS3Versions();
    for (let i = 0; i < 5; i++) s.put(key(i), OLD);
    s.put(OBJ, OLD);
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-lost-"));
    const path = join(dir, "s.sqlite");
    const first = new SyncState(path);
    first.dispatchWrite(key(2), "exporter:1"); // an outstanding PUT ...
    first.close();
    // TOTAL loss: the pointer AND its sibling database (a lost pointer alone is quarantined, below).
    for (const f of [path, `${path}.obligations.sqlite`, `${path}.obligations.sqlite-wal`, `${path}.obligations.sqlite-shm`]) await rm(f, { force: true });
    // ... forgotten with the files. A fresh state is UNATTESTED, and nothing attests it.
    const st = new SyncState(path);
    expect(st.obligationGenesis()).toMatchObject({ attestedAt: null, basis: "none" });
    expect(st.unresolvedWrites(PREFIX)).toEqual([]);
    const clock = Date.parse("2026-11-07T00:00:00Z");
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }, { key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], clock - 1000);
    let now = clock;
    const j = new ErasureJob(authority(), st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => now, async () => {});
    for (let i = 0; i < 4; i++) {
      await j.drain();
      now += 30 * 86_400_000; // no amount of time settles it
    }
    expect(s.count(PREFIX)).toBe(0); // erasure still proceeds ...
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key).sort()).toEqual([OBJ, PREFIX].sort()); // ... never done
    expect(j.counters.prefixesErased + j.counters.objectsErased).toBe(0);
    // Pre-existing / forged attested metadata does NOT unlock (the reader ignores attestedAt).
    st.setMeta("obligations_genesis", JSON.stringify({ at: 1, attestedAt: Date.now(), basis: "forged" }));
    expect(st.obligationsAttested()).toBe(false);
    expect(st.obligationGenesis().attestedAt).toBeNull();
    await j.drain();
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key).sort()).toEqual([OBJ, PREFIX].sort());
    // No CLI / PID / lock-file unlock exists.
    const main = await import("../neon-s3-erasure/main.ts");
    expect("attestObligationGenesis" in main).toBe(false);
    expect(typeof (st as unknown as { attestObligationGenesis?: unknown }).attestObligationGenesis).toBe("undefined");
  });

  test("a LEGACY (pre-obligation) state file is REFUSED by ON (fresh state only); its bytes are unchanged", async () => {
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-legacy-"));
    const path = join(dir, "s.sqlite");
    const { DatabaseSync } = await import("node:sqlite");
    const legacy = new DatabaseSync(path);
    legacy.exec("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT); CREATE TABLE pending_keys (org_id TEXT NOT NULL, session_id TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (org_id, session_id, key))");
    legacy.prepare("INSERT INTO pending_keys VALUES ('org-1', 'sess_1', ?)").run(`${PREFIX}manifest.json`);
    legacy.close();
    const { readFile } = await import("node:fs/promises");
    const before = await readFile(path);
    expect(() => new SyncState(path, { obligations: true })).toThrow(expect.objectContaining({ code: "obligations_requires_fresh_state" }));
    expect((await readFile(path)).equals(before)).toBe(true);
  });

  test("a USED sibling whose pointer DISAPPEARS is QUARANTINED: ON refuses, nothing is adopted, reset or deleted", async () => {
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-lostptr-"));
    const path = join(dir, "s.sqlite");
    const st = new SyncState(path, { obligations: true });
    const inv = st.dispatchWrite(key(1), "exporter:1");
    expect(st.reserveErase(`${PREFIX}raw/`, st.obligationRev(`${PREFIX}raw/`), "S", [{ key: key(2), versionId: "v2" }]).ok).toBe(true);
    st.close();
    await rm(path); // the published pointer is lost
    expect(() => new SyncState(path, { obligations: true })).toThrow(expect.objectContaining({ code: "obligations_state_conflict" }));
    expect(() => new SyncState(path, { obligations: true })).toThrow(expect.objectContaining({ code: "obligations_state_conflict" }));
    // The retained state is untouched (read through the sibling directly, read-only).
    const { DatabaseSync } = await import("node:sqlite");
    const db = new DatabaseSync(`${path}.obligations.sqlite`, { readOnly: true });
    expect((db.prepare("SELECT id FROM write_invocations").all() as { id: string }[]).map((r) => r.id)).toEqual([inv]);
    expect((db.prepare("SELECT count(*) AS n FROM erase_reservations").get() as { n: number }).n).toBe(1);
    db.close();
  });
});

describe("publication proves its own invocation + lease generation; protected terminal history", () => {
  test("a DELAYED exporter cannot publish after the eraser took its expired lease, erased the version and cleared everything", async () => {
    const w = await world(0);
    const k = key(7);
    // The exporter leased the prefix long ago (now expired), PUT v, got its own ack, then stalled.
    const proof = lease(w.st, PREFIX, "exporter:1", Date.parse("2026-11-06T00:00:00Z"));
    const inv = w.st.dispatchWrite(k, "exporter:1");
    const v = w.s.put(k, OLD);
    w.st.ackWrite(inv, v);
    // The eraser takes the expired lease, reserves + deletes v, resolves the invocation, clears its reservations, retires.
    await w.j.drain();
    expect(w.s.count(PREFIX)).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.st.writeTerminal(inv)).toEqual({ outcome: "erased", versionId: v });
    // The old exporter resumes: its publication is refused (the lease generation moved, then the erased history).
    const code = (f: () => void) => { try { f(); return null; } catch (e) { return (e as { code?: string }).code; } };
    expect(code(() => w.st.publishWrites([{ key: k, versionId: v }], proof))).toBe("publish_lease_lost");
    // Even with a NEW lease generation, the erased version (protected history, reservations long cleared) is refused.
    const fresh = lease(w.st, PREFIX, "exporter:1");
    expect(fresh.gens[PREFIX]).not.toBe(proof.gens[PREFIX]);
    expect(code(() => w.st.publishWrites([{ key: k, versionId: v }], fresh))).toBe("publish_erased");
    expect(w.st.publishedUnder(PREFIX)).toEqual([]);
  });

  test("an exact publication must consume its own live acked invocation; a resolved one is refused; renew keeps the generation", async () => {
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-pub-")), "s.sqlite"));
    const proof = lease(st, PREFIX);
    expect(st.renewLease(PREFIX, "exporter:1")).toBe(true);
    expect(st.leaseGen(PREFIX)).toBe(proof.gens[PREFIX]);
    const code = (f: () => void) => { try { f(); return null; } catch (e) { return (e as { code?: string }).code; } };
    // No invocation at all for that version: refused.
    expect(code(() => st.publishWrites([{ key: key(1), versionId: "v-unknown" }], proof))).toBe("publish_unresolved");
    const inv = st.dispatchWrite(key(1), "exporter:1");
    st.ackWrite(inv, "v-1");
    st.publishWrites([{ key: key(1), versionId: "v-1" }], proof); // consumes it
    expect(st.writeTerminal(inv)).toEqual({ outcome: "published", versionId: "v-1" });
    expect(st.unresolvedWrites(PREFIX)).toEqual([]);
    st.publishWrites([{ key: key(1), versionId: "v-1" }], proof); // idempotent: already owned exactly
    // A different holder never validates against this lease.
    expect(code(() => st.publishWrites([{ key: key(1), versionId: "*" }], { holder: "exporter:2", gens: proof.gens }))).toBe("publish_lease_lost");
    // A reservation (eraser) refuses a key-level claim at that key.
    expect(st.reserveErase(`${PREFIX}raw/`, st.obligationRev(`${PREFIX}raw/`), "e3", [{ key: key(2), versionId: "v-2" }]).ok).toBe(true);
    expect(code(() => st.publishWrites([{ key: key(2), versionId: "*" }], proof))).toBe("publish_reserved");
  });
});

describe("terminal cleanup covers exactly the versions a proof showed absent", () => {
  test("a single-key erase leaves a SIBLING's reservation and erased history untouched (its bytes remain)", async () => {
    const w = await world(0);
    const OBJ = `${PREFIX}raw/orphan.ndjson.zst`;
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    w.s.put(OBJ, OLD);
    const sib = key(5);
    const sibV = w.s.put(sib, OLD);
    // An earlier (other) erasure reserved the sibling's version; its bytes are still there.
    const dir = `${PREFIX}raw/`;
    expect(w.st.reserveErase(dir, w.st.obligationRev(dir), "other-subset", [{ key: sib, versionId: sibV }]).ok).toBe(true);
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], Date.parse("2026-11-06T00:00:00Z"));
    await w.j.drain();
    expect(w.queued()).toEqual([]); // the object is done ...
    expect(w.st.erasedAt(OBJ).length).toBe(1);
    expect(w.s.count(sib)).toBe(1); // ... the sibling's bytes remain, and so do its reservation and (empty) erased history
    expect(w.st.reservationsAt(sib)).toEqual([sibV]);
    expect(w.st.erasedAt(sib)).toEqual([]);
  });

  test("a kept successor's reservation (time-subset) is not retired as erased", async () => {
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-ret-")), "s.sqlite"));
    const k = key(1);
    expect(st.reserveErase(PREFIX, 0, "s", [{ key: k, versionId: "v-old" }, { key: k, versionId: "v-kept" }]).ok).toBe(true);
    // The complete inventory shows v-kept still present: only v-old is retired.
    st.retireReservations(PREFIX, (key, v) => !(key === k && v === "v-kept"));
    expect(st.erasedAt(k)).toEqual(["v-old"]);
    expect(st.reservationsAt(k)).toEqual(["v-kept"]);
  });

  // BLOCKED on the shared primitive (eraser.ts, Babysitter lane; not forked here): verifyErased lists the whole
  // directory from its start under the 10k-version budget, so an exact key with >10k unrelated KEPT siblings never
  // gets a fresh strict proof. Needs an exact-key (or start-marker) verifyErased in the primitive.
  test("exact-key erase: the target + 10,001 kept siblings in its directory still completes (the primitive's exactKey mode)", async () => {
    const w = await world(0);
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    const OBJ = `${PREFIX}raw/target.ndjson.zst`;
    for (let i = 0; i < 10_001; i++) w.s.put(`${PREFIX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, OLD); // sorts BEFORE the target
    w.s.put(OBJ, OLD);
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], Date.parse("2026-11-06T00:00:00Z"));
    await w.j.drain();
    expect(w.s.keys.get(OBJ)?.length ?? 0).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.s.count(`${PREFIX}raw/b=`)).toBe(10_001); // every sibling kept
  });

  test("a LEGACY directory-mode checkpoint under an exact-key scope restarts the traversal (never refused or stuck)", async () => {
    const w = await world(0);
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    const OBJ = `${PREFIX}raw/legacy.ndjson.zst`;
    w.s.put(OBJ, OLD);
    const enq = Date.parse("2026-11-06T00:00:00Z");
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], enq);
    // The checkpoint an earlier (directory-mode) run persisted for exactly this item's scope.
    const scope = eraseScope(["queued-object", "supabase", "org-1", OBJ, "unreferenced", enq, "lease:" + PREFIX]);
    w.st.setEraseCheckpoint(scope, { v: 1, bucket: "b", prefix: `${PREFIX}raw/`, scope, afterKey: null, inKey: null });
    await w.j.drain();
    expect(w.s.keys.get(OBJ)?.length ?? 0).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.j.counters.failed).toBe(0);
  });

  test("a FOREIGN stored checkpoint (another bucket, same scope and mode) is ignored and the traversal restarts", async () => {
    const w = await world(0);
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    const OBJ = `${PREFIX}raw/foreign.ndjson.zst`;
    w.s.put(OBJ, OLD);
    const enq = Date.parse("2026-11-06T00:00:00Z");
    w.st.enqueueErasure([{ key: OBJ, kind: "object", org_id: "org-1", reason: "unreferenced" }], enq);
    const scope = eraseScope(["queued-object", "supabase", "org-1", OBJ, "unreferenced", enq, "lease:" + PREFIX]);
    w.st.setEraseCheckpoint(scope, { v: 1, bucket: "another-bucket", prefix: `${PREFIX}raw/`, scope, afterKey: null, inKey: null, exactKey: OBJ });
    await w.j.drain();
    expect(w.s.keys.get(OBJ)?.length ?? 0).toBe(0);
    expect(w.queued()).toEqual([]);
    expect(w.j.counters.failed).toBe(0);
  });

});

describe("queueing an exact OBJECT never unpublishes another key that merely starts with it (dogpatch 7772 P1)", () => {
  const A = `${PREFIX}raw/0000000000`;
  const B = `${PREFIX}raw/0000000000-0-abcdef01.ndjson.zst`;
  test("unit: claim B, enqueue exact object A -> B is still published", async () => {
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-ab-")), "s.sqlite"));
    st.enqueueErasure([{ key: B, kind: "object", org_id: "org-1", reason: "stale" }]);
    st.publishWrites([{ key: B, versionId: "*" }], lease(st, PREFIX));
    st.enqueueErasure([{ key: A, kind: "object", org_id: "org-1", reason: "unreferenced" }]);
    expect(st.publishedUnder(PREFIX)).toEqual([{ key: B, versionId: "*" }]);
    // A prefix item still unpublishes everything under its prefix.
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }]);
    expect(st.publishedUnder(PREFIX)).toEqual([]);
  });

  test("end to end: B reused + claimed, orphan A queued, crash before cancelErasures, lease expires -> E3 erases A and B SURVIVES", async () => {
    const w = await world(0);
    w.st.erasureDone(PREFIX, Date.parse("2026-11-07T00:00:00Z") - 1000);
    w.s.put(A, OLD);
    w.s.put(B, OLD);
    // A stale queued erasure of B; a new export reuses and claims B (its manifest references B) ...
    w.st.enqueueErasure([{ key: B, kind: "object", org_id: "org-1", reason: "stale" }], Date.parse("2026-11-05T00:00:00Z"));
    w.st.publishWrites([{ key: B, versionId: "*" }], lease(w.st, PREFIX, "exporter:1", Date.parse("2026-11-06T00:00:00Z")));
    // ... queues orphan A, then hard-crashes before cancelErasures (B's stale item stays queued; the lease expires).
    w.st.enqueueErasure([{ key: A, kind: "object", org_id: "org-1", reason: "unreferenced" }], Date.parse("2026-11-06T00:00:00Z"));
    w.advance(3 * 3_600_000);
    await w.j.drain();
    w.advance(3 * 3_600_000);
    await w.j.drain();
    // Exact-key counts (FakeS3Versions.count is by prefix, and A is a string prefix of B).
    expect(w.s.keys.get(A)?.length ?? 0).toBe(0);
    expect(w.s.keys.get(B)?.length ?? 0).toBe(1); // the manifest's reference is never erased
    expect(w.queued()).toEqual([B]); // its stale item is refused (owned), never done
  });
});

describe("bounded, resumable reservation retirement (dogpatch 5548 P2)", () => {
  const D = `${PREFIX}raw/`;
  async function reserved(n: number, scope = "S") {
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-retire-"));
    const path = join(dir, "s.sqlite");
    const st = new SyncState(path);
    const vs = Array.from({ length: n }, (_, i) => ({ key: `${D}b=${String(i).padStart(6, "0")}.zst`, versionId: `v${i}` }));
    for (let i = 0; i < vs.length; i += 500) expect(st.reserveErase(D, st.obligationRev(D), scope, vs.slice(i, i + 500)).ok).toBe(true);
    return { st, path, vs };
  }
  const count = (st: SyncState, sql: string) => ((st as unknown as { db: { prepare(s: string): { get(): { n: number } } } }).db.prepare(sql).get().n);

  test("a large set retires in bounded pages (<= 1000 rows per transaction); a crash mid-retirement RESUMES from the durable cursor", async () => {
    const r = await reserved(2_500);
    const first = r.st.retireReservationsPage(D, "S", undefined, () => true, 1000, Number.MAX_SAFE_INTEGER, "proof-1");
    expect(first).toMatchObject({ retired: 1000, done: false });
    expect(r.st.retireCursor("S")).toEqual([r.vs[999]!.key, "v999"]);
    // Crash: a new process on the same file resumes after the cursor (nothing re-scanned, nothing lost).
    r.st.close();
    const st2 = new SyncState(r.path);
    expect(st2.retireCursor("S")).toEqual([r.vs[999]!.key, "v999"]);
    // The SAME proof (unchanged workset) resumes after the cursor.
    expect(st2.retireReservationsPage(D, "S", undefined, () => true, 1000, Number.MAX_SAFE_INTEGER, "proof-1")).toMatchObject({ retired: 1000, done: false });
    expect(st2.retireReservationsPage(D, "S", undefined, () => true, 1000, Number.MAX_SAFE_INTEGER, "proof-1")).toMatchObject({ retired: 500, done: true });
    expect(st2.retireCursor("S")).toBeNull();
    expect(count(st2, "SELECT count(*) AS n FROM erased_versions")).toBe(2_500);
    expect(count(st2, "SELECT count(*) AS n FROM erase_reservations")).toBe(0);
  });

  test("only this scope's proven-absent rows are retired: another scope's reservations and kept versions are untouched", async () => {
    const r = await reserved(1_200, "S");
    const other = `${D}zz-other.zst`;
    expect(r.st.reserveErase(D, r.st.obligationRev(D), "T", [{ key: other, versionId: "vo" }]).ok).toBe(true);
    const kept = r.vs[5]!;
    while (!r.st.retireReservationsPage(D, "S", undefined, (k, v) => !(k === kept.key && v === kept.versionId)).done);
    expect(r.st.reservationsAt(other)).toEqual(["vo"]);
    expect(r.st.reservationsAt(kept.key)).toEqual([kept.versionId]);
    expect(r.st.erasedAt(kept.key)).toEqual([]);
    expect(count(r.st, "SELECT count(*) AS n FROM erased_versions")).toBe(1_199);
  });

  test("the exact-key scan touches only that key's rows (SQL key filter, index search), never the directory", async () => {
    const r = await reserved(1_500);
    const target = r.vs[700]!.key;
    expect(r.st.retirePlan(target, "S")).toMatch(/SEARCH .* USING (COVERING )?(INDEX|PRIMARY KEY)/);
    let seen = 0;
    const res = r.st.retireReservationsPage(D, "S", target, (k) => { seen += 1; expect(k).toBe(target); return true; });
    expect(res).toMatchObject({ retired: 1, done: true });
    expect(seen).toBe(1);
    expect(count(r.st, "SELECT count(*) AS n FROM erase_reservations")).toBe(1_499);
  });

  test("lead schedule 1 (state method): after page 1, a FRESH proof with an earlier-key new reservation (HW 5 -> 6) is not skipped; done only with eligibleRemaining 0", async () => {
    const r = await reserved(2_500);
    const all = () => true;
    const hw1 = r.st.reservationHighWater(D);
    expect(r.st.retireReservationsPage(D, "S", undefined, all, 1000, hw1, "proof-1")).toMatchObject({ retired: 1000, done: false });
    const early = { key: `${D}a-early.zst`, versionId: "v-early" };
    expect(r.st.reserveErase(D, r.st.obligationRev(D), "S", [early]).ok).toBe(true);
    const hw2 = r.st.reservationHighWater(D);
    let last;
    do last = r.st.retireReservationsPage(D, "S", undefined, all, 1000, hw2, "proof-2"); while (!last.done);
    expect(r.st.eligibleRemainingPage(D, "S", undefined, all, hw2, null).eligible).toBe(0);
    expect(r.st.reservationsAt(early.key)).toEqual([]);
  });

  test("lead schedule 2 (state method): a formerly KEPT row behind the cursor, absent under a fresh proof at the SAME HW, is not skipped", async () => {
    const r = await reserved(2_500);
    const kept = r.vs[3]!; // sorts before page 1's cursor
    const hw = r.st.reservationHighWater(D);
    const proof1 = (k: string, v: string) => !(k === kept.key && v === kept.versionId);
    expect(r.st.retireReservationsPage(D, "S", undefined, proof1, 1000, hw, "proof-1")).toMatchObject({ done: false });
    expect(r.st.reservationsAt(kept.key)).toEqual([kept.versionId]);
    // A fresh proof (same HW) now finds the kept version absent: a different workset identity.
    const proof2 = () => true;
    let last;
    do last = r.st.retireReservationsPage(D, "S", undefined, proof2, 1000, hw, "proof-2"); while (!last.done);
    expect(r.st.eligibleRemainingPage(D, "S", undefined, proof2, hw, null).eligible).toBe(0);
    expect(r.st.reservationsAt(kept.key)).toEqual([]);
  });

  test("a reservation added AFTER the proof's generation stays reserved (never retired against the older proof)", async () => {
    const r = await reserved(10);
    const hw = r.st.reservationHighWater(D);
    const late = { key: `${D}late.zst`, versionId: "v-late" };
    expect(r.st.reserveErase(D, r.st.obligationRev(D), "S", [late]).ok).toBe(true); // concurrent, after the proof
    while (!r.st.retireReservationsPage(D, "S", undefined, () => true, 1000, hw).done);
    expect(r.st.reservationsAt(late.key)).toEqual(["v-late"]);
    expect(r.st.erasedAt(late.key)).toEqual([]);
    expect(count(r.st, "SELECT count(*) AS n FROM erased_versions")).toBe(10);
  });

  test("job level: a reservation made concurrently DURING the proof's inventory (same scope) stays reserved after completion", async () => {
    const w = await world(20);
    const scope = eraseScope(["queued-prefix", "supabase", "org-1", PREFIX, "fenced", Date.parse("2026-11-07T00:00:00Z") - 1000, "lease:" + PREFIX]);
    const late = { key: `${PREFIX}raw/late.ndjson.zst`, versionId: "v-late-concurrent" };
    let emptyLists = 0;
    w.s.rewrite = (_call, xml) => {
      // The first empty listing is the fresh verification (one page erased everything, no relist), taken AFTER the
      // proof generation was captured: a concurrent reservation lands right then.
      if (w.s.count(PREFIX) === 0 && ++emptyLists === 1) expect(w.st.reserveErase(PREFIX, w.st.obligationRev(PREFIX), scope, [late]).ok).toBe(true);
      return xml;
    };
    await w.j.drain();
    expect(emptyLists).toBeGreaterThanOrEqual(1);
    // The writer-side change after the load refuses this run (compare-and-set on the prefix revision) before any
    // retirement; the concurrent reservation stays reserved and out of the erased history, and the item stays queued.
    expect(w.st.reservationsAt(late.key)).toEqual(["v-late-concurrent"]);
    expect(w.st.erasedAt(late.key)).toEqual([]);
    expect(w.queued()).toEqual([PREFIX]);
  });

  test("job level, across runs: a row that sorts BEFORE a saved cursor under a FRESH proof is not skipped (no erased until it is handled)", async () => {
    const s = new FakeS3Versions();
    // dogpatch's independent case: 1,500 rows, a crash after page 1, then an earlier-key reservation at H1+1.
    for (let i = 0; i < 1_500; i++) s.put(key(1_000 + i), OLD);
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-xrun-")), "s.sqlite"));
    let clock = Date.parse("2026-11-07T00:00:00Z");
    const enq = clock - 1000;
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], enq);
    const scope = eraseScope(["queued-prefix", "supabase", "org-1", PREFIX, "fenced", enq, "lease:" + PREFIX]);
    // Run 1 "crashes" right after its first retirement page saved the cursor (the authority check before page 2 fails).
    let crash = true;
    const a = { ...authority(), ready: async () => { if (crash && st.retireCursor(scope) !== null) throw Object.assign(new Error("crash"), { code: "crash" }); } };
    const j = new ErasureJob(a, st, s.eraser(), () => {}, { ...DEFAULT_ERASURE_CONFIG, obligationsSettled: () => true }, () => clock, async () => {});
    await j.drain();
    const cursor = st.retireCursor(scope);
    expect(cursor).not.toBeNull();
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key)).toEqual([PREFIX]);
    // Between runs: a reservation (same scope) at a key that sorts BEFORE the cursor, whose version is absent.
    const early = { key: key(0), versionId: "v-early" };
    expect(early.key < cursor![0]).toBe(true);
    const h1 = st.reservationHighWater(PREFIX);
    expect(st.reserveErase(PREFIX, st.obligationRev(PREFIX), scope, [early]).ok).toBe(true);
    expect(st.reservationHighWater(PREFIX)).toBeGreaterThan(h1); // the early row sits at H1+1
    // Run 2 with a fresh proof: the early row must be handled (retired) before the prefix is reported erased.
    crash = false;
    clock += 3_600_000;
    await j.drain();
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10)).toEqual([]);
    expect(st.reservationsAt(early.key)).toEqual([]);
    expect(st.erasedAt(early.key)).toEqual(["v-early"]);
  });

  test("a write landing DURING retirement (during the awaited authority check between pages) refuses the done claim; the queue row and the new invocation stay", async () => {
    const s = new FakeS3Versions();
    for (let i = 0; i < 2_500; i++) s.put(key(1_000 + i), OLD);
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-during-")), "s.sqlite"));
    const clock = Date.parse("2026-11-07T00:00:00Z");
    const enq = clock - 1000;
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], enq);
    const scope = eraseScope(["queued-prefix", "supabase", "org-1", PREFIX, "fenced", enq, "lease:" + PREFIX]);
    let inv: string | undefined;
    const a = { ...authority(), ready: async () => {
      // Once retirement has begun (page 1 saved its cursor), an exporter dispatch lands under the prefix.
      if (inv === undefined && st.retireCursor(scope) !== null) inv = st.dispatchWrite(`${PREFIX}raw/new.ndjson.zst`, "exporter:1");
    } };
    const j = new ErasureJob(a, st, s.eraser(), () => {}, { ...DEFAULT_ERASURE_CONFIG, obligationsSettled: () => true }, () => clock, async () => {});
    await j.drain();
    expect(inv).toBeDefined();
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key)).toEqual([PREFIX]); // no done claim
    expect(j.counters.prefixesErased).toBe(0);
    expect(st.unresolvedWrites(PREFIX).map((w) => w.id)).toEqual([inv]); // the new pending effect is kept
  });

  test("job level: a prefix whose reservations span several retirement pages still completes, and every proven-absent version is in the history", async () => {
    const w = await world(2_500);
    await w.j.drain();
    expect(w.queued()).toEqual([]);
    expect(count(w.st, "SELECT count(*) AS n FROM erased_versions")).toBe(2_500);
    expect(count(w.st, "SELECT count(*) AS n FROM erase_reservations")).toBe(0);
  });
});

describe("terminal claim: non-reverting revision + the eraser's lease generation (lead 10466e9c ABA)", () => {
  async function fresh() {
    const st = new SyncState(join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-obl-aba-")), "s.sqlite"));
    const enq = Date.parse("2026-11-06T00:00:00Z");
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], enq);
    return { st, enq };
  }

  test("the lead's schedule: proof at rev R; lease expires, a writer takes it, dispatch + ACK + publish, then enqueue/unpublish empties the active rows -> the old completion is REFUSED", async () => {
    const { st, enq } = await fresh();
    expect(st.acquireLease(PREFIX, "eraser:1", undefined, Date.parse("2026-11-06T00:00:00Z"))).toBe(true);
    const eraserLease = { prefix: PREFIX, holder: "eraser:1", gen: st.leaseGen(PREFIX)! };
    const proofRev = st.obligationRev(PREFIX);
    // The eraser's lease expires; a writer takes it, writes, gets its own ACK and publishes.
    expect(st.acquireLease(PREFIX, "writer:new")).toBe(true);
    const writer = { holder: "writer:new", gens: { [PREFIX]: st.leaseGen(PREFIX)! } };
    const inv = st.dispatchWrite(key(1), "writer:new");
    st.ackWrite(inv, "v-new");
    st.publishWrites([{ key: key(1), versionId: "v-new" }], writer);
    // An enqueue / unpublish removes the active rows again (the old active-row revision would be back at R).
    st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], enq);
    expect(st.unresolvedWrites(PREFIX)).toEqual([]);
    expect(st.publishedUnder(PREFIX)).toEqual([]);
    expect(st.obligationRev(PREFIX)).toBeGreaterThan(proofRev); // non-reverting
    expect(st.completeErasure(PREFIX, enq, PREFIX, proofRev, eraserLease)).toBe(false);
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key)).toEqual([PREFIX]);
  });

  test("the lead's EXACT harness schedule (pr309-1046 retirement-recheck.mjs): the stale completion is refused", () => {
    const P = PREFIX;
    const st = new SyncState(":memory:", { inMemoryForTests: true });
    const item = { key: P, kind: "prefix" as const, org_id: "synthetic-org", reason: "synthetic" };
    st.enqueueErasure([item], 1);
    expect(st.acquireLease(P, "eraser:old", 7_200_000, 0)).toBe(true);
    const oldGen = st.leaseGen(P)!;
    const proofRev = st.obligationRev(P);
    expect(st.acquireLease(P, "writer:new", 7_200_000, 10_800_000)).toBe(true);
    const writerGen = st.leaseGen(P)!;
    expect(writerGen).not.toBe(oldGen);
    const k = `${P}new-object`;
    const id = st.dispatchWrite(k, "writer:new");
    st.ackWrite(id, "native-v-new");
    st.publishWrites([{ key: k, versionId: "native-v-new" }], { holder: "writer:new", gens: { [P]: writerGen } });
    expect(st.obligationRev(P)).toBeGreaterThan(proofRev);
    st.enqueueErasure([item], 10_800_001);
    // On 10466e9c the active-row revision reverted to proofRev here and the completion was accepted.
    expect(st.obligationRev(P)).toBeGreaterThan(proofRev);
    expect(st.completeErasure(P, 1, P, proofRev, { prefix: P, holder: "eraser:old", gen: oldGen })).toBe(false);
    expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 10).map((e) => e.key)).toEqual([P]);
  });

  test("the queue identity must still exist: a completion for an already-removed item is refused", async () => {
    const { st, enq } = await fresh();
    const rev = st.obligationRev(PREFIX);
    st.erasureDone(PREFIX, enq);
    expect(st.completeErasure(PREFIX, enq, PREFIX, rev)).toBe(false);
  });

  test("pure revision ABA (no lease change): dispatch then refuse leaves no active row, yet the old revision is stale", async () => {
    const { st, enq } = await fresh();
    expect(st.acquireLease(PREFIX, "eraser:1")).toBe(true);
    const eraserLease = { prefix: PREFIX, holder: "eraser:1", gen: st.leaseGen(PREFIX)! };
    const proofRev = st.obligationRev(PREFIX);
    st.refuseWrite(st.dispatchWrite(key(2), "exporter:1"));
    expect(st.unresolvedWrites(PREFIX)).toEqual([]);
    expect(st.completeErasure(PREFIX, enq, PREFIX, proofRev, eraserLease)).toBe(false);
    // With the CURRENT revision and the held lease, the claim succeeds.
    expect(st.completeErasure(PREFIX, enq, PREFIX, st.obligationRev(PREFIX), eraserLease)).toBe(true);
  });

  test("a lease taken over before completion refuses it, even at an unchanged revision; a re-acquired lease has a NEW generation", async () => {
    const { st, enq } = await fresh();
    expect(st.acquireLease(PREFIX, "eraser:1", undefined, Date.parse("2026-11-06T00:00:00Z"))).toBe(true);
    const eraserLease = { prefix: PREFIX, holder: "eraser:1", gen: st.leaseGen(PREFIX)! };
    const rev = st.obligationRev(PREFIX);
    expect(st.acquireLease(PREFIX, "writer:new")).toBe(true); // expired: taken over
    expect(st.completeErasure(PREFIX, enq, PREFIX, rev, eraserLease)).toBe(false);
    st.releaseLeases("writer:new", [PREFIX]);
    expect(st.acquireLease(PREFIX, "eraser:1")).toBe(true); // the same holder again: a NEW generation
    expect(st.leaseGen(PREFIX)).not.toBe(eraserLease.gen);
    expect(st.completeErasure(PREFIX, enq, PREFIX, rev, eraserLease)).toBe(false);
    expect(st.retireReservationsPage(PREFIX, "S", undefined, () => true, 1000, Number.MAX_SAFE_INTEGER, "p", rev, eraserLease)).toMatchObject({ ok: false });
  });
});
