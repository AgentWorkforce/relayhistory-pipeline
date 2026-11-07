/**
 * #309 M-C rev 2: ON uses a FRESH state only (a pointer + a fixed sibling database), bootstrapped durably before any
 * external effect; existing SQLite / used state is never converted; leftovers are quarantined; the pre-obligation code
 * (main 341dd5a9, frozen fixture + its real entrypoint when the commit is available) refuses at open with no send.
 * Filesystem guarantees are stated for the tested local storage only (not network filesystems).
 */
import { describe, expect, test } from "bun:test";
import { chmodSync, existsSync, linkSync, readFileSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { SyncState } from "../neon-s3-sync/state.ts";
import { pointerBytes } from "../neon-s3-sync/state-path.ts";

const tmp = async () => join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-sp-")), "s.sqlite");
const code = (f: () => unknown) => { try { f(); return null; } catch (e) { return (e as { code?: string }).code ?? String((e as Error).message); } };
const bytes = (p: string) => readFileSync(p);
const FROZEN = new URL("./fixtures/frozen-341dd5a9/", import.meta.url).pathname;

describe("fresh ON bootstrap", () => {
  test("an ABSENT path: sibling (0600) + canonical pointer (0600); the commit is durable (checkpointed + closed before publish); reopen matches state_id", async () => {
    const path = await tmp();
    const st = new SyncState(path, { obligations: true });
    st.dispatchWrite("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/raw/a.zst", "exporter:1");
    st.close();
    const sib = `${path}.obligations.sqlite`;
    expect(statSync(path).mode & 0o777).toBe(0o600);
    expect(statSync(sib).mode & 0o777).toBe(0o600);
    const ptr = JSON.parse(readFileSync(path, "utf8"));
    expect(readFileSync(path, "utf8")).toBe(pointerBytes(path, ptr.state_id));
    expect(existsSync(`${path}.pointer.tmp`)).toBe(false);
    const db = new DatabaseSync(sib, { readOnly: true });
    expect((db.prepare("SELECT v FROM meta WHERE k = 'obligations_state_id'").get() as { v: string }).v).toBe(ptr.state_id);
    db.close();
    const again = new SyncState(path, { obligations: true });
    expect(again.unresolvedWrites("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/").length).toBe(1);
    again.close();
  });
});

describe("existing state is never converted (both already-open orders)", () => {
  for (const order of ["pre-pragma (opened, not yet attached)", "attached (pragma run, idle)"] as const)
    test(`an existing SQLite file with another handle ${order}: ON refuses; the file, sidecars and that handle are unchanged`, async () => {
      const path = await tmp();
      new SyncState(path, { obligations: false }).close();
      const before = bytes(path);
      const A = new DatabaseSync(path);
      if (order.startsWith("attached")) A.exec("PRAGMA journal_mode = WAL");
      expect(code(() => new SyncState(path, { obligations: true }))).toBe("obligations_requires_fresh_state");
      expect(bytes(path).equals(before)).toBe(true);
      expect(existsSync(`${path}.obligations.sqlite`)).toBe(false);
      A.exec("PRAGMA journal_mode = WAL");
      expect((A.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name = 'groups'").get() as { n: number }).n).toBe(1);
      A.close();
    });
});

describe("strict pointer validation and quarantine (state preserved)", () => {
  async function onState() {
    const path = await tmp();
    new SyncState(path, { obligations: true }).close();
    return { path, sib: `${path}.obligations.sqlite`, ptr: readFileSync(path, "utf8") };
  }
  const id = (p: string) => JSON.parse(readFileSync(p, "utf8")).state_id as string;
  for (const [name, mutate, want] of [
    ["duplicate key", (p: string) => writeFileSync(p, readFileSync(p, "utf8").replace('"v":1,', '"v":1,"v":1,'), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["extra key", (p: string) => writeFileSync(p, readFileSync(p, "utf8").replace('"v":1,', '"v":1,"x":1,'), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["wrong version", (p: string) => writeFileSync(p, readFileSync(p, "utf8").replace('"v":1', '"v":2'), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["oversized", (p: string) => writeFileSync(p, readFileSync(p, "utf8") + " ".repeat(2048), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["absolute db", (p: string) => writeFileSync(p, readFileSync(p, "utf8").replace(/"db":"[^"]+"/, '"db":"/etc/passwd"'), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["dot-dot db", (p: string) => writeFileSync(p, readFileSync(p, "utf8").replace(/"db":"[^"]+"/, '"db":"../s.sqlite.obligations.sqlite"'), { mode: 0o600 }), "obligations_pointer_invalid"],
    ["group-readable pointer", (p: string) => chmodSync(p, 0o640), "obligations_state_conflict"],
    ["state_id mismatch", (p: string) => writeFileSync(p, pointerBytes(p, "00000000-0000-4000-8000-000000000000"), { mode: 0o600 }), "obligations_state_conflict"],
  ] as const)
    test(`a ${name} refuses (${want}); the sibling is untouched`, async () => {
      const s = await onState();
      const sib = bytes(s.sib);
      mutate(s.path);
      expect(code(() => new SyncState(s.path, { obligations: true }))).toBe(want);
      expect(bytes(s.sib).equals(sib)).toBe(true);
    });

  test("a symlinked pointer, a symlinked or group-readable sibling, and a stray pointer temp file all refuse", async () => {
    const a = await onState();
    const real = `${a.path}.real`;
    linkSync(a.path, real);
    await rm(a.path);
    symlinkSync(real, a.path);
    expect(code(() => new SyncState(a.path, { obligations: true }))).toBe("obligations_state_conflict");
    const b = await onState();
    chmodSync(b.sib, 0o644);
    expect(code(() => new SyncState(b.path, { obligations: true }))).toBe("obligations_state_conflict");
    const c = await onState();
    writeFileSync(`${c.path}.pointer.tmp`, c.ptr, { mode: 0o600 });
    expect(code(() => new SyncState(c.path, { obligations: true }))).toBe("obligations_state_conflict");
    expect(id(c.path)).toBe(JSON.parse(c.ptr).state_id);
  });

  test("crash leftovers without a published pointer (sibling only; temp only) are QUARANTINED, never adopted or overwritten", async () => {
    const a = await tmp();
    writeFileSync(`${a}.obligations.sqlite`, "", { mode: 0o600 });
    expect(code(() => new SyncState(a, { obligations: true }))).toBe("obligations_state_conflict");
    expect(existsSync(a)).toBe(false);
    const b = await tmp();
    writeFileSync(`${b}.pointer.tmp`, "x", { mode: 0o600 });
    expect(code(() => new SyncState(b, { obligations: true }))).toBe("obligations_state_conflict");
    expect(readFileSync(`${b}.pointer.tmp`, "utf8")).toBe("x");
  });

  test("OFF refuses a pointer (obligations_state_in_use); the pointer bytes are unchanged", async () => {
    const s = await onState();
    expect(code(() => new SyncState(s.path, { obligations: false }))).toBe("obligations_state_in_use");
    expect(readFileSync(s.path, "utf8")).toBe(s.ptr);
  });
});

describe("the pre-obligation code (main 341dd5a9) refuses an ON state at open, in an INDEPENDENT process, with no send", () => {
  test("the frozen fixture is 341dd5a9's state.ts byte for byte", () => {
    expect(createHash("sha256").update(readFileSync(join(FROZEN, "state.ts"))).digest("hex")).toBe("73dc74e527ca5b6059536f2033026dc5acbbf54054fc53fab50cda3ed4c9f30f");
  });

  test("frozen SyncState in another process: 'file is not a database'; pointer + sibling bytes unchanged", async () => {
    const path = await tmp();
    new SyncState(path, { obligations: true }).close();
    const ptr = bytes(path), sib = bytes(`${path}.obligations.sqlite`);
    const { spawnSync } = await import("node:child_process");
    const run = spawnSync(process.execPath, [join(FROZEN, "open-runner.ts"), path], { encoding: "utf8" });
    expect(run.status).toBe(1);
    const r = JSON.parse(run.stdout.trim().split("\n").at(-1)!);
    expect(r.opened).toBe(false);
    expect(String(r.message)).toMatch(/not a database/i);
    expect(bytes(path).equals(ptr)).toBe(true);
    expect(bytes(`${path}.obligations.sqlite`).equals(sib)).toBe(true);
  }, 30_000);

  // The frozen ENTRYPOINTS (verbatim 341dd5a9 sources, .txt so they are never executed or type-checked): both
  // construct SyncState BEFORE any database connection, S3 client call, sweep or run. With SyncState refusing at open
  // (above), neither can reach an external effect against an ON state. (A full frozen-entrypoint execution would need
  // a build-heavy worktree with its own install; the source-order proof + the independent-process open stand in.)
  test("frozen 341dd5a9 entrypoints construct SyncState before any external effect (verbatim, pinned)", () => {
    const er = readFileSync(join(FROZEN, "eraser-main.ts.txt"), "utf8");
    const ex = readFileSync(join(FROZEN, "exporter-main.ts.txt"), "utf8");
    expect(createHash("sha256").update(er).digest("hex")).toBe("f9e67d053611e56f64ccb14fcdfb2b7771f1ce1404fe89ee8dd83d406981f7e0");
    expect(createHash("sha256").update(ex).digest("hex")).toBe("9f984d2bad807df772a2823238ffbd69436aa164ab58992a413494ec9510cc6f");
    const main = (src: string) => src.slice(src.indexOf("export async function main"));
    const at = (src: string, needle: string) => { const k = main(src).indexOf(needle); expect([needle, k >= 0]).toEqual([needle, true]); return k; };
    const e = main(er);
    expect(at(er, "new SyncState(")).toBeLessThan(at(er, "client.connect()"));
    expect(at(er, "new SyncState(")).toBeLessThan(at(er, "job.run("));
    expect(e.indexOf("eraserFor(")).toBeLessThan(e.indexOf("new SyncState(")); // constructs the client object only
    expect(at(ex, "new SyncState(")).toBeLessThan(at(ex, "c.connect()"));
    expect(at(ex, "new SyncState(")).toBeLessThan(at(ex, "syncer.run("));
    // The exporter's ONLY pre-open external calls are its READ-ONLY preflight (S3 list + head, with --execute): no PUT
    // or DELETE can precede the refusing open.
    const pre = main(ex).slice(0, main(ex).indexOf("new SyncState("));
    expect(pre).toContain("preflight(store");
    expect(pre).not.toMatch(/\.put\(|\.delete\(|enqueue|erase/);
    const pf = ex.slice(ex.indexOf("async function preflight"), ex.indexOf("async function preflight") + 600);
    expect(pf).toContain("store.list(");
    expect(pf).toContain("store.head(");
    expect(pf).not.toMatch(/\.put\(|\.delete\(/);
  });
});

describe("4f7b1670 review regressions", () => {
  test("lead P1: ON state + unknown, ONLY the pointer removed -> OFF refuses (no new DB beside the retained sibling); the sibling is unchanged", async () => {
    const path = await tmp();
    const st = new SyncState(path, { obligations: true });
    st.dispatchWrite("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/raw/a.zst", "exporter:1");
    st.close();
    const sib = bytes(`${path}.obligations.sqlite`);
    await rm(path);
    expect(code(() => new SyncState(path, { obligations: false }))).toBe("obligations_state_conflict");
    expect(existsSync(path)).toBe(false);
    expect(bytes(`${path}.obligations.sqlite`).equals(sib)).toBe(true);
    // ... and an existing legacy SQLite path with a retained sibling refuses too.
    new SyncState(join(path, "..", "other.sqlite"), { obligations: false }).close();
    writeFileSync(path, readFileSync(join(path, "..", "other.sqlite")), { mode: 0o600 });
    expect(code(() => new SyncState(path, { obligations: false }))).toBe("obligations_state_conflict");
  });

  test("lead P2: a canonical pointer to a FOREIGN 0600 SQLite (other state_id) refuses BEFORE any mutation: target bytes + tables unchanged", async () => {
    const path = await tmp();
    new SyncState(path, { obligations: true }).close();
    const sib = `${path}.obligations.sqlite`;
    await rm(sib);
    const foreign = new DatabaseSync(sib);
    foreign.exec("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT); CREATE TABLE unrelated (x); INSERT INTO meta VALUES ('obligations_state_id', '00000000-0000-4000-8000-000000000000')");
    foreign.close();
    chmodSync(sib, 0o600);
    const before = bytes(sib);
    expect(code(() => new SyncState(path, { obligations: true }))).toBe("obligations_state_conflict");
    expect(bytes(sib).equals(before)).toBe(true);
    const ro = new DatabaseSync(sib, { readOnly: true });
    expect((ro.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((r) => r.name)).toEqual(["meta", "unrelated"]);
    ro.close();
  });

  test("dogpatch P1: S3X_STATE=:memory: is refused (both modes) without the explicit test-only option", () => {
    expect(code(() => new SyncState(":memory:", { obligations: true }))).toBe("state_memory_refused");
    expect(code(() => new SyncState(":memory:", { obligations: false }))).toBe("state_memory_refused");
    new SyncState(":memory:", { obligations: true, inMemoryForTests: true }).close();
  });

  test("dogpatch P2-a: after an ON handle stamps use, an OFF handle's releaseStaleLeases / releaseLeases REFUSE (B's live lease survives)", async () => {
    const path = await tmp();
    const A = new SyncState(path, { obligations: false });
    const B = new SyncState(path, { obligations: true, direct: true });
    const P = "96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/";
    expect(B.acquireLease(P, "eraser:B")).toBe(true);
    B.dispatchWrite(`${P}raw/a.zst`, "exporter:1"); // stamps use AFTER A's admission
    expect(code(() => A.releaseStaleLeases("eraser:", "eraser:A"))).toBe("obligations_state_in_use");
    expect(code(() => A.releaseLeases("eraser:B", [P]))).toBe("obligations_state_in_use");
    expect(B.leaseGen(P)).toBeDefined();
    A.close();
    B.close();
  });

  test("dogpatch P2-a: the OFF erase path re-checks the mode AFTER the awaited authorization: use stamped during it -> no delete", async () => {
    const { FakeS3Versions } = await import("./fake-s3-versions.ts");
    const { DEFAULT_ERASURE_CONFIG, ErasureJob } = await import("../neon-s3-erasure/job.ts");
    const path = await tmp();
    const A = new SyncState(path, { obligations: false });
    const PFX = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
    A.enqueueErasure([{ key: PFX, kind: "prefix", org_id: "org-1", reason: "fenced" }], 1);
    const s = new FakeS3Versions();
    s.put(`${PFX}raw/b=000001.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
    let B: SyncState | undefined;
    const auth = { kind: "supabase" as const, lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }), fencedGroups: async () => [], fencedLivePage: async () => [], stillFenced: async () => true,
      ready: async () => { if (!B) { B = new SyncState(path, { obligations: true, direct: true }); B.dispatchWrite(`${PFX}raw/x.zst`, "exporter:1"); } } };
    const job = new ErasureJob(auth, A, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => Date.now(), async () => {});
    await job.drain().catch(() => undefined);
    expect(B).toBeDefined();
    expect(s.deletes).toEqual([]);
    B!.close();
    A.close();
  });

  test("dogpatch P2-b / lead P2: an ON dry run's own scratch tuple is fully removed; a non-scratch (foreign-id) sibling is never deleted", async () => {
    const { stateForRun } = await import("../neon-s3-sync/main.ts");
    const scratch = join(await tmp(), "..", "dry");
    const run = stateForRun(false, join(scratch, "absent.sqlite"), scratch);
    new SyncState(run.path, { obligations: true }).close();
    expect(existsSync(`${run.path}.obligations.sqlite`)).toBe(true);
    run.cleanup();
    for (const f of [run.path, `${run.path}.obligations.sqlite`, `${run.path}.obligations.sqlite-wal`, `${run.path}.obligations.sqlite-shm`, `${run.path}.pointer.tmp`]) expect(existsSync(f)).toBe(false);
    const run2 = stateForRun(false, join(scratch, "absent.sqlite"), scratch);
    new SyncState(run2.path, { obligations: true }).close();
    writeFileSync(run2.path, pointerBytes(run2.path, "00000000-0000-4000-8000-000000000000"), { mode: 0o600 }); // not this run's
    run2.cleanup();
    expect(existsSync(`${run2.path}.obligations.sqlite`)).toBe(true); // quarantined, kept
  });
});
