/**
 * `neon-s3-erasure --stale-prefixes-only`: one pass over the queued stale session prefixes (kind 'prefix' AND reason
 * 'stale_prefix'), erasing one only when the old prefix holds no current key and a current, date-moved copy of the
 * same session has a live manifest. No sweep, no erasure_seen, nothing else in the queue touched; --max, stop at the
 * first failed erase, a read-only dry run, the session-prefix lease. Real SyncState on a temp file (S3X_OBLIGATIONS
 * off, the only mode it runs in) and the offline version provider.
 */
import { afterAll as __modeAfterAll, beforeEach as __modeBeforeEach } from "bun:test";
import { describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../shared/vendor.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { storeEraser, type LiveObjectProbe } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, sessionPrefixParts } from "../neon-s3-erasure/job.ts";
import { staleprefixOptions } from "../neon-s3-erasure/main.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { FORBIDDEN_CREDENTIAL_ENV } from "../stdin-credentials.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

// The mode runs only with S3X_OBLIGATIONS off: pinned per module, re-applied before every test, restored after.
const OBLIGATIONS_MODE = "off";
const __modeBefore = process.env.S3X_OBLIGATIONS;
process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE;
__modeBeforeEach(() => void (process.env.S3X_OBLIGATIONS = OBLIGATIONS_MODE));
__modeAfterAll(() => { if (__modeBefore === undefined) delete process.env.S3X_OBLIGATIONS; else process.env.S3X_OBLIGATIONS = __modeBefore; });

const ORG = "org-1";
const OLD = Date.parse("2026-11-01T00:00:00Z");
const NOW = Date.parse("2026-11-07T00:00:00Z");
const base = (ws = "ws-1", m = "m-1", h = "claude", org = ORG) => `96bulls/${org}/v1/ws=${ws}/m=${m}/h=${h}`;
const pfx = (d: string, s = "sess-1", ws?: string, m?: string, h?: string, org?: string) => `${base(ws, m, h, org)}/d=${d}/s=${s}/`;
const OLD_P = pfx("2026/11/01");
const NEW_P = pfx("2026/11/02");

type Logged = Record<string, unknown>;
function authority(calls: string[] = []): ErasureAuthority {
  const forbidden = (name: string) => async () => { calls.push(name); throw new Error(`${name} must not be called by the stale-prefix mode`); };
  return {
    kind: "neon",
    ready: async () => void calls.push("ready"),
    beginSweep: () => void calls.push("beginSweep"),
    lockWaiters: forbidden("lockWaiters") as ErasureAuthority["lockWaiters"],
    fencedGroups: forbidden("fencedGroups") as ErasureAuthority["fencedGroups"],
    fencedLivePage: forbidden("fencedLivePage") as ErasureAuthority["fencedLivePage"],
    stillFenced: forbidden("stillFenced") as ErasureAuthority["stillFenced"],
    presence: new Proxy({}, { get: (_t, k) => forbidden(`presence.${String(k)}`) }) as ErasureAuthority["presence"],
  };
}

/** Every table, every row (leases included), order-independent. */
function dump(path: string): Record<string, string[]> {
  const db = new DatabaseSync(path);
  try {
    const out: Record<string, string[]> = {};
    for (const { name } of db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as { name: string }[])
      out[name] = (db.prepare(`SELECT * FROM "${name}"`).all() as object[]).map((r) => JSON.stringify(r)).sort();
    return out;
  } finally {
    db.close();
  }
}
const rowsOf = (path: string, table: string) => dump(path)[table]!;
const erasureRow = (st: SyncState, key: string) => st.db.prepare("SELECT * FROM erasures WHERE key = ?").get(key) as Record<string, unknown> | undefined;

/** A session copy in the bucket: manifest plus two chunks. */
function seedCopy(s: FakeS3Versions, prefix: string) {
  s.put(`${prefix}manifest.json`, OLD);
  s.put(`${prefix}raw/c-0001.ndjson.zst`, OLD);
  s.put(`${prefix}raw/c-0002.ndjson.zst`, OLD);
}

async function fixture() {
  const path = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-")), "s.sqlite");
  const st = new SyncState(path, { obligations: false });
  const s = new FakeS3Versions();
  const logs: Logged[] = [];
  const calls: string[] = [];
  const job = (now = NOW) => new ErasureJob(authority(calls), st, s.eraser(), (f) => void logs.push(f as Logged), DEFAULT_ERASURE_CONFIG, () => now, async () => {});
  /** The canonical date move: the group now exports NEW_P; OLD_P is queued stale; both copies are in the bucket. */
  const dateMove = (session = "sess-1", group = `g-${session}`, at = NOW - 60_000) => {
    const oldP = pfx("2026/11/01", session), newP = pfx("2026/11/02", session);
    seedCopy(s, oldP);
    seedCopy(s, newP);
    st.markExported(ORG, group, 1, [`${newP}manifest.json`], at);
    st.enqueueErasure([{ key: oldP, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], at);
    return { oldP, newP };
  };
  return { path, st, s, logs, calls, job, dateMove };
}
const passLine = (logs: Logged[]) => logs.filter((l) => l.message === "stale prefix pass").at(-1)!;

describe("stale-prefixes-only mode", () => {
  test("selection: only kind prefix AND reason stale_prefix; every other queued entry's full row and objects stay exactly as they were", async () => {
    const f = await fixture();
    const { oldP, newP } = f.dateMove();
    // Others, each of which WOULD pass the guard or be erased by a drain: a date-moved prefix queued for another
    // reason, an object queued as stale_prefix, an object and a prefix with other reasons.
    const fenced = f.dateMove("sess-2");
    f.st.db.prepare("UPDATE erasures SET reason = 'fenced' WHERE key = ?").run(fenced.oldP);
    const objKey = `${pfx("2026/11/01", "sess-3")}raw/c-0001.ndjson.zst`;
    f.s.put(objKey, OLD);
    f.st.enqueueErasure([
      { key: objKey, kind: "object", org_id: ORG, reason: "stale_prefix" },
      { key: `${pfx("2026/11/01", "sess-4")}raw/c-0009.ndjson.zst`, kind: "object", org_id: ORG, reason: "unreferenced" },
      { key: pfx("2026/11/01", "sess-5"), kind: "prefix", org_id: ORG, reason: "gone" },
    ], NOW - 30_000);
    const others = () => rowsOf(f.path, "erasures").filter((r) => !r.includes(JSON.stringify(oldP)));
    const before = others();
    const objectsBefore = f.s.count() - f.s.count(oldP);
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, selected: 1, due: 1, erased: 1, otherEntries: 4, skipped: {} });
    expect(others()).toEqual(before);
    expect(before.length).toBe(4);
    expect(f.s.count(oldP)).toBe(0);
    expect(f.s.count()).toBe(objectsBefore);
    expect(f.s.count(newP)).toBe(3);
    expect(f.s.deletes.every((d) => d.startsWith(oldP))).toBe(true);
    expect(f.st.staleprefixErasures().map((e) => e.key)).toEqual([]);
  });

  test("happy path: only the old prefix is erased (every version), the row is done, the live copy and its pointer stay", async () => {
    const f = await fixture();
    const { oldP, newP } = f.dateMove();
    f.s.put(`${oldP}manifest.json`, OLD + 1000); // two versions of the old manifest: both go
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, eligible: 1, erased: 1, versionsDeleted: 4, siblingChecked: true });
    expect(f.s.count(oldP)).toBe(0);
    expect(f.s.count(newP)).toBe(3);
    expect(erasureRow(f.st, oldP)).toBeUndefined();
    expect(rowsOf(f.path, "leases")).toEqual([]);
    expect(rowsOf(f.path, "exported")).toHaveLength(1);
    const line = passLine(f.logs);
    expect(line).toMatchObject({ execute: true, ok: true, selected: 1, eligible: 1, erased: 1, sibling_checked: true });
    // Logs carry counts and key hashes only: no key, org or session text anywhere.
    const text = JSON.stringify(f.logs);
    expect(text).not.toContain(ORG);
    expect(text).not.toContain("sess-1");
    expect(f.logs.find((l) => l.message === "stale prefix erased")?.key_sha256).toMatch(/^[0-9a-f]{64}$/);
  });

  test("no sweep of any kind: no authority sweep reads, no erasure_seen rows, no sweep bookkeeping, no stale-lease release", async () => {
    const f = await fixture();
    f.dateMove();
    // Another eraser's live lease on an unrelated prefix: a default run() would release it; the mode never does.
    f.st.acquireLease(pfx("2026/10/01", "sess-9"), "eraser:424242", undefined, NOW - 1000);
    const metaBefore = rowsOf(f.path, "meta");
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r.erased).toBe(1);
    expect(f.calls.filter((c) => c !== "ready")).toEqual([]);
    expect(f.calls.length).toBeGreaterThan(0); // the per-page authority readiness gate still ran
    expect(rowsOf(f.path, "erasure_seen")).toEqual([]);
    expect(rowsOf(f.path, "meta")).toEqual(metaBefore);
    expect(rowsOf(f.path, "leases")).toEqual([JSON.stringify({ prefix: pfx("2026/10/01", "sess-9"), holder: "eraser:424242", at: NOW - 1000 })]);
  });

  test("guard (a): an old prefix that still holds a current exported or pending key is skipped current_key, untouched", async () => {
    for (const where of ["exported", "pending"] as const) {
      const f = await fixture();
      const { oldP } = f.dateMove();
      if (where === "exported") f.st.markExported(ORG, "g-other", 1, [`${oldP}manifest.json`], NOW - 1000);
      else f.st.recordPendingKey(ORG, "g-other", `${oldP}raw/c-0003.ndjson.zst`);
      const before = dump(f.path);
      const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
      expect(r).toMatchObject({ ok: true, erased: 0, skipped: { current_key: 1 } });
      expect(f.s.deletes).toEqual([]);
      expect(dump(f.path)).toEqual(before);
    }
  });

  const refusals: [string, (f: Awaited<ReturnType<typeof fixture>>, oldP: string, newP: string) => void | LiveObjectProbe][] = [
    ["no_live_sibling", (f) => void f.st.markExported(ORG, "g-sess-1", 2, [`${pfx("2026/11/02", "sess-other")}manifest.json`], NOW - 1000)],
    // A machine move (same session, same date or a new date, another machine) is never a date move.
    ["not_date_move", (f) => void f.st.markExported(ORG, "g-sess-1", 2, [`${pfx("2026/11/02", "sess-1", "ws-1", "m-2")}manifest.json`], NOW - 1000)],
    ["not_date_move", (f) => void f.st.markExported(ORG, "g-sess-1", 2, [`${pfx("2026/11/01", "sess-1", "ws-1", "m-2")}manifest.json`], NOW - 1000)],
    ["not_date_move", (f) => void f.st.markExported(ORG, "g-sess-1", 2, [`${pfx("2026/11/02", "sess-1", "ws-2")}manifest.json`], NOW - 1000)],
    ["sibling_manifest_missing", (f, _o, newP) => void f.s.keys.delete(`${newP}manifest.json`)],
    ["sibling_manifest_missing", (f, _o, newP) => void f.s.keys.set(`${newP}manifest.json`, [{ id: "vdm", at: OLD + 5, marker: true }, ...f.s.keys.get(`${newP}manifest.json`)!])],
    ["sibling_export_failed", (f, _o, newP) => void f.st.markFailed(ORG, "g-sess-1", "put_failed", [`${newP}manifest.json`], 2, NOW - 1000)],
    ["sibling_check_failed", () => ({ objectLive: async () => { throw new Error("store unavailable"); } })],
  ];
  for (const [reason, arrange] of refusals)
    test(`guard (b): ${reason} skips the entry and writes nothing (case ${refusals.filter((x) => x[0] === reason).findIndex((x) => x[1] === arrange) + 1})`, async () => {
      const f = await fixture();
      const { oldP, newP } = f.dateMove();
      const probe = arrange(f, oldP, newP) ?? f.s.eraser();
      const before = dump(f.path);
      const oldCount = f.s.count(oldP);
      const r = await f.job().staleprefixPass({ execute: true, probe });
      expect(r).toMatchObject({ ok: true, erased: 0, skipped: { [reason]: 1 } });
      expect(f.s.deletes).toEqual([]);
      expect(f.s.count(oldP)).toBe(oldCount);
      expect(dump(f.path)).toEqual(before);
    });

  test("fenced session's former copy: no exported row, or an exported row with keys [], is skipped no_live_sibling in dry run and execute, no eraser call, row byte-identical", async () => {
    for (const shape of ["no exported row", "exported row with keys []"] as const)
      for (const execute of [false, true]) {
        const f = await fixture();
        // What the exporter does when a deletion fence lands on an exported session: exports nothing, queues the
        // session's earlier prefix as stale_prefix (the date-move loop) and markExported with keys [].
        seedCopy(f.s, OLD_P);
        f.st.markExported(ORG, "g-fenced", 1, [`${OLD_P}manifest.json`], NOW - 120_000);
        if (shape === "exported row with keys []") f.st.db.prepare("INSERT INTO groups (org_id, session_id) VALUES (?, ?)").run(ORG, "g-fenced");
        f.st.enqueueErasure([{ key: OLD_P, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 60_000);
        f.st.markExported(ORG, "g-fenced", 2, [], NOW - 60_000);
        const exported = rowsOf(f.path, "exported");
        expect(exported).toHaveLength(shape === "no exported row" ? 0 : 1);
        if (exported.length) expect(JSON.parse(exported[0]!).keys).toBe("[]");
        const row = JSON.stringify(erasureRow(f.st, OLD_P));
        const before = dump(f.path);
        let probed = 0;
        const probe: LiveObjectProbe = { objectLive: async () => { probed += 1; return true; } };
        const r = await f.job().staleprefixPass({ execute, ...(execute ? { probe } : {}) });
        expect(r).toMatchObject({ ok: true, eligible: 0, erased: 0, skipped: { no_live_sibling: 1 } });
        expect([f.s.lists, f.s.deletes.length, probed]).toEqual([0, 0, 0]);
        expect(f.s.count(OLD_P)).toBe(3);
        expect(JSON.stringify(erasureRow(f.st, OLD_P))).toBe(row);
        expect(dump(f.path)).toEqual(before);
      }
  });

  test("shape_invalid: a stale_prefix entry outside a session prefix of its own org is skipped, never erased", async () => {
    const f = await fixture();
    const foreign = pfx("2026/11/01", "sess-1", "ws-1", "m-1", "claude", "org-2");
    seedCopy(f.s, foreign);
    f.st.enqueueErasure([
      { key: foreign, kind: "prefix", org_id: ORG, reason: "stale_prefix" },
      { key: `${base()}/d=2026/11/01/`, kind: "prefix", org_id: ORG, reason: "stale_prefix" },
    ], NOW - 1000);
    const before = dump(f.path);
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 0, skipped: { shape_invalid: 2 } });
    expect(f.s.deletes).toEqual([]);
    expect(dump(f.path)).toEqual(before);
    expect(sessionPrefixParts("96bulls", ORG, OLD_P)).toEqual({ ws: "ws-1", m: "m-1", h: "claude", d: "2026/11/01", s: "sess-1" });
    for (const bad of [`${base()}/d=2026/11/01/s=x/raw/`, `${base()}/../d=2026/11/01/s=x/`, `${base()}/d=2026/11/1/s=x/`, pfx("2026/11/01", "x", "ws-1", "m-1", "claude", "org-2")])
      expect(sessionPrefixParts("96bulls", ORG, bad)).toBeUndefined();
  });

  test("--max n: stops after n successful erases; the rest stay queued and in the bucket", async () => {
    const f = await fixture();
    const copies = ["sess-1", "sess-2", "sess-3"].map((s, i) => f.dateMove(s, `g-${s}`, NOW - 60_000 + i));
    const r = await f.job().staleprefixPass({ execute: true, max: 2, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 2, stoppedBy: "max" });
    expect(copies.map((c) => f.s.count(c.oldP))).toEqual([0, 0, 3]);
    expect(f.st.staleprefixErasures().map((e) => e.key)).toEqual([copies[2]!.oldP]);
    expect(erasureRow(f.st, copies[2]!.oldP)).toMatchObject({ attempts: 0, error: null, next_try: 0 });
    await expect(f.job().staleprefixPass({ execute: true, max: 0, probe: f.s.eraser() })).rejects.toMatchObject({ code: "stale_prefix_mode_max_invalid" });
  });

  test("stops at the first failed erase: recorded as drain records it, later entries untouched, not ok", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.s.onDelete = (k) => (k.startsWith(a.oldP) ? "lose" : undefined);
    const bRow = JSON.stringify(erasureRow(f.st, b.oldP));
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r.ok).toBe(false);
    expect(r.erased).toBe(0);
    expect(r.stoppedBy).toBe("failed");
    const aRow = erasureRow(f.st, a.oldP)!;
    expect(aRow.error).toBeTruthy();
    expect(aRow.attempts).toBe(1);
    expect(Number(aRow.next_try)).toBeGreaterThan(NOW);
    expect(JSON.stringify(erasureRow(f.st, b.oldP))).toBe(bRow);
    expect(f.s.count(b.oldP)).toBe(3);
    expect(f.s.deletes.every((d) => d.startsWith(a.oldP))).toBe(true);
    expect(rowsOf(f.path, "leases")).toEqual([]);
    expect(f.logs.some((l) => l.message === "erasure failed")).toBe(true);
    expect(f.logs.find((l) => l.message === "stale prefix pass stopped")).toMatchObject({ outcome: "failed" });
  });

  test("dry run: no delete, no lease, no queue write (full table dumps identical); counts eligible and skips, sibling_checked false", async () => {
    const f = await fixture();
    f.dateMove("sess-1");
    const moved = f.dateMove("sess-2");
    f.st.markExported(ORG, "g-sess-2", 2, [`${pfx("2026/11/02", "sess-2", "ws-1", "m-2")}manifest.json`], NOW - 1000); // a machine move
    f.st.acquireLease(pfx("2026/10/01", "sess-9"), "exporter:777", undefined, NOW - 1000);
    f.st.enqueueErasure([{ key: pfx("2026/10/20", "sess-8"), kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 1000);
    f.st.erasureFailed(pfx("2026/10/20", "sess-8"), "x", NOW); // backing off
    const before = dump(f.path);
    const r = await f.job().staleprefixPass({ execute: false });
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(0);
    expect(dump(f.path)).toEqual(before);
    expect(r).toMatchObject({ ok: true, execute: false, selected: 3, due: 2, backingOff: 1, eligible: 1, erased: 0, skipped: { not_date_move: 1 }, siblingChecked: false });
    expect(passLine(f.logs)).toMatchObject({ execute: false, eligible: 1, due: 2, backing_off: 1, sibling_checked: false, skipped: { not_date_move: 1 } });
    expect(f.s.count(moved.oldP)).toBe(3);
  });

  test("lease: taken for the erase and released after; a prefix leased by another holder is skipped untouched", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.st.acquireLease(b.oldP, "exporter:31337", undefined, NOW - 1000);
    const seen: string[] = [];
    f.s.onDelete = () => void seen.push(...(f.st.db.prepare("SELECT prefix, holder FROM leases WHERE prefix = ?").all(a.oldP) as { holder: string }[]).map((l) => l.holder));
    const bRow = JSON.stringify(erasureRow(f.st, b.oldP));
    const r = await f.job().staleprefixPass({ execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 1, skipped: { lease_busy: 1 } });
    expect(new Set(seen)).toEqual(new Set([`eraser:${process.pid}`]));
    expect(rowsOf(f.path, "leases")).toEqual([JSON.stringify({ prefix: b.oldP, holder: "exporter:31337", at: NOW - 1000 })]);
    expect(JSON.stringify(erasureRow(f.st, b.oldP))).toBe(bRow);
    expect(f.s.count(b.oldP)).toBe(3);
  });

  test("refusals in the job: S3X_OBLIGATIONS on, and --execute without an existence check", async () => {
    const f = await fixture();
    await expect(f.job().staleprefixPass({ execute: true })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_existence_check" });
    const path = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-on-")), "s.sqlite");
    const on = new SyncState(path, { obligations: true });
    const job = new ErasureJob(authority(), on, f.s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => NOW, async () => {});
    await expect(job.staleprefixPass({ execute: false })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_obligations_off" });
    on.close();
  });

  test("default path unchanged: drain still erases a queued stale prefix without the guard and counts it", async () => {
    const f = await fixture();
    const { oldP } = f.dateMove();
    f.s.keys.delete(`${pfx("2026/11/02")}manifest.json`); // the mode would refuse (sibling_manifest_missing)
    expect(await f.job().drain()).toBe(1);
    expect(f.s.count(oldP)).toBe(0);
    expect(erasureRow(f.st, oldP)).toBeUndefined();
    expect(f.logs.some((l) => String(l.message).startsWith("stale prefix"))).toBe(false);
  });
});

describe("existence check (objectLive)", () => {
  test("S3 eraser: live latest version true; absent, delete-marker latest, or another key false; a non-object key refused", async () => {
    const s = new FakeS3Versions();
    const e = s.eraser();
    s.put(`${NEW_P}manifest.json`, OLD);
    s.put(`${NEW_P}manifest.json.bak`, OLD);
    s.put(`${OLD_P}manifest.json`, OLD);
    s.keys.set(`${OLD_P}manifest.json`, [{ id: "vdm", at: OLD + 1, marker: true }, ...s.keys.get(`${OLD_P}manifest.json`)!]);
    expect(await e.objectLive(`${NEW_P}manifest.json`)).toBe(true);
    expect(await e.objectLive(`${OLD_P}manifest.json`)).toBe(false);
    expect(await e.objectLive(`${pfx("2026/11/03")}manifest.json`)).toBe(false);
    s.keys.delete(`${NEW_P}manifest.json`);
    expect(await e.objectLive(`${NEW_P}manifest.json`)).toBe(false); // the .bak sibling is not the key
    for (const bad of ["", NEW_P, `${NEW_P}../manifest.json`])
      await expect(e.objectLive(bad)).rejects.toMatchObject({ code: "erasure_key_shape" });
    expect(s.deletes).toEqual([]);
  });

  test("store eraser: head-based existence", async () => {
    const root = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-fs-"));
    const store = createFsStore({ root });
    await store.put(objectKey(`${NEW_P}manifest.json`), new TextEncoder().encode("{}"), { contentType: "application/json" });
    const e = storeEraser(store);
    expect(await e.objectLive(`${NEW_P}manifest.json`)).toBe(true);
    expect(await e.objectLive(`${OLD_P}manifest.json`)).toBe(false);
    await expect(e.objectLive(NEW_P)).rejects.toMatchObject({ code: "erasure_key_shape" });
  });
});

describe("CLI options", () => {
  const code = (f: () => unknown) => { try { f(); return null; } catch (e) { return (e as { code?: string }).code ?? "uncoded"; } };
  const env = (vars: Record<string, string> = {}) => (k: string) => vars[k];

  test("absent switch: undefined (default job); --max without it refused", () => {
    expect(staleprefixOptions([], env())).toBeUndefined();
    expect(staleprefixOptions(["--execute", "--once"], env())).toBeUndefined();
    expect(code(() => staleprefixOptions(["--execute", "--max", "3"], env()))).toBe("stale_prefix_max_without_mode");
    expect(code(() => staleprefixOptions(["--max=3"], env()))).toBe("stale_prefix_max_without_mode");
  });

  test("accepted forms", () => {
    expect(staleprefixOptions(["--stale-prefixes-only"], env())).toEqual({ execute: false });
    expect(staleprefixOptions(["--stale-prefixes-only", "--execute", "--once", "--max", "25"], env({ BABYSITTER_RETENTION_SWEEP: " OFF " }))).toEqual({ execute: true, max: 25 });
  });

  test("--max must be one positive decimal integer", () => {
    for (const v of ["0", "-1", "1.5", "1e3", "0x10", " 3", "03", "", "abc", "9007199254740993"])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max", v], env()))).toBe("stale_prefix_max_invalid");
    expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max"], env()))).toBe("stale_prefix_max_invalid");
    expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max", "2", "--max", "3"], env()))).toBe("stale_prefix_max_invalid");
  });

  test("contradicting or unknown flags refused", () => {
    for (const extra of [["--max=3"], ["--loop"], ["--dry-run"], ["--stale-prefixes-only"], ["extra"]])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only", ...extra], env()))).toBe("stale_prefix_mode_flag_refused");
  });

  test("retention phase and obligations refused", () => {
    for (const v of ["on", "dry_run", "bogus"])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only"], env({ BABYSITTER_RETENTION_SWEEP: v })))).toBe("stale_prefix_mode_retention_refused");
    expect(staleprefixOptions(["--stale-prefixes-only"], env({ BABYSITTER_RETENTION_SWEEP: "off" }))).toEqual({ execute: false });
    expect(code(() => staleprefixOptions(["--stale-prefixes-only"], env({ S3X_OBLIGATIONS: "on" })))).toBe("stale_prefix_mode_requires_obligations_off");
  });

  test("the CLI refuses before reading stdin, exits 1 and logs only the code", async () => {
    const main = new URL("../neon-s3-erasure/main.ts", import.meta.url).pathname;
    const run = (args: string[], extra: Record<string, string> = {}) =>
      new Promise<{ lines: Record<string, unknown>[]; code: number | null }>(async (resolve, reject) => {
        const home = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-cli-"));
        const e: Record<string, string | undefined> = { ...process.env, HOME: home, S3X_STATE: join(home, "s.sqlite"), ...extra };
        for (const k of FORBIDDEN_CREDENTIAL_ENV) delete e[k];
        delete e.BABYSITTER_RETENTION_SWEEP;
        Object.assign(e, extra);
        const child = spawn(process.execPath, [main, ...args], { stdio: ["pipe", "pipe", "inherit"], env: e, cwd: home });
        let text = "";
        child.stdout.on("data", (b) => (text += b));
        child.on("error", reject);
        child.on("exit", (c) => resolve({ lines: text.trim().split("\n").filter(Boolean).map((l) => JSON.parse(l)), code: c }));
        child.stdin.end("{}");
      });
    for (const [args, extra, want] of [
      [["--stale-prefixes-only", "--max", "0"], {}, "stale_prefix_max_invalid"],
      [["--execute", "--max", "5"], {}, "stale_prefix_max_without_mode"],
      [["--stale-prefixes-only", "--execute"], { BABYSITTER_RETENTION_SWEEP: "on" }, "stale_prefix_mode_retention_refused"],
    ] as [string[], Record<string, string>, string][]) {
      const r = await run(args, extra);
      expect(r.code).toBe(1);
      expect(r.lines.map((l) => [l.message, l.error_code])).toEqual([["exit on error", want]]);
    }
  });
});
