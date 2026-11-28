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
import { existsSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../shared/vendor.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { storeEraser, type LiveObjectProbe } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, sessionPrefixParts, STALE_RUN_LEASE, staleprefixIdentityHash, type StalePrefixPassOptions } from "../neon-s3-erasure/job.ts";
import { exitStatusForError, staleprefixOptions, stalePassStatus } from "../neon-s3-erasure/main.ts";
import { ShortLivedCredentialError } from "../short-lived-credentials.ts";
import { LEASE_TTL_MS, SyncState } from "../neon-s3-sync/state.ts";
import { FORBIDDEN_CREDENTIAL_ENV } from "../stdin-credentials.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { attestation, BUCKET_SHA, buildCopy, seedBodies } from "./stale-prefix-fixture.ts";

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

/** A session copy in the bucket: a real manifest plus two raw chunks (stale-prefix-fixture.ts), identical records per session. */
function seedCopy(s: FakeS3Versions, prefix: string) {
  seedBodies(s, prefix, OLD);
}

async function fixture() {
  const path = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-")), "s.sqlite");
  const st = new SyncState(path, { obligations: false });
  const s = new FakeS3Versions();
  const logs: Logged[] = [];
  const calls: string[] = [];
  const job = (now = NOW) => new ErasureJob(authority(calls), st, s.eraser(), (f) => void logs.push(f as Logged), DEFAULT_ERASURE_CONFIG, () => now, async () => {});
  /**
   * The pass as the CLI runs it. Dry: unchanged. Execute: the read-only verifier first (its reads and log lines are
   * then reset, so assertions see only the execute), then the receipt-fenced execute with a fresh attestation.
   */
  const exec = async (opts: StalePrefixPassOptions, j = job()) => {
    if (!opts.execute) return j.staleprefixPass(opts);
    const logged = logs.length;
    const receipt = opts.receipt ?? await j.staleprefixVerify({ onlyIdentities: opts.onlyIdentities, reader: s.reader(), bucketSha256: BUCKET_SHA });
    logs.length = logged;
    s.lists = 0;
    s.gets = [];
    return j.staleprefixPass({ receipt, fence: s.eraser(), bucketSha256: BUCKET_SHA, attestation: attestation(NOW), ...opts });
  };
  /** The canonical date move: the group now exports NEW_P; OLD_P is queued stale; both copies are in the bucket. */
  const dateMove = (session = "sess-1", group = `g-${session}`, at = NOW - 60_000) => {
    const oldP = pfx("2026/11/01", session), newP = pfx("2026/11/02", session);
    seedCopy(s, oldP);
    seedCopy(s, newP);
    st.markExported(ORG, group, 1, [`${newP}manifest.json`], at);
    st.enqueueErasure([{ key: oldP, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], at);
    return { oldP, newP };
  };
  return { path, st, s, logs, calls, job, exec, dateMove };
}
/** An identity no fixture session has: keeps an allowlist non-empty. */
const NO_ID = "0".repeat(64);
/** Every derivable identity of the queued stale prefixes (the pre-allowlist behaviour), plus NO_ID. */
const allIds = (st: SyncState) => [...new Set([...st.staleprefixErasures().map((e) => staleprefixIdentityHash("96bulls", e.org_id, e.key)).filter((h): h is string => h !== undefined), NO_ID])];
// Hand-computed offline: printf '%s' 'org-1/ws-1/m-1/claude/sess-1' | shasum -a 256 (and sess-2).
const ID_A = "8c92d2fb0f2e5f259c3268f061f94ca22eda4f91834959d5402edd1b49de0bff";
const ID_B = "6fdf52f1e97923a0a31c77efb8e9c070507b7f9926b2d1f376da1b20e92f2071";
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
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
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
    f.s.put(`${oldP}manifest.json`, OLD + 1000, undefined, buildCopy(oldP).manifest); // two versions of the old manifest: both go
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
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
    // Another eraser's expired lease on an unrelated prefix (a live one refuses execute): a default run() would
    // release it; the mode never does.
    f.st.acquireLease(pfx("2026/10/01", "sess-9"), "eraser:424242", undefined, NOW - LEASE_TTL_MS);
    const metaBefore = rowsOf(f.path, "meta");
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    expect(r.erased).toBe(1);
    expect(f.calls.filter((c) => c !== "ready")).toEqual([]);
    expect(f.calls.length).toBeGreaterThan(0); // the per-page authority readiness gate still ran
    expect(rowsOf(f.path, "erasure_seen")).toEqual([]);
    expect(rowsOf(f.path, "meta")).toEqual(metaBefore);
    expect(rowsOf(f.path, "leases")).toEqual([JSON.stringify({ prefix: pfx("2026/10/01", "sess-9"), holder: "eraser:424242", at: NOW - LEASE_TTL_MS })]);
  });

  test("guard (a): an old prefix that still holds a current exported or pending key is skipped current_key, untouched", async () => {
    for (const where of ["exported", "pending"] as const) {
      const f = await fixture();
      const { oldP } = f.dateMove();
      if (where === "exported") f.st.markExported(ORG, "g-other", 1, [`${oldP}manifest.json`], NOW - 1000);
      else f.st.recordPendingKey(ORG, "g-other", `${oldP}raw/c-0003.ndjson.zst`);
      const before = dump(f.path);
      const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
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
      const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe });
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
        const r = await f.exec({ onlyIdentities: allIds(f.st), execute, ...(execute ? { probe } : {}) });
        expect(r).toMatchObject({ ok: true, eligible: 0, erased: 0, skipped: { no_live_sibling: 1 } });
        expect([f.s.lists, f.s.deletes.length, probed]).toEqual([0, 0, 0]);
        expect(f.s.count(OLD_P)).toBe(3);
        expect(JSON.stringify(erasureRow(f.st, OLD_P))).toBe(row);
        expect(dump(f.path)).toEqual(before);
      }
  });

  test("identity_unresolved: a stale_prefix entry outside a session prefix of its own org has no identity, is skipped, never erased", async () => {
    const f = await fixture();
    const foreign = pfx("2026/11/01", "sess-1", "ws-1", "m-1", "claude", "org-2");
    seedCopy(f.s, foreign);
    f.st.enqueueErasure([
      { key: foreign, kind: "prefix", org_id: ORG, reason: "stale_prefix" },
      { key: `${base()}/d=2026/11/01/`, kind: "prefix", org_id: ORG, reason: "stale_prefix" },
    ], NOW - 1000);
    const before = dump(f.path);
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    // The allowlist check runs first; an entry whose identity cannot be derived is exactly one shape_invalid would catch.
    expect(r).toMatchObject({ ok: true, erased: 0, skipped: { identity_unresolved: 2 } });
    expect(f.s.deletes).toEqual([]);
    expect(dump(f.path)).toEqual(before);
    expect(sessionPrefixParts("96bulls", ORG, OLD_P)).toEqual({ ws: "ws-1", m: "m-1", h: "claude", d: "2026/11/01", s: "sess-1" });
    for (const bad of [`${base()}/d=2026/11/01/s=x/raw/`, `${base()}/../d=2026/11/01/s=x/`, `${base()}/d=2026/11/1/s=x/`, pfx("2026/11/01", "x", "ws-1", "m-1", "claude", "org-2")])
      expect(sessionPrefixParts("96bulls", ORG, bad)).toBeUndefined();
  });

  test("--max n: stops after n successful erases; the rest stay queued and in the bucket", async () => {
    const f = await fixture();
    const copies = ["sess-1", "sess-2", "sess-3"].map((s, i) => f.dateMove(s, `g-${s}`, NOW - 60_000 + i));
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, max: 2, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 2, stoppedBy: "max" });
    expect(copies.map((c) => f.s.count(c.oldP))).toEqual([0, 0, 3]);
    expect(f.st.staleprefixErasures().map((e) => e.key)).toEqual([copies[2]!.oldP]);
    expect(erasureRow(f.st, copies[2]!.oldP)).toMatchObject({ attempts: 0, error: null, next_try: 0 });
    await expect(f.exec({ onlyIdentities: allIds(f.st), execute: true, max: 0, probe: f.s.eraser() })).rejects.toMatchObject({ code: "stale_prefix_mode_max_invalid" });
  });

  test("stops at the first failed erase: recorded as drain records it, later entries untouched, not ok", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.s.onDelete = (k) => (k.startsWith(a.oldP) ? "lose" : undefined);
    const bRow = JSON.stringify(erasureRow(f.st, b.oldP));
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
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
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: false });
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
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 1, skipped: { lease_busy: 1 }, otherEraserLeases: 0 });
    expect(r.refused).toBeUndefined();
    expect(new Set(seen)).toEqual(new Set([`eraser:${process.pid}`]));
    expect(rowsOf(f.path, "leases")).toEqual([JSON.stringify({ prefix: b.oldP, holder: "exporter:31337", at: NOW - 1000 })]);
    expect(JSON.stringify(erasureRow(f.st, b.oldP))).toBe(bRow);
    expect(f.s.count(b.oldP)).toBe(3);
  });

  test("another eraser's unexpired lease: execute refuses to start (nothing erased, state byte-identical); dry run reports it", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    // Live by one millisecond under acquireLease's own expiry rule, on an unrelated prefix.
    f.st.acquireLease(pfx("2026/10/01", "sess-9"), "eraser:424242", undefined, NOW - LEASE_TTL_MS + 1);
    const before = dump(f.path);
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: false, refused: "stale_prefix_mode_other_eraser_lease", otherEraserLeases: 1, erased: 0, eligible: 0, versionsDeleted: 0, skipped: {} });
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(0);
    expect(dump(f.path)).toEqual(before);
    expect(f.s.count(a.oldP)).toBe(3);
    expect(f.logs.find((l) => l.message === "stale prefix pass refused")).toMatchObject({ error_code: "stale_prefix_mode_other_eraser_lease", other_eraser_leases: 1 });
    // The dry run does not refuse: it reports the count and writes nothing.
    const dry = await f.exec({ onlyIdentities: allIds(f.st), execute: false });
    expect(dry).toMatchObject({ ok: true, otherEraserLeases: 1, eligible: 1 });
    expect(dry.refused).toBeUndefined();
    expect(passLine(f.logs)).toMatchObject({ execute: false, other_eraser_leases: 1 });
    expect(dump(f.path)).toEqual(before);
  });

  test("another eraser's expired lease, and this process's own lease, do not refuse; neither lease is released", async () => {
    for (const [holder, at] of [["eraser:424242", NOW - LEASE_TTL_MS], [`eraser:${process.pid}`, NOW - 1000]] as const) {
      const f = await fixture();
      const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
      const other = pfx("2026/10/01", "sess-9");
      f.st.acquireLease(other, holder, undefined, at);
      const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
      expect(r).toMatchObject({ ok: true, erased: 1, otherEraserLeases: 0 });
      expect(r.refused).toBeUndefined();
      expect(f.s.count(a.oldP)).toBe(0);
      expect(rowsOf(f.path, "leases")).toEqual([JSON.stringify({ prefix: other, holder, at })]);
    }
  });

  test("a state error inside the guard during execute is recorded as a failure on that entry (backoff) and stops the pass; the dry run surfaces it as an error", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    const bRow = JSON.stringify(erasureRow(f.st, b.oldP));
    const receipt = await f.job().staleprefixVerify({ onlyIdentities: allIds(f.st), reader: f.s.reader(), bucketSha256: BUCKET_SHA });
    f.st.exportedKeysOf = () => { throw Object.assign(new Error("state read"), { code: "state_read_failed" }); };
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser(), receipt });
    expect(r).toMatchObject({ ok: false, stoppedBy: "failed", erased: 0 });
    const aRow = erasureRow(f.st, a.oldP)!;
    expect(aRow).toMatchObject({ error: "state_read_failed", attempts: 1 });
    expect(Number(aRow.next_try)).toBeGreaterThan(NOW);
    expect(JSON.stringify(erasureRow(f.st, b.oldP))).toBe(bRow);
    expect(f.s.deletes).toEqual([]);
    expect(rowsOf(f.path, "leases")).toEqual([]);
    await expect(f.exec({ onlyIdentities: allIds(f.st), execute: false })).rejects.toMatchObject({ code: "state_read_failed" });
  });

  test("refusals in the job: S3X_OBLIGATIONS on, and --execute without an existence check", async () => {
    const f = await fixture();
    await expect(f.exec({ onlyIdentities: allIds(f.st), execute: true })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_existence_check" });
    const path = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-on-")), "s.sqlite");
    const on = new SyncState(path, { obligations: true });
    const job = new ErasureJob(authority(), on, f.s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => NOW, async () => {});
    await expect(job.staleprefixPass({ execute: false, onlyIdentities: [NO_ID] })).rejects.toMatchObject({ code: "stale_prefix_mode_requires_obligations_off" });
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

describe("identity allowlist (--only-identity)", () => {
  /** Records every prefix the job tries to lease. */
  const leaseSpy = (st: SyncState) => {
    const leased: string[] = [];
    const orig = st.acquireLease.bind(st);
    st.acquireLease = ((prefix: string, ...rest: unknown[]) => (leased.push(prefix), (orig as (...a: unknown[]) => boolean)(prefix, ...rest))) as typeof st.acquireLease;
    return leased;
  };

  test("identity hash: sha256_hex of utf8 '<org>/<ws>/<m>/<h>/<s>' (date dropped) matches the hand-computed vector", () => {
    expect(staleprefixIdentityHash("96bulls", ORG, OLD_P)).toBe(ID_A);
    expect(staleprefixIdentityHash("96bulls", ORG, NEW_P)).toBe(ID_A); // every date of one session: one identity
    expect(staleprefixIdentityHash("96bulls", ORG, pfx("2026/11/01", "sess-2"))).toBe(ID_B);
    expect(staleprefixIdentityHash("96bulls", ORG, pfx("2026/11/01", "sess-1", "ws-1", "m-2"))).not.toBe(ID_A);
    // A key that is not a session prefix of the org's own tenant prefix has no identity.
    for (const bad of [`${OLD_P}manifest.json`, `${base()}/d=2026/11/01/`, pfx("2026/11/01", "sess-1", "ws-1", "m-1", "claude", "org-2")])
      expect(staleprefixIdentityHash("96bulls", ORG, bad)).toBeUndefined();
  });

  test("only allowlisted identities are erased (execute and dry-run counts); unlisted due entries are skipped, never leased, rows and objects untouched", async () => {
    for (const execute of [false, true]) {
      const f = await fixture();
      const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
      const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
      const c = f.dateMove("sess-3", "g-3", NOW - 40_000);
      const leased = leaseSpy(f.st);
      const rows = () => [b.oldP, c.oldP].map((k) => JSON.stringify(erasureRow(f.st, k)));
      const before = rows();
      const dumpBefore = dump(f.path);
      const r = await f.exec({ execute, onlyIdentities: [ID_A], ...(execute ? { probe: f.s.eraser() } : {}) });
      expect(r).toMatchObject({ ok: true, selected: 3, due: 3, eligible: 1, erased: execute ? 1 : 0, skipped: { not_in_identity_allowlist: 2 } });
      expect(rows()).toEqual(before);
      expect([f.s.count(b.oldP), f.s.count(c.oldP)]).toEqual([3, 3]);
      expect(f.s.count(a.oldP)).toBe(execute ? 0 : 3);
      expect(f.s.deletes.every((d) => d.startsWith(a.oldP))).toBe(true);
      // Execute: the eraser-wide run lease, the old prefix, then the survivor prefix (kept from the exporter).
      expect(leased).toEqual(execute ? [STALE_RUN_LEASE, a.oldP, a.newP] : []);
      if (!execute) expect(dump(f.path)).toEqual(dumpBefore);
      const skips = f.logs.filter((l) => l.message === "stale prefix skipped");
      expect(skips.map((l) => l.reason)).toEqual(["not_in_identity_allowlist", "not_in_identity_allowlist"]);
      expect(skips.map((l) => Object.keys(l).sort())).toEqual(skips.map(() => ["execute", "key_sha256", "message", "reason"]));
      expect(JSON.stringify(f.logs)).not.toContain("sess-");
    }
  });

  test("an unlisted entry is skipped before its lease: another holder's lease on it does not turn into lease_busy", async () => {
    const f = await fixture();
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.st.acquireLease(b.oldP, "exporter:31337", undefined, NOW - 1000);
    const leased = leaseSpy(f.st);
    const r = await f.exec({ execute: true, onlyIdentities: [ID_A], probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 0, skipped: { not_in_identity_allowlist: 1 } });
    expect(leased).toEqual([STALE_RUN_LEASE]); // only the run lease: the unlisted entry is never leased
  });

  test("one identity with several queued prefixes: every due prefix of it is eligible and erased", async () => {
    const f = await fixture();
    const { oldP, newP } = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const older = pfx("2026/10/31", "sess-1");
    seedCopy(f.s, older);
    f.st.enqueueErasure([{ key: older, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 30_000);
    f.dateMove("sess-2", "g-2", NOW - 20_000);
    const dry = await f.exec({ execute: false, onlyIdentities: [ID_A] });
    expect(dry).toMatchObject({ eligible: 2, skipped: { not_in_identity_allowlist: 1 }, allowlistMatchedIdentities: 1 });
    const r = await f.exec({ execute: true, onlyIdentities: [ID_A], probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, eligible: 2, erased: 2, skipped: { not_in_identity_allowlist: 1 }, allowlistSize: 1, allowlistMatchedIdentities: 1, allowlistUnmatched: 0 });
    expect([f.s.count(oldP), f.s.count(older), f.s.count(newP)]).toEqual([0, 0, 3]);
    expect(f.st.staleprefixErasures().map((e) => e.key)).toEqual([pfx("2026/11/01", "sess-2")]);
  });

  test("--max counts erases of allowlisted entries only; unlisted skips do not consume it", async () => {
    const f = await fixture();
    f.dateMove("sess-2", "g-2", NOW - 70_000); // unlisted, first in order
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const older = pfx("2026/10/31", "sess-1");
    seedCopy(f.s, older);
    f.st.enqueueErasure([{ key: older, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 30_000);
    const r = await f.exec({ execute: true, max: 1, onlyIdentities: [ID_A], probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 1, stoppedBy: "max", skipped: { not_in_identity_allowlist: 1 } });
    expect([f.s.count(a.oldP), f.s.count(older)]).toEqual([0, 3]);
  });

  test("allowlist counts: size, matched identities (any queued entry, backing-off included) and unmatched, in the result and the pass line", async () => {
    const f = await fixture();
    f.dateMove("sess-1", "g-1", NOW - 60_000);
    f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.st.erasureFailed(pfx("2026/11/01", "sess-2"), "x", NOW); // backing off: matched, not processed
    const r = await f.exec({ execute: true, onlyIdentities: [ID_A, ID_B, NO_ID], probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, selected: 2, due: 1, erased: 1, allowlistSize: 3, allowlistMatchedIdentities: 2, allowlistUnmatched: 1 });
    expect(passLine(f.logs)).toMatchObject({ allowlist_size: 3, allowlist_matched_identities: 2, allowlist_unmatched: 1 });
  });

  test("allowlist_eligible_identities: A with two eligible dates, B only backing off -> eligible 2 prefixes but 1 identity (B has none)", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const older = pfx("2026/10/31", "sess-1");
    seedCopy(f.s, older);
    f.st.enqueueErasure([{ key: older, kind: "prefix", org_id: ORG, reason: "stale_prefix" }], NOW - 55_000);
    const b = f.dateMove("sess-2", "g-2", NOW - 50_000);
    f.st.erasureFailed(b.oldP, "x", NOW); // B's only entry backs off
    const before = dump(f.path);
    const dry = await f.exec({ execute: false, onlyIdentities: [ID_A, ID_B] });
    expect(dry).toMatchObject({ ok: true, eligible: 2, allowlistSize: 2, allowlistMatchedIdentities: 2, allowlistUnmatched: 0, allowlistEligibleIdentities: 1 });
    expect(passLine(f.logs)).toMatchObject({ eligible: 2, allowlist_size: 2, allowlist_matched_identities: 2, allowlist_unmatched: 0, allowlist_eligible_identities: 1 });
    expect(dump(f.path)).toEqual(before);
    // Execute: eligible identities are those with an erase.
    const r = await f.exec({ execute: true, onlyIdentities: [ID_A, ID_B], probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 2, allowlistEligibleIdentities: 1 });
    expect([f.s.count(a.oldP), f.s.count(older), f.s.count(b.oldP)]).toEqual([0, 0, 3]);
    // Both identities eligible: the gate equality holds.
    const g = await fixture();
    g.dateMove("sess-1", "g-1", NOW - 60_000);
    g.dateMove("sess-2", "g-2", NOW - 50_000);
    expect(await g.exec({ execute: false, onlyIdentities: [ID_A, ID_B] })).toMatchObject({ eligible: 2, allowlistSize: 2, allowlistEligibleIdentities: 2 });
  });

  test("a sparse allowlist ([<hole>, A]) is refused at the job boundary: no queue read, no lease, no write, no erase", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const reads: string[] = [];
    for (const m of ["staleprefixErasures", "erasureBacklog", "otherEraserLeases", "exportedKeysOf", "pendingKeysOf"] as const) {
      const orig = (f.st[m] as (...x: unknown[]) => unknown).bind(f.st);
      (f.st as unknown as Record<string, unknown>)[m] = (...x: unknown[]) => (reads.push(m), orig(...x));
    }
    const leased = leaseSpy(f.st);
    const before = dump(f.path);
    const sparse: string[] = [];
    sparse[1] = ID_A;
    expect(sparse.length).toBe(2);
    expect(sparse.every((h) => /^[0-9a-f]{64}$/.test(h))).toBe(true); // what the old check relied on
    for (const ids of [sparse, [ID_A, , ID_B] as string[]])
      await expect(f.exec({ execute: true, onlyIdentities: ids, probe: f.s.eraser() })).rejects.toMatchObject({ code: "stale_prefix_mode_allowlist_invalid" });
    expect(reads).toEqual([]);
    expect(leased).toEqual([]);
    expect(f.s.deletes).toEqual([]);
    expect(f.s.lists).toBe(0);
    expect(f.s.count(a.oldP)).toBe(3);
    expect(f.logs).toEqual([]);
    expect(dump(f.path)).toEqual(before);
  });

  test("the job refuses a missing, empty, malformed, duplicated or oversized allowlist before reading the queue", async () => {
    const f = await fixture();
    f.dateMove();
    const before = dump(f.path);
    const many = Array.from({ length: 101 }, (_, i) => i.toString(16).padStart(64, "0"));
    for (const ids of [undefined, [], [ID_A.toUpperCase()], ["sess-1"], [ID_A, ID_A], many])
      await expect(f.exec({ execute: false, onlyIdentities: ids as string[] })).rejects.toMatchObject({ code: "stale_prefix_mode_allowlist_invalid" });
    expect(f.logs).toEqual([]);
    expect(dump(f.path)).toEqual(before);
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

  test("near-miss switches and --max forms without the exact switch are refused (the default job would ignore them)", () => {
    for (const args of [["--stale-prefix-only", "--execute"], ["--stale-prefixes", "--execute"], ["--stale_prefixes_only"], ["--stale-prefixes-only=1"], ["--maximum", "3"], ["--max-count=3"], ["--execute", "--stale"]])
      expect(code(() => staleprefixOptions(args, env()))).toBe("stale_prefix_mode_flag_refused");
    // Unchanged for every other default-path argument.
    expect(staleprefixOptions(["--execute", "--once", "--loop", "--dry-run", "extra"], env())).toBeUndefined();
  });

  test("wider near misses: misspellings that do not start with --stale or --max are refused, alone and with --execute", () => {
    const dash = String.fromCharCode(0x2013);
    const misses = [
      "--stal-prefixes-only", "-stale-prefixes-only", "--Stale-prefixes-only", "--STALE-PREFIXES-ONLY", "--prefixes-only",
      "stale-prefixes-only", "--stale-prefixes-only ", `${dash}stale-prefixes-only`, "-max", "--Max", "max=3",
    ];
    for (const m of misses) {
      expect(code(() => staleprefixOptions([m], env()))).toBe("stale_prefix_mode_flag_refused");
      expect(code(() => staleprefixOptions(["--execute", m], env()))).toBe("stale_prefix_mode_flag_refused");
      expect(code(() => staleprefixOptions([m, "--execute"], env()))).toBe("stale_prefix_mode_flag_refused");
    }
    // Exact --max / --max= keep their own code (checked first).
    expect(code(() => staleprefixOptions(["--execute", "--max", "3"], env()))).toBe("stale_prefix_max_without_mode");
    expect(code(() => staleprefixOptions(["--max=3", "--Max"], env()))).toBe("stale_prefix_max_without_mode");
    // Not a strict allowlist: unrelated arguments stay the default job's business.
    for (const args of [[], ["--execute"], ["--once"], ["--execute", "--once", "--loop", "--dry-run", "extra"]])
      expect(staleprefixOptions(args, env())).toBeUndefined();
  });

  test("accepted forms", () => {
    expect(staleprefixOptions(["--stale-prefixes-only", "--only-identity", ID_A], env())).toEqual({ execute: false, verifyOnly: false, onlyIdentities: [ID_A] });
    expect(staleprefixOptions(["--stale-prefixes-only", "--execute", "--once", "--max", "25", "--only-identity", ID_A, "--only-identity", ID_B,
      "--verified-receipt", "r.json", "--protection-attestation", "a.json"], env({ BABYSITTER_RETENTION_SWEEP: " OFF " })))
      .toEqual({ execute: true, verifyOnly: false, max: 25, onlyIdentities: [ID_A, ID_B], verifiedReceipt: "r.json", protectionAttestation: "a.json" });
    const hundred = Array.from({ length: 100 }, (_, i) => ["--only-identity", i.toString(16).padStart(64, "0")]).flat();
    expect(staleprefixOptions(["--stale-prefixes-only", ...hundred], env())?.onlyIdentities).toHaveLength(100);
  });

  test("--only-identity: required in the mode (dry or --execute); one 64-lowercase-hex value each; distinct; at most 100", () => {
    for (const execute of [[], ["--execute"]]) {
      const mode = ["--stale-prefixes-only", ...execute];
      expect(code(() => staleprefixOptions(mode, env()))).toBe("stale_prefix_only_identity_required");
      expect(code(() => staleprefixOptions([...mode, "--max", "3"], env()))).toBe("stale_prefix_only_identity_required");
      expect(code(() => staleprefixOptions([...mode, "--only-identity"], env()))).toBe("stale_prefix_only_identity_invalid");
      for (const bad of ["", ID_A.toUpperCase(), ID_A.slice(1), `${ID_A}0`, ` ${ID_A}`, `${ID_A.slice(1)}g`, "--execute", "sess-1"])
        expect(code(() => staleprefixOptions([...mode, "--only-identity", bad], env()))).toBe("stale_prefix_only_identity_invalid");
      expect(code(() => staleprefixOptions([...mode, "--only-identity", ID_A, "--only-identity", ID_B, "--only-identity", ID_A], env()))).toBe("stale_prefix_only_identity_duplicate");
      const many = Array.from({ length: 101 }, (_, i) => ["--only-identity", i.toString(16).padStart(64, "0")]).flat();
      expect(code(() => staleprefixOptions([...mode, ...many], env()))).toBe("stale_prefix_only_identity_too_many");
      expect(code(() => staleprefixOptions([...mode, `--only-identity=${ID_A}`], env()))).toBe("stale_prefix_mode_flag_refused");
    }
  });

  test("--only-identity outside the mode, and anything resembling it, is refused (the full job would ignore it)", () => {
    for (const args of [["--only-identity", ID_A], ["--execute", "--only-identity", ID_A], [`--only-identity=${ID_A}`]])
      expect(code(() => staleprefixOptions(args, env()))).toBe("stale_prefix_only_identity_without_mode");
    for (const m of ["--only-identities", "--only-identiy", "--onlyidentity", "--identity", "-only-identity", "--Only-Identity", "--ONLY-IDENTITY", "only-identity", "--only"])
      for (const args of [[m, ID_A], ["--execute", m, ID_A], [m]])
        expect(code(() => staleprefixOptions(args, env()))).toBe("stale_prefix_mode_flag_refused");
    expect(staleprefixOptions(["--execute", "--once"], env())).toBeUndefined();
  });

  test("--max must be one positive decimal integer", () => {
    for (const v of ["0", "-1", "1.5", "1e3", "0x10", " 3", "03", "", "abc", "9007199254740993"])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max", v], env()))).toBe("stale_prefix_max_invalid");
    expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max"], env()))).toBe("stale_prefix_max_invalid");
    expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--max", "2", "--max", "3"], env()))).toBe("stale_prefix_max_invalid");
  });

  test("contradicting or unknown flags refused", () => {
    for (const extra of [["--max=3"], ["--loop"], ["--dry-run"], ["--stale-prefixes-only"], ["extra"]])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--only-identity", ID_A, ...extra], env()))).toBe("stale_prefix_mode_flag_refused");
  });

  test("retention phase and obligations refused", () => {
    for (const v of ["on", "dry_run", "bogus"])
      expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--only-identity", ID_A], env({ BABYSITTER_RETENTION_SWEEP: v })))).toBe("stale_prefix_mode_retention_refused");
    expect(staleprefixOptions(["--stale-prefixes-only", "--only-identity", ID_A], env({ BABYSITTER_RETENTION_SWEEP: "off" }))).toEqual({ execute: false, verifyOnly: false, onlyIdentities: [ID_A] });
    expect(code(() => staleprefixOptions(["--stale-prefixes-only", "--only-identity", ID_A], env({ S3X_OBLIGATIONS: "on" })))).toBe("stale_prefix_mode_requires_obligations_off");
  });

  test("the CLI refuses before reading stdin, exits 1 and logs only the code", async () => {
    const main = new URL("../neon-s3-erasure/main.ts", import.meta.url).pathname;
    const run = (args: string[], extra: Record<string, string> = {}) =>
      new Promise<{ lines: Record<string, unknown>[]; code: number | null; state: string }>(async (resolve, reject) => {
        const home = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-stale-cli-"));
        const e: Record<string, string | undefined> = { ...process.env, HOME: home, S3X_STATE: join(home, "s.sqlite"), ...extra };
        for (const k of FORBIDDEN_CREDENTIAL_ENV) delete e[k];
        delete e.BABYSITTER_RETENTION_SWEEP;
        Object.assign(e, extra);
        const child = spawn(process.execPath, [main, ...args], { stdio: ["pipe", "pipe", "inherit"], env: e, cwd: home });
        let text = "";
        child.stdout.on("data", (b) => (text += b));
        child.on("error", reject);
        child.on("exit", (c) => resolve({ lines: text.trim().split("\n").filter(Boolean).map((l) => JSON.parse(l)), code: c, state: join(home, "s.sqlite") }));
        child.stdin.end("{}");
      });
    for (const [args, extra, want] of [
      [["--stale-prefixes-only", "--max", "0"], {}, "stale_prefix_max_invalid"],
      [["--execute", "--max", "5"], {}, "stale_prefix_max_without_mode"],
      [["--stale-prefixes-only", "--execute", "--only-identity", ID_A], { BABYSITTER_RETENTION_SWEEP: "on" }, "stale_prefix_mode_retention_refused"],
      [["--stale-prefixes-only"], {}, "stale_prefix_only_identity_required"],
      [["--stale-prefixes-only", "--execute"], {}, "stale_prefix_only_identity_required"],
      [["--stale-prefixes-only", "--execute", "--only-identity"], {}, "stale_prefix_only_identity_invalid"],
      [["--stale-prefixes-only", "--only-identity", ID_A.toUpperCase()], {}, "stale_prefix_only_identity_invalid"],
      [["--stale-prefixes-only", "--execute", "--only-identity", ID_A, "--only-identity", ID_A], {}, "stale_prefix_only_identity_duplicate"],
      [["--stale-prefixes-only", ...Array.from({ length: 101 }, (_, i) => ["--only-identity", i.toString(16).padStart(64, "0")]).flat()], {}, "stale_prefix_only_identity_too_many"],
      [["--execute", "--only-identity", ID_A], {}, "stale_prefix_only_identity_without_mode"],
      [["--execute", "--only-identiy", ID_A], {}, "stale_prefix_mode_flag_refused"],
      [["--stale-prefix-only", "--execute"], {}, "stale_prefix_mode_flag_refused"],
      [["--stale-prefixes", "--execute"], {}, "stale_prefix_mode_flag_refused"],
      [["--stale_prefixes_only"], {}, "stale_prefix_mode_flag_refused"],
      [["--stale-prefixes-only=1"], {}, "stale_prefix_mode_flag_refused"],
      [["--maximum", "3"], {}, "stale_prefix_mode_flag_refused"],
      [["--max-count=3"], {}, "stale_prefix_mode_flag_refused"],
      [["--execute", "--stal-prefixes-only"], {}, "stale_prefix_mode_flag_refused"],
      [["--execute", "--prefixes-only"], {}, "stale_prefix_mode_flag_refused"],
    ] as [string[], Record<string, string>, string][]) {
      const r = await run(args, extra);
      expect(r.code).toBe(1);
      expect(r.lines.map((l) => [l.message, l.error_code])).toEqual([["exit on error", want]]);
      // Refused before stdin is read (that would fail on the empty JSON with another code) and before the state opens.
      expect(existsSync(r.state)).toBe(false);
    }
  });
});

describe("short-lived eraser key expiring inside an admitted item (T56 review)", () => {
  test("refused before signing: the pass throws the code (CLI 75), the item stays queued with no attempt, its lease is released; after renewal it erases", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    const before = erasureRow(f.st, a.oldP)!;
    let expired = true; let refusals = 0;
    // The key expires mid-erase: the eraser's request path refuses before signing (eraser.ts refuseIfExpired).
    f.s.onDelete = (k) => { if (expired && k.startsWith(a.oldP)) { refusals++; throw new ShortLivedCredentialError("short_lived_expired.eraser"); } };
    let err: unknown;
    try { await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() }); } catch (e) { err = e; }
    expect((err as { code?: string }).code).toBe("short_lived_expired.eraser");
    expect(exitStatusForError(err)).toBe(75);
    expect(refusals).toBeGreaterThan(0);
    const row = erasureRow(f.st, a.oldP)!;
    expect(row.attempts).toBe(before.attempts); // not the item's failure: no backoff, attempts kept
    expect(row.error ?? null).toBe(before.error ?? null);
    expect(rowsOf(f.path, "leases")).toEqual([]); // lease released by the finally
    expect(f.logs.some((l) => l.message === "erasure failed")).toBe(false);
    expect(f.logs.some((l) => l.message === "erasure stopped: credential")).toBe(true);
    // Renewal: a fresh key, a rerun of the pass resumes and erases the same item.
    expired = false;
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: true, erased: 1 });
    expect(f.s.count(a.oldP)).toBe(0);
    expect(f.st.staleprefixErasures().map((e) => e.key)).toEqual([]);
  });

  test("control: a genuine failed erase is still recorded as the item's failure and exits 1, not 75", async () => {
    const f = await fixture();
    const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
    f.s.onDelete = (k) => (k.startsWith(a.oldP) ? "lose" : undefined);
    const r = await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe: f.s.eraser() });
    expect(r).toMatchObject({ ok: false, stoppedBy: "failed" });
    expect(erasureRow(f.st, a.oldP)!.attempts).toBe(1);
    let err: unknown; try { stalePassStatus(r, true); } catch (e) { err = e; } // even with the expiry timer fired
    expect((err as { code?: string }).code).toBe("stale_prefix_pass_stopped");
    expect(exitStatusForError(err)).toBe(1);
  });
});

test("full job drain: a credential refusal inside a leased item stops the drain (no erasureFailed, lease released)", async () => {
  const f = await fixture();
  const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
  const before = erasureRow(f.st, a.oldP)!;
  f.s.onDelete = (k) => { if (k.startsWith(a.oldP)) throw new ShortLivedCredentialError("short_lived_expired.eraser"); };
  let err: unknown; try { await f.job().drain(); } catch (e) { err = e; }
  expect((err as { code?: string }).code).toBe("short_lived_expired.eraser");
  expect(erasureRow(f.st, a.oldP)!.attempts).toBe(before.attempts);
  expect(rowsOf(f.path, "leases")).toEqual([]);
});

test("stale-prefix execute: a refusal from the sibling probe (objectLive) stops the pass with the code, not a sibling_check_failed skip", async () => {
  const f = await fixture();
  const a = f.dateMove("sess-1", "g-1", NOW - 60_000);
  const before = erasureRow(f.st, a.oldP)!;
  const probe = { objectLive: async () => { throw new ShortLivedCredentialError("short_lived_expired.eraser"); } } as unknown as LiveObjectProbe;
  let err: unknown;
  try { await f.exec({ onlyIdentities: allIds(f.st), execute: true, probe }); } catch (e) { err = e; }
  expect((err as { code?: string }).code).toBe("short_lived_expired.eraser");
  expect(exitStatusForError(err)).toBe(75);
  expect(erasureRow(f.st, a.oldP)!.attempts).toBe(before.attempts);
  expect(rowsOf(f.path, "leases")).toEqual([]);
  expect(f.logs.some((l) => l.message === "stale prefix skipped" && l.reason === "sibling_check_failed")).toBe(false);
  expect(f.s.count(a.oldP)).toBe(3); // nothing erased
});
