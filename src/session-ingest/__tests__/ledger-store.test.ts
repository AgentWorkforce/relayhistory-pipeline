/**
 * The separate ledger bucket (ledger-store.ts): which keys move, the routed store's per-key routing and merged
 * listing (same keys, same order as one bucket, stale copies hidden), the ledger-home fork guard in ledger-s3 and in
 * the re-arm sweeper, the config, and the offline relocation (ledger-relocate.ts) end to end.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { journalKey } from "../async-admission.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { fenceAuthorityKey, fenceAuthorityStopKey, s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { assertLedgerHomeHere, isLedgerKey, isRoutedCursor, ledgerHomeGuard, ledgerHomeKey, ledgerHomeVerdict, ledgerListScope, ledgerRoutedStore, readLedgerHome } from "../ledger-store.ts";
import { parseRelocateArgs, relocateLedger, RelocateRefused, type RelocateArgs, type RelocateDeps } from "../ledger-relocate.ts";
import { selfhostLedgerHomeGuard, selfhostLedgerStore } from "../selfhost-config.ts";
import { hostedUploadAdmissionConfig } from "../upload-admission-hosted.ts";
import { selfhostStores } from "../selfhost-server.ts";
import { TENANT } from "./support.ts";

const ROOT = "96bulls";
const keys = s3LedgerKeys(ROOT, TENANT);
const enc = new TextEncoder();
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fsStore(pageSize?: number): ObjectStore {
  const base = process.env.TMPDIR ?? join(process.env.HOME ?? ".", ".agent-tmp", "tmp");
  mkdirSync(base, { recursive: true, mode: 0o700 });
  const root = mkdtempSync(join(base, "ledger-store-"));
  cleanups.push(() => rmSync(root, { recursive: true, force: true }));
  return createFsStore({ root, ...(pageSize ? { pageSize } : {}) }) as unknown as ObjectStore;
}
const put = (s: ObjectStore, k: string, v: unknown = { k }) => s.put(objectKey(k), enc.encode(JSON.stringify(v)), { contentType: "application/json" });
const text = async (s: ObjectStore, k: string) => new TextDecoder().decode(await readAll((await s.getObject(objectKey(k))).body));
async function listAll(s: ObjectStore, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do { const p = await s.list(objectKey(prefix), cursor); out.push(...p.items.map((i) => String(i.key))); cursor = p.nextCursor; } while (cursor);
  return out;
}

const dir = keys.dir;
const LEDGER = [keys.batch("o1", "b1"), keys.revisions("o1", "rec-1"), keys.owner("o1", "claude", "s1"), keys.rearm("o1", "claude", "s1"), `${keys.rearm("o1", "claude", "s1").slice(0, -5)}.done/abc.json`, `${dir}/ledger/log/o=x/e/0/000.json`, ledgerHomeKey(ROOT)];
const MAIN = [keys.fence, journalKey(ROOT, TENANT), `${dir}/r=${"a".repeat(64)}/a=000000000000001-0123456789abcdef.ndjson.zst`, `${dir}/r=${"a".repeat(64)}/outcome-admitted.json`,
  fenceAuthorityKey(ROOT), fenceAuthorityStopKey(ROOT), `${ROOT}/org-a/v1/ws=ws-a/m=m1/h=claude/d=2026/11/16/s=s1/manifest.json`, `${ROOT}/.upload-inbox/org-a/v1/x.json`,
  `${dir}/ledgerx/y.json`, `${dir}/upload-fences.json.bak`, `other/.admission-inbox/org-a/v1/ws=ws-a/m=m/ledger/z.json`];

describe("which keys are ledger keys", () => {
  test("everything under ledger/ and the home marker; never the fence, journal, inbox, outcomes, markers or canonical", () => {
    for (const k of LEDGER) expect([k, isLedgerKey(ROOT, k)]).toEqual([k, true]);
    for (const k of MAIN) expect([k, isLedgerKey(ROOT, k)]).toEqual([k, false]);
  });
  test("list scopes", () => {
    expect(ledgerListScope(ROOT, ROOT)).toBe("both");
    expect(ledgerListScope(ROOT, `${ROOT}/.ledger`)).toBe("both");
    expect(ledgerListScope(ROOT, `${ROOT}/.ledger/fence-authority`)).toBe("both");
    expect(ledgerListScope(ROOT, `${ROOT}/.ledger/rearm-sweeper`)).toBe("main");
    expect(ledgerListScope(ROOT, `${ROOT}/.admission-inbox`)).toBe("both");
    expect(ledgerListScope(ROOT, `${ROOT}/.admission-inbox/org-a`)).toBe("both");
    expect(ledgerListScope(ROOT, dir)).toBe("both");
    expect(ledgerListScope(ROOT, `${dir}/ledger`)).toBe("ledger");
    expect(ledgerListScope(ROOT, keys.ownersRoot)).toBe("ledger");
    expect(ledgerListScope(ROOT, `${dir}/r=${"a".repeat(64)}`)).toBe("main");
    expect(ledgerListScope(ROOT, `${ROOT}/.upload-inbox`)).toBe("main");
    expect(ledgerListScope(ROOT, `${ROOT}/org-a/v1`)).toBe("main");
    expect(ledgerListScope(ROOT, `${ROOT}/.admission-inbox/org-a/v2`)).toBe("main");
  });
});

describe("the routed store", () => {
  test("each key goes to its own store", async () => {
    const main = fsStore(), ledger = fsStore(), routed = ledgerRoutedStore(main, ledger, ROOT);
    for (const k of [...LEDGER, ...MAIN]) await put(routed, k);
    for (const k of LEDGER) { expect(await ledger.head(objectKey(k))).toBeDefined(); expect(await main.head(objectKey(k))).toBeUndefined(); }
    for (const k of MAIN) { expect(await main.head(objectKey(k))).toBeDefined(); expect(await ledger.head(objectKey(k))).toBeUndefined(); }
    const b = keys.batch("o1", "b1");
    expect(await text(routed, b)).toBe(JSON.stringify({ k: b }));
    await routed.delete(objectKey(b));
    expect(await ledger.head(objectKey(b))).toBeUndefined();
  });

  test("refuses one store as both", () => {
    const s = fsStore();
    expect(() => ledgerRoutedStore(s, s, ROOT)).toThrow("ledger_store_same");
  });

  for (const pageSize of [1, 2, 3, 7, 1000]) {
    test(`a listing that spans both is the one-bucket listing, in key order (page size ${pageSize})`, async () => {
      const single = fsStore(pageSize), main = fsStore(pageSize), ledger = fsStore(pageSize), routed = ledgerRoutedStore(main, ledger, ROOT);
      const all = [...LEDGER, ...MAIN].filter((k) => k.startsWith(`${ROOT}/`));
      // More keys interleaved in both stores under one admission dir.
      for (let i = 0; i < 12; i++) { all.push(`${dir}/ledger/batches/o=a/b=${i}.json`); all.push(`${dir}/r=${String(i).padStart(64, "0")}/x.json`); }
      for (const k of all) { await put(single, k); await put(routed, k); }
      for (const prefix of [ROOT, `${ROOT}/.admission-inbox`, `${ROOT}/.admission-inbox/org-a`, dir, `${ROOT}/.ledger`, `${ROOT}/.ledger/fence-authority`, `${dir}/ledger`, `${ROOT}/org-a`]) {
        const got = await listAll(routed, prefix);
        expect([prefix, got]).toEqual([prefix, await listAll(single, prefix)]);
        expect([...got].sort()).toEqual(got);
      }
    });
  }

  test("a stale copy of a ledger key in the Locker bucket (after a relocation) is never listed or read", async () => {
    const main = fsStore(2), ledger = fsStore(2), routed = ledgerRoutedStore(main, ledger, ROOT);
    const b = keys.batch("o1", "b1");
    await put(main, b, { stale: true });
    await put(main, keys.batch("o1", "gone"), { stale: true });
    await put(ledger, b, { live: true });
    await put(ledger, keys.fence, { misplaced: true });
    await put(main, keys.fence, { live: true });
    expect(await listAll(routed, dir)).toEqual([b, keys.fence]);
    expect(JSON.parse(await text(routed, b))).toEqual({ live: true });
    expect(JSON.parse(await text(routed, keys.fence))).toEqual({ live: true });
    expect(await listAll(routed, `${dir}/ledger`)).toEqual([b]);
  });

  test("a side whose page is empty but not the last (e.g. only stale keys filtered server-side) never loses keys", async () => {
    const single = fsStore(2), main = fsStore(2), ledger = fsStore(2);
    const keysAll = [keys.batch("o1", "a"), keys.batch("o1", "b"), keys.batch("o1", "c"), journalKey(ROOT, TENANT), `${dir}/r=${"0".repeat(64)}/x.json`, `${dir}/r=${"1".repeat(64)}/x.json`];
    const routedReal = ledgerRoutedStore(main, ledger, ROOT);
    for (const k of keysAll) { await put(single, k); await put(routedReal, k); }
    // The ledger side answers an empty page with a cursor first (S3 may), then its real pages.
    let first = true;
    const flaky = { ...ledger, backend: ledger.backend, list: async (p: never, c?: string) => {
      if (c === undefined && first) { first = false; return { items: [], nextCursor: "__restart__" }; }
      return ledger.list(p, c === "__restart__" ? undefined : c);
    } } as ObjectStore;
    const routed = ledgerRoutedStore(main, flaky, ROOT);
    expect(await listAll(routed, dir)).toEqual(await listAll(single, dir));
  });

  test("merged cursors are recognisable; a foreign cursor is refused, never silently restarted", async () => {
    const main = fsStore(1), ledger = fsStore(1), routed = ledgerRoutedStore(main, ledger, ROOT);
    for (const k of [keys.batch("o1", "b2"), keys.batch("o1", "b1"), journalKey(ROOT, TENANT), `${dir}/r=${"a".repeat(64)}/x.json`]) await put(routed, k);
    const first = await routed.list(objectKey(dir));
    expect(first.nextCursor && isRoutedCursor(first.nextCursor)).toBe(true);
    await expect(routed.list(objectKey(dir), "plain-s3-token")).rejects.toThrow("ledger_store_cursor_invalid");
  });

  test("conditional writes hold through the router", async () => {
    const routed = ledgerRoutedStore(fsStore(), fsStore(), ROOT);
    const b = keys.batch("o1", "b1");
    await put(routed, b, { a: 1 });
    await expect(routed.put(objectKey(b), enc.encode("{\"a\":2}"), { ifNoneMatch: true })).rejects.toMatchObject({ code: "already_exists" });
    const got = await routed.getObject(objectKey(b));
    await routed.put(objectKey(b), enc.encode("{\"a\":3}"), { ifMatch: got.meta.version });
    await expect(routed.put(objectKey(b), enc.encode("{\"a\":4}"), { ifMatch: got.meta.version })).rejects.toMatchObject({ code: "precondition_failed" });
  });
});

describe("the ledger-home fork guard", () => {
  const authority = { v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1 };
  const claim = { tenant: TENANT, originId: "o1", batchId: "b1", batchDigest: "a".repeat(64), receiptId: `rhr_${"b".repeat(64)}`, sessions: [], records: [] };
  async function claimCode(store: ObjectStore, ledgerHome?: { expect: string; required: boolean }) {
    const ledger = s3Ledger(store, ROOT, { fenceAuthority: "required", sleep: async () => {}, ...(ledgerHome ? { ledgerHome } : {}) });
    try { await ledger.claim(claim); return "admitted"; } catch (e) { if (e instanceof LedgerUnavailableError) return e.message; throw e; }
  }
  const home = (h: string) => ({ v: 1, home: h, writtenAtMs: 5 });

  test("verdicts", () => {
    expect(ledgerHomeVerdict(null, "b", false)).toBe("ok");
    expect(ledgerHomeVerdict(null, "b", true)).toBe("ledger_home_missing");
    expect(ledgerHomeVerdict(home("b") as never, "b", true)).toBe("ok");
    expect(ledgerHomeVerdict(home("c") as never, "b", false)).toBe("ledger_home_moved");
  });

  test("a claim refuses when the home marker names another bucket, or is missing where required; admits otherwise", async () => {
    const main = fsStore();
    await put(main, fenceAuthorityKey(ROOT), authority);
    expect(await claimCode(main)).toBe("admitted");
    expect(await claimCode(main, { expect: "locker", required: false })).toBe("admitted");
    await put(main, ledgerHomeKey(ROOT), home("ledger-bucket"));
    expect(await claimCode(main, { expect: "locker", required: false })).toContain("ledger_home_moved");

    const ledger = fsStore(), routed = ledgerRoutedStore(main, ledger, ROOT);
    expect(await claimCode(routed, { expect: "ledger-bucket", required: true })).toContain("ledger_home_missing");
    await put(ledger, ledgerHomeKey(ROOT), home("ledger-bucket"));
    expect(await claimCode(routed, { expect: "ledger-bucket", required: true })).toBe("admitted");
    await put(ledger, ledgerHomeKey(ROOT), { v: 1, home: "x" });
    expect(await claimCode(ledgerRoutedStore(main, ledger, ROOT), { expect: "ledger-bucket", required: true })).toContain("ledger_home_invalid");
  });

  test("ledgerHome needs the fence-authority gate", () => {
    expect(() => s3Ledger(fsStore(), ROOT, { ledgerHome: { expect: "b", required: false } })).toThrow("ledger_s3_options_refused");
  });

  test("readLedgerHome", async () => {
    const s = fsStore();
    expect(await readLedgerHome(s, ROOT)).toBeNull();
    await put(s, ledgerHomeKey(ROOT), home("b"));
    expect(await readLedgerHome(s, ROOT)).toEqual(home("b") as never);
  });
});

describe("configuration", () => {
  const base = { LOCKER_BUCKET: "locker-b", LOCKER_REGION: "us-east-1" };
  test("unset is unchanged; set needs the S3 ledger, a different bucket and a valid region", () => {
    expect(selfhostLedgerStore(base, "s3")).toBeUndefined();
    expect(selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "" }, undefined)).toBeUndefined();
    expect(selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "ledger-b", INGEST_LEDGER_REGION: "us-west-2" }, "s3")).toEqual({ bucket: "ledger-b", region: "us-west-2" });
    expect(selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "ledger-b" }, "s3")).toEqual({ bucket: "ledger-b", region: "us-east-1" });
    expect(() => selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "ledger-b" }, undefined)).toThrow("INGEST_LEDGER=s3");
    expect(() => selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "locker-b" }, "s3")).toThrow("must differ");
    expect(() => selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "Bad_Bucket" }, "s3")).toThrow("bucket name");
    expect(() => selfhostLedgerStore({ ...base, INGEST_LEDGER_REGION: "us-west-2" }, "s3")).toThrow("needs INGEST_LEDGER_BUCKET");
    expect(() => selfhostLedgerStore({ ...base, INGEST_LEDGER_BUCKET: "ledger-b", INGEST_LEDGER_REGION: "us west" }, "s3")).toThrow("region");
  });

  test("selfhostStores: one store and an optional home check by default; routed with a required home check when set", () => {
    const built: Record<string, string | undefined>[] = [];
    const build = ((env: NodeJS.ProcessEnv) => { built.push({ bucket: env.LOCKER_BUCKET, region: env.LOCKER_REGION }); return { provider: "s3", bucket: env.LOCKER_BUCKET, store: fsStore() }; }) as never;
    expect(selfhostStores({ keyRoot: ROOT }, base, build).ledgerHome).toBeUndefined();
    // Flag off (the default): byte-identical, no marker read at all.
    expect(selfhostStores({ keyRoot: ROOT, ledger: "s3" }, base, build).ledgerHome).toBeUndefined();
    expect(selfhostStores({ keyRoot: ROOT, ledger: "s3", ledgerHomeGuard: true }, base, build).ledgerHome).toEqual({ expect: "locker-b", required: false });
    built.length = 0;
    const r = selfhostStores({ keyRoot: ROOT, ledger: "s3", ledgerStore: { bucket: "ledger-b", region: "us-west-2" } }, base, build);
    expect(r.ledgerHome).toEqual({ expect: "ledger-b", required: true });
    expect(r.ledgerStore).toBeDefined();
    expect(r.store).not.toBe(r.main);
    expect(built).toEqual([{ bucket: "locker-b", region: "us-east-1" }, { bucket: "ledger-b", region: "us-west-2" }]);
  });

  test("INGEST_LEDGER_HOME_GUARD: default off, exactly on, needs s3", () => {
    expect(ledgerHomeGuard({})).toBe(false);
    expect(ledgerHomeGuard({ INGEST_LEDGER_HOME_GUARD: "off" })).toBe(false);
    expect(ledgerHomeGuard({ INGEST_LEDGER_HOME_GUARD: "on" })).toBe(true);
    expect(() => ledgerHomeGuard({ INGEST_LEDGER_HOME_GUARD: "true" })).toThrow();
    expect(selfhostLedgerHomeGuard({ INGEST_LEDGER_HOME_GUARD: "on" }, "s3")).toBe(true);
    expect(() => selfhostLedgerHomeGuard({ INGEST_LEDGER_HOME_GUARD: "on" }, undefined)).toThrow("INGEST_LEDGER=s3");
  });

  test("an endpoint is reused only in the same region; otherwise INGEST_LEDGER_ENDPOINT is required", () => {
    const ep = { ...base, LOCKER_ENDPOINT: "https://minio.example:9000", INGEST_LEDGER_BUCKET: "ledger-b" };
    expect(selfhostLedgerStore(ep, "s3")).toEqual({ bucket: "ledger-b", region: "us-east-1", endpoint: "https://minio.example:9000" });
    expect(() => selfhostLedgerStore({ ...ep, INGEST_LEDGER_REGION: "us-west-2" }, "s3")).toThrow("set INGEST_LEDGER_ENDPOINT");
    expect(selfhostLedgerStore({ ...ep, INGEST_LEDGER_REGION: "us-west-2", INGEST_LEDGER_ENDPOINT: "https://s3.us-west-2.amazonaws.com" }, "s3"))
      .toEqual({ bucket: "ledger-b", region: "us-west-2", endpoint: "https://s3.us-west-2.amazonaws.com" });
    expect(() => selfhostLedgerStore({ ...ep, INGEST_LEDGER_ENDPOINT: "https://x/path" }, "s3")).toThrow("bare origin");
    const built: Record<string, string | undefined>[] = [];
    const build = ((env: NodeJS.ProcessEnv) => { built.push({ bucket: env.LOCKER_BUCKET, endpoint: env.LOCKER_ENDPOINT }); return { provider: "s3", bucket: env.LOCKER_BUCKET, store: fsStore() }; }) as never;
    selfhostStores({ keyRoot: ROOT, ledger: "s3", ledgerStore: { bucket: "ledger-b", region: "us-west-2" } }, { ...base, LOCKER_ENDPOINT: "https://e" }, build);
    expect(built[1]).toEqual({ bucket: "ledger-b", endpoint: undefined });
  });

  test("the hosted upload-admission Lambda: the guard is opt-in, and a separate ledger bucket refuses", () => {
    const env = { INGEST_LEDGER: "s3", AWS_REGION: "us-east-1", UPLOAD_ADMISSION_BUCKET: "locker-b", UPLOAD_ADMISSION_SECRET_ID: "96bulls/prod/upload-admission" };
    expect(hostedUploadAdmissionConfig(env).ledgerHomeGuard).toBeUndefined();
    expect(hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER_HOME_GUARD: "on" }).ledgerHomeGuard).toBe(true);
    expect(() => hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER_HOME_GUARD: "yes" })).toThrow();
    expect(() => hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER_BUCKET: "ledger-b" })).toThrow();
  });

  test("assertLedgerHomeHere (the single-bucket operator tools)", async () => {
    const s = fsStore();
    await assertLedgerHomeHere(s, ROOT, "locker-b");
    await put(s, ledgerHomeKey(ROOT), { v: 1, home: "locker-b", writtenAtMs: 1 });
    await assertLedgerHomeHere(s, ROOT, "locker-b");
    await put(s, ledgerHomeKey(ROOT), { v: 1, home: "ledger-b", writtenAtMs: 2 });
    await expect(assertLedgerHomeHere(s, ROOT, "locker-b")).rejects.toThrow("ledger_home_moved");
    await put(s, ledgerHomeKey(ROOT), { v: 1 });
    await expect(assertLedgerHomeHere(s, ROOT, "locker-b")).rejects.toThrow("ledger_home_invalid");
  });
});

describe("the offline relocation", () => {
  const NOW = 10_000_000;
  function deps(): RelocateDeps & { main: { store: ObjectStore }; ledger: { store: ObjectStore } } {
    return { main: { store: fsStore(3), bucket: "locker-b" }, ledger: { store: fsStore(3), bucket: "ledger-b" }, keyRoot: ROOT, nowMs: () => NOW };
  }
  const args = (a: string[]): RelocateArgs => parseRelocateArgs(a);
  const stop = (d: RelocateDeps, at = NOW - 600_000) => put(d.main.store, fenceAuthorityStopKey(ROOT), { v: 1, stop: "all", writtenAtMs: at });
  async function seed(d: RelocateDeps) {
    for (const k of LEDGER.filter((k) => k !== ledgerHomeKey(ROOT))) await put(d.main.store, k);
    for (const k of MAIN.filter((k) => k.startsWith(`${ROOT}/`) && k !== fenceAuthorityStopKey(ROOT))) await put(d.main.store, k);
  }

  test("args: the mode is never implied; writes need --confirm", () => {
    expect(() => parseRelocateArgs([])).toThrow(RelocateRefused);
    expect(() => parseRelocateArgs(["copy"])).toThrow("confirm_required");
    expect(() => parseRelocateArgs(["switch"])).toThrow("confirm_required");
    expect(() => parseRelocateArgs(["verify", "--replace-stale"])).toThrow("usage");
    expect(parseRelocateArgs(["copy", "--confirm", "--direction", "back", "--replace-stale"])).toMatchObject({ mode: "copy", direction: "back", replaceStale: true });
  });

  test("copy and switch refuse while admission runs, too soon after the stop, or from a location that is not home", async () => {
    const d = deps();
    await seed(d);
    await expect(relocateLedger(d, args(["copy", "--confirm"]))).rejects.toThrow("admission_not_stopped");
    await stop(d, NOW - 1000);
    await expect(relocateLedger(d, args(["copy", "--confirm"]))).rejects.toThrow("stop_too_recent");
    await stop(d);
    await put(d.main.store, ledgerHomeKey(ROOT), { v: 1, home: "ledger-b", writtenAtMs: 1 });
    await expect(relocateLedger(d, args(["copy", "--confirm"]))).rejects.toThrow("source_not_home");
  });

  test("plan, copy, verify, switch: the routed store then serves the copy; a process left on the old bucket refuses", async () => {
    const d = deps();
    await seed(d);
    const plan = await relocateLedger(d, args(["plan"]));
    expect(plan).toMatchObject({ ok: true, source: { objects: LEDGER.length - 1 }, destination: { objects: 0 }, stop: "absent", sourceHome: "absent", destinationHome: "absent" });
    expect((await relocateLedger(d, args(["verify"]))).ok).toBe(false);
    await stop(d);
    const copy = await relocateLedger(d, args(["copy", "--confirm"]));
    expect(copy).toMatchObject({ ok: true, copied: LEDGER.length - 1, skipped: 0, diverged: 0 });
    // A rerun copies nothing.
    expect(await relocateLedger(d, args(["copy", "--confirm"]))).toMatchObject({ ok: true, copied: 0, skipped: LEDGER.length - 1 });
    const v = await relocateLedger(d, args(["verify"]));
    expect(v).toMatchObject({ ok: true, missing: 0, extra: 0, mismatched: 0 });
    expect(v.digest).toMatch(/^[0-9a-f]{64}$/);
    // Nothing but ledger keys moved.
    for (const k of MAIN.filter((k) => k.startsWith(`${ROOT}/`))) expect(await d.ledger.store.head(objectKey(k))).toBeUndefined();
    const sw = await relocateLedger(d, args(["switch", "--confirm"]));
    expect(sw).toMatchObject({ ok: true, sourceHome: "other", destinationHome: "self" });
    expect(await readLedgerHome(d.main.store, ROOT)).toMatchObject({ home: "ledger-b" });
    expect(await readLedgerHome(d.ledger.store, ROOT)).toMatchObject({ home: "ledger-b" });
    const routed = ledgerRoutedStore(d.main.store, d.ledger.store, ROOT);
    expect(await text(routed, keys.batch("o1", "b1"))).toBe(JSON.stringify({ k: keys.batch("o1", "b1") }));
    expect(await text(routed, keys.fence)).toBe(JSON.stringify({ k: keys.fence })); // the fence never moved
    expect(ledgerHomeVerdict(await readLedgerHome(routed, ROOT), "ledger-b", true)).toBe("ok");
    expect(ledgerHomeVerdict(await readLedgerHome(d.main.store, ROOT), "locker-b", false)).toBe("ledger_home_moved");
    // The plan after the switch.
    expect(await relocateLedger(d, args(["plan"]))).toMatchObject({ sourceHome: "other", destinationHome: "self", stop: "present" });
  });

  test("switch --side destination: only after the source's marker names the destination", async () => {
    const d = deps();
    await seed(d);
    await stop(d);
    await relocateLedger(d, args(["copy", "--confirm"]));
    await expect(relocateLedger(d, args(["switch", "--confirm", "--side", "destination"]))).rejects.toThrow("source_not_moved");
    expect(await readLedgerHome(d.ledger.store, ROOT)).toBeNull();
    await put(d.main.store, ledgerHomeKey(ROOT), { v: 1, home: "ledger-b", writtenAtMs: 7 });
    expect(await relocateLedger(d, args(["switch", "--confirm", "--side", "destination"]))).toMatchObject({ ok: true });
    expect(await readLedgerHome(d.ledger.store, ROOT)).toMatchObject({ home: "ledger-b" });
    expect(await readLedgerHome(d.main.store, ROOT)).toMatchObject({ writtenAtMs: 7 }); // untouched
    expect(() => parseRelocateArgs(["copy", "--confirm", "--side", "destination"])).toThrow("usage");
    // back, source side only: the ledger bucket says "moved home"; the operator writes the Locker bucket's marker.
    expect(await relocateLedger(d, args(["switch", "--confirm", "--direction", "back", "--side", "source"]))).toMatchObject({ ok: true });
    expect(await readLedgerHome(d.ledger.store, ROOT)).toMatchObject({ home: "locker-b" });
    expect(await readLedgerHome(d.main.store, ROOT)).toMatchObject({ writtenAtMs: 7 });
  });

  test("a destination object with different bytes is diverged (fails), and verify catches extras and mismatches", async () => {
    const d = deps();
    await seed(d);
    await stop(d);
    await put(d.ledger.store, keys.batch("o1", "b1"), { different: true });
    expect(await relocateLedger(d, args(["copy", "--confirm"]))).toMatchObject({ ok: false, diverged: 1 });
    expect(await relocateLedger(d, args(["verify"]))).toMatchObject({ ok: false, mismatched: 1 });
    await put(d.ledger.store, keys.batch("o1", "b1"), { k: keys.batch("o1", "b1") });
    await put(d.ledger.store, keys.batch("o1", "extra"));
    expect(await relocateLedger(d, args(["verify"]))).toMatchObject({ ok: false, extra: 1, mismatched: 0 });
    await expect(relocateLedger(d, args(["switch", "--confirm"]))).resolves.toMatchObject({ ok: false });
    expect(await readLedgerHome(d.main.store, ROOT)).toBeNull();
  });

  test("back: after the new location admitted, --replace-stale brings its state home and the switch reverses", async () => {
    const d = deps();
    await seed(d);
    await stop(d);
    await relocateLedger(d, args(["copy", "--confirm"]));
    await relocateLedger(d, args(["switch", "--confirm"]));
    // The new location moves on.
    await put(d.ledger.store, keys.batch("o1", "b1"), { newer: true });
    await put(d.ledger.store, keys.batch("o1", "new"));
    await expect(relocateLedger(d, args(["copy", "--confirm", "--direction", "back"]))).resolves.toMatchObject({ ok: false, diverged: 1 });
    expect(await relocateLedger(d, args(["copy", "--confirm", "--direction", "back", "--replace-stale"]))).toMatchObject({ ok: true, replaced: 1 });
    expect(await relocateLedger(d, args(["switch", "--confirm", "--direction", "back"]))).toMatchObject({ ok: true });
    expect(ledgerHomeVerdict(await readLedgerHome(d.main.store, ROOT), "locker-b", false)).toBe("ok");
    expect(ledgerHomeVerdict(await readLedgerHome(d.ledger.store, ROOT), "ledger-b", true)).toBe("ledger_home_moved");
    expect(JSON.parse(await text(d.main.store, keys.batch("o1", "b1")))).toEqual({ newer: true });
  });
});
