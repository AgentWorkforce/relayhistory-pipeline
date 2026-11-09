/**
 * Self-host per-session byte eraser against the offline versioned S3 provider
 * (src/migration/__tests__/fake-s3-versions.ts): every version and delete marker of the
 * transcript dirs, the live pointer and the supplied staged objects; verification by a fresh
 * listing; prefix allow-list refusals; dry run; default-off entry; idempotence.
 */
import { describe, expect, test } from "bun:test";
import { FakeS3Versions } from "../../migration/__tests__/fake-s3-versions.ts";
import { STAGED_ERASE_AFTER_MS } from "../../migration/neon-s3-erasure/job.ts";
import { liveMachineId } from "../../session-ingest/writer.ts";
import { stagedKey } from "../../session-ingest/staging.ts";
import { assertAllowed, eraseSessionBytes, planTargets, STAGED_MIN_AGE_MS, SessionEraseRefused, type SessionEraseBytesRequest } from "../selfhost-eraser.ts";
import { runSessionEraseBytes, sessionEraseBytesEnabled } from "../selfhost-eraser-main.ts";

const ROOT = "96bulls", ORG = "org-a", WS = "ws-1", SRC = "claude-code", SID = "sess:one/a";
const SEG = "sess_one_a"; // layoutComponent(SID)
const M = liveMachineId(WS, "origin-1", "user-1");
const M2 = "delivery_exporter-machine";
const NOW = Date.UTC(2026, 10, 9, 12);
const OLD = NOW - 3 * 24 * 3600_000;
const DIR = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SEG}/`;
const DIR2 = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/02/s=${SEG}/`;
const DIR_EXPORT = `${ROOT}/${ORG}/v1/ws=${WS}/m=${M2}/h=${SRC}/d=2026/10/30/s=${SEG}/`;
const POINTER = `${ROOT}/${ORG}/live-sessions/v1/ws=${WS}/m=${M}/h=${SRC}/s=${SEG}.json`;
const staged = (n: string) =>
  stagedKey(ROOT, { org_id: ORG, workspace_id: WS, origin_id: "origin-1", user_id: "user-1", batch_digest: `batch-${n}`, claim_token: `claim-${n}` }, "ab".repeat(32));
const S1 = staged("1"), S2 = staged("2"), S_OTHER = staged("3");
// Neighbours that must survive.
const NEIGHBOURS = [
  `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SEG}x/manifest.json`, // prefix-sharing session id
  `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=other/manifest.json`,
  `${ROOT}/${ORG}/live-sessions/v1/ws=${WS}/m=${M}/h=${SRC}/s=${SEG}x.json`,
  `${ROOT}/org-b/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SEG}/manifest.json`,
  S_OTHER,
];

const request = (over: Partial<SessionEraseBytesRequest> = {}): SessionEraseBytesRequest => ({
  key: { orgId: ORG, workspaceId: WS, source: SRC, sessionId: SID },
  locations: [
    { workspaceId: WS, machineId: M, harness: SRC, sessionId: SEG, manifestKey: `${DIR}manifest.json`, dates: ["2026-11-02"], storageProvider: "s3", bucket: "b" },
    { workspaceId: WS, machineId: M2, harness: SRC, sessionId: SEG, manifestKey: `${DIR_EXPORT}manifest.json` },
  ],
  stagedKeys: [S1, S2],
  ...over,
});

function seed(): FakeS3Versions {
  const s = new FakeS3Versions();
  s.pageSize = 3; // force paging
  for (const d of [DIR, DIR2, DIR_EXPORT]) {
    s.put(`${d}manifest.json`, OLD); s.put(`${d}manifest.json`, OLD + 1); // two versions
    for (let i = 0; i < 4; i++) s.put(`${d}raw/${String(i).padStart(10, "0")}-0-abcd1234.ndjson.zst`, OLD);
  }
  // A delete marker on top of a version, and a key that is only a delete marker.
  s.put(`${DIR}raw/9999999999-0-dead0000.ndjson.zst`, OLD);
  marker(s, `${DIR}raw/9999999999-0-dead0000.ndjson.zst`, OLD + 5);
  marker(s, `${DIR}raw/9999999998-0-dead0001.ndjson.zst`, OLD + 6);
  s.put(POINTER, OLD); s.put(POINTER, OLD + 2); marker(s, POINTER, OLD + 3);
  s.put(S1, OLD); s.put(S2, OLD);
  for (const k of NEIGHBOURS) s.put(k, OLD);
  return s;
}
function marker(s: FakeS3Versions, key: string, at: number) {
  s.keys.set(key, [{ id: `m${key.length}${at}`, at, marker: true }, ...(s.keys.get(key) ?? [])]);
}
const opts = (extra: Record<string, unknown> = {}) => ({ keyRoot: ROOT, bucket: "b", now: () => NOW, log: () => {}, ...extra });
const survivors = (s: FakeS3Versions) => NEIGHBOURS.every((k) => s.keys.has(k));

describe("self-host session byte eraser", () => {
  test("deletes every version and delete marker of every transcript dir, the live pointer and the staged objects, then verifies", async () => {
    const s = seed();
    const before = s.count(DIR) + s.count(DIR2) + s.count(DIR_EXPORT) + s.count(POINTER) + s.count(S1) + s.count(S2);
    const r = await eraseSessionBytes(s.eraser(), request(), opts());
    expect(r).toEqual({
      dryRun: false, transcriptObjects: 3 * 5 + 2, pointerObjects: 1, stagedObjects: 2,
      versionsDeleted: before, stagedPendingVersions: 0, incompleteTargets: 0, verified: true,
    });
    for (const p of [DIR, DIR2, DIR_EXPORT, POINTER, S1, S2]) expect(s.count(p)).toBe(0);
    expect(survivors(s)).toBe(true);
    // Delete markers were deleted by version id too.
    expect(s.deletes.some((d) => d.startsWith(`${POINTER}@m`))).toBe(true);
  });

  test("the live pointer is erased after the transcript dirs and staged objects", async () => {
    const s = seed();
    await eraseSessionBytes(s.eraser(), request(), opts());
    const firstPointer = s.deletes.findIndex((d) => d.startsWith(POINTER));
    const lastOther = Math.max(...s.deletes.map((d, i) => (d.startsWith(POINTER) ? -1 : i)));
    expect(firstPointer).toBeGreaterThan(lastOther);
  });

  test("staged objects: deleted when older than the 48 h redrive window; younger versions kept and reported, verified=false", async () => {
    expect(STAGED_MIN_AGE_MS).toBe(STAGED_ERASE_AFTER_MS);
    const s = seed();
    s.put(S2, NOW - 3600_000); // a fresh version inside the window
    const r = await eraseSessionBytes(s.eraser(), request(), opts());
    expect(s.count(S1)).toBe(0);
    expect(s.count(S2)).toBe(1);
    expect(r).toMatchObject({ stagedObjects: 2, stagedPendingVersions: 1, verified: false });
    // After the window passes a rerun finishes the job.
    const later = await eraseSessionBytes(s.eraser(), request(), opts({ now: () => NOW + STAGED_MIN_AGE_MS }));
    expect(later).toMatchObject({ stagedObjects: 1, versionsDeleted: 1, verified: true });
  });

  test("a late write between delete and verify makes verification fail (verified=false)", async () => {
    const s = seed();
    s.onDelete = (key) => {
      if (key === POINTER && !s.keys.has(`${DIR}raw/late.ndjson.zst`)) s.put(`${DIR}raw/late.ndjson.zst`, NOW);
    };
    const r = await eraseSessionBytes(s.eraser(), request(), opts());
    expect(r.verified).toBe(false);
    expect(s.count(DIR)).toBe(1);
    s.onDelete = undefined;
    expect(await eraseSessionBytes(s.eraser(), request(), opts())).toMatchObject({ transcriptObjects: 1, versionsDeleted: 1, verified: true });
  });

  test("idempotent: a second run deletes 0 and verifies true", async () => {
    const s = seed();
    await eraseSessionBytes(s.eraser(), request(), opts());
    const deletes = s.deletes.length;
    const r = await eraseSessionBytes(s.eraser(), request(), opts());
    expect(r).toEqual({ dryRun: false, transcriptObjects: 0, pointerObjects: 0, stagedObjects: 0, versionsDeleted: 0, stagedPendingVersions: 0, incompleteTargets: 0, verified: true });
    expect(s.deletes.length).toBe(deletes);
  });

  test("dry run lists and counts but deletes nothing (and never claims verified)", async () => {
    const s = seed();
    const total = s.count();
    const r = await eraseSessionBytes(s.eraser(), request(), opts({ dryRun: true }));
    expect(r).toMatchObject({ dryRun: true, transcriptObjects: 17, pointerObjects: 1, stagedObjects: 2, verified: false });
    expect(r.versionsDeleted).toBe(s.count(DIR) + s.count(DIR2) + s.count(DIR_EXPORT) + s.count(POINTER) + s.count(S1) + s.count(S2));
    expect(s.deletes).toEqual([]);
    expect(s.count()).toBe(total);
  });

  test("bounded runs: a target larger than its run budget is incomplete and never verified; a rerun resumes to verified", async () => {
    const s = seed();
    for (let i = 0; i < 300; i++) s.put(`${DIR}raw/x${String(i).padStart(4, "0")}.ndjson.zst`, OLD);
    s.pageSize = 2; // 100 pages per run x 2 < 300 keys
    const r = await eraseSessionBytes(s.eraser(), request(), opts({ maxRunsPerTarget: 1 }));
    expect(r).toMatchObject({ incompleteTargets: 1, verified: false });
    expect(s.count(DIR)).toBeGreaterThan(0);
    expect(await eraseSessionBytes(s.eraser(), request(), opts())).toMatchObject({ incompleteTargets: 0, verified: true });
    expect(s.count(DIR)).toBe(0);
  });

  test("bounded concurrency: at most `concurrency` targets in flight; out-of-range widths refused", async () => {
    const s = seed();
    const inner = s.eraser();
    let live = 0, peak = 0;
    const counted = {
      ...inner,
      eraseStep: async (...a: Parameters<typeof inner.eraseStep>) => {
        live++; peak = Math.max(peak, live);
        await new Promise((r) => setTimeout(r, 2));
        try { return await inner.eraseStep(...a); } finally { live--; }
      },
    };
    expect(await eraseSessionBytes(counted, request(), opts({ concurrency: 2 }))).toMatchObject({ verified: true });
    expect(peak).toBe(2);
    for (const c of [0, 17, 1.5]) await expect(eraseSessionBytes(s.eraser(), request(), opts({ concurrency: c }))).rejects.toThrow("session_erase_concurrency_invalid");
  });

  test("logs carry counts only: no key, session id, org, workspace or machine", async () => {
    const s = seed();
    const lines: string[] = [];
    await eraseSessionBytes(s.eraser(), request(), opts({ log: (l: unknown) => lines.push(JSON.stringify(l)) }));
    expect(lines.length).toBe(1);
    for (const needle of [SEG, "sess_", "one", ORG, WS, M, ROOT, "/", "b="]) expect(lines[0]).not.toContain(needle);
  });
});

describe("never deletes outside the computed prefixes", () => {
  const refusedWithoutTouching = async (req: SessionEraseBytesRequest, code: string) => {
    const s = seed();
    await expect(eraseSessionBytes(s.eraser(), req, opts())).rejects.toThrow(`session_erase_${code}`);
    expect(s.lists).toBe(0);
    expect(s.deletes).toEqual([]);
  };

  test("a staged key outside the session's machines, another org, or not a staged object is refused before any listing", async () => {
    const otherMachine = stagedKey(ROOT, { org_id: ORG, workspace_id: WS, origin_id: "o2", user_id: "u2", batch_digest: "b", claim_token: "c" }, "cd".repeat(32));
    const otherOrg = S1.replace(`/${ORG}/`, "/org-b/");
    for (const k of [otherMachine, otherOrg, `${DIR}manifest.json`, S1.replace(".staged.ndjson.zst", ".json"), S1.replace("/b=", "/../b="), "x"])
      await refusedWithoutTouching(request({ stagedKeys: [k] }), "staged_outside_session");
  });

  test("buffered-batches objects are refused (the buffered-GC redaction protocol owns them)", async () => {
    await refusedWithoutTouching(request({ stagedKeys: [S1.replace("/live-batches/", "/buffered-batches/")] }), "staged_buffered_owned_by_gc");
  });

  test("a location of another session, a mismatched manifest key, a non-S3 store or another bucket is refused", async () => {
    const base = request().locations[0]!;
    await refusedWithoutTouching(request({ locations: [{ ...base, sessionId: "other" }] }), "location_not_this_session");
    await refusedWithoutTouching(request({ locations: [{ ...base, workspaceId: "ws-2" }] }), "location_not_this_session");
    await refusedWithoutTouching(request({ locations: [{ ...base, harness: "codex" }] }), "location_not_this_session");
    await refusedWithoutTouching(request({ locations: [{ ...base, manifestKey: `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=other/manifest.json` }] }), "manifest_key_mismatch");
    await refusedWithoutTouching(request({ locations: [{ ...base, manifestKey: `${ROOT}/org-b/v1/ws=${WS}/m=${M}/h=${SRC}/d=2026/11/01/s=${SEG}/manifest.json` }] }), "manifest_key_mismatch");
    await refusedWithoutTouching(request({ locations: [{ ...base, storageProvider: "gcs" }] }), "store_not_versioned_s3");
    await refusedWithoutTouching(request({ locations: [{ ...base, bucket: "other" }] }), "bucket_mismatch");
    await refusedWithoutTouching(request({ locations: [{ ...base, machineId: "../x" }] }), "location_invalid");
    await refusedWithoutTouching(request({ locations: [{ ...base, manifestKey: null, dates: [] }] }), "date_required");
    await refusedWithoutTouching(request({ locations: [{ ...base, dates: ["2026-02-30"] }] }), "date_invalid");
    await refusedWithoutTouching(request({ key: { orgId: "../x", workspaceId: WS, source: SRC, sessionId: SID } }), "org_invalid");
  });

  test("assertAllowed refuses any key outside the plan (sibling session, prefix-sharing id, dot segments, other pointer)", () => {
    const plan = planTargets(request(), { keyRoot: ROOT, bucket: "b" });
    expect(() => assertAllowed(plan, [`${DIR}raw/a`, POINTER, S1])).not.toThrow();
    for (const k of [...NEIGHBOURS, DIR, `${DIR}../s=other/manifest.json`, `${POINTER}.bak`, `${DIR2.replace("11/02", "11/03")}manifest.json`])
      expect(() => assertAllowed(plan, [k])).toThrow(SessionEraseRefused);
  });

  test("a listing that returns a foreign key is refused before that page deletes anything", async () => {
    const s = seed();
    s.rewrite = (_call, xml) => xml.replace(/<Key>[^<]*<\/Key>/, `<Key>${DIR}../../s=other/manifest.json</Key>`);
    await expect(eraseSessionBytes(s.eraser(), request(), opts())).rejects.toThrow();
    expect(s.deletes).toEqual([]);
    expect(survivors(s)).toBe(true);
  });

  test("the eraser's own guard stops a delete the primitive would allow (defence in depth)", async () => {
    const s = seed();
    const inner = s.eraser();
    // A primitive whose select was widened to the whole org: the guard in beforeDelete still refuses.
    const widened = {
      ...inner,
      eraseStep: (_prefix: string, o: Parameters<typeof inner.eraseStep>[1]) =>
        inner.eraseStep(`${ROOT}/${ORG}/`, { ...o, scope: "widened", exactKey: undefined, select: () => true }),
    };
    await expect(eraseSessionBytes(widened, request(), opts())).rejects.toThrow("session_erase_key_outside_allowed_prefixes");
    expect(survivors(s)).toBe(true);
  });
});

describe("process entry (default off)", () => {
  const ENV = { SESSION_ERASE_S3_BUCKET: "bkt", SESSION_ERASE_S3_REGION: "us-east-1", SESSION_ERASE_S3_ENDPOINT: "http://127.0.0.1:9000", SESSION_ERASE_S3_ACCESS_KEY_ID: "erase-key", SESSION_ERASE_S3_SECRET_ACCESS_KEY: "erase-secret-x" };

  test("flag unset, empty or 0 returns disabled without reading the request or touching the store", async () => {
    for (const flag of [undefined, "", "0"]) {
      let built = 0, read = 0;
      const env: NodeJS.ProcessEnv = { ...ENV, ...(flag !== undefined ? { SESSION_ERASE_BYTES_ENABLED: flag } : {}) };
      const r = await runSessionEraseBytes(env, () => { read++; return "{}"; }, { eraser: () => { built++; throw new Error("touched"); } });
      expect(r).toEqual({ enabled: false });
      expect({ built, read }).toEqual({ built: 0, read: 0 });
    }
  });

  test("only the literal 1 enables; other values fail closed", () => {
    expect(sessionEraseBytesEnabled({ SESSION_ERASE_BYTES_ENABLED: "1" })).toBe(true);
    for (const v of ["true", "TRUE", "yes", " 1", "1 ", "2"]) expect(() => sessionEraseBytesEnabled({ SESSION_ERASE_BYTES_ENABLED: v })).toThrow("session_erase_flag_invalid");
  });

  test("flag on runs the eraser over the configured bucket and returns counts", async () => {
    const s = seed();
    const req = request();
    req.locations = req.locations.map((l) => ({ ...l, bucket: "bkt" }));
    const r = await runSessionEraseBytes({ ...ENV, SESSION_ERASE_BYTES_ENABLED: "1" }, () => JSON.stringify(req), {
      eraser: (cfg) => { expect(cfg).toMatchObject({ bucket: "bkt", endpoint: "http://127.0.0.1:9000" }); return s.eraser(); }, now: () => NOW, log: () => {},
    });
    expect(r).toMatchObject({ enabled: true, verified: true, stagedObjects: 2, pointerObjects: 1 });
  });

  test("config refusals carry codes, never values", async () => {
    const run = (env: NodeJS.ProcessEnv) => runSessionEraseBytes({ SESSION_ERASE_BYTES_ENABLED: "1", ...env }, () => "{}", { eraser: () => { throw new Error("touched"); } });
    await expect(run({ ...ENV, SESSION_ERASE_S3_ENDPOINT: "http://minio.example:9000" })).rejects.toThrow("session_erase_endpoint_invalid");
    await expect(run({ ...ENV, SESSION_ERASE_S3_SECRET_ACCESS_KEY_FILE: "/x" })).rejects.toThrow("session_erase_secret_source_ambiguous");
    await expect(run({ ...ENV, SESSION_ERASE_S3_SECRET_ACCESS_KEY: undefined })).rejects.toThrow("session_erase_secret_invalid");
    await expect(run({ ...ENV, SESSION_ERASE_DRY_RUN: "yes" })).rejects.toThrow("session_erase_config_invalid");
    try { await run({ ...ENV, SESSION_ERASE_S3_ACCESS_KEY_ID: "bad key with spaces" }); } catch (e) { expect(String(e)).not.toContain("bad key"); }
  });
});
