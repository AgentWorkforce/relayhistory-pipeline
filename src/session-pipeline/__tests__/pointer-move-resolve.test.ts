/**
 * SESSION_POINTER_RESOLVE_MOVE_PENDING on: through the real handler (processBatch -> indexPointer) on PGlite with
 * every migration and an fs object store. Each safe resolution, each refusal, idempotent retries, the outcome on
 * the log line through the real log path, and that no object is ever written or deleted by the stage.
 * Synthetic, secret-free data only.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { objectKey, rawChunkKey } from "../../shared/vendor.ts";
import { MOVE_RESOLUTION_VALUES, logLine } from "../../shared/log.ts";
import { MOVE_PENDING_REASONS, MAX_EXTRA_SIBLINGS } from "../move-resolve.ts";
import { lambdaEnv, moveResolveDeps, pointerFlagDeps, pointerResolveMovePendingFromEnv } from "../lambda.ts";
import type { PointerDeps } from "../pointer.ts";
import { at, captureLogs, pointerRig, type ManifestSpec, type Rig } from "./pointer-move-harness.ts";

let r: Rig;
beforeEach(async () => {
  r = await pointerRig();
}, 120_000);
afterEach(async () => {
  await r.close();
});

const OLD = at("2026-11-01");
const NEW = at("2026-10-31");
const THIRD = at("2026-10-30");
const on = (extra: Partial<PointerDeps> = {}): Partial<PointerDeps> => ({ resolveMovePending: { head: r.head }, ...extra });

/** The old copy (revision 3, origin o at 3) indexed; returns its key. */
async function indexedOld(spec: ManifestSpec = {}) {
  const key = await r.writeExport(OLD, { revision: 3, watermarks: { o: 3 }, chunks: [["r1", "r2"]], updatedAt: "2026-11-01T11:00:00.000Z", ...spec });
  expect(await r.failures("old", r.body(key, "ObjectCreated:Put", "01"))).toEqual([]);
  return key;
}
/** The exporter's re-keyed copy: newer, an earlier record prepended, origin o at 4. */
const newer = (spec: ManifestSpec = {}) =>
  r.writeExport(NEW, { revision: 1, watermarks: { o: 4 }, chunks: [["r0", "r1", "r2"]], updatedAt: "2026-11-01T12:00:00.000Z", ...spec });

/** Every object key under the store, with its bytes' length: a stage that writes or deletes nothing leaves it equal. */
async function objects() {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await r.store.list(objectKey("96bulls"), cursor);
    for (const item of page.items) out.push(`${item.key}:${item.size}`);
    cursor = page.nextCursor;
  } while (cursor);
  return out.sort();
}

async function runLogged(id: string, key: string, name = "ObjectCreated:Put", seq = "05", extra = on()) {
  const { result, lines } = await captureLogs(() => r.run(id, r.body(key, name, seq), extra));
  return { failed: result.batchItemFailures.map((f) => f.itemIdentifier), lines };
}
const resolutionOf = (lines: Record<string, unknown>[]) => lines.filter((l) => "move_resolution" in l).map((l) => l.move_resolution);

describe("safe resolutions", () => {
  test("resolved_complete by watermarks: the new key wins, the row is re-keyed, both copies stay in S3", async () => {
    const old = await indexedOld();
    const keyNew = await newer();
    const before = await objects();
    const { failed, lines } = await runLogged("n1", keyNew);
    expect(failed).toEqual([]);
    expect(await r.row()).toMatchObject([{ manifest_key: keyNew, revision: "1", deleted_at: null }]);
    expect(lines.find((l) => l.message === "pointer indexed")).toMatchObject({ outcome: "moved", move_resolution: "resolved_complete" });
    expect(await objects()).toEqual(before);
    expect((await r.store.head(objectKey(old))) !== undefined).toBe(true);
    const dir = (await r.pg.query<{ deleted_at: string | null }>("SELECT deleted_at FROM sessions.session_directory WHERE session_id = 'sess_1'")).rows;
    expect(dir.some((d) => d.deleted_at === null)).toBe(true);
  }, 60_000);

  test("resolved_complete by content: no watermarks, every old chunk is in the new copy", async () => {
    await indexedOld({ watermarks: undefined, chunks: [["r1"]] });
    const keyNew = await newer({ watermarks: undefined, chunks: [["r1"], ["r2"]] });
    const { failed, lines } = await runLogged("n1", keyNew);
    expect(failed).toEqual([]);
    expect(resolutionOf(lines)).toEqual(["resolved_complete"]);
    expect(await r.row()).toMatchObject([{ manifest_key: keyNew }]);
  }, 60_000);

  test("resolved_skip: an event for the stale key while the row names the newer key acknowledges it, row untouched", async () => {
    const old = await indexedOld();
    const keyNew = await newer();
    expect(await r.failures("n1", r.body(keyNew, "ObjectCreated:Put", "02"), on())).toEqual([]);
    // The new copy moves on to revision 3, so the stale copy's revision 3 is fenced out (not greater).
    await newer({ revision: 3, watermarks: { o: 5 }, chunks: [["r0", "r1", "r2"], ["r3"]], updatedAt: "2026-11-01T13:00:00.000Z" });
    expect(await r.failures("n3", r.body(keyNew, "ObjectCreated:Put", "03"), on())).toEqual([]);
    const rowBefore = await r.row();
    const before = await objects();
    const { failed, lines } = await runLogged("s1", old);
    expect(failed).toEqual([]);
    expect(lines.find((l) => l.message === "pointer indexed")).toMatchObject({ outcome: "unchanged", move_resolution: "resolved_skip" });
    expect(await r.row()).toEqual(rowBefore);
    expect(await objects()).toEqual(before);
    // Idempotent: the same stale event again is skipped again.
    expect(resolutionOf((await runLogged("s1", old)).lines)).toEqual(["resolved_skip"]);
    expect(await r.row()).toEqual(rowBefore);
  }, 60_000);

  test("multi-hop: a third, older, covered copy is live too; the newest copy wins", async () => {
    await r.writeExport(THIRD, { revision: 2, watermarks: { o: 2 }, chunks: [["r1"]], updatedAt: "2026-11-01T10:30:00.000Z" });
    await indexedOld();
    const keyNew = await newer();
    const { failed, lines } = await runLogged("n1", keyNew);
    expect(failed).toEqual([]);
    expect(resolutionOf(lines)).toEqual(["resolved_complete"]);
    expect(await r.row()).toMatchObject([{ manifest_key: keyNew }]);
  }, 60_000);

  test("a stale hop's delete that follows to the newest copy resolves too", async () => {
    const third = await r.writeExport(THIRD, { revision: 2, watermarks: { o: 2 }, chunks: [["r1"]], updatedAt: "2026-11-01T10:30:00.000Z" });
    await indexedOld();
    const keyNew = await newer();
    await r.store.delete(objectKey(third));
    // The delete finds the nearest live sibling (the new key, one day later) and indexes it.
    const { failed, lines } = await runLogged("d3", third, "ObjectRemoved:DeleteMarkerCreated");
    expect(failed).toEqual([]);
    expect(resolutionOf(lines)).toEqual(["resolved_complete"]);
    expect(await r.row()).toMatchObject([{ manifest_key: keyNew, deleted_at: null }]);
  }, 60_000);

  test("idempotent: after resolved_complete, retries and the old prefix's later erasure change nothing", async () => {
    const old = await indexedOld();
    const keyNew = await newer();
    expect(resolutionOf((await runLogged("n1", keyNew)).lines)).toEqual(["resolved_complete"]);
    const settled = await r.row();
    const retry = await runLogged("n1", keyNew);
    expect(retry.failed).toEqual([]);
    expect(resolutionOf(retry.lines)).toEqual([]); // the row already names this key: the normal path
    expect(await r.row()).toEqual(settled);
    await r.store.delete(objectKey(old));
    const del = await runLogged("d1", old, "ObjectRemoved:DeleteMarkerCreated", "06");
    expect(del.failed).toEqual([]);
    expect(await r.row()).toEqual(settled);
  }, 60_000);
});

describe("refusals keep pointer_move_pending, with a still_pending reason, and change nothing", () => {
  async function refused(reason: string, keyNew: string, extra = on()) {
    const rowBefore = await r.row();
    const before = await objects();
    const { failed, lines } = await runLogged("n1", keyNew, "ObjectCreated:Put", "05", extra);
    expect(failed).toEqual(["n1"]);
    const line = lines.find((l) => l.message === "pointer index failed");
    expect(line).toMatchObject({ error_code: "pointer_move_pending", retryable: true, move_resolution: `still_pending:${reason}` });
    expect(await r.row()).toEqual(rowBefore);
    expect(await objects()).toEqual(before);
    // A retry gives the same answer.
    expect(resolutionOf((await runLogged("n1", keyNew, "ObjectCreated:Put", "05", extra)).lines)).toEqual([`still_pending:${reason}`]);
  }

  test("dest_chunk_missing: the new copy's manifest is there, its chunks are not", async () => {
    await indexedOld();
    await refused("dest_chunk_missing", await newer({ skipChunks: true }));
  }, 60_000);

  test("dest_chunk_mismatch: a chunk of another length", async () => {
    await indexedOld();
    const keyNew = await newer();
    const m = (await r.store.getObject(objectKey(keyNew))).body;
    const manifest = JSON.parse(new TextDecoder().decode(new Uint8Array(await new Response(m).arrayBuffer())));
    const c = manifest.raw.chunks[0];
    const chunk = rawChunkKey(NEW, c.seq, c.start_offset, c.sha256, "ndjson");
    await r.store.delete(chunk);
    await r.store.put(chunk, new Uint8Array(c.stored_bytes + 3), { metadata: { "raw-sha256": c.raw_sha256 } });
    await refused("dest_chunk_mismatch", keyNew);
  }, 60_000);

  test("dest_chunk_mismatch: a chunk without the digest metadata the manifest names", async () => {
    await indexedOld();
    await refused("dest_chunk_mismatch", await newer({ noChunkMetadata: true }));
  }, 60_000);

  test("concurrent_write: the old copy is re-published while the proof runs", async () => {
    await indexedOld();
    const keyNew = await newer();
    let fired = false;
    const head: NonNullable<PointerDeps["resolveMovePending"]>["head"] = async (key) => {
      if (!fired) {
        fired = true;
        await r.writeExport(OLD, { revision: 3, watermarks: { o: 3 }, chunks: [["r1", "r2"]], updatedAt: "2026-11-01T11:30:00.000Z" });
      }
      return r.head(key);
    };
    const rowBefore = await r.row();
    const { failed, lines } = await runLogged("n1", keyNew, "ObjectCreated:Put", "05", on({ resolveMovePending: { head } }));
    expect(failed).toEqual(["n1"]);
    expect(resolutionOf(lines)).toEqual(["still_pending:concurrent_write"]);
    expect(await r.row()).toEqual(rowBefore);
  }, 60_000);

  test("epoch_mismatch: the new copy carries a rewritten epoch", async () => {
    await indexedOld();
    const epochs = [
      { epoch: 0, opened_at: "2026-11-01T10:00:00.000Z", reason: "initial" as const, first_seq: 0, opened_by: "migration:grant_synthetic" },
      // A valid, empty current epoch after the one chunk of epoch 0.
      { epoch: 1, opened_at: "2026-11-01T11:45:00.000Z", reason: "rewritten" as const, first_seq: 1, opened_by: "migration:grant_synthetic" },
    ];
    await refused("epoch_mismatch", await newer({ epochs }));
  }, 60_000);

  test("open_session: the new copy is still open (a writer may be appending)", async () => {
    await indexedOld();
    await refused("open_session", await newer({ state: "open" }));
  }, 60_000);

  test("writer_mismatch: another writer owns the new copy", async () => {
    await indexedOld();
    await refused("writer_mismatch", await newer({ owner: "migration:grant_other" }));
  }, 60_000);

  test("not_covered: the new copy's watermark is behind the old one's", async () => {
    await indexedOld();
    await refused("not_covered", await newer({ watermarks: { o: 2 } }));
  }, 60_000);

  test("not_covered: an origin of the old copy is missing from the new one", async () => {
    await indexedOld({ watermarks: { o: 3, p: 1 } });
    await refused("not_covered", await newer({ watermarks: { o: 9 } }));
  }, 60_000);

  test("ambiguous_order: both copies claim the same update time", async () => {
    await indexedOld();
    await refused("ambiguous_order", await newer({ updatedAt: "2026-11-01T11:00:00.000Z" }));
  }, 60_000);

  test("multi_hop: a third live copy that is open", async () => {
    await indexedOld();
    const keyNew = await newer();
    await r.writeExport(THIRD, { revision: 1, state: "open", watermarks: { o: 1 }, updatedAt: "2026-11-01T10:30:00.000Z" });
    await refused("multi_hop", keyNew);
  }, 60_000);

  test("multi_hop: a third live copy newer than the landed one", async () => {
    await indexedOld();
    const keyNew = await newer();
    await r.writeExport(THIRD, { revision: 1, watermarks: { o: 9 }, chunks: [["r-1", "r0", "r1", "r2"]], updatedAt: "2026-11-01T15:00:00.000Z" });
    await refused("multi_hop", keyNew);
  }, 60_000);

  test("multi_hop: more live copies than MAX_EXTRA_SIBLINGS", async () => {
    await indexedOld();
    const keyNew = await newer();
    for (let i = 0; i <= MAX_EXTRA_SIBLINGS; i++)
      await r.writeExport(at(`2026-10-${String(20 + i).padStart(2, "0")}`), { revision: 1, watermarks: { o: 1 }, chunks: [["r1"]], updatedAt: "2026-11-01T10:30:00.000Z" });
    await refused("multi_hop", keyNew);
  }, 60_000);

  test("check_failed: a HEAD error while proving", async () => {
    await indexedOld();
    await refused("check_failed", await newer(), on({ resolveMovePending: { head: async () => { throw new Error("boom"); } } }));
  }, 60_000);
});

describe("the flag on, no move: the stage is as with it off", () => {
  test("a repeated put issues the same object-store calls and statements with the flag on as off", async () => {
    const key = await r.writeExport(OLD, { revision: 1, watermarks: { o: 1 } });
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"));
    const shape = () => r.calls.map((c) => [c.at, c.kind, c.kind === "sql" ? (c.detail as { sql: string }).sql : c.detail]);
    r.calls.length = 0;
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"), on());
    const withFlag = shape();
    r.calls.length = 0;
    await r.run("a", r.body(key, "ObjectCreated:Put", "01"));
    expect(withFlag).toEqual(shape());
    expect(withFlag.some((c) => c[1] === "s3.head_meta")).toBe(false);
  }, 60_000);
});

describe("the flag, its grammar and its wiring", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "bucket", PIPELINE_KEY_PREFIX: "96bulls", SESSION_PIPELINE_SECRET_ARN: "secret",
    SESSION_PIPELINE_QUEUE_URL: "queue", PIPELINE_ORG_FROM_PREFIX: "true",
  };
  test("unset, empty and false are off: the env object is as without the flag", () => {
    const plain = lambdaEnv(base);
    for (const v of ["", "false"]) expect(lambdaEnv({ ...base, SESSION_POINTER_RESOLVE_MOVE_PENDING: v })).toEqual(plain);
    expect("pointerResolveMovePending" in plain).toBe(false);
    expect(lambdaEnv({ ...base, SESSION_POINTER_RESOLVE_MOVE_PENDING: "true" })).toEqual({ ...plain, pointerResolveMovePending: true });
  });
  test("anything else fails init naming the variable, never the value", () => {
    for (const v of ["1", "TRUE", " true", "yes", "on"]) {
      expect(() => pointerResolveMovePendingFromEnv({ SESSION_POINTER_RESOLVE_MOVE_PENDING: v })).toThrow("session pipeline: SESSION_POINTER_RESOLVE_MOVE_PENDING must be true or false");
      try {
        lambdaEnv({ ...base, SESSION_POINTER_RESOLVE_MOVE_PENDING: v });
      } catch (e) {
        expect(String(e)).not.toContain(`: ${v}`);
      }
    }
  });
  test("it needs no SEARCH_INDEX_ENABLED", () => {
    expect(lambdaEnv({ ...base, SESSION_POINTER_RESOLVE_MOVE_PENDING: "true" }).pointerResolveMovePending).toBe(true);
  });
  test("pointerFlagDeps: off adds no field; on adds the HEAD dep over the store", async () => {
    const store = { head: async () => ({ key: objectKey("k"), size: 7, lastModified: new Date(0), version: {}, metadata: { "raw-sha256": "ab" } }) };
    expect(pointerFlagDeps({}, store)).toEqual({});
    expect(pointerFlagDeps({ pointerSkipOriginBucketEnabled: true }, store)).toEqual({ skipOriginBucket: true });
    const deps = pointerFlagDeps({ pointerResolveMovePending: true }, store);
    expect(Object.keys(deps)).toEqual(["resolveMovePending"]);
    expect(await deps.resolveMovePending!.head("k")).toEqual({ size: 7, metadata: { "raw-sha256": "ab" } });
    expect(await moveResolveDeps({ head: async () => undefined }).head("k")).toBeUndefined();
  });
  test("init passes the store to pointerFlagDeps (init itself needs AWS)", () => {
    const src = readFileSync(join(import.meta.dir, "../lambda.ts"), "utf8");
    expect(src).toContain("...pointerFlagDeps(env, store),");
  });
});

describe("the move_resolution log field", () => {
  test("its value rule is exactly the resolution's outcomes", () => {
    expect([...MOVE_RESOLUTION_VALUES].sort()).toEqual(["resolved_complete", "resolved_skip", ...MOVE_PENDING_REASONS.map((x) => `still_pending:${x}`)].sort());
  });
  test("a known value is kept, anything else dropped", () => {
    expect(JSON.parse(logLine("info", "m", { move_resolution: "resolved_skip" })).move_resolution).toBe("resolved_skip");
    expect(JSON.parse(logLine("info", "m", { move_resolution: "still_pending:other" })).move_resolution).toBeUndefined();
  });
});
