import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { RELAYHISTORY_EXPORT_EXTENSION, parseManifest, rawChunkKey, objectKey } from "../../shared/vendor.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler } from "../handler.ts";
import { extractiveSummarizer, type SessionDigest } from "../summarizer.ts";
import { readSummaryObject } from "../summary-objects.ts";

const MIG = (f: string) => readFileSync(join(import.meta.dir, "../../hoopdb/migrations", f), "utf8");

const open: PGlite[] = [];
// PGlite's WASM runtime must be closed, or the test process exits non-zero.
afterEach(async () => {
  while (open.length) await open.pop()!.close();
});

async function setup() {
  const pg = new PGlite();
  open.push(pg);
  await pg.exec(`CREATE SCHEMA sessions; CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;`);
  await pg.exec(MIG("0002_session_ext_and_summaries.sql"));
  // A pre-0004 row holding plaintext: the migration must remove it.
  await pg.exec(`INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256, summary)
                 VALUES ('o','w','claude','old','m','short','01a188e1-ef86-7b16-938a-e91b6b78643b',1,'${"d".repeat(64)}','OLD PLAINTEXT')`);
  await pg.exec(MIG("0004_summary_pointers.sql"));
  const q = async (text: string, params?: unknown[]) => ({ rows: (await pg.query(text, params as never)).rows as never[] });
  const db = { query: q, pool: { connect: async () => ({ query: q, release() {} }) }, end: async () => {} } as never;
  return { pg, db };
}

function manifest(scrubbed: boolean, chunks: unknown[]) {
  return {
    revision: 2,
    trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b",
    session: { workspace_id: "w", machine_id: "m", harness: "claude", session_id: "s1", started_at: "2026-10-29T10:00:00Z" },
    raw: { format: "jsonl", content_sha256: "c".repeat(64), chunks, ...(scrubbed ? { scrub: { ruleset: "r", version: 1 } } : {}) },
  };
}

const loc = { workspaceId: "w", machineId: "m", harness: "claude", sessionId: "s1", startDate: "2026-10-29" };
const message = { payload: { manifestKey: "k" }, ctx: { orgId: "o" } } as never;
const recorder = { log: { info() {} } } as never;

describe("Bob Costas stores summaries as encrypted Locker objects, pointers in HoopDb", () => {
  it("ready: object summary/short.json.enc decrypts; the row has only the pointer; old plaintext rows are gone", async () => {
    const { pg, db } = await setup();
    expect((await pg.query(`SELECT count(*)::int AS n FROM sessions.session_summaries`)).rows[0]).toEqual({ n: 0 });
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const line = JSON.stringify({ type: "user", message: { role: "user", content: "PLANTED-PROMPT fix the build" } });
    await store.put(rawChunkKey(loc, 0, 0, "abcdef12", "jsonl"), zstdCompressSync(Buffer.from(line + "\n")));
    const m = manifest(true, [{ seq: 0, start_offset: 0, sha256_8: "abcdef12" }]);
    const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
    const handler = bobCostasHandler("short", { db, reader: { provider: "fs", read: async () => ({ manifest: m }) } as never, store, summarizer: extractiveSummarizer, keyring });
    await handler(message, recorder);
    const row = (await pg.query<Record<string, unknown>>(`SELECT * FROM sessions.session_summaries`)).rows[0]!;
    expect(row.status).toBe("ready");
    expect(row.object_key).toBe("v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/short.json.enc");
    expect(row.key_id).toBe("hkdf1:k1");
    expect("summary" in row).toBe(false);
    expect("model_id" in row).toBe(false);
    expect(JSON.stringify(row)).not.toContain("PLANTED-PROMPT");
    const rec = await readSummaryObject({ store, keyring, ids: { orgId: "o", workspaceId: "w" }, key: String(row.object_key), sha256: String(row.object_sha256), kind: "short", revision: 2 });
    expect(rec?.status).toBe("ready");
    expect(rec?.summary).toContain("PLANTED-PROMPT");
    // A rerun of the same revision and content is a no-op.
    expect(await handler(message, recorder)).toEqual({ status: "skipped", reason: "summary_current" });
    // After a master rotation the same summary is re-encrypted under the new key, not skipped.
    const rotated = await masterKeyring(parseMasterKeys(`k2:${"b".repeat(64)},k1:${"a".repeat(64)}`));
    await bobCostasHandler("short", { db, reader: { provider: "fs", read: async () => ({ manifest: m }) } as never, store, summarizer: extractiveSummarizer, keyring: rotated })(message, recorder);
    const after = (await pg.query<Record<string, unknown>>(`SELECT key_id, object_sha256 FROM sessions.session_summaries`)).rows[0]!;
    expect(after.key_id).toBe("hkdf1:k2");
    const onlyK2 = await masterKeyring(parseMasterKeys(`k2:${"b".repeat(64)}`));
    expect((await readSummaryObject({ store, keyring: onlyK2, ids: { orgId: "o", workspaceId: "w" }, key: String(row.object_key), sha256: String(after.object_sha256), kind: "short", revision: 2 }))?.summary).toContain("PLANTED-PROMPT");
  }, 30_000);

  it("unscrubbed: recorded quarantined with no pointer, no object created, raw never read", async () => {
    const { pg, db } = await setup();
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bch-")) });
    const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
    const m = manifest(false, [{ seq: 0, start_offset: 0, sha256_8: "deadbeef" }]); // chunk absent: reading it would throw
    await bobCostasHandler("long", { db, reader: { provider: "fs", read: async () => ({ manifest: m }) } as never, store, summarizer: extractiveSummarizer, keyring })(message, recorder);
    const row = (await pg.query<Record<string, unknown>>(`SELECT status, object_key, object_sha256, key_id FROM sessions.session_summaries`)).rows[0]!;
    expect(row).toEqual({ status: "quarantined", object_key: null, object_sha256: null, key_id: null });
    expect(await store.head(objectKey("v1/ws=w/m=m/h=claude/d=2026/10/29/s=s1/summary/long.json.enc"))).toBeUndefined();
  }, 30_000);

  it("the pointer constraint refuses a ready row without a pointer", async () => {
    const { pg } = await setup();
    await expect(pg.query(`INSERT INTO sessions.session_summaries (org_id, workspace_id, harness, session_id, machine_id, kind, trace_id, revision, content_sha256, status)
                           VALUES ('o','w','claude','x','m','short','01a188e1-ef86-7b16-938a-e91b6b78643b',1,'${"e".repeat(64)}','ready')`)).rejects.toThrow(/session_summaries_pointer/);
  }, 30_000);
});

describe("Bob Costas reads merged streams, never raw epochs", () => {
  const t = Date.parse("2026-10-25T18:00:00Z");
  const K = "ndjson" as const;
  const envelope = (record_id: string, revision: string, payload: Record<string, unknown> | null, operation = "upsert", kind = "session_event") =>
    JSON.stringify({ kind, origin_id: "o-1", record_id, revision, operation, received_at: "2026-10-30 12:00:00+00", payload });
  const scrub = { ruleset: "r", version: 1 };
  const marker = { version: 1, source: "neon.delivery_records" };

  async function stored(lines: string[][], epochs: unknown[], extensions: unknown = { [RELAYHISTORY_EXPORT_EXTENSION]: marker }, format = K) {
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bcm-")) });
    const chunks = [];
    for (const [seq, group] of lines.entries()) {
      const raw = Buffer.from(`${group.join("\n")}\n`);
      const compressed = zstdCompressSync(raw);
      const sha = await sha256Hex(raw);
      await store.put(rawChunkKey(loc, seq, 0, sha, format), compressed);
      chunks.push({ seq, epoch: seq, start_offset: 0, bytes: raw.byteLength, sha256: sha, sha256_8: sha.slice(0, 8), stored_bytes: compressed.byteLength, stored_sha256: await sha256Hex(compressed), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: group.length });
    }
    const m = { ...manifest(true, chunks), extensions };
    m.raw = { ...m.raw, format, epochs, epoch: epochs.length - 1, scrub } as never;
    return { m, store };
  }
  const run = async (m: unknown, store: unknown) => {
    const { db, pg } = await setup();
    const seen: SessionDigest[] = [];
    const summarizer = { id: "spy", summarize: async (kind: "short" | "long", d: SessionDigest) => (seen.push(d), extractiveSummarizer.summarize(kind, d)) };
    const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
    const handler = bobCostasHandler("short", { db, reader: { provider: "fs", read: async () => ({ manifest: m }) } as never, store: store as never, summarizer, keyring });
    return { pg, seen, go: () => handler(message, recorder) };
  };
  const epoch = (i: number, reason: string, extra = {}) => ({ epoch: i, opened_at: "2026-10-30T12:00:00Z", reason, first_seq: i, opened_by: "w", ...extra });
  const threeEpochs = [epoch(0, "initial"), epoch(1, "backfill", { opened_by: "migration:g" }), epoch(2, "resumed", { resumes_epoch: 0 })];

  it("merges live, sealed backfill and resumed epochs: highest revision, a later delete wins, event order", async () => {
    const { m, store } = await stored(
      [
        [envelope("live", "1", { role: "user", text: "LIVE", ts_ms: t + 20 })],
        [
          envelope("old", "1", { role: "user", text: "HISTORIC", ts_ms: t }),
          envelope("edited", "1", { role: "user", text: "STALE", ts_ms: t + 1 }),
          envelope("gone", "1", { role: "user", text: "DELETED", ts_ms: t + 2 }),
          envelope("tool", "1", { name: "Read", ts_ms: t + 3 }, "upsert", "tool_call"),
        ],
        [
          envelope("edited", "2", { role: "user", text: "FRESH", ts_ms: t + 1 }),
          envelope("gone", "2", null, "delete"),
          envelope("ai", "1", { role: "assistant", model: "opus", ts_ms: t + 30 }),
          envelope("meta", "1", { role: "user", text: "PLUMBING", is_meta: 1, ts_ms: t + 4 }),
          envelope("side", "1", { role: "user", text: "SIDECHAIN", is_sidechain: "true", ts_ms: t + 5 }),
        ],
      ],
      threeEpochs,
    );
    const { pg, seen, go } = await run(m, store);
    await go();
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ userTurns: ["HISTORIC", "FRESH", "LIVE"], assistantTurns: 1, models: ["opus"], toolCalls: { Read: 1 }, records: 7 });
    expect((await pg.query<{ status: string }>("SELECT status FROM sessions.session_summaries")).rows[0]?.status).toBe("ready");
  });

  it.each([
    ["an unknown export marker version", { [RELAYHISTORY_EXPORT_EXTENSION]: { ...marker, version: 2 } }, "relayhistory_export_marker_missing"],
    ["a backfill with no export marker", {}, "relayhistory_export_marker_missing"],
  ])("refuses %s without retry and without summarizing", async (_name, extensions, code) => {
    const { m, store } = await stored([[envelope("a", "1", { role: "user", text: "x", ts_ms: t })]], threeEpochs, extensions);
    const { seen, go } = await run(m, store);
    await expect(go()).rejects.toMatchObject({ code, retryable: false });
    expect(seen).toEqual([]);
  });

  it("refuses a backfilled session that is not ndjson, never concatenating it", async () => {
    const { m, store } = await stored([[envelope("a", "1", { role: "user", text: "x", ts_ms: t })]], threeEpochs, undefined, "jsonl" as never);
    const { seen, go } = await run(m, store);
    await expect(go()).rejects.toMatchObject({ code: "relayhistory_export_format", retryable: false });
    expect(seen).toEqual([]);
  });

  it("refuses an equal revision with different content", async () => {
    const { m, store } = await stored(
      [[envelope("a", "1", { role: "user", text: "one", ts_ms: t })], [envelope("a", "1", { role: "user", text: "two", ts_ms: t })]],
      threeEpochs.slice(0, 2),
    );
    const { seen, go } = await run(m, store);
    await expect(go()).rejects.toMatchObject({ code: "relayhistory_revision_conflict", retryable: false });
    expect(seen).toEqual([]);
  });

  it("refuses a chunk whose stored digest does not match", async () => {
    const { m, store } = await stored([[envelope("a", "1", { role: "user", text: "x", ts_ms: t })]], threeEpochs.slice(0, 1));
    (m.raw.chunks[0] as { stored_sha256: string }).stored_sha256 = "0".repeat(64);
    const { seen, go } = await run(m, store);
    await expect(go()).rejects.toMatchObject({ code: "chunk_digest_mismatch", retryable: false });
    expect(seen).toEqual([]);
  });

  // What an un-upgraded reader does: the manifest stays schema_v 1, so a reader
  // that predates an epoch reason stops at validation. Safe, but that session's
  // summaries stall until the reader is upgraded. A reader from before #46 does
  // this on "resumed"; this reader does it on the next new reason.
  it("stops at validation on an epoch reason it does not know, before any read", async () => {
    const fixture = JSON.parse(readFileSync(join(import.meta.dir, "../../../packages/object-store/test/fixtures/rust-manifest-v1-backfill.json"), "utf8"));
    const future = structuredClone(fixture.manifest);
    future.raw.epochs.at(-1).reason = "compacted";
    await expect(parseManifest(JSON.stringify(future), fixture.key)).rejects.toThrow();
    expect(future.schema_v).toBe(1);
  });

  it("agrees with the Rust writer's fixture: it validates and takes the merged path", async () => {
    const fixture = JSON.parse(readFileSync(join(import.meta.dir, "../../../packages/object-store/test/fixtures/rust-manifest-v1-backfill.json"), "utf8"));
    const parsed = await parseManifest(JSON.stringify(fixture.manifest), fixture.key);
    expect(parsed.raw.epochs.map((e) => e.reason)).toEqual(expect.arrayContaining(["backfill", "resumed"]));
    const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bcf-")) });
    const { seen, go } = await run({ ...parsed, extensions: undefined }, store);
    await expect(go()).rejects.toMatchObject({ code: "relayhistory_export_marker_missing" });
    expect(seen).toEqual([]);
  });
});
