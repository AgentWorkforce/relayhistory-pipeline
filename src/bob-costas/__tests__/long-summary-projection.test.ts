/**
 * Bob Costas's optional long summary projection (SESSION_LONG_SUMMARY_PROJECTION_ENABLED,
 * src/hoopdb/session-long-summary.ts). Off (no `longSummary` dep, the default) issues no SQL
 * for it and calls nothing; on, the ready long summary is published once after COMMIT and
 * reconciled on a `summary_current` retry. The SQL writer itself is covered by
 * src/hoopdb/__tests__/session-long-summary.test.ts and session-long-summary.pglite.test.ts.
 */
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { rawChunkKey } from "../../shared/vendor.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler, sqlLongSummaryDirectory, type LongSummaryDirectory } from "../handler.ts";
import { extractiveSummarizer } from "../summarizer.ts";
import { LongSummaryRejected, type LongSummaryInput } from "../../hoopdb/session-long-summary.ts";
import { exactTrueFlag } from "../../session-read-api/config.ts";

const MIG = (f: string) => readFileSync(join(import.meta.dir, "../../hoopdb/migrations", f), "utf8");
const open: PGlite[] = [];
afterEach(async () => {
  while (open.length) await open.pop()!.close();
});

async function setup() {
  const pg = new PGlite();
  open.push(pg);
  await pg.exec(`CREATE SCHEMA sessions; CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;`);
  await pg.exec(MIG("0002_session_ext_and_summaries.sql"));
  await pg.exec(MIG("0004_summary_pointers.sql"));
  const sql: string[] = [];
  const q = async (text: string, params?: unknown[]) => {
    sql.push(text);
    return { rows: (await pg.query(text, params as never)).rows as never[] };
  };
  const db = { query: q, pool: { connect: async () => ({ query: q, release() {} }) }, end: async () => {} } as never;
  return { pg, db, sql };
}

const loc = { workspaceId: "w", machineId: "m", harness: "claude", sessionId: "s1", startDate: "2026-10-29" };
const m = {
  revision: 2,
  trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b",
  session: { workspace_id: "w", machine_id: "m", harness: "claude", session_id: "s1", started_at: "2026-10-29T10:00:00Z" },
  raw: { format: "jsonl", content_sha256: "c".repeat(64), chunks: [{ seq: 0, start_offset: 0, sha256_8: "abcdef12" }], scrub: { ruleset: "r", version: 1 } },
};
const message = { payload: { manifestKey: "k" }, ctx: { orgId: "o" } } as never;
const recorder = { log: { info() {} } } as never;
const noDirectory = { write: async () => ({ written: false }), clear: async () => false };

async function world() {
  const { pg, db, sql } = await setup();
  const store = createFsStore({ root: mkdtempSync(join(tmpdir(), "bcl-")) });
  const line = JSON.stringify({ type: "user", message: { role: "user", content: "fix the flaky build" } });
  await store.put(rawChunkKey(loc, 0, 0, "abcdef12", "jsonl"), zstdCompressSync(Buffer.from(line + "\n")));
  const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
  const base = {
    db,
    reader: { provider: "fs", read: async () => ({ manifest: m }) } as never,
    store,
    summarizer: extractiveSummarizer,
    directory: noDirectory,
    keyring,
    now: () => new Date("2026-11-09T00:00:00Z"),
  };
  return { pg, sql, base };
}

function recording(write: (input: LongSummaryInput) => Promise<{ written: boolean }> = async () => ({ written: true })) {
  const writes: LongSummaryInput[] = [];
  const clears: unknown[] = [];
  const directory: LongSummaryDirectory = {
    write: async (input) => {
      writes.push(input);
      return write(input);
    },
    clear: async (key) => {
      clears.push(key);
      return false;
    },
  };
  return { directory, writes, clears };
}

describe("Bob Costas long summary projection", () => {
  it("only the exact string \"true\" turns SESSION_LONG_SUMMARY_PROJECTION_ENABLED on", () => {
    expect(exactTrueFlag("true")).toBe(true);
    for (const raw of [undefined, "", "1", "TRUE", "true ", "yes"]) expect(exactTrueFlag(raw)).toBe(false);
  });

  it("flag off (no longSummary dep): no long_summary SQL, same SQL as before for both kinds", async () => {
    for (const kind of ["long", "short"] as const) {
      const { sql, base } = await world();
      await bobCostasHandler(kind, base)(message, recorder);
      expect(await bobCostasHandler(kind, base)(message, recorder)).toEqual({ status: "skipped", reason: "summary_current" });
      expect(sql.some((text) => text.includes("long_summary"))).toBe(false);
      expect(sql.some((text) => text.includes("session_directory"))).toBe(false);
    }
  }, 30_000);

  it("flag on: the ready long summary is published with provenance, and reconciled on a current retry", async () => {
    const { sql, base } = await world();
    const rec = recording();
    const handler = bobCostasHandler("long", { ...base, longSummary: rec.directory });
    await handler(message, recorder);
    expect(rec.writes).toHaveLength(1);
    expect(rec.writes[0]).toMatchObject({
      orgId: "o", workspaceId: "w", harness: "claude", sessionId: "s1",
      source: "bob-costas/extractive/v1", generatedAt: new Date("2026-11-09T00:00:00Z"),
    });
    expect(rec.writes[0]!.summary).toContain("flaky build");
    expect(rec.clears).toEqual([]);
    expect(await handler(message, recorder)).toEqual({ status: "skipped", reason: "summary_current" });
    expect(rec.writes).toHaveLength(2);
    expect(rec.writes[1]).toEqual(rec.writes[0]);
    // The SQL writer is not used through `db` when a directory is injected.
    expect(sql.some((text) => text.includes("long_summary"))).toBe(false);
    // The short kind never touches the long projection.
    const short = recording();
    await bobCostasHandler("short", { ...base, longSummary: short.directory })(message, recorder);
    expect([short.writes, short.clears]).toEqual([[], []]);
  }, 30_000);

  it("flag on: a refused long summary is logged, not written, and does not fail the message", async () => {
    const { pg, base } = await world();
    const rec = recording(async () => {
      throw new LongSummaryRejected("over_length");
    });
    await bobCostasHandler("long", { ...base, longSummary: rec.directory })(message, recorder);
    expect(rec.writes).toHaveLength(1);
    // The encrypted object and pointer are published regardless.
    expect((await pg.query<{ status: string }>(`SELECT status FROM sessions.session_summaries`)).rows).toEqual([{ status: "ready" }]);
  }, 30_000);

  it("flag on: a non-refusal writer error still fails the message (retry), after the pointer committed", async () => {
    const { base } = await world();
    const rec = recording(async () => {
      throw new Error("connection reset");
    });
    await expect(bobCostasHandler("long", { ...base, longSummary: rec.directory })(message, recorder)).rejects.toThrow("connection reset");
  }, 30_000);

  it("sqlLongSummaryDirectory: an invalid source is refused; the only SQL is the fenced clear of older text", async () => {
    const seen: string[] = [];
    const db = { query: async (text: string) => { seen.push(text); return { rows: [] }; } } as never;
    const dir = sqlLongSummaryDirectory(db);
    await expect(dir.write({ orgId: "o", workspaceId: "w", harness: "claude", sessionId: "s1", summary: "ok", source: "Bad Source", generatedAt: new Date() }))
      .rejects.toBeInstanceOf(LongSummaryRejected);
    expect(seen).toHaveLength(1);
    expect(seen[0]).toContain("SET long_summary = NULL");
  });
});
