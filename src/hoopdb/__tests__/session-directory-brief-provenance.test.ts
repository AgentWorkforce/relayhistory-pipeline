/**
 * The directory projector reads NATIVE brief titles only (F2 containment, registry finding A1).
 * A Neon-backfilled brief (20261105043000, engine_version 'neon-backfill:*', origin
 * 'neon_backfill') must never reach session_directory.title, search_text, or the
 * brief-document publication built from the directory row. (Its readers are
 * sessions.session_brief_titles_for and sessions.session_brief(); this file proves only the
 * directory/publication containment.)
 *
 * Proved through the real projector (projectSessionDirectory) and the real document
 * consumer (sessions.session_brief_document_json), on PGlite:
 *   - after 043000: a native title still wins over the pointer title; an ordinary and a
 *     line-break backfilled title never project, and the pointer title stays;
 *   - CAUSAL: the projector's own SQL with the provenance predicate removed (the pre-fix
 *     body) does project the backfilled title, so the predicate is what contains it;
 *   - before 043000 (no origin column): the projector still runs, and a native title still
 *     projects. The predicate reads engine_version, which exists from 20261102000000.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import { database, hoopDb, KEY, pointer, read, root } from "../../session-brief/__tests__/harness.ts";
import { recordBrief, type BriefText } from "../../session-brief/store.ts";
import { loadBackfill, type NeonBriefRow } from "../../migration/neon-brief-titles/backfill.ts";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

const text = (over: Partial<BriefText> = {}): BriefText => ({
  title: "Native title", description: "A native description.", state: "completed",
  scrubRuleset: "relayhistory-scrub", scrubVersion: 1, fingerprint: "f".repeat(64),
  promptVersion: "session-summary-v3", engineVersion: "session-brief-v3", model: "gpt-5.6-luna",
  messageCount: 3, shortenedMessages: 0, inputTokens: 10, outputTokens: 5, durationMs: 20,
  generatedAt: new Date("2026-11-01T00:00:00Z"), ...over,
});
const neon = (over: Partial<NeonBriefRow> = {}): NeonBriefRow => ({
  org_id: KEY.orgId, workspace_id: KEY.workspaceId, source_provider: KEY.harness, session_id: KEY.sessionId,
  job_id: "0b6f6a39-7d5e-4c0e-9c1e-2f1d8b8f0a01", title: "Neon backfilled title", description: "A Neon description.",
  state: "completed", prompt_version: "session-summary-v3", summary_version: "session-summary-v3", model: "gpt-5.6-luna",
  summary_model_version: "gpt-5.6-luna", generated_at: "2026-10-20T10:00:00.000Z", completed_at: null,
  message_count: "4", shortened_messages: "0", input_tokens: "1", output_tokens: "1", ...over,
});
const target = (pg: PGlite) => ({ query: async (q: string, p?: unknown[]) => ({ rows: (await pg.query(q, p as never)).rows as never[] }) });

/** The directory row as the publication consumer sees it: title, search_text, document JSON. */
async function published(pg: PGlite, key = KEY) {
  const r = await pg.query<{ title: string | null; search_text: string; doc: { title: string } }>(
    `SELECT d.title, d.search_text, sessions.session_brief_document_json(d) AS doc
       FROM sessions.session_directory AS d
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = $4`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId],
  );
  const row = r.rows[0]!;
  return { title: row.title, searchText: row.search_text, documentTitle: row.doc.title };
}

/** Every migration up to (not including) `before`, as the harness orders them, without 043000. */
async function databaseBefore(before: string): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  for (const f of ["supabase/tests/supabase_shim.sql", "supabase/bootstrap/session_directory_search_worker.sql",
                   "supabase/bootstrap/session_token_verifier.sql", "supabase/bootstrap/session_ingest_roles.sql",
                   "supabase/bootstrap/session_read_api_roles.sql"]) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    if (file < before) await db.exec(read(`supabase/migrations/${file}`));
  for (const file of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(read(`src/hoopdb/migrations/${file}`));
  return db;
}

describe("directory projection reads native brief titles only (after 043000)", () => {
  test("a native title wins over the pointer title and reaches search_text and the document", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { title: "Pointer title" });
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    await projectSessionDirectory(hoopDb(pg), KEY);
    const p = await published(pg);
    expect(p.title).toBe("Native title");
    expect(p.searchText).toContain("Native title");
    expect(p.documentTitle).toBe("Native title");
  });

  for (const [label, title] of [["ordinary", "Neon backfilled title"], ["line-break", "Fix login\nand logout"]] as const) {
    test(`an ${label} backfilled title never reaches the directory, search_text or the document; the pointer title stays`, async () => {
      const pg = await database();
      open.push(pg);
      await pointer(pg, { title: "Pointer title" });
      expect((await loadBackfill(target(pg), [neon({ title })], "execute")).outcomes.inserted).toBe(1);
      expect((await pg.query<{ origin: string; title: string }>("SELECT origin, title FROM sessions.session_briefs")).rows).toEqual([{ origin: "neon_backfill", title }]);
      // A re-projection (new evidence, or a controlled re-project) keeps the pointer title.
      await projectSessionDirectory(hoopDb(pg), KEY);
      const p = await published(pg);
      expect(p.title).toBe("Pointer title");
      expect(p.documentTitle).toBe("Pointer title");
      expect(p.searchText.toLowerCase()).not.toContain(title.split("\n")[0]!.toLowerCase());
      // A native brief later replaces the backfill (origin native) and then projects.
      await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
      await projectSessionDirectory(hoopDb(pg), KEY);
      expect((await published(pg)).title).toBe("Native title");
    });
  }

  test("CAUSAL: the projector's SQL without the provenance predicate (the pre-fix body) projects the backfilled title", async () => {
    const source = readFileSync(join(root, "src/hoopdb/session-directory-projector.ts"), "utf8");
    const predicate = " AND engine_version NOT LIKE 'neon-backfill:%'";
    expect(source.split(predicate).length).toBe(2);
    // The same module with only the predicate removed, written to a temp dir (never the source
    // tree). Its two imports are type-only, so it needs no module resolution there.
    expect(source.match(/^import (?!type )/gm)).toBeNull();
    const dir = mkdtempSync(join(tmpdir(), "f2-unfiltered-projector-"));
    const unfilteredPath = join(dir, "session-directory-projector.ts");
    writeFileSync(unfilteredPath, source.replace(predicate, ""));
    try {
      const unfiltered = (await import(unfilteredPath)) as { projectSessionDirectory: typeof projectSessionDirectory };
      for (const title of ["Neon backfilled title", "Fix login\nand logout"]) {
        const pg = await database();
        open.push(pg);
        await pointer(pg, { title: "Pointer title" });
        await loadBackfill(target(pg), [neon({ title })], "execute");
        await unfiltered.projectSessionDirectory(hoopDb(pg), KEY);
        expect((await published(pg)).title).toBe(title);
        await projectSessionDirectory(hoopDb(pg), KEY);
        expect((await published(pg)).title).toBe("Pointer title");
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("directory projection before 043000 (no origin column)", () => {
  test("the projector runs, a native title projects, and the pointer fallback holds", async () => {
    const pg = await databaseBefore("20261105043000");
    open.push(pg);
    expect((await pg.query("SELECT 1 FROM pg_attribute WHERE attrelid = 'sessions.session_briefs'::regclass AND attname = 'origin' AND NOT attisdropped")).rows).toEqual([]);
    await pointer(pg, { title: "Pointer title" });
    await projectSessionDirectory(hoopDb(pg), KEY);
    expect((await published(pg)).title).toBe("Pointer title");
    await recordBrief(hoopDb(pg), KEY, { "m-1": 1 }, { status: "ready", text: text() });
    await projectSessionDirectory(hoopDb(pg), KEY);
    expect((await published(pg)).title).toBe("Native title");
  });
});
