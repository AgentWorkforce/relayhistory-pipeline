/**
 * `neon-brief-titles` CLI: backfill Cloud's session-list brief titles from Neon into
 * Supabase sessions.session_briefs (feature F2), and verify them. backfill.ts has the rules.
 *
 *   bun src/migration/neon-brief-titles/main.ts [--source-fixture=<file.json>]
 *       dry run (default): every batch runs and ROLLBACKs; prints counts only
 *   ... --execute --expect-supabase-ref=<ref>   (or --expect-target-host=<host> for a rehearsal)
 *       writes (target-guard.ts); the live run is the lead's, with the user's OK
 *   ... --verify
 *       read-only parity: every Cloud-visible Neon title against the title SP serves, with
 *       the read path's resolution; exit 2 when any is missing or different, or when the
 *       verify output does not cover every key with exactly one well-formed row. Keys a
 *       well-formed row reports without a live pointer are excluded from the comparison
 *       (no_pointer), which is not a proof of copy completeness
 *
 * Env, injected at run time (e.g. `op run`; never a file on disk):
 *   NEON_BRIEF_TITLES_SOURCE_URL   Neon (relayhistory_readonly) or the frozen copy; the
 *                                  session is forced READ ONLY UTC. Not needed with
 *                                  --source-fixture (a JSON array of NEON_SELECT_SQL rows).
 *   BULLS_TARGET_URL               Supabase as the schema owner (writes session_briefs).
 * The report is JSON on stdout: counts, skip reasons and (verify) up to 20 keys per class.
 * No title, description or URL is ever printed.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { sessionReadOnly } from "../neon-s3-sync/neon-reader.ts";
import { guardFromArgv } from "../target-guard.ts";
import { extractNeon, loadBackfill, verifyBackfill, verifyPassed, type NeonBriefRow } from "./backfill.ts";

const env = (k: string) => process.env[k]?.trim() || undefined;
const arg = (name: string) => process.argv.find((v) => v.startsWith(`--${name}=`))?.slice(name.length + 3);

async function source(): Promise<{ rows: NeonBriefRow[]; url?: string }> {
  const fixture = arg("source-fixture");
  if (fixture) {
    const rows = JSON.parse(readFileSync(fixture, "utf8")) as unknown;
    if (!Array.isArray(rows)) throw new Error("--source-fixture must hold a JSON array");
    return { rows: rows as NeonBriefRow[] };
  }
  const url = env("NEON_BRIEF_TITLES_SOURCE_URL");
  if (!url) throw new Error("NEON_BRIEF_TITLES_SOURCE_URL (or --source-fixture) is required");
  const client = new pg.Client({ connectionString: url, application_name: "96bulls-neon-brief-titles" });
  await client.connect();
  try {
    return { rows: await extractNeon(await sessionReadOnly(client)), url };
  } finally {
    await client.end();
  }
}

export async function main(argv = process.argv): Promise<void> {
  const target = env("BULLS_TARGET_URL");
  if (!target) throw new Error("BULLS_TARGET_URL is required");
  const verify = argv.includes("--verify");
  if (verify && argv.includes("--execute")) throw new Error("--verify is read-only; it does not take --execute");
  const src = await source();
  const mode = verify ? "dry-run" : guardFromArgv(argv, target, src.url);
  const client = new pg.Client({ connectionString: target, application_name: "96bulls-neon-brief-titles" });
  await client.connect();
  try {
    if (verify) {
      await client.query("SET default_transaction_read_only = on");
      await client.query("SET statement_timeout = 20000");
      const report = await verifyBackfill(client, src.rows);
      process.stdout.write(`${JSON.stringify({ verify: report, passed: verifyPassed(report) })}\n`);
      // Any Cloud-visible Neon title SP would not serve exactly fails the run.
      if (!verifyPassed(report)) process.exitCode = 2;
      return;
    }
    process.stdout.write(`${JSON.stringify({ backfill: await loadBackfill(client, src.rows, mode) })}\n`);
  } finally {
    await client.end();
  }
}

if (import.meta.main) {
  main().catch((error: unknown) => {
    // The message only: pg errors can quote values, so the code and a fixed text are printed.
    const code = (error as { code?: string })?.code;
    process.stderr.write(`${JSON.stringify({ error: code ?? (error instanceof Error ? error.message : "failed") })}\n`);
    process.exitCode = 1;
  });
}
