/**
 * Operator CLI for the counts-only presence of ONE session (src/session-ingest/session-presence.ts), run only by the
 * guarded backlog-ops workflow as mode presence-dry (scripts/backlog-ops.ts, docs/backlog-ops-workflow.md).
 *
 *   stdin (one JSON line from the parent step, never argv, a file or a log):
 *     {"dbUrl": "postgresql://user:password@host:port/db?sslmode=verify-full", "session": "<session id>"}
 *
 *   bun scripts/session-presence.ts --dry-run --statement-timeout-ms 60000 --total-budget-ms 180000 [--org <org uuid> ...]
 *
 * Dry only: there is no --execute. One REPEATABLE READ READ ONLY transaction (SHOW transaction_read_only checked,
 * always rolled back) on a session with default_transaction_read_only=on. It reads no S3 and ignores any AWS_*
 * variable. Output: one JSON line of counts only (never the session id, an org id, a key or a URL), or
 * {"refused": <code>} with exit code 1.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { presenceClientOptions, sessionPresenceMain } from "../src/session-ingest/session-presence.ts";

if (import.meta.main) {
  let stdin = "";
  try { stdin = readFileSync(0, "utf8"); } catch { stdin = ""; }
  const { line, code } = await sessionPresenceMain(process.argv.slice(2), stdin, async (dbUrl) => {
    // Discrete pinned fields only (verified TLS, no PG* env or .pgpass fallback), and read-only by default. The owner
    // config's own options (idle_in_transaction_session_timeout) are kept and the read-only default is appended.
    const base = ownerClientConfig(dbUrl, "96bulls-session-presence");
    const client = new pg.Client({ ...base, options: presenceClientOptions(base.options) });
    client.on("error", () => {});
    await client.connect();
    return {
      query: async (text: string, params?: unknown[]) => ({ rows: (await client.query(text, params)).rows }),
      end: () => client.end(),
    };
  });
  process.stdout.write(line + "\n");
  process.exitCode = code;
}
