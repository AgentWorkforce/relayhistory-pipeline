// Operator step only, DRAFT (docs/design/ingest-bookkeeping-cleanup.md). Nothing schedules this.
// It requires the draft SQL to have been registered and applied, which needs Khaliq's explicit OK.
// stdin (one JSON line, piped from 1Password, never written to disk):
//   {"ownerUrl": "postgresql://user:password@host:port/db"}
// No query parameters except sslmode=verify-full (src/shared/owner-pg-config.ts).
// Flags: --phase decisions|seats|custody|frozen [--limit N] [--max-pages 1] [--pause-ms 500]
//        [--timeout-ms 3000] [--after <cursor JSON>] [--execute]
// The default is DRY-RUN (frozen: always a read-only census). --execute deletes only if the owner has
// ALSO enabled the phase in sessions.ingest_bookkeeping_cleanup_settings; otherwise SQL refuses (55000).
// Output is one sanitized JSON line: counts and cursors, or {"refused": <closed reason>}. Never a host,
// message or stack.
import { readFileSync } from "node:fs";
import pg from "pg";
import { parseCleanupArgs, redactedReason, runBookkeepingCleanup } from "../../src/session-ingest/bookkeeping-cleanup.ts";
import { ownerClientConfig } from "../../src/shared/owner-pg-config.ts";
let client: pg.Client | undefined;
try {
  const args = parseCleanupArgs(process.argv.slice(2));
  let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]!) as { ownerUrl?: string };
  if (typeof ownerUrl !== "string" || !ownerUrl) throw Object.assign(Error("owner_url_required"), { name: "OwnerUrlError", code: "owner_url_required" });
  // Discrete pinned fields only: no connectionString, verified TLS against the pinned CA, no PG* env or .pgpass.
  client = new pg.Client(ownerClientConfig(ownerUrl, "96bulls-bookkeeping-cleanup"));
  ownerUrl = "";
  client.on("error", () => {});
  await client.connect();
  const c = client;
  const sql = { transaction: async <T>(work: (q: (s: string, p: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>) => Promise<T>) => {
    await c.query("BEGIN");
    try { const r = await work((s, p) => c.query(s, p as never) as never); await c.query("COMMIT"); return r; }
    catch (e) { await c.query("ROLLBACK").catch(() => {}); throw e; }
  } };
  console.log(JSON.stringify(await runBookkeepingCleanup({ sql, ...args })));
} catch (e) {
  console.log(JSON.stringify({ refused: redactedReason(e) }));
  process.exitCode = 1;
} finally { await client?.end().catch(() => {}); }
