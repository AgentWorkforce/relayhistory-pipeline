// Operator step only, DRAFT (docs/design/ingest-bookkeeping-cleanup.md). Nothing schedules this.
// It requires the draft SQL to have been registered and applied, which needs Khaliq's explicit OK.
// stdin (one JSON line, piped from 1Password, never written to disk): {"ownerUrl": "..."}
// Flags: --phase decisions|seats|custody|frozen [--limit N] [--max-pages 1] [--pause-ms 500]
//        [--timeout-ms 3000] [--after <cursor JSON>] [--execute]
// The default is DRY-RUN (frozen: always a read-only census). --execute deletes only if the owner has
// ALSO enabled the phase in sessions.ingest_bookkeeping_cleanup_settings; otherwise SQL refuses (55000).
// Output is one sanitized JSON report: counts and the resume cursor only.
import { readFileSync } from "node:fs";
import pg from "pg";
import { runBookkeepingCleanup, type CleanupPhase } from "../../src/session-ingest/bookkeeping-cleanup.ts";
const args = process.argv.slice(2), flag = (n: string) => { const i = args.indexOf("--" + n); return i < 0 ? undefined : args[i + 1]; };
const phase = flag("phase") as CleanupPhase | undefined, execute = args.includes("--execute");
if (!phase || !["decisions", "seats", "custody", "frozen"].includes(phase)) throw Error("phase_required");
const limit = Number(flag("limit") ?? (phase === "decisions" ? "2000" : "1000")), maxPages = Number(flag("max-pages") ?? "1");
const pauseMs = Number(flag("pause-ms") ?? "500"), statementTimeoutMs = Number(flag("timeout-ms") ?? "3000");
const afterText = flag("after"), after = afterText === undefined ? null : JSON.parse(afterText) as unknown;
let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]!) as { ownerUrl?: string };
if (typeof ownerUrl !== "string" || !ownerUrl) throw Error("owner_url_required");
const client = new pg.Client({ connectionString: ownerUrl, ssl: { rejectUnauthorized: true }, application_name: "96bulls-bookkeeping-cleanup" });
ownerUrl = "";
await client.connect();
try {
  const sql = { transaction: async <T>(work: (q: (s: string, p: unknown[]) => Promise<{ rows: Record<string, unknown>[] }>) => Promise<T>) => {
    await client.query("BEGIN");
    try { const r = await work((s, p) => client.query(s, p as never) as never); await client.query("COMMIT"); return r; }
    catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
  } };
  const report = await runBookkeepingCleanup({ sql, phase, limit, maxPages, execute, pauseMs, statementTimeoutMs, after });
  console.log(JSON.stringify(report));
} finally { await client.end(); }
