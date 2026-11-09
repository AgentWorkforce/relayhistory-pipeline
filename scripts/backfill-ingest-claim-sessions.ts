// Operator step only (slot 044080). Fills sessions.ingest_claim_sessions from ingest_claim_attempts.records,
// resuming from the policy row's backfill_cursor. Runbook: docs/handoffs/ingest-claim-sessions-044080/operator-packet.md.
// stdin (one JSON line, piped from 1Password, never written to disk): {"ownerUrl": "postgresql://user:password@host:port/db"}
// No query parameters except sslmode=verify-full (src/shared/owner-pg-config.ts).
// Flags: [--batch-size 500] [--statement-timeout-ms 30000] [--max-batches N] [--dry-run]
// Output: one JSON line of counts and booleans only (never a claim token, cursor, tenant id or record value).
// Completion (backfill_complete_at) is marked only when the walk reaches the end while write_enabled is true;
// otherwise the report says why it was not marked and the exit code is still 0.
import { readFileSync } from "node:fs";
import pg from "pg";
import { backfillClaimSessions, ClaimSessionsBackfillError } from "../src/session-ingest/claim-sessions-backfill.ts";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";

// A closed, redacted reason only: a pg error message or stack can carry the DB host.
const redactedReason = (e: unknown): string => {
  const code = (e as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? "sqlstate_" + code
    : (e as { name?: unknown })?.name === "OwnerUrlError" && typeof code === "string" ? code
    : e instanceof ClaimSessionsBackfillError && /^[a-z_]{1,64}$/.test(e.code) ? e.code
    : e instanceof Error && /^[a-z_]{1,64}$/.test(e.message) ? e.message : "internal_error";
};

let client: pg.Client | undefined;
try {
  const args = process.argv.slice(2);
  const known = new Set(["--batch-size", "--statement-timeout-ms", "--max-batches", "--dry-run"]);
  for (let i = 0; i < args.length; i++) {
    if (!known.has(args[i]!)) throw Error("unknown_flag");
    if (args[i] !== "--dry-run") {
      if (args[i + 1] === undefined || args[i + 1]!.startsWith("--")) throw Error("flag_value_required");
      i++;
    }
  }
  const flag = (n: string) => { const i = args.indexOf("--" + n); return i < 0 ? undefined : args[i + 1]; };
  const int = (v: string | undefined, d: number | undefined, code: string) => {
    if (v === undefined) return d;
    if (!/^[0-9]{1,7}$/.test(v)) throw Error(code);
    return Number(v);
  };
  const batchSize = int(flag("batch-size"), 500, "batch_size_invalid")!;
  const statementTimeoutMs = int(flag("statement-timeout-ms"), 30000, "statement_timeout_invalid")!;
  const maxBatches = int(flag("max-batches"), undefined, "max_batches_invalid");
  const dryRun = args.includes("--dry-run");
  let { ownerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0]!) as { ownerUrl?: string };
  if (typeof ownerUrl !== "string" || !ownerUrl) throw Error("owner_url_required");
  // Discrete pinned fields only (no connectionString), verified TLS, no PG* env or .pgpass fallback.
  // The client-level timeout is a backstop; each batch also sets its own statement_timeout.
  client = new pg.Client({ ...ownerClientConfig(ownerUrl, "96bulls-claim-sessions-backfill"), statement_timeout: statementTimeoutMs + 5000 });
  ownerUrl = "";
  client.on("error", () => {});
  await client.connect();
  const report = await backfillClaimSessions({ sql: { query: (s, p) => client!.query(s, p) }, batchSize, statementTimeoutMs, maxBatches, dryRun });
  console.log(JSON.stringify(report));
} catch (e) {
  console.log(JSON.stringify({ refused: redactedReason(e) }));
  process.exitCode = 1;
} finally { await client?.end().catch(() => {}); }
