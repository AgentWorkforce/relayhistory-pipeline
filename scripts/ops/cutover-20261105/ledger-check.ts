// Read-only: prove live ledger = approved base (175cd1eb) + exactly the 13 planned migrations.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
const WT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104";
const pg = (await import(WT + "/node_modules/pg/lib/index.js")).default;
const { parseCliLogin } = await import(WT + "/scripts/schema-release-owner-operator-plan.ts");
const { migrationLedgerSha256, MIGRATION_LEDGER, generation } = await import(WT + "/src/hoopdb/migrate.ts");
const { ROLE_VALIDATION_TABLE } = await import(WT + "/src/shared/db.ts");
const { SUPABASE_PROD_CA_2021 } = await import(WT + "/src/session-pipeline/supabase-ca.ts");
const plan = JSON.parse(readFileSync(process.argv[2], "utf8"));
const pending = plan.inventory.filter((r: any) => r.pending);
const raw = execFileSync("/home/khaliqgant/.local/bin/supabase", ["db","dump","--linked","--project-ref","pyamkrhoyszgzirqqffq","--dry-run"], { cwd: WT, encoding: "utf8", stdio: ["ignore","pipe","pipe"], timeout: 30000 });
const login = parseCliLogin(raw);
const c = new pg.Client({ host: "aws-0-us-east-1.pooler.supabase.com", port: 5432, database: "postgres", user: "cli_login_postgres.pyamkrhoyszgzirqqffq", password: login.password, ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: "aws-0-us-east-1.pooler.supabase.com" }, connectionTimeoutMillis: 10000, options: "-c statement_timeout=15000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=20000", application_name: "96bulls-bulls-migrator-ledger-readonly" });
login.password = "";
await c.connect();
try {
  await c.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); await c.query("SET LOCAL ROLE postgres");
  const rows = (await c.query(`SELECT name, sha256 FROM ${MIGRATION_LEDGER}`)).rows;
  const marker = (await c.query(`SELECT generation,status,updated_at FROM ${ROLE_VALIDATION_TABLE}`)).rows;
  const locks = (await c.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name LIKE '96bulls-owner-schema%'")).rows[0];
  await c.query("ROLLBACK");
  const live = new Map<string,string>(rows.map((r: any) => [r.name, r.sha256]));
  const base = new Map(live);
  const mismatch: string[] = [];
  for (const p of pending) { if (live.get(p.name) !== p.sha256) mismatch.push(p.name); base.delete(p.name); }
  console.log(JSON.stringify({ at: new Date().toISOString(), sqlSelectOnly: true, liveRows: live.size, pendingPlanned: pending.length, pendingMismatch: mismatch,
    liveLedgerSha256: migrationLedgerSha256(live), baseLedgerSha256: migrationLedgerSha256(base), baseMatchesApproved175: migrationLedgerSha256(base) === plan.ledgerSha256,
    sourceGeneration: await generation(), marker, ownerApplySessions: locks.n }));
} finally { await c.end(); }
