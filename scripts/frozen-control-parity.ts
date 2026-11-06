/**
 * P1 privacy/control parity census and seed-only / qualified-repair operator.
 * Logic and refusal rules: src/migration/frozen-control-parity.ts. No record copy, seal, flag or Neon write.
 *
 * Usage (stdin: ONE JSON line {"neonOwnerUrl": "..."}; Supabase owner = the CLI temp login, SET ROLE postgres,
 * exactly as scripts/frozen-source-copy.ts):
 *   bun scripts/frozen-control-parity.ts --census [--seed-pending-missing]
 *        read-only on BOTH sides (session READ ONLY + READ ONLY transactions). Prints aggregate counts,
 *        set hashes and the dry-run plan (planSha256). This is the dry run.
 *   bun scripts/frozen-control-parity.ts --apply --expect-plan <planSha256> [--seed-pending-missing]
 *        re-reads both sides under lock, refuses unless the plan hash is unchanged, seeds missing privacy rows
 *        and qualified missing heads exactly, then repairs qualified stale heads. Prints aggregate only.
 * Output never contains identifiers, payloads or credentials; errors print a refusal code or SQLSTATE only.
 *   bun scripts/frozen-control-parity.ts --handoff-census [--settle-seconds N]
 *        OPTION B dry run, read-only: two Neon snapshots N s apart (default 120, 60..900) must be identical,
 *        then the exact-sync plan (pending heads included). Run only with the fence closed (mirror ON, unsealed).
 *   bun scripts/frozen-control-parity.ts --handoff-sync --expect-plan <planSha256> [--settle-seconds N]
 *        exact sync under the fence; see src/migration/frozen-control-handoff.ts for the proof and refusals.
 * Exit codes: 0 ok and full parity; 3 ok but HELD categories remain (operator decision); 1 refused/failed; 64 usage.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import pg from "pg";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { applySeedRepair, census, OPERATOR_APP, ParityRefusal } from "../src/migration/frozen-control-parity.ts";
import { handoffCensus, handoffSync } from "../src/migration/frozen-control-handoff.ts";

const REF = "pyamkrhoyszgzirqqffq", HOST = "aws-0-us-east-1.pooler.supabase.com", CLI = process.env.SUPABASE_CLI ?? "/home/khaliqgant/.local/bin/supabase";
const args = process.argv.slice(2);
const usage = (): never => { console.error("usage: --census [--seed-pending-missing] | --apply --expect-plan <sha256> [--seed-pending-missing] | --handoff-census [--settle-seconds N] | --handoff-sync --expect-plan <sha256> [--settle-seconds N]"); process.exit(64); };
const MODES = ["--census", "--apply", "--handoff-census", "--handoff-sync"] as const;
const known = new Set<string>([...MODES, "--expect-plan", "--seed-pending-missing", "--settle-seconds"]);
const picked = MODES.filter((m) => args.includes(m));
if (picked.length !== 1) usage();
const mode = picked[0]!.slice(2) as "census" | "apply" | "handoff-census" | "handoff-sync";
const ei = args.indexOf("--expect-plan"), expect = ei >= 0 ? args[ei + 1] ?? "" : "";
const si = args.indexOf("--settle-seconds"), settleRaw = si >= 0 ? args[si + 1] ?? "" : "120";
if (args.some((a, i) => !known.has(a) && !(ei >= 0 && i === ei + 1) && !(si >= 0 && i === si + 1))) usage();
const needsPlan = mode === "apply" || mode === "handoff-sync", handoff = mode.startsWith("handoff");
if (needsPlan ? !/^[0-9a-f]{64}$/u.test(expect) : ei >= 0) usage();
if (!/^[0-9]{2,3}$/u.test(settleRaw) || Number(settleRaw) < 60 || Number(settleRaw) > 900 || (si >= 0 && !handoff)) usage();
if (handoff && args.includes("--seed-pending-missing")) usage();
const opts = { seedPendingMissing: args.includes("--seed-pending-missing"), settleMs: Number(settleRaw) * 1000 };

// Usage is validated before any credential is read.
const { neonOwnerUrl } = JSON.parse(readFileSync(0, "utf8").split("\n")[0] || "{}");
if (!/^postgresql:\/\/neondb_owner:.+ep-floral-pond-apkvat38/.test(neonOwnerUrl ?? "")) { console.error("neonOwnerUrl (production owner) required on stdin"); process.exit(64); }
const log = (m: string, f: Record<string, unknown> = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), m, mode, ...f }));

async function neon() {
  const c = new pg.Client({ connectionString: neonOwnerUrl, ssl: { rejectUnauthorized: true }, application_name: OPERATOR_APP + "-readonly", statement_timeout: 60000 });
  await c.connect();
  // Neon is never written by this tool, in either mode.
  try { await c.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"); return c; }
  catch (error) { await c.end().catch(() => {}); throw error; }
}
async function supa(readOnly: boolean) {
  const login = parseCliLogin(execFileSync(CLI, ["db", "dump", "--linked", "--project-ref", REF, "--dry-run"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 }));
  const c = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: `cli_login_postgres.${REF}`, password: login.password,
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=120000 -c lock_timeout=5000", application_name: OPERATOR_APP });
  login.password = "";
  try {
    await c.connect(); await c.query("SET SESSION ROLE postgres");
    if (readOnly) await c.query("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY");
    return c;
  } catch (error) { await c.end().catch(() => {}); throw error; }
}

async function main() {
  const n = await neon();
  try {
    const s = await supa(mode === "census" || mode === "handoff-census");
    try {
      if (mode === "handoff-census") {
        const c = await handoffCensus(n, s, opts);
        log("handoff census", { handoff: c });
        return c.stable && c.exactParity && !c.handoff.writes && !c.handoff.blocked.length ? 0 : 3;
      }
      if (mode === "handoff-sync") {
        const c = await handoffSync(n, s, expect, opts);
        log("handoff synced", { handoff: c });
        return c.exactParity && !c.handoff.writes ? 0 : 3;
      }
      if (mode === "census") {
        const c = await census(n, s, opts);
        log("census", { census: c });
        return c.parity.held || c.plan.writes ? 3 : 0;
      }
      const r = await applySeedRepair(n, s, expect, opts);
      log("applied", { seeded: r.seeded, repairs: r.repairs, after: r.after });
      return Object.keys(r.repairs.refused).length || r.after.parity.held || r.after.plan.writes ? 3 : 0;
    } finally { await s.end().catch(() => {}); }
  } finally { await n.end().catch(() => {}); }
}
main().then((code) => { process.exitCode = code; }, (e) => {
  // Never print messages: pg error details can carry key values. Refusal code or SQLSTATE only.
  const code = e instanceof ParityRefusal ? e.code : typeof e?.code === "string" && /^[0-9A-Z]{5}$/u.test(e.code) ? "sqlstate_" + e.code : "failed";
  log("STOPPED", { code }); process.exitCode = 1;
});
