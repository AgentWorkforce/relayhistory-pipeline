/**
 * Operator CLI for the bounded generation overlap (migration 20261105043885; docs/design/zero-503-schema-windows.md).
 * Owner, production Supabase via the CLI temp login + SET ROLE postgres, exactly as scripts/ingest-generation-rebind.ts.
 *   bun scripts/ingest-generation-overlap.ts status
 *   bun scripts/ingest-generation-overlap.ts open --from <PASS64hex> --to <target64hex> --ttl-seconds <60..14400> \
 *       --approval-sha <approval.json sha256> [--admit-during-apply]
 * admitDuringApply is OFF unless --admit-during-apply is given; turn it on only after a full-union rehearsal
 * under concurrent synthetic uploads (docs/design/zero-503-schema-windows.md section 4).
 *   bun scripts/ingest-generation-overlap.ts close --from <64hex> --to <64hex>
 * Run `open` immediately BEFORE the owner-apply (readiness must still be PASS for --from). The rebind closes the
 * window itself; `close` is the owner's early stop. Prints only the window record. Opening a window on a real
 * database is part of the schema window and needs the same explicit approval as the apply.
 */
import { execFileSync } from "node:child_process";
import pg from "pg";
import { parseCliLogin } from "../src/shared/supabase-cli-login.ts";
import { requiredSupabaseProjectRef } from "../src/shared/deploy-target.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { closeGenerationOverlap, generationOverlapStatus, openGenerationOverlap, validateOverlapOpen } from "../src/hoopdb/ingest-generation-overlap.ts";

const HOST = "aws-0-us-east-1.pooler.supabase.com";
const a = process.argv.slice(2), cmd = a[0], get = (k: string) => { const i = a.indexOf(k); return i >= 0 ? a[i + 1] : undefined; };
if (cmd !== "status" && cmd !== "open" && cmd !== "close") { console.log(JSON.stringify({ status: "refused", code: "usage" })); process.exit(2); }
const from = get("--from") ?? "", to = get("--to") ?? "";
let openInput: Parameters<typeof openGenerationOverlap>[1] | undefined;
if (cmd === "open") {
  const admit = a.includes("--admit-during-apply");
  openInput = { from, to, ttlSeconds: Number(get("--ttl-seconds")), admitDuringApply: admit, approvalSha256: get("--approval-sha") ?? "" };
  try { validateOverlapOpen(openInput); } catch (e) { console.log(JSON.stringify({ status: "refused", code: (e as { code?: string }).code })); process.exit(2); }
}
const REF = requiredSupabaseProjectRef(process.env);
const login = parseCliLogin(execFileSync(process.env.SUPABASE_CLI ?? "/home/khaliqgant/.local/bin/supabase", ["db", "dump", "--linked", "--project-ref", REF, "--dry-run"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 30000 }), REF);
const c = new pg.Client({ host: HOST, port: 5432, database: "postgres", user: "cli_login_postgres." + REF, password: login.password,
  ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: HOST }, options: "-c statement_timeout=30000 -c lock_timeout=5000", application_name: "generation-overlap" });
login.password = "";
await c.connect();
try {
  await c.query("SET SESSION ROLE postgres");
  if (cmd === "status") console.log(JSON.stringify({ window: await generationOverlapStatus(c) }));
  else if (cmd === "open") console.log(JSON.stringify({ window: await openGenerationOverlap(c, openInput!) }));
  else console.log(JSON.stringify(await closeGenerationOverlap(c, { from, to, reason: "owner" })));
} catch (e) { console.log(JSON.stringify({ status: "refused", code: (e as { code?: string }).code ?? "error" })); process.exitCode = 1; }
finally { await c.end(); }
