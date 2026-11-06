/** Spawns the real scripts/frozen-source-copy.ts under frozen-seal-preload.ts and parses its output. */
import { spawnSync } from "node:child_process";
import { readFileSync, existsSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ORGS } from "./frozen-seal-fixture.ts";

export const SCRIPT = fileURLToPath(new URL("../../../../scripts/frozen-source-copy.ts", import.meta.url));
const PRELOAD = fileURLToPath(new URL("./frozen-seal-preload.ts", import.meta.url));
const NEON_URL = "postgresql://neondb_owner:synthetic-not-authorized@ep-floral-pond-apkvat38.synthetic.invalid/neondb";

export interface RunResult { status: number | null; stdout: string; stderr: string; lines: any[]; log: { statements: any[]; clients: number; live: number[]; neonPages: number; wrote: boolean } }
export function runScript(args: string[], backendEnv: Record<string, string>, logPath: string, extra: Record<string, string> = {}): RunResult {
  if (existsSync(logPath)) rmSync(logPath);
  const r = spawnSync(process.execPath, ["--preload", PRELOAD, SCRIPT, ...args], {
    input: JSON.stringify({ neonOwnerUrl: NEON_URL }) + "\n", encoding: "utf8", timeout: 120000,
    env: { PATH: process.env.PATH!, SUPABASE_CLI: "/synthetic-supabase-refuse-network", SEAL_TEST_LOG: logPath, ...backendEnv, ...extra },
  });
  const lines = r.stdout.split("\n").filter((x) => x.startsWith("{")).map((x) => JSON.parse(x));
  const log = existsSync(logPath) ? JSON.parse(readFileSync(logPath, "utf8")) : { statements: [], clients: 0, live: [], neonPages: 0, wrote: false };
  return { status: r.status, stdout: r.stdout, stderr: r.stderr, lines, log };
}

/** Statements a read-only timer/plan may issue. */
const ALLOWED = /^(SELECT |BEGIN READ ONLY$|BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY$|ROLLBACK$|SET SESSION ROLE postgres$|SET default_transaction_read_only = on$)/;
export function readOnlyViolations(statements: any[]): string[] {
  const bad: string[] = [];
  for (const s of statements) {
    if (!ALLOWED.test(s.sql)) bad.push("statement:" + s.sql.slice(0, 40));
    else if (s.sql.startsWith("SELECT ") && !(s.inTx && s.txReadOnly) && !s.sessionReadOnly) bad.push("select-outside-read-only:" + s.side + ":" + s.sql.slice(0, 40));
    if (s.probe !== undefined && s.sql.startsWith("SELECT ") && s.probe !== "on") bad.push("probe:" + s.sql.slice(0, 40));
  }
  return bad;
}

/** True when the text names any fixture org identifier. */
export const leaksOrgIds = (text: string) => Object.keys(ORGS).some((o) => text.includes(o));

// Writes that land on an org the serial seal has already verified. The unchanged serial seal
// still writes a seal for each (pre-existing gaps); the fenced seal refuses every one.
export const ESCAPES: { name: string; hook: Record<string, string>; fenced: RegExp }[] = [
  // A permitted selection that only rewrites delivery_records (its deletion row already existed); head mirrored.
  { name: "records-only control", hook: { SEAL_TEST_WRITE: "disable", SEAL_TEST_WRITE_WHEN_ORG: "org-z", SEAL_TEST_WRITE_WHEN_ORG_TARGET: "org-a", SEAL_TEST_WRITE_MIRROR: "1" }, fenced: /^source_changed/ },
  // A zero-session reset: only session_resets changes, which the serial seal never compares.
  { name: "zero-session reset", hook: { SEAL_TEST_WRITE: "empty-reset", SEAL_TEST_WRITE_WHEN_ORG: "org-z", SEAL_TEST_WRITE_WHEN_ORG_TARGET: "Org-B", SEAL_TEST_WRITE_MIRROR: "1" }, fenced: /^(reset_copy_mismatch|source_changed)/ },
  // The cutover leaves phase active (halted): the serial seal checks the phase only at start.
  { name: "cutover halted", hook: { SEAL_TEST_WRITE: "halt", SEAL_TEST_WRITE_WHEN_ORG: "org-z" }, fenced: /^source_changed/ },
  // The owner disables a freeze guard (the fence's premise); nothing else changes.
  { name: "disabled freeze guard", hook: { SEAL_TEST_WRITE: "unguard", SEAL_TEST_WRITE_WHEN_ORG: "org-z" }, fenced: /^source_changed/ },
];
