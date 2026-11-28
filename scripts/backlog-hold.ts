/**
 * The backlog-ops hold-lifecycle tool (contract: src/session-ingest/hold-lifecycle.ts; run only by scripts/backlog-ops.ts
 * with fixed argv and its inputs on stdin):
 *
 *   bun scripts/backlog-hold.ts dry --sha <sha> --org <org> ...                         stdin {"dbUrl"}
 *   bun scripts/backlog-hold.ts install|seal|rollback --sha <sha> --dry-run <id> --org <org> ...
 *       stdin {"dbUrl", "approved": [...], "full"?: {...} (seal), "evidence"?: <hex64> (rollback)}
 *       Every execute is refused unless BACKLOG_OPS_HOLD_EXECUTE_ENABLED (backlog-ops.ts; false in this tree).
 *
 * The dry's sessions are read-only (default_transaction_read_only=on). An execute's statements run in one READ
 * COMMITTED transaction per org (hold-lifecycle.ts). Output: one JSON line, counts, codes and hashes only; refusals as
 * {"refused": <code>}. Nothing is written to stderr.
 */
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import pg from "pg";
import { ownerClientConfig } from "../src/shared/owner-pg-config.ts";
import { BACKLOG_OPS_HOLD_EXECUTE_ENABLED } from "../src/session-ingest/backlog-ops.ts";
import { HOLD_LIFECYCLE_CONCURRENCY, HOLD_TOOL_REFUSALS, holdRun, type HoldSqlSession, type HoldStep } from "../src/session-ingest/hold-lifecycle.ts";
import { waveOrgList } from "../src/session-ingest/org-wave.ts";
import { sessionPool } from "./backlog-marker.ts";

const fail = (code: string): never => { throw Object.assign(new Error(code), { code }); };
const STATEMENT_TIMEOUT_MS = 120_000, LOCK_TIMEOUT_MS = 30_000;

export function parseHoldArgs(argv: readonly string[]): { step: HoldStep; sha: string; dryRun: string | null; orgs: string[] } {
  const [cmd, ...rest] = argv;
  if (cmd !== "dry" && cmd !== "install" && cmd !== "seal" && cmd !== "rollback") return fail("usage");
  let values;
  try {
    ({ values } = parseArgs({ args: [...rest], options: { sha: { type: "string" }, "dry-run": { type: "string" }, org: { type: "string", multiple: true } },
      strict: true, allowPositionals: false }));
  } catch { return fail("usage"); }
  if (!/^[0-9a-f]{40}$/u.test(values.sha ?? "")) fail("usage");
  const dryRun = values["dry-run"] ?? null;
  if ((cmd === "dry") !== (dryRun === null) || (dryRun !== null && !/^[1-9][0-9]{0,19}$/u.test(dryRun))) fail("usage");
  let orgs: string[];
  try { orgs = waveOrgList(values.org ?? []); } catch (e) { return fail((e as { code: string }).code); }
  return { step: cmd, sha: values.sha!, dryRun, orgs };
}

export function holdStdin(line: string, step: HoldStep): { dbUrl: string; approved?: unknown; full?: unknown; evidence?: unknown } {
  if (line.length > 256 * 1024) fail("stdin_too_large");
  let p: unknown;
  try { p = JSON.parse(line); } catch { return fail("stdin_invalid"); }
  const keys = ["dbUrl", ...(step === "dry" ? [] : ["approved"]), ...(step === "seal" ? ["full"] : []), ...(step === "rollback" ? ["evidence"] : [])];
  if (!p || typeof p !== "object" || Array.isArray(p) || Object.keys(p).length !== keys.length || !keys.every((k) => Object.hasOwn(p as object, k))) fail("stdin_invalid");
  const o = p as Record<string, unknown>;
  if (typeof o.dbUrl !== "string" || o.dbUrl === "") fail("db_url_required");
  return o as { dbUrl: string };
}

/** One pg client as a HoldSqlSession. */
export function pgHoldSession(client: { query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }> }): HoldSqlSession {
  const begin = async (access: string) => {
    await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED ${access}`);
    await client.query(`SET LOCAL statement_timeout = ${STATEMENT_TIMEOUT_MS}`);
    await client.query(`SET LOCAL lock_timeout = ${LOCK_TIMEOUT_MS}`);
  };
  return {
    async read(text, params) {
      await begin("READ ONLY");
      try { const rows = (await client.query(text, [...params])).rows; await client.query("COMMIT"); return rows; }
      catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
    },
    async tx(fn) {
      await begin("READ WRITE");
      let out;
      try { out = await fn(async (text, params) => (await client.query(text, [...params])).rows); }
      catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
      await client.query("COMMIT");
      return out;
    },
  };
}

export async function main(argv: readonly string[], deps: { stdin?: () => string } = {}) {
  const a = parseHoldArgs(argv);
  if (a.step !== "dry" && BACKLOG_OPS_HOLD_EXECUTE_ENABLED !== true) fail("hold_execute_held");
  const input = holdStdin((deps.stdin ?? (() => readFileSync(0, "utf8")))().split("\n")[0] ?? "", a.step);
  const dbUrl = input.dbUrl;
  input.dbUrl = "";
  const pool = sessionPool(async () => {
    const b = ownerClientConfig(dbUrl, "96bulls-backlog-hold");
    const c = new pg.Client({ ...b, ...(a.step === "dry" ? { options: `${b.options ?? ""} -c default_transaction_read_only=on`.trim() } : {}), keepAlive: true });
    c.on("error", () => {});
    await c.connect();
    return { session: pgHoldSession(c) as never, close: () => c.end() };
  }, HOLD_LIFECYCLE_CONCURRENCY);
  try {
    const summary = await holdRun({ step: a.step, orgs: a.orgs, sha: a.sha, ...(a.dryRun ? { dryRun: a.dryRun } : {}),
      ...(input.approved !== undefined ? { approved: input.approved as never } : {}), ...(input.full !== undefined ? { full: input.full as never } : {}),
      ...(input.evidence !== undefined ? { evidence: input.evidence as never } : {}),
      withSession: pool.withSession as never, now: () => Date.now() });
    return { summary, ok: a.step === "dry" ? summary.done_count > 0 : summary.complete };
  } finally { await pool.close(); }
}

if (import.meta.main) {
  main(process.argv.slice(2))
    .then(({ summary, ok }) => { process.stdout.write(JSON.stringify(summary) + "\n"); process.exitCode = ok ? 0 : 1; })
    .catch((error) => {
      const c = (error as { code?: unknown })?.code;
      process.stdout.write(JSON.stringify({ refused: typeof c === "string" && (HOLD_TOOL_REFUSALS as readonly string[]).includes(c) ? c : "internal_error" }) + "\n");
      process.exitCode = 1;
    });
}
