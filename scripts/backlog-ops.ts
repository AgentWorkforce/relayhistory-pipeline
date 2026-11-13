/**
 * The one credential-scoped step of .github/workflows/backlog-ops.yml (contract: src/session-ingest/backlog-ops.ts).
 *
 * Before reading any credential it re-parses the dispatch (dormant switch, typed bindings), checks the checkout is
 * the dispatched commit, checks the protected target variables (bucket, region, KMS key, the two role ARNs) and
 * checks every bound run's downloaded receipt (backfill complete; export failed = fence_late = orgs.incomplete = 0).
 * Only then does it exchange the job's GitHub OIDC token for a role session (the read-only role for a dry run, the
 * writer role for an execute; STS AssumeRoleWithWebIdentity, nothing on disk) and run the tool as a child with fixed
 * argv and an allowlisted environment: the AWS session in the child's environment (the tools read AWS_* only from
 * there), the database URL only on the child's stdin pipe as {"dbUrl": ...} (backfill and export; the drain gets no
 * database), never argv, a file or a log. The owner URL is the environment's SUPABASE_MIGRATION_OWNER_URL, checked
 * against the pinned release connection (releaseConnection) and handed to the tool in one normal form
 * (sslmode=verify-full; ownerClientConfig pins TLS to the Supabase CA itself and never forwards channel_binding).
 *
 * The child's single stdout summary line becomes a counts-only receipt (any non-code string refuses it), written to
 * $RUNNER_TEMP/backlog-ops-out and echoed to the log. The step's exit code is the tool's.
 */
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { releaseConnection } from "../src/hoopdb/schema-release-contract.ts";
import {
  BACKLOG_OPS_RECEIPT_FILE,
  BACKLOG_OPS_SESSION_SECONDS,
  backlogOpsBindings,
  backlogOpsDispatch,
  backlogOpsReceipt,
  backlogOpsRole,
  backlogOpsSessionName,
  backlogOpsTarget,
  backlogOpsToolArgs,
  checkBoundReceipt,
  checkStsExpiration,
  parseStsCredentials,
  refuseBacklogOps as refuse,
  type BacklogOpsDispatch,
} from "../src/session-ingest/backlog-ops.ts";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
/** The only parent variables a child inherits (plus the AWS session the parent adds). */
export const BACKLOG_OPS_CHILD_ENV = ["PATH", "HOME", "LANG", "TMPDIR"] as const;
const STDOUT_LIMIT = 1024 * 1024;

export function backlogOpsPaths(env: Record<string, string | undefined>) {
  const temp = env.RUNNER_TEMP ?? "";
  if (!temp.startsWith("/")) refuse("backlog_ops_runner_temp");
  return {
    bound: (kind: string) => join(temp, "backlog-ops-bound", kind, BACKLOG_OPS_RECEIPT_FILE),
    receipt: join(temp, "backlog-ops-out", BACKLOG_OPS_RECEIPT_FILE),
  };
}

/** The tool's database URL: the pinned release login, re-encoded with only sslmode=verify-full. */
export function toolDbUrl(ownerUrl: string | undefined): string {
  const c = releaseConnection(ownerUrl);
  return `postgresql://${encodeURIComponent(c.user)}:${encodeURIComponent(c.password)}@${c.host}:${c.port}/${encodeURIComponent(c.database)}?sslmode=verify-full`;
}

/** Spawn spec: fixed argv, allowlisted env plus the AWS session, the URL only in the stdin payload. */
export function backlogOpsChildSpawn(
  env: Record<string, string | undefined>,
  d: BacklogOpsDispatch,
  aws: { accessKeyId: string; secretAccessKey: string; sessionToken: string },
  dbUrl: string | null,
) {
  const t = backlogOpsTarget(env), spec = backlogOpsToolArgs(d, t);
  if (spec.needsDb !== (dbUrl !== null)) refuse("backlog_ops_db_binding");
  const childEnv: Record<string, string> = {};
  for (const key of BACKLOG_OPS_CHILD_ENV) if (typeof env[key] === "string") childEnv[key] = env[key]!;
  childEnv.AWS_ACCESS_KEY_ID = aws.accessKeyId;
  childEnv.AWS_SECRET_ACCESS_KEY = aws.secretAccessKey;
  childEnv.AWS_SESSION_TOKEN = aws.sessionToken;
  return {
    command: process.execPath,
    args: [join(ROOT, spec.script), ...spec.args],
    env: childEnv,
    input: dbUrl === null ? null : JSON.stringify({ dbUrl }) + "\n",
  };
}

/** Every bound run's receipt, from the artifacts the workflow downloaded, before any credential. */
export function checkBoundReceipts(env: Record<string, string | undefined>, d: BacklogOpsDispatch, read = (p: string) => readFileSync(p, "utf8")) {
  const paths = backlogOpsPaths(env);
  for (const b of backlogOpsBindings(d)) {
    const path = paths.bound(b.kind);
    let raw: string;
    try { raw = read(path); } catch { return refuse(`backlog_ops_bound_${b.kind}_missing`); }
    checkBoundReceipt(raw, { mode: b.mode, sha: d.sha, run: b.run });
  }
}

/** GitHub OIDC token (audience sts.amazonaws.com) for STS: in memory only. */
async function oidcToken(env: Record<string, string | undefined>, fetcher: typeof fetch): Promise<string> {
  const url = env.ACTIONS_ID_TOKEN_REQUEST_URL, bearer = env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
  if (!url || !bearer || !url.startsWith("https://")) refuse("backlog_ops_oidc_unavailable");
  const res = await fetcher(url + (url.includes("?") ? "&" : "?") + "audience=sts.amazonaws.com", {
    headers: { Authorization: `bearer ${bearer}`, Accept: "application/json" },
  });
  if (!res.ok) refuse("backlog_ops_oidc_failed");
  const value = ((await res.json()) as { value?: unknown }).value;
  if (typeof value !== "string" || value.split(".").length !== 3) refuse("backlog_ops_oidc_failed");
  return value;
}

export async function assumeRole(
  env: Record<string, string | undefined>,
  roleArn: string,
  region: string,
  sessionName: string,
  fetcher: typeof fetch = fetch,
) {
  const token = await oidcToken(env, fetcher);
  const body = new URLSearchParams({
    Action: "AssumeRoleWithWebIdentity", Version: "2011-06-15", RoleArn: roleArn,
    RoleSessionName: sessionName, WebIdentityToken: token, DurationSeconds: String(BACKLOG_OPS_SESSION_SECONDS),
  });
  const res = await fetcher(`https://sts.${region}.amazonaws.com/`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body,
  });
  if (!res.ok) refuse("backlog_ops_sts_refused");
  const creds = parseStsCredentials(await res.text());
  checkStsExpiration(creds.expiration);
  return creds;
}

function writePrivate(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temp = path + ".tmp";
  writeFileSync(temp, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  renameSync(temp, path);
}

export async function backlogOpsRun(env: Record<string, string | undefined>) {
  const d = backlogOpsDispatch(env);
  if (env.GITHUB_SHA !== d.sha || !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? "")) refuse("backlog_ops_checkout");
  const t = backlogOpsTarget(env), spec = backlogOpsToolArgs(d, t);
  checkBoundReceipts(env, d);
  const paths = backlogOpsPaths(env);
  if (existsSync(paths.receipt)) refuse("backlog_ops_receipt_exists");
  // Credentials from here on: the database URL (backfill, export) and the role session.
  const dbUrl = spec.needsDb ? toolDbUrl(env.SUPABASE_MIGRATION_OWNER_URL) : null;
  const aws = await assumeRole(env, backlogOpsRole(d, t), t.region, backlogOpsSessionName(d.mode, env.GITHUB_RUN_ID!));
  const child = backlogOpsChildSpawn(env, d, aws, dbUrl);
  const { code, stdout } = await new Promise<{ code: number; stdout: string }>((resolve) => {
    const p = spawn(child.command, child.args, { cwd: ROOT, env: child.env, stdio: [child.input === null ? "ignore" : "pipe", "pipe", "inherit"] });
    let out = "";
    p.stdout!.on("data", (chunk: Buffer) => { if (out.length < STDOUT_LIMIT) out += chunk.toString("utf8"); });
    if (child.input !== null) { p.stdin!.on("error", () => undefined); p.stdin!.end(child.input); }
    p.on("error", () => resolve({ code: 1, stdout: "" }));
    p.on("close", (c) => resolve({ code: c ?? 1, stdout: out }));
  });
  let receipt;
  try { receipt = backlogOpsReceipt(d, env.GITHUB_RUN_ID!, code, stdout); }
  catch (e) { receipt = backlogOpsReceipt(d, env.GITHUB_RUN_ID!, code || 1, JSON.stringify({ refused: (e as { code?: string }).code ?? "receipt" })); }
  writePrivate(paths.receipt, receipt);
  console.log("backlog_ops_receipt " + JSON.stringify(receipt));
  return receipt.exitCode === 0 && code === 0 ? 0 : 1;
}

if (import.meta.main)
  backlogOpsRun(process.env)
    .then((c) => { process.exitCode = c; })
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("backlog_ops_refused " + (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
