/**
 * Contract for the guarded backlog-ops workflow (.github/workflows/backlog-ops.yml): the one way the 0005 backlog
 * tools run against production. They are the fence backfill (scripts/upload-fence-backfill.ts), the accepted-stage
 * export (scripts/accepted-stage-export.ts) and the S3 drain (scripts/buffered-drain.ts); see
 * docs/decisions/0005-s3-native-upload-ledger.md "The buffered backlog" and docs/backlog-ops-workflow.md.
 *
 * Six modes, a dry and an execute of each tool. Every execute is dormant (BACKLOG_OPS_EXECUTE_ENABLED, flipped only
 * by a reviewed commit) and bound by a typed confirmation to earlier successful runs of this workflow at the same
 * commit:
 *   backfill-execute  DRY <backfill-dry run>
 *   export-execute    DRY <export-dry run> BACKFILL <backfill-execute run>
 *   drain-execute     DRY <drain-dry run>  BACKFILL <backfill-execute run> EXPORT <export-execute run>
 * The drain's BACKFILL must be fresh (BACKLOG_OPS_FRESH_BACKFILL_MS) and its EXPORT must come after it, because a
 * control made after a backfill is not in the fence (the drain's own --execute precondition). The receipts of the
 * bound runs are downloaded and checked before any credential is read: every backfill section complete; the export
 * with failed = fence_late = orgs.incomplete = 0. The tools enforce the rest themselves: the export refuses while
 * the v2 claim-sessions reader gate is off (export_claim_sessions_gate_off), and the drain refuses beside a live
 * lease (96bulls/.drain/lock.json). The workflow never passes --checkpoint-file or --start-after: the drain's
 * create-only `drained` markers make a rerun skip what is written, so a restart only re-lists.
 *
 * Org scope (BACKLOG_OPS_ORGS): an optional environment secret on both environments (a secret, so this public
 * repository's logs mask it): comma-separated lowercase org UUIDs; unset or empty runs every org, as before. The org
 * ids never reach the repository, a log or a receipt: the receipt records only the scope's id (`all`, or `orgs_` +
 * 32 hex of the sorted list's sha256). Every bound run must carry the same scope as the run it feeds, except that an
 * unscoped (`all`) backfill or export may feed a scoped execute. A receipt from before scoping has no scope and
 * counts as `all`. Scoping changes no tool rule: the drain still writes nothing for an org without its
 * fence-authority marker.
 *
 * Receipts are counts only: every leaf a number, a boolean or a short lowercase code; any other string (an id, a
 * key, a URL) refuses the receipt. Errors carry fixed codes only.
 *
 * presence-dry (scripts/session-presence.ts, src/session-ingest/session-presence.ts) is a seventh, DRY-ONLY mode: a
 * counts-only look at ONE session for support diagnosis ("No conversation recorded yet"). There is no
 * presence-execute: the dispatch refuses it with backlog_ops_presence_dry_only before anything else, and every
 * execute-only path refuses the presence tool again. It binds nothing, runs in the dry environment with the read-only
 * role, and takes its session id only from the dry environment's secret SESSION_PRESENCE_SESSION
 * (backlogOpsPresenceSession), handed to the tool on its stdin pipe beside the database URL, never argv, a log or the
 * receipt.
 */
import { createHash } from "node:crypto";

/** Execute modes stay refused until a reviewed commit flips this. Dry modes are always allowed. */
export const BACKLOG_OPS_EXECUTE_ENABLED: boolean = true;

export const BACKLOG_OPS_WORKFLOW = ".github/workflows/backlog-ops.yml";
export const BACKLOG_OPS_MODES = [
  "backfill-dry", "backfill-execute", "export-dry", "export-execute", "drain-dry", "drain-execute", "presence-dry",
] as const;
export type BacklogOpsMode = (typeof BACKLOG_OPS_MODES)[number];
export type BacklogOpsTool = "backfill" | "export" | "drain" | "presence";
/** Tools with a dry mode only: no `<tool>-execute` exists, is accepted or can be enabled. */
export const BACKLOG_OPS_DRY_ONLY_TOOLS: readonly BacklogOpsTool[] = ["presence"];
/** The receipt artifact of every run (retention 30 days) and its single file. */
export const BACKLOG_OPS_RECEIPT_ARTIFACT = "backlog-ops-receipt";
export const BACKLOG_OPS_RECEIPT_FILE = "backlog-ops-receipt.json";
export const BACKLOG_OPS_RECEIPT_CONTRACT = "backlog-ops-receipt/v1";
/** A bound dry run, backfill or export older than this cannot feed an execute. */
export const BACKLOG_OPS_BINDING_MAX_AGE_MS = 24 * 60 * 60 * 1000;
/** The drain's backfill must be this fresh: "a rerun of the fence backfill immediately before". */
export const BACKLOG_OPS_FRESH_BACKFILL_MS = 3 * 60 * 60 * 1000;
/** The role session (360 min). The operate job's timeout (355 min) is below it, so a tool never outlives its credentials. */
export const BACKLOG_OPS_SESSION_SECONDS = 6 * 60 * 60;
/**
 * The protected GitHub environments. Dry modes run in the schema release environment; every execute runs in its own
 * environment, so the write role can trust only that environment's OIDC subject and no other job in the schema
 * environment (this workflow's dry runs, or the other guarded operators there) can assume it. The environment is
 * derived from the mode here, never taken from an input.
 */
export const BACKLOG_OPS_DRY_ENVIRONMENT = "session-schema-production";
export const BACKLOG_OPS_EXECUTE_ENVIRONMENT = "session-backlog-execute";
export const backlogOpsEnvironment = (mode: BacklogOpsMode) =>
  isExecute(mode) ? BACKLOG_OPS_EXECUTE_ENVIRONMENT : BACKLOG_OPS_DRY_ENVIRONMENT;
/** A role session must outlive the job (355 min timeout) with this margin, or the run refuses before the tool starts. */
export const BACKLOG_OPS_MIN_SESSION_REMAINING_MS = 5 * 60 * 60 * 1000;
export const BACKLOG_OPS_MAX_OBJECTS_CEILING = 1_000_000;

const SHA40 = /^[0-9a-f]{40}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u;
const REGION = /^[a-z]{2}(-gov)?-[a-z]+-[0-9]$/u;
const KMS_KEY_ARN = /^arn:aws:kms:([a-z]{2}(?:-gov)?-[a-z]+-[0-9]):([0-9]{12}):key\/(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$/u;
const ROLE_ARN = /^arn:aws:iam::([0-9]{12}):role\/[A-Za-z0-9+=,.@_-]{1,64}$/u;
const CODE = /^[a-z][a-z0-9_]{0,39}$/u;
const ORG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const SCOPE_ID = /^(all|orgs_[0-9a-f]{32})$/u;
/** The most orgs one scoped run may name. */
export const BACKLOG_OPS_MAX_ORGS = 64;
const KEY = /^[a-z][a-z0-9_]{0,63}$/u;

export class BacklogOpsRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "BacklogOpsRefusal";
  }
}
export function refuseBacklogOps(code: string): never {
  throw new BacklogOpsRefusal(code);
}
function refuse(code: string): never {
  throw new BacklogOpsRefusal(code);
}

export const toolOf = (mode: BacklogOpsMode): BacklogOpsTool => mode.split("-")[0] as BacklogOpsTool;
export const isExecute = (mode: BacklogOpsMode) => mode.endsWith("-execute");

export interface BacklogOpsDispatch {
  mode: BacklogOpsMode;
  sha: string;
  /** Execute only: the same tool's successful dry run at this commit. */
  dryRun: string | null;
  /** export-execute and drain-execute: a successful backfill-execute at this commit. */
  backfillRun: string | null;
  /** drain-execute: a successful export-execute at this commit, after the backfill. */
  exportRun: string | null;
  /** Drain modes only: the per-run object budget (empty = unbounded). */
  maxObjects: number | null;
  confirmation: string;
}

/** The typed binding an execute must carry, exactly. */
export function backlogOpsConfirmation(d: Pick<BacklogOpsDispatch, "mode" | "sha" | "dryRun" | "backfillRun" | "exportRun">): string {
  return [d.mode.toUpperCase(), d.sha, "DRY", d.dryRun,
    ...(d.backfillRun ? ["BACKFILL", d.backfillRun] : []), ...(d.exportRun ? ["EXPORT", d.exportRun] : [])].join(" ");
}

/** The dispatch inputs, checked before any GitHub read or credential. */
export function backlogOpsDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = BACKLOG_OPS_EXECUTE_ENABLED,
): BacklogOpsDispatch {
  const mode = env.BACKLOG_OPS_MODE ?? "", sha = env.BACKLOG_OPS_SHA ?? "";
  // A dry-only tool's execute is not a mode: refused with its own code, before the dormant switch is even read.
  if (BACKLOG_OPS_DRY_ONLY_TOOLS.some((t) => mode === `${t}-execute`)) refuse("backlog_ops_presence_dry_only");
  const dryRun = env.BACKLOG_OPS_DRY_RUN ?? "", backfillRun = env.BACKLOG_OPS_BACKFILL_RUN ?? "",
    exportRun = env.BACKLOG_OPS_EXPORT_RUN ?? "", maxObjects = env.BACKLOG_OPS_MAX_OBJECTS ?? "",
    confirmation = env.BACKLOG_OPS_CONFIRMATION ?? "";
  if (!(BACKLOG_OPS_MODES as readonly string[]).includes(mode) || !SHA40.test(sha)) refuse("backlog_ops_inputs");
  const m = mode as BacklogOpsMode, tool = toolOf(m), execute = isExecute(m);
  if (execute && enabled !== true) refuse("backlog_ops_dormant");
  const wantsBackfill = execute && tool !== "backfill", wantsExport = execute && tool === "drain";
  if (execute ? !RUN_ID.test(dryRun) : dryRun !== "") refuse("backlog_ops_dry_run");
  if (wantsBackfill ? !RUN_ID.test(backfillRun) : backfillRun !== "") refuse("backlog_ops_backfill_run");
  if (wantsExport ? !RUN_ID.test(exportRun) : exportRun !== "") refuse("backlog_ops_export_run");
  let budget: number | null = null;
  if (maxObjects !== "") {
    if (tool !== "drain" || !/^[1-9][0-9]{0,6}$/u.test(maxObjects) || Number(maxObjects) > BACKLOG_OPS_MAX_OBJECTS_CEILING)
      refuse("backlog_ops_max_objects");
    budget = Number(maxObjects);
  }
  const runs = [dryRun, backfillRun, exportRun].filter((r) => r !== "");
  if (new Set(runs).size !== runs.length) refuse("backlog_ops_bindings");
  const d = { mode: m, sha, dryRun: execute ? dryRun : null, backfillRun: wantsBackfill ? backfillRun : null,
    exportRun: wantsExport ? exportRun : null, maxObjects: budget, confirmation };
  if (execute ? confirmation !== backlogOpsConfirmation(d) : confirmation !== "") refuse("backlog_ops_confirmation");
  return d;
}

/** Bucket, region and key from the environment's protected variables, shape-checked. */
export interface BacklogOpsTarget { bucket: string; region: string; kmsKeyArn: string; dryRoleArn: string; executeRoleArn: string }
export function backlogOpsTarget(env: Record<string, string | undefined>): BacklogOpsTarget {
  const bucket = env.BACKLOG_OPS_BUCKET ?? "", region = env.BACKLOG_OPS_REGION ?? "", kmsKeyArn = env.BACKLOG_OPS_KMS_KEY_ARN ?? "",
    dryRoleArn = env.BACKLOG_OPS_DRY_ROLE_ARN ?? "", executeRoleArn = env.BACKLOG_OPS_EXECUTE_ROLE_ARN ?? "";
  if (!BUCKET.test(bucket) || bucket.includes("..")) refuse("backlog_ops_target_bucket");
  if (!REGION.test(region)) refuse("backlog_ops_target_region");
  const key = KMS_KEY_ARN.exec(kmsKeyArn);
  if (!key || key[1] !== region) refuse("backlog_ops_target_kms");
  const dry = ROLE_ARN.exec(dryRoleArn), exe = ROLE_ARN.exec(executeRoleArn);
  if (!dry || !exe || dry[1] !== key[2] || exe[1] !== key[2] || dryRoleArn === executeRoleArn) refuse("backlog_ops_target_roles");
  return { bucket, region, kmsKeyArn, dryRoleArn, executeRoleArn };
}

/** A run's org scope: the orgs (sorted, distinct) or null for every org, and its id for receipts. */
export interface BacklogOpsScope { orgs: string[] | null; id: string }
export const BACKLOG_OPS_SCOPE_ALL = "all";
/** BACKLOG_OPS_ORGS, from the environment secret: unset or empty = every org. */
export function backlogOpsScope(env: Record<string, string | undefined>): BacklogOpsScope {
  const raw = (env.BACKLOG_OPS_ORGS ?? "").trim();
  if (raw === "") return { orgs: null, id: BACKLOG_OPS_SCOPE_ALL };
  const parts = raw.split(",");
  if (parts.length > BACKLOG_OPS_MAX_ORGS || !parts.every((o) => ORG_UUID.test(o))) refuse("backlog_ops_scope");
  const orgs = [...new Set(parts)].sort();
  if (orgs.length !== parts.length) refuse("backlog_ops_scope");
  return { orgs, id: "orgs_" + createHash("sha256").update(orgs.join(",")).digest("hex").slice(0, 32) };
}

/**
 * The backfill's paging for production. Its users page is a DISTINCT over six tables, one of which (ingest_batches)
 * has no index on the owner key, so every page scans in full: the tool's 30 s default timed out on the first page
 * (sqlstate_57014). Fewer, larger pages and the tool's maximum per-statement timeout. Each page is still its own
 * READ ONLY transaction.
 */
export const BACKLOG_OPS_BACKFILL_PAGING = ["--page-size", "5000", "--row-page-size", "20000", "--statement-timeout-ms", "600000"] as const;
/** The export's per-statement timeout: its org, candidate and per-user reads over the buffer are as heavy (same 30 s default). */
export const BACKLOG_OPS_EXPORT_PAGING = ["--statement-timeout-ms", "600000"] as const;

/**
 * The presence tool's per-statement timeout and total transaction budget. Its statements (the counts, the distinct-org
 * check and the two generation marker reads) run one after another in ONE REPEATABLE READ READ ONLY transaction on
 * the owner primary, each in its own savepoint. A statement that hits the 60 s timeout is null (count_timeout); the
 * 180 s budget caps the whole transaction: no statement starts with less than 1 s of it left, and each statement's
 * timeout is lowered to what is left, so the snapshot is held for about 3 min at most (budget_exhausted, counts
 * null, otherwise). Both flags are passed explicitly.
 */
export const BACKLOG_OPS_PRESENCE_TIMEOUT = ["--statement-timeout-ms", "60000", "--total-budget-ms", "180000"] as const;

/** Fixed argv for the tool: the scope's validated orgs only, never a checkpoint, a start key or an unvalidated value. */
export function backlogOpsToolArgs(d: BacklogOpsDispatch, t: BacklogOpsTarget, scope: BacklogOpsScope = { orgs: null, id: BACKLOG_OPS_SCOPE_ALL }): { script: string; args: string[]; needsDb: boolean } {
  const tool = toolOf(d.mode), execute = isExecute(d.mode);
  if (tool === "presence") {
    // Dry only, no bucket, region or key (it reads no S3): the per-statement timeout, the budget and the scope's orgs.
    if (execute) refuse("backlog_ops_presence_dry_only");
    return { script: "scripts/session-presence.ts", args: ["--dry-run", ...BACKLOG_OPS_PRESENCE_TIMEOUT, ...(scope.orgs ?? []).flatMap((o) => ["--org", o])], needsDb: true };
  }
  const script = { backfill: "scripts/upload-fence-backfill.ts", export: "scripts/accepted-stage-export.ts", drain: "scripts/buffered-drain.ts" }[tool];
  const args = [execute ? "--execute" : "--dry-run", "--bucket", t.bucket, "--region", t.region,
    ...(execute ? ["--kms-key-arn", t.kmsKeyArn] : []),
    ...(tool === "backfill" ? BACKLOG_OPS_BACKFILL_PAGING : tool === "export" ? BACKLOG_OPS_EXPORT_PAGING : []),
    ...(tool === "drain" && d.maxObjects !== null ? ["--max-objects", String(d.maxObjects)] : []),
    ...(scope.orgs ?? []).flatMap((o) => ["--org", o])];
  return { script, args, needsDb: tool !== "drain" };
}

/** The role for a mode: dry modes get the read-only role, executes the writer role. */
export const backlogOpsRole = (d: BacklogOpsDispatch, t: BacklogOpsTarget) => {
  if (isExecute(d.mode) && BACKLOG_OPS_DRY_ONLY_TOOLS.includes(toolOf(d.mode))) refuse("backlog_ops_presence_dry_only");
  return isExecute(d.mode) ? t.executeRoleArn : t.dryRoleArn;
};

/**
 * presence-dry's one session id, from the dry environment's secret SESSION_PRESENCE_SESSION (trimmed as
 * BACKLOG_OPS_ORGS is): a lowercase UUID or a bounded [A-Za-z0-9_-]{8,128} token. Missing or empty refuses
 * backlog_ops_presence_session_missing, any other shape backlog_ops_presence_session_invalid; the value is never in
 * an error. Every other mode must see it empty (the workflow passes it only to presence-dry):
 * backlog_ops_presence_session_unexpected. Returns null for those modes.
 */
export const SESSION_PRESENCE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
export const SESSION_PRESENCE_TOKEN = /^[A-Za-z0-9_-]{8,128}$/u;
export const isPresenceSession = (v: string) => SESSION_PRESENCE_UUID.test(v) || SESSION_PRESENCE_TOKEN.test(v);
export function backlogOpsPresenceSession(env: Record<string, string | undefined>, d: Pick<BacklogOpsDispatch, "mode">): string | null {
  const raw = (env.SESSION_PRESENCE_SESSION ?? "").trim();
  if (toolOf(d.mode) !== "presence") {
    if (raw !== "") refuse("backlog_ops_presence_session_unexpected");
    return null;
  }
  if (raw === "") refuse("backlog_ops_presence_session_missing");
  if (!isPresenceSession(raw)) refuse("backlog_ops_presence_session_invalid");
  return raw;
}

/**
 * Counts only: every key a lowercase identifier, every leaf a finite number, a boolean, null or a short lowercase
 * code. A string that is not a code (an org id, a key, a URL, a message) refuses the whole receipt.
 */
export function countsOnly(value: unknown, depth = 0): unknown {
  if (depth > 6) refuse("backlog_ops_receipt_shape");
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number") { if (!Number.isFinite(value)) refuse("backlog_ops_receipt_shape"); return value; }
  if (typeof value === "string") { if (!CODE.test(value)) refuse("backlog_ops_receipt_string"); return value; }
  if (typeof value !== "object" || Array.isArray(value)) refuse("backlog_ops_receipt_shape");
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (!KEY.test(k)) refuse("backlog_ops_receipt_key");
    out[k] = countsOnly(v, depth + 1);
  }
  return out;
}

export interface BacklogOpsReceipt {
  contract: typeof BACKLOG_OPS_RECEIPT_CONTRACT;
  mode: BacklogOpsMode;
  sha: string;
  run: string;
  /** The org scope's id (backlogOpsScope): `all` or `orgs_<32 hex>`; never an org id. */
  scope: string;
  exitCode: number;
  summary: unknown;
}

/** Parse the tool's last stdout line into a counts-only receipt (or a refused one). */
export function backlogOpsReceipt(d: BacklogOpsDispatch, run: string, exitCode: number, stdout: string, scope: string = BACKLOG_OPS_SCOPE_ALL): BacklogOpsReceipt {
  if (!SCOPE_ID.test(scope)) refuse("backlog_ops_scope");
  const lines = stdout.split("\n").map((l) => l.trim()).filter(Boolean);
  let summary: unknown = { refused: "no_summary" };
  const last = lines.at(-1);
  if (last !== undefined) {
    let parsed: unknown;
    try { parsed = JSON.parse(last); } catch { parsed = undefined; }
    summary = parsed === undefined ? { refused: "summary_unparsed" } : countsOnly(parsed);
  }
  return { contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode: d.mode, sha: d.sha, run, scope, exitCode, summary };
}

const obj = (v: unknown): Record<string, any> | null => (v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, any> : null);

/**
 * A bound run's receipt, read from its downloaded artifact. It must name that run, this commit and the expected mode,
 * have exited 0, carry the execute's org scope (a bound backfill or export may also be unscoped; a receipt without a
 * scope is unscoped), and (per tool) pass its readiness check.
 */
export function checkBoundReceipt(raw: string, expect: { mode: BacklogOpsMode; sha: string; run: string; scope?: string; kind?: "dry" | "backfill" | "export" }): void {
  // A presence receipt never feeds an execute (presence binds nothing and nothing binds it).
  if (toolOf(expect.mode) === "presence") refuse("backlog_ops_bound_presence_receipt");
  let r: Record<string, any> | null;
  try { r = obj(JSON.parse(raw)); } catch { r = null; }
  if (!r || r.contract !== BACKLOG_OPS_RECEIPT_CONTRACT || r.mode !== expect.mode || r.sha !== expect.sha ||
      r.run !== expect.run || r.exitCode !== 0)
    refuse(`backlog_ops_bound_${toolOf(expect.mode)}_receipt`);
  const want = expect.scope ?? BACKLOG_OPS_SCOPE_ALL;
  const got = r.scope === undefined ? BACKLOG_OPS_SCOPE_ALL : r.scope;
  if (typeof got !== "string" || !SCOPE_ID.test(got) || !SCOPE_ID.test(want) ||
      (got !== want && !(expect.kind !== "dry" && expect.kind !== undefined && got === BACKLOG_OPS_SCOPE_ALL)))
    refuse(`backlog_ops_bound_${toolOf(expect.mode)}_scope`);
  const s = obj(r.summary);
  if (!s || s.refused !== undefined) refuse(`backlog_ops_bound_${toolOf(expect.mode)}_refused`);
  const wantMode = isExecute(expect.mode) ? "execute" : "dry_run";
  if (s.mode !== wantMode) refuse(`backlog_ops_bound_${toolOf(expect.mode)}_receipt`);
  const tool = toolOf(expect.mode);
  if (tool === "backfill") {
    const sec = obj(s.sections);
    if (!sec || ["fences", "ownership", "revisions"].some((k) => sec[k] !== "complete")) refuse("backlog_ops_backfill_incomplete");
  }
  if (tool === "export") {
    const orgs = obj(s.orgs);
    if (s.failed !== 0 || s.fence_late !== 0 || !orgs || orgs.incomplete !== 0) refuse("backlog_ops_export_not_ready");
  }
  if (tool === "drain") {
    if (typeof s.listed !== "number") refuse("backlog_ops_bound_drain_receipt");
  }
}

/** Which earlier runs an execute binds, with the mode each must have been. */
export function backlogOpsBindings(d: BacklogOpsDispatch): Array<{ run: string; mode: BacklogOpsMode; kind: "dry" | "backfill" | "export" }> {
  const tool = toolOf(d.mode), out: Array<{ run: string; mode: BacklogOpsMode; kind: "dry" | "backfill" | "export" }> = [];
  if (tool === "presence") {
    if (d.dryRun || d.backfillRun || d.exportRun) refuse("backlog_ops_bindings");
    return out;
  }
  if (d.dryRun) out.push({ run: d.dryRun, mode: `${tool}-dry` as BacklogOpsMode, kind: "dry" });
  if (d.backfillRun) out.push({ run: d.backfillRun, mode: "backfill-execute", kind: "backfill" });
  if (d.exportRun) out.push({ run: d.exportRun, mode: "export-execute", kind: "export" });
  return out;
}

/** The run-name the workflow gives every dispatch: the gate reads a bound run's mode back from it. */
export const backlogOpsRunName = (mode: string, sha: string) => `backlog ops ${mode} ${sha}`;

/** AssumeRoleWithWebIdentity (unsigned STS call) response: the three credential fields, or a refusal. */
export function parseStsCredentials(xml: string): { accessKeyId: string; secretAccessKey: string; sessionToken: string; expiration: string } {
  const pick = (tag: string) => {
    const m = new RegExp(`<${tag}>([^<]{1,4096})</${tag}>`, "u").exec(xml);
    return m ? m[1]! : "";
  };
  const accessKeyId = pick("AccessKeyId"), secretAccessKey = pick("SecretAccessKey"), sessionToken = pick("SessionToken"), expiration = pick("Expiration");
  if (!/^ASIA[A-Z0-9]{12,124}$/u.test(accessKeyId) || !secretAccessKey || !sessionToken || !expiration) refuse("backlog_ops_sts_response");
  return { accessKeyId, secretAccessKey, sessionToken, expiration };
}

/** The session must be valid now and for at least BACKLOG_OPS_MIN_SESSION_REMAINING_MS more. */
export function checkStsExpiration(expiration: string, now = Date.now()): void {
  const at = Date.parse(expiration);
  if (!/^\d{4}-\d{2}-\d{2}T/u.test(expiration) || !Number.isFinite(at) || at - now < BACKLOG_OPS_MIN_SESSION_REMAINING_MS)
    refuse("backlog_ops_sts_expiration");
}

/** A role session name that names the run, never a person or tenant. */
export const backlogOpsSessionName = (mode: BacklogOpsMode, run: string) =>
  `backlog-ops-${mode}-${run}`.slice(0, 64);
